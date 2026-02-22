"""
cassachange - A Cassandra schema migration tool inspired by schemachange.

Script naming conventions:
  V{version}__{description}.cql   Versioned:  runs once, in order
  U{version}__{description}.cql   Undo:       paired rollback script for a versioned migration
  R__{description}.cql            Repeatable: reruns when file checksum changes
  A__{description}.cql            Always:     runs every deploy regardless

Commands:
  cassachange deploy    [options]          Apply pending migrations
  cassachange rollback  [options]          Roll back to a previous version using undo scripts
  cassachange status    [options]          Show migration history
  cassachange validate  [options]          Lint all scripts without connecting to Cassandra
  cassachange repair    [options]          Fix stuck locks or FAILED scripts
  cassachange audit     [options]          View the immutable audit log
  cassachange baseline  [options]          Generate a V0.0.0__baseline.cql from a live keyspace
"""

import os
import re
import sys
import time
import logging
import argparse
import datetime
import socket

import yaml

from cassachange.connection    import build_session
from cassachange.history       import HistoryTable, LockError
from cassachange.scripts       import (
    discover_scripts,
    discover_all_scripts,
    discover_undo_scripts,
    ScriptType,
)
from cassachange.secrets       import resolve_secrets, cleanup_temp_files, SecretsError
from cassachange.notifications import notify
from cassachange.baseline      import generate_baseline

LOG_FORMAT = "%(asctime)s [%(levelname)s] %(message)s"
logging.basicConfig(format=LOG_FORMAT, datefmt="%Y-%m-%d %H:%M:%S")
log = logging.getLogger("cassachange")


# ---------------------------------------------------------------------------
# Config loading
# ---------------------------------------------------------------------------

def load_config(config_path: str, profile: str = None) -> dict:
    """
    Load cassachange.yml.  If *profile* is given (via --profile or
    CASSACHANGE_PROFILE env var), merge the matching profiles.<name>
    block on top of the base config.

    Example cassachange.yml profiles block:

        profiles:
          dev:
            hosts: [127.0.0.1]
            keyspace: myapp_dev
          prod:
            hosts: [cass1.prod, cass2.prod]
            keyspace: myapp_prod
            timeout: 120
            notifications:
              on_events: [deploy_success, deploy_failed]
              channels:
                - type: slack
                  webhook_url_env: SLACK_WEBHOOK_URL
    """
    if not os.path.exists(config_path):
        return {}
    with open(config_path) as f:
        cfg = yaml.safe_load(f) or {}
    log.debug("Loaded config from %s", config_path)

    active = profile or os.getenv("CASSACHANGE_PROFILE") or cfg.get("default_profile")
    if active:
        profiles = cfg.get("profiles") or {}
        if active not in profiles:
            available = list(profiles.keys())
            raise SystemExit(
                f"ERROR  Profile '{active}' not found in {config_path}. "
                f"Available: {available or 'none defined'}"
            )
        base = {k: v for k, v in cfg.items() if k != "profiles"}
        for k, v in (profiles[active] or {}).items():
            if isinstance(v, dict) and isinstance(base.get(k), dict):
                base[k] = {**base[k], **v}
            else:
                base[k] = v
        base["profile"] = active
        log.debug("Applied profile '%s'", active)
        return base

    return cfg


def merge_config(args, cfg: dict) -> dict:
    """
    Priority (highest to lowest):
      1. CLI args
      2. Environment variables (CASSANDRA_* / CASSACHANGE_*)
      3. YAML config file
    """
    merged = dict(cfg)

    env_map = {
        "hosts":                 os.getenv("CASSANDRA_HOSTS"),
        "port":                  os.getenv("CASSANDRA_PORT"),
        "keyspace":              os.getenv("CASSANDRA_KEYSPACE"),
        "username":              os.getenv("CASSANDRA_USERNAME"),
        "password":              os.getenv("CASSANDRA_PASSWORD"),
        "astra_token":           os.getenv("ASTRA_TOKEN"),
        "secure_connect_bundle": os.getenv("ASTRA_SECURE_CONNECT_BUNDLE"),
        "root_folder":           os.getenv("CASSACHANGE_ROOT_FOLDER"),
        "history_keyspace":      os.getenv("CASSACHANGE_HISTORY_KEYSPACE"),
        "history_table":         os.getenv("CASSACHANGE_HISTORY_TABLE"),
        "timeout":               os.getenv("CASSACHANGE_TIMEOUT"),
        "environment":           os.getenv("CASSACHANGE_ENV"),
    }
    for key, val in env_map.items():
        if val is not None:
            merged[key] = val

    cli = vars(args)
    for key in (
        "hosts", "port", "keyspace", "keyspaces", "username", "password",
        "astra_token", "secure_connect_bundle",
        "root_folder", "history_keyspace", "history_table",
        "dry_run", "verbose", "target_version",
        "release_lock", "list_only", "script_name", "timeout",
        "run_id", "limit",
        "profile", "tag", "rollback_tag", "tag_filter",
        "dry_run_output", "baseline_version", "output_dir",
    ):
        if cli.get(key) is not None:
            merged[key] = cli[key]

    if isinstance(merged.get("hosts"), str):
        merged["hosts"] = [h.strip() for h in merged["hosts"].split(",")]

    if isinstance(merged.get("keyspaces"), str):
        merged["keyspaces"] = [k.strip() for k in merged["keyspaces"].split(",")]

    if merged.get("timeout") is not None:
        merged["timeout"] = int(merged["timeout"])

    merged.setdefault("hosts",       ["127.0.0.1"])
    merged.setdefault("port",        9042)
    merged.setdefault("history_table", "change_history")  # table name only; keyspace must be set explicitly
    merged.setdefault("root_folder", "./migrations")
    merged.setdefault("dry_run",     False)
    merged.setdefault("verbose",     False)
    merged.setdefault("timeout",     None)  # None = driver default (~10s)
    merged.setdefault("tag",         "")
    merged.setdefault("dry_run_output", None)

    return merged


# ---------------------------------------------------------------------------
# Shared helpers
# ---------------------------------------------------------------------------

def _set_verbosity(cfg: dict):
    log.setLevel(logging.DEBUG if cfg["verbose"] else logging.INFO)


def _execute_script(session, keyspace: str, content: str, timeout: int = None):
    """
    Split CQL on semicolons and execute each statement in the target keyspace.

    Args:
        timeout: per-statement timeout in seconds. None uses the driver default (10s).
                 Raises cassandra.OperationTimedOut if any statement exceeds the limit.
    """
    session.set_keyspace(keyspace)
    statements = [s.strip() for s in content.split(";") if s.strip()]
    for stmt in statements:
        session.execute(stmt, timeout=timeout)


def _write_dry_run_output(cfg: dict, plan: list):
    """Persist the dry-run migration plan as JSON if --dry-run-output is set."""
    import json as _json
    path = cfg.get("dry_run_output")
    if not path:
        return
    out = {
        "generated_at":  datetime.datetime.utcnow().isoformat() + "Z",
        "profile":       cfg.get("profile", ""),
        "tag":           cfg.get("tag", ""),
        "keyspaces":     _resolve_keyspaces(cfg),
        "root_folder":   cfg.get("root_folder", ""),
        "total_actions": len(plan),
        "actions":       plan,
    }
    with open(path, "w", encoding="utf-8") as fh:
        _json.dump(out, fh, indent=2, default=str)
    log.info("Dry-run plan written → %s  (%d actions)", path, len(plan))


def _resolve_keyspaces(cfg: dict) -> list:
    if cfg.get("keyspaces"):
        return list(cfg["keyspaces"])
    if cfg.get("keyspace"):
        return [cfg["keyspace"]]
    return []


def _vtuple(v: str) -> tuple:
    return tuple(int(x) for x in re.split(r"[._]", v))


def _ensure_keyspaces(session, keyspaces: list):
    """
    Verify all keyspaces exist. Exit with a clear, actionable error for
    any that are missing.

    cassachange does NOT create keyspaces automatically -- keyspace
    creation requires elevated Cassandra permissions that the migration
    user typically does not have. Create keyspaces via your admin UI,
    cqlsh as a superuser, Terraform, or any other admin tool, then
    run cassachange.
    """
    rows     = session.execute("SELECT keyspace_name FROM system_schema.keyspaces")
    existing = {row.keyspace_name for row in rows}
    missing  = [ks for ks in keyspaces if ks not in existing]
    if missing:
        for ks in missing:
            log.error(
                "Keyspace '%s' does not exist. "
                "Create it via your admin UI or cqlsh before running cassachange:\n"
                "  CREATE KEYSPACE IF NOT EXISTS %s\n"
                "    WITH replication = {'class': 'NetworkTopologyStrategy', 'datacenter1': 3};",
                ks, ks,
            )
        sys.exit(1)
    for ks in keyspaces:
        log.debug("Keyspace '%s' exists.", ks)


def _ensure_history_keyspace(session, history_keyspace: str):
    """Verify history keyspace exists, exit with clear error if not."""
    _ensure_keyspaces(session, [history_keyspace])


# ---------------------------------------------------------------------------
# deploy
# ---------------------------------------------------------------------------

def cmd_deploy(cfg: dict):
    _set_verbosity(cfg)
    dry_run   = cfg["dry_run"]
    root      = cfg["root_folder"]
    keyspaces = _resolve_keyspaces(cfg)

    if not os.path.isdir(root):
        log.error("Root folder does not exist: %s", root)
        sys.exit(1)

    scripts, _ = discover_all_scripts(root)
    if not scripts:
        log.warning("No migration scripts found in %s", root)
        return

    session = build_session(cfg)
    # Ensure history keyspace + all target keyspaces exist (create if missing)
    all_ks = [cfg["history_keyspace"]] + keyspaces
    _ensure_keyspaces(session, all_ks)
    history = HistoryTable(session, cfg["history_keyspace"], cfg["history_table"])
    if not dry_run:
        history.ensure_table()
        try:
            history.acquire_lock()
        except LockError as e:
            log.error("%s", e)
            sys.exit(1)
        operator = cfg.get("username") or socket.gethostname()
        cfg["_run_id"] = history._run_id
        history.audit("DEPLOY_START", operator,
                      detail=f"keyspaces={keyspaces} dry_run={dry_run} tag={cfg.get('tag','')}")
        notify(cfg, "deploy_start",
               keyspace=",".join(keyspaces), detail="deploy started")

    total_errors = 0
    _dry_run_plan: list = []

    for ks in keyspaces:
        log.info("=" * 60)
        log.info("KEYSPACE  %s  (dry_run=%s)", ks, dry_run)
        log.info("=" * 60)

        applied           = history.applied_versions(ks) if not dry_run else set()
        applied_checksums = history.applied_checksums(ks) if not dry_run else {}
        executed = skipped = errors = 0

        for script in scripts:
            label = f"[{script.script_type.value}] {script.filename}"

            if script.script_type == ScriptType.VERSIONED:
                if script.version in applied:
                    log.debug("SKIP  %s", label)
                    if dry_run:
                        _dry_run_plan.append({
                            "action": "skip", "type": "versioned",
                            "script": script.filename, "keyspace": ks,
                            "reason": "already_applied",
                        })
                    skipped += 1
                    continue

            elif script.script_type == ScriptType.REPEATABLE:
                if applied_checksums.get(script.filename) == script.checksum:
                    log.debug("SKIP  %s  (checksum unchanged)", label)
                    if dry_run:
                        _dry_run_plan.append({
                            "action": "skip", "type": "repeatable",
                            "script": script.filename, "keyspace": ks,
                            "reason": "checksum_unchanged",
                        })
                    skipped += 1
                    continue

            log.info("RUN   %s", label)

            if dry_run:
                _dry_run_plan.append({
                    "action": "run",
                    "type": script.script_type.value,
                    "script": script.filename,
                    "relative_path": script.relative_path,
                    "version": script.version,
                    "keyspace": ks,
                    "checksum": script.checksum,
                })
                executed += 1
                continue

            start    = time.time()
            status   = "SUCCESS"
            operator = cfg.get("username") or socket.gethostname()
            try:
                _execute_script(session, ks, script.content,
                                timeout=cfg.get("timeout"))
            except Exception as exc:
                status = "FAILED"
                errors += 1
                log.error("FAIL  %s  ->  %s", label, exc)
                elapsed_ms = int((time.time() - start) * 1000)
                history.record(script, elapsed_ms, status, operator, ks,
                               tag=cfg.get("tag", ""),
                               run_id=cfg.get("_run_id", ""))
                history.audit("SCRIPT_FAILED", operator, ks,
                              script=script.filename, status="FAILED",
                              detail=str(exc))
                notify(cfg, "script_failed",
                       keyspace=ks, script=script.filename, detail=str(exc))
                log.error("Stopping deployment for keyspace %s due to error.", ks)
                break

            elapsed_ms = int((time.time() - start) * 1000)
            history.record(script, elapsed_ms, status, operator, ks,
                           tag=cfg.get("tag", ""),
                           run_id=cfg.get("_run_id", ""))
            history.audit("SCRIPT_RUN", operator, ks,
                          script=script.filename, status="SUCCESS",
                          detail=f"{elapsed_ms}ms")
            log.info("DONE  %s  (%dms)", label, elapsed_ms)
            executed += 1

        log.info("-" * 60)
        log.info("[%s]  Executed: %d  |  Skipped: %d  |  Errors: %d",
                 ks, executed, skipped, errors)
        total_errors += errors

    if dry_run:
        _write_dry_run_output(cfg, _dry_run_plan)
    else:
        operator = cfg.get("username") or socket.gethostname()
        _audit_ev = "DEPLOY_END" if total_errors == 0 else "DEPLOY_FAILED"
        _notif_ev = "deploy_success" if total_errors == 0 else "deploy_failed"
        history.audit(_audit_ev, operator,
                      detail=f"total_errors={total_errors} tag={cfg.get('tag','')}")
        notify(cfg, _notif_ev,
               keyspace=",".join(keyspaces),
               detail=f"errors={total_errors} tag={cfg.get('tag','')}",
               status="SUCCESS" if total_errors == 0 else "FAILED")
        history.release_lock()

    if total_errors:
        sys.exit(1)


# ---------------------------------------------------------------------------
# rollback
# ---------------------------------------------------------------------------

def cmd_rollback(cfg: dict):
    """
    Roll back one or more versioned migrations using their paired U__ undo scripts.

    Behaviour:
      - Without --target-version: rolls back the single most recent migration
      - With --target-version X:  rolls back all versions > X, newest first
      - Each version must have a matching U{version}__.cql undo script
      - A ROLLED_BACK sentinel row is written to change_history after each undo
    """
    _set_verbosity(cfg)
    dry_run        = cfg["dry_run"]
    root           = cfg["root_folder"]
    keyspaces      = _resolve_keyspaces(cfg)
    target_version = cfg.get("target_version")
    rollback_tag   = cfg.get("rollback_tag")  # --tag: roll back all versions with this tag

    if not os.path.isdir(root):
        log.error("Root folder does not exist: %s", root)
        sys.exit(1)

    undo_map = discover_undo_scripts(root)
    if not undo_map:
        log.error("No undo scripts (U__*.cql) found in %s", root)
        sys.exit(1)

    session = build_session(cfg)
    _ensure_history_keyspace(session, cfg["history_keyspace"])
    history = HistoryTable(session, cfg["history_keyspace"], cfg["history_table"])
    history.ensure_table()
    if not dry_run:
        try:
            history.acquire_lock()
        except LockError as e:
            log.error("%s", e)
            sys.exit(1)
        operator = cfg.get("username") or socket.gethostname()
        cfg["_run_id"] = history._run_id
        history.audit("ROLLBACK_START", operator,
                      detail=f"keyspaces={keyspaces} target={target_version} tag={cfg.get('tag','')}")
        notify(cfg, "rollback_start",
               keyspace=",".join(keyspaces), detail=f"target={target_version}")

    total_errors = 0

    for ks in keyspaces:
        log.info("=" * 60)
        mode = f"tag={rollback_tag}" if rollback_tag else (target_version or "latest-1")
        log.info("ROLLBACK  keyspace=%s  target=%s", ks, mode)
        log.info("=" * 60)

        applied_ordered = history.applied_versions_ordered(ks)

        if not applied_ordered:
            log.warning("[%s] No applied migrations found - nothing to roll back.", ks)
            continue

        if rollback_tag:
            to_undo = history.versions_for_tag(rollback_tag, target_keyspace=ks)
            if not to_undo:
                log.info("[%s] No versions found with tag '%s' — nothing to do.", ks, rollback_tag)
                continue
        elif target_version is None:
            to_undo = [applied_ordered[0][0]]
        else:
            to_undo = [
                v for v, _ in applied_ordered
                if _vtuple(v) > _vtuple(target_version)
            ]

        if not to_undo:
            log.info("[%s] Already at or below version %s - nothing to do.", ks, target_version)
            continue

        log.info("[%s] Will undo: %s", ks, ", ".join(to_undo))

        errors = 0
        for version in to_undo:
            undo_script = undo_map.get(version)
            if not undo_script:
                log.error(
                    "[%s] No undo script for version %s (expected U%s__*.cql). Stopping.",
                    ks, version, version,
                )
                errors += 1
                break

            label = f"[undo] {undo_script.filename}"
            log.info("UNDO  %s", label)

            if dry_run:
                continue

            start    = time.time()
            status   = "SUCCESS"
            operator = cfg.get("username") or socket.gethostname()
            try:
                _execute_script(session, ks, undo_script.content,
                                timeout=cfg.get("timeout"))
            except Exception as exc:
                status = "FAILED"
                errors += 1
                log.error("FAIL  %s  ->  %s", label, exc)
                elapsed_ms = int((time.time() - start) * 1000)
                history.record(undo_script, elapsed_ms, status, operator, ks,
                               tag=cfg.get("tag", ""),
                               run_id=cfg.get("_run_id", ""))
                history.audit("SCRIPT_FAILED", operator, ks,
                              script=undo_script.filename, status="FAILED",
                              detail=str(exc))
                notify(cfg, "script_failed",
                       keyspace=ks, script=undo_script.filename, detail=str(exc))
                log.error("Stopping rollback for keyspace %s.", ks)
                break

            elapsed_ms = int((time.time() - start) * 1000)
            history.record(undo_script, elapsed_ms, status, operator, ks,
                           tag=cfg.get("tag", ""),
                           run_id=cfg.get("_run_id", ""))
            history.audit("SCRIPT_RUN", operator, ks,
                          script=undo_script.filename, status="SUCCESS",
                          detail=f"{elapsed_ms}ms rollback")
            history.mark_rolled_back(version, ks,
                                     tag=cfg.get("tag", ""),
                                     run_id=cfg.get("_run_id", ""))
            log.info("DONE  %s  (%dms)  ->  version %s rolled back", label, elapsed_ms, version)

        log.info("-" * 60)
        log.info("[%s] Rollback %s.", ks, "complete" if errors == 0 else "FAILED")
        total_errors += errors

    if not dry_run:
        operator  = cfg.get("username") or socket.gethostname()
        _audit_ev = "ROLLBACK_END" if total_errors == 0 else "ROLLBACK_FAILED"
        _notif_ev = "rollback_success" if total_errors == 0 else "rollback_failed"
        history.audit(_audit_ev, operator,
                      detail=f"total_errors={total_errors} tag={cfg.get('tag','')}")
        notify(cfg, _notif_ev,
               keyspace=",".join(keyspaces),
               detail=f"errors={total_errors} tag={cfg.get('tag','')}",
               status="SUCCESS" if total_errors == 0 else "FAILED")
        history.release_lock()

    if total_errors:
        sys.exit(1)


# ---------------------------------------------------------------------------
# validate
# ---------------------------------------------------------------------------

def cmd_validate(cfg: dict):
    """
    Lint all migration scripts without connecting to Cassandra.

    Checks:
      1. All .cql filenames match a recognised naming convention
      2. No duplicate versioned script versions
      3. Every undo (U__) script has a matching forward (V__) script
      4. No empty scripts
      5. At least one executable CQL statement per script
    """
    _set_verbosity(cfg)
    root = cfg["root_folder"]

    if not os.path.isdir(root):
        log.error("Root folder does not exist: %s", root)
        sys.exit(1)

    log.info("Validating scripts in: %s", root)
    log.info("-" * 60)

    from cassachange.scripts import VERSIONED_RE, UNDO_RE, REPEATABLE_RE, ALWAYS_RE, _walk_cql_files
    from cassachange.cql_validate import lint_cql_content
    known_patterns = [VERSIONED_RE, UNDO_RE, REPEATABLE_RE, ALWAYS_RE]

    cql_files = _walk_cql_files(root)

    if not cql_files:
        log.warning("No .cql files found in %s", root)
        return

    errors   = []
    warnings = []

    forward_scripts, undo_map = discover_all_scripts(root)

    # Check 1: unrecognised filenames
    for filepath in cql_files:
        filename = os.path.basename(filepath)
        if not any(p.match(filename) for p in known_patterns):
            rel = os.path.relpath(filepath, root)
            warnings.append(f"Unrecognised filename (will be ignored): {rel}")

    # Check 2: duplicate versioned versions
    seen_versions: dict = {}
    for script in forward_scripts:
        if script.script_type != ScriptType.VERSIONED:
            continue
        if script.version in seen_versions:
            errors.append(
                f"Duplicate version {script.version}: "
                f"{seen_versions[script.version]} vs {script.filename}"
            )
        else:
            seen_versions[script.version] = script.filename

    # Check 3: orphaned undo scripts
    for version, undo_script in undo_map.items():
        if version not in seen_versions:
            errors.append(
                f"Undo script {undo_script.filename} has no matching "
                f"V{version}__*.cql forward script"
            )

    # Check 4 & 5: per-script content + CQL syntax checks
    all_scripts = forward_scripts + list(undo_map.values())
    for script in sorted(all_scripts, key=lambda s: s.filename):
        content = script.content.strip()

        if not content:
            errors.append(f"Empty script: {script.filename}")
            continue

        # Strip comment lines and check for real statements
        executable_lines = [
            l for l in content.splitlines()
            if l.strip() and not l.strip().startswith("--")
        ]
        body       = " ".join(executable_lines)
        statements = [s.strip() for s in body.split(";") if s.strip()]

        if not statements:
            warnings.append(f"No executable statements in: {script.filename}")
            continue

        # CQL syntax lint — catches typos like MDDADD, SELCT, DRPO, etc.
        lint_errors = lint_cql_content(script.content, filename=script.filename)
        if lint_errors:
            for le in lint_errors:
                preview = le.statement[:120].strip()
                errors.append(
                    f"CQL syntax error in {script.filename} "
                    f"(line ~{le.line_hint}): {le.message} | {preview}"
                )
        else:
            log.debug("OK    %-50s  (%d statement(s))", script.relative_path, len(statements))

    # Report
    log.info("")
    if warnings:
        log.info("WARNINGS:")
        for w in warnings:
            log.warning("  WARN  %s", w)
    if errors:
        log.info("ERRORS:")
        for e in errors:
            log.error("  ERR   %s", e)

    log.info("-" * 60)
    total_scripts = len(forward_scripts) + len(undo_map)
    log.info(
        "Validated %d script(s) across %s  |  %d warning(s)  |  %d error(s)",
        total_scripts, root, len(warnings), len(errors),
    )

    if errors:
        log.error("Validation FAILED.")
        sys.exit(1)
    else:
        log.info("Validation PASSED.")


# ---------------------------------------------------------------------------
# status
# ---------------------------------------------------------------------------

def cmd_status(cfg: dict):
    _set_verbosity(cfg)

    keyspaces  = _resolve_keyspaces(cfg)
    ks_filter  = keyspaces[0] if len(keyspaces) == 1 else None
    tag_filter = cfg.get("tag_filter")

    session = build_session(cfg)
    _ensure_history_keyspace(session, cfg["history_keyspace"])
    history = HistoryTable(session, cfg["history_keyspace"], cfg["history_table"])
    history.ensure_table()

    rows = history.all_rows(tag_filter=tag_filter)
    if not rows:
        label = f" for tag '{tag_filter}'" if tag_filter else ""
        log.info("No migrations found%s.", label)
        return

    if ks_filter:
        rows = [r for r in rows if getattr(r, "keyspace_name", "") == ks_filter]

    rows.sort(key=lambda r: r.installed_on or datetime.datetime.min, reverse=True)

    header = (
        f"{'VERSION':<12} {'KEYSPACE':<20} {'TAG':<18} {'SCRIPT':<42} "
        f"{'STATUS':<12} {'INSTALLED_ON':<24}"
    )
    log.info(header)
    log.info("-" * len(header))
    for row in rows:
        log.info(
            "%-12s %-20s %-18s %-42s %-12s %s",
            str(row.version or ""),
            str(getattr(row, "keyspace_name", "") or ""),
            str(getattr(row, "tag", "") or ""),
            row.script,
            row.status,
            str(row.installed_on),
        )



# ---------------------------------------------------------------------------
# repair
# ---------------------------------------------------------------------------

def cmd_repair(cfg: dict):
    """
    Repair command -- two sub-operations:

      cassachange repair
        Lists all FAILED scripts across all keyspaces and marks them as
        REPAIRED so they will be retried on the next deploy. The original
        FAILED rows are preserved in change_history for audit purposes.

      cassachange repair --release-lock
        Forcibly releases a stuck deploy lock. Use this when a previous
        deploy crashed and left the lock behind (rather than waiting for
        the TTL to expire).

      cassachange repair --list
        Lists FAILED scripts and any active locks without making any changes.
    """
    _set_verbosity(cfg)

    release_lock = cfg.get("release_lock", False)
    list_only    = cfg.get("list_only", False)
    keyspaces    = _resolve_keyspaces(cfg)

    session = build_session(cfg)
    _ensure_history_keyspace(session, cfg["history_keyspace"])
    history = HistoryTable(session, cfg["history_keyspace"], cfg["history_table"])
    history.ensure_table()

    # ------------------------------------------------------------------
    # Show lock status always
    # ------------------------------------------------------------------
    locks = history.lock_status()
    if locks:
        log.info("Active locks:")
        for lock in locks:
            log.info(
                "  key=%-20s  held_by=%-30s  since=%s",
                lock.lock_key, lock.locked_by, lock.locked_at,
            )
    else:
        log.info("No active locks.")

    # ------------------------------------------------------------------
    # --release-lock
    # ------------------------------------------------------------------
    if release_lock:
        _operator = cfg.get("username") or socket.gethostname()
        history.force_release_lock()
        history.audit("LOCK_FORCE_RELEASE", _operator,
                      detail="forced via cassachange repair --release-lock")
        log.info("Lock force-released.")
        return

    # ------------------------------------------------------------------
    # List / reset FAILED scripts
    # ------------------------------------------------------------------
    ks_filter   = keyspaces[0] if len(keyspaces) == 1 else None
    script_name = cfg.get("script_name")   # None = all failed scripts
    failed      = history.failed_scripts(target_keyspace=ks_filter,
                                         script_name=script_name)

    if not failed:
        scope = []
        if ks_filter:   scope.append(f"keyspace '{ks_filter}'")
        if script_name: scope.append(f"script '{script_name}'")
        log.info("No FAILED scripts found%s.",
                 f" for {' and '.join(scope)}" if scope else "")
        return

    log.info("")
    log.info("FAILED scripts:")
    header = f"  {'SCRIPT':<50} {'KEYSPACE':<20} {'FAILED_AT':<24}"
    log.info(header)
    log.info("  " + "-" * (len(header) - 2))
    for row in failed:
        log.info(
            "  %-50s %-20s %s",
            row.script,
            getattr(row, "keyspace_name", ""),
            row.installed_on,
        )

    if list_only:
        log.info("")
        log.info("Run without --list to mark these scripts as REPAIRED.")
        return

    log.info("")
    repaired_by = cfg.get("username") or socket.gethostname()
    for row in failed:
        ks_name = getattr(row, "keyspace_name", "")
        history.reset_failed_script(
            script_filename=row.script,
            target_keyspace=ks_name,
            repaired_by=repaired_by,
        )
        history.audit("REPAIR", repaired_by, ks_name,
                      script=row.script, status="REPAIRED",
                      detail="marked for retry by repair command")

    log.info("")
    log.info(
        "Marked %d script(s) as REPAIRED. Run cassachange deploy to retry them.",
        len(failed),
    )


# ---------------------------------------------------------------------------
# audit-log
# ---------------------------------------------------------------------------

def cmd_baseline(cfg: dict):
    """
    Introspect a live Cassandra keyspace and generate a versioned baseline CQL file.

    Usage:
        cassachange baseline --keyspace myapp
        cassachange baseline --keyspace myapp --output ./migrations/ --baseline-version 1.0.0

    The generated file is named V{version}__baseline_{keyspace}.cql.
    All statements use IF NOT EXISTS so the file is safe to re-run.
    """
    _set_verbosity(cfg)
    keyspaces  = _resolve_keyspaces(cfg)
    if not keyspaces:
        log.error("--keyspace is required for 'cassachange baseline'")
        sys.exit(1)

    output_dir = cfg.get("output_dir") or cfg.get("root_folder") or "./migrations"
    version    = cfg.get("baseline_version") or "0.0.0"
    session    = build_session(cfg)

    for ks in keyspaces:
        log.info("Generating baseline for keyspace '%s'...", ks)
        try:
            fpath = generate_baseline(session, ks, output_dir, version=version)
            log.info("Done: %s", fpath)
            log.info("Next steps:")
            log.info("  1. Review and commit %s", fpath)
            log.info("  2. cassachange validate")
            log.info("  3. cassachange deploy  (records it as applied in history)")
        except Exception as exc:
            log.error("Baseline failed for '%s': %s", ks, exc)
            sys.exit(1)


def cmd_audit(cfg: dict):
    """
    Immutable audit log viewer — enterprise feature.
    Upgrade to cassachange-enterprise to use this command.
    """
    from cassachange.history import EnterpriseFeatureError
    raise EnterpriseFeatureError("cassachange audit")

def _add_connection_args(p):
    p.add_argument("--hosts",    help="Comma-separated Cassandra hosts")
    p.add_argument("--port",     type=int)
    p.add_argument("--username", "-u")
    p.add_argument("--password", "-p")
    p.add_argument("--astra-token",           dest="astra_token",
                   help="AstraDB application token (AstraCS:...)")
    p.add_argument("--secure-connect-bundle", dest="secure_connect_bundle",
                   help="Path to AstraDB Secure Connect Bundle .zip")
    p.add_argument("--history-keyspace", dest="history_keyspace")
    p.add_argument("--history-table",    dest="history_table")


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        prog="cassachange",
        description="Cassandra schema migration tool",
    )
    parser.add_argument("--config",  "-c", default="cassachange.yml")
    parser.add_argument("--verbose", "-v", action="store_true", default=None)

    sub = parser.add_subparsers(dest="command")

    # deploy
    deploy_p = sub.add_parser("deploy", help="Apply pending migrations")
    _add_connection_args(deploy_p)
    deploy_p.add_argument("--root-folder", "-f", dest="root_folder")
    deploy_p.add_argument("--keyspace",  "-k",
                          help="Single target keyspace")
    deploy_p.add_argument("--keyspaces",
                          help="Comma-separated keyspaces for multi-keyspace deploy")
    deploy_p.add_argument("--dry-run", dest="dry_run",
                          action="store_true", default=None)
    deploy_p.add_argument(
        "--timeout", type=int, dest="timeout", default=None,
        metavar="SECONDS",
        help="Per-CQL-statement timeout in seconds (default: driver default ~10s). "
             "Raises OperationTimedOut if exceeded.",
    )
    deploy_p.add_argument(
        "--profile", dest="profile", default=None,
        help="Named config profile from cassachange.yml profiles: block",
    )
    deploy_p.add_argument(
        "--tag", dest="tag", default=None, metavar="TAG",
        help="Release marker stored in history (e.g. release-2.1.0)",
    )
    deploy_p.add_argument(
        "--dry-run-output", dest="dry_run_output", default=None, metavar="FILE",
        help="Write dry-run plan to this JSON file (implies --dry-run)",
    )

    # rollback
    rollback_p = sub.add_parser("rollback", help="Roll back using undo scripts")
    _add_connection_args(rollback_p)
    rollback_p.add_argument("--root-folder", "-f", dest="root_folder")
    rollback_p.add_argument("--keyspace",  "-k")
    rollback_p.add_argument("--keyspaces")
    rollback_p.add_argument(
        "--target-version", dest="target_version", default=None,
        help="Roll back all versions ABOVE this. Omit to roll back only the latest.",
    )
    rollback_p.add_argument("--dry-run", dest="dry_run",
                            action="store_true", default=None)
    rollback_p.add_argument(
        "--timeout", type=int, dest="timeout", default=None,
        metavar="SECONDS",
        help="Per-CQL-statement timeout in seconds.",
    )
    rollback_p.add_argument(
        "--profile", dest="profile", default=None,
        help="Named config profile",
    )
    rollback_p.add_argument(
        "--tag", dest="rollback_tag", default=None, metavar="TAG",
        help=(
            "Roll back all versions that were deployed with this tag. "
            "Alternative to --target-version when you think in releases, "
            "not version numbers. Example: --tag release-2.1.0"
        ),
    )

    # status
    status_p = sub.add_parser("status", help="Show migration history")
    _add_connection_args(status_p)
    status_p.add_argument("--keyspace",  "-k")
    status_p.add_argument("--keyspaces")
    status_p.add_argument(
        "--tag", dest="tag_filter", default=None, metavar="TAG",
        help="Show only migrations deployed with this tag",
    )
    status_p.add_argument(
        "--profile", dest="profile", default=None,
        help="Named config profile (overrides CASSACHANGE_PROFILE env var)",
    )

    # validate
    validate_p = sub.add_parser("validate", help="Lint scripts without connecting")
    validate_p.add_argument("--root-folder", "-f", dest="root_folder")
    validate_p.add_argument(
        "--profile", dest="profile", default=None,
        help="Named config profile (uses root_folder from that profile)",
    )

    # repair
    repair_p = sub.add_parser("repair", help="Fix stuck locks or FAILED scripts")
    _add_connection_args(repair_p)
    repair_p.add_argument("--keyspace",  "-k")
    repair_p.add_argument("--keyspaces")
    repair_p.add_argument(
        "--release-lock", dest="release_lock",
        action="store_true", default=False,
        help="Forcibly release a stuck deploy lock",
    )
    repair_p.add_argument(
        "--list", dest="list_only",
        action="store_true", default=False,
        help="List FAILED scripts and locks without making changes",
    )
    repair_p.add_argument(
        "--script", dest="script_name", default=None,
        metavar="SCRIPT_FILENAME",
        help=(
            "Repair only this specific script filename "
            "(e.g. V1.3.0__add_index.cql). "
            "Omit to repair all FAILED scripts in the target keyspace."
        ),
    )
    repair_p.add_argument(
        "--profile", dest="profile", default=None,
        help="Named config profile (overrides CASSACHANGE_PROFILE env var)",
    )

    # baseline
    baseline_p = sub.add_parser(
        "baseline",
        help="Introspect a live keyspace and generate a V0.0.0__baseline.cql",
    )
    _add_connection_args(baseline_p)
    baseline_p.add_argument("--keyspace", "-k", help="Keyspace to introspect")
    baseline_p.add_argument(
        "--output", dest="output_dir", default=None, metavar="DIR",
        help="Output directory (default: root_folder from config)",
    )
    baseline_p.add_argument(
        "--baseline-version", dest="baseline_version", default="0.0.0",
        help="Version for the generated filename (default: 0.0.0)",
    )
    baseline_p.add_argument(
        "--profile", dest="profile", default=None,
        help="Named config profile",
    )

    # audit
    audit_p = sub.add_parser("audit", help="View the immutable audit log")
    _add_connection_args(audit_p)
    audit_p.add_argument("--keyspace", "-k")
    audit_p.add_argument("--keyspaces")
    audit_p.add_argument(
        "--run-id", dest="run_id", default=None,
        help="Filter to a specific deploy run ID",
    )
    audit_p.add_argument(
        "--limit", type=int, default=50,
        help="Maximum rows to show, most recent first (default: 50)",
    )
    audit_p.add_argument(
        "--profile", dest="profile", default=None,
        help="Named config profile (overrides CASSACHANGE_PROFILE env var)",
    )

    return parser


def main():
    # Load enterprise features if cassachange-enterprise is installed
    try:
        from importlib.metadata import entry_points
        for ep in entry_points(group="cassachange.enterprise"):
            ep.load()  # triggers cassachange_enterprise.loader
    except Exception:
        pass  # enterprise not installed — community edition continues

    parser = build_parser()
    args   = parser.parse_args()

    if not args.command:
        parser.print_help()
        sys.exit(1)

    cfg_file = getattr(args, "config", "cassachange.yml")
    _profile = getattr(args, "profile", None) or os.getenv("CASSACHANGE_PROFILE")
    cfg      = merge_config(args, load_config(cfg_file, profile=_profile))

    # Resolve secrets — enterprise feature (no-op in community if no secrets block)
    try:
        cfg = resolve_secrets(cfg)
    except SecretsError as exc:
        log.error("%s", exc)
        sys.exit(1)

    # --dry-run-output implies --dry-run
    if cfg.get("dry_run_output"):
        cfg["dry_run"] = True

    if args.command in ("deploy", "rollback", "status"):
        if not _resolve_keyspaces(cfg):
            log.error(
                "keyspace / keyspaces not supplied -- use --keyspace, --keyspaces, "
                "CASSANDRA_KEYSPACE env var, or set keyspace / keyspaces in cassachange.yml"
            )
            sys.exit(1)

    if args.command in ("deploy", "rollback", "status", "repair", "audit", "baseline"):
        if not cfg.get("history_keyspace") and args.command != "baseline":
            log.error(
                "history_keyspace not supplied -- use --history-keyspace, "
                "CASSACHANGE_HISTORY_KEYSPACE env var, or set history_keyspace in cassachange.yml"
            )
            sys.exit(1)

    dispatch = {
        "deploy":   cmd_deploy,
        "rollback": cmd_rollback,
        "status":   cmd_status,
        "validate": cmd_validate,
        "repair":   cmd_repair,
        "audit":    cmd_audit,
        "baseline": cmd_baseline,
    }
    try:
        dispatch[args.command](cfg)
    finally:
        # Always delete temp files written for file secrets (certs, SCB zip).
        # They contain private key material and must not outlive the process.
        cleanup_temp_files()


if __name__ == "__main__":
    main()
