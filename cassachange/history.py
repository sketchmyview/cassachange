"""
Manages the change_history and deploy_lock tables stored in Cassandra.

change_history table:
  Tracks every script execution across all keyspaces. Single source of truth
  for what has been applied, failed, or rolled back.

deploy_lock table:
  Distributed mutex that prevents concurrent deployments. Uses Cassandra
  Lightweight Transactions (LWT) via INSERT IF NOT EXISTS to guarantee
  only one cassachange process can hold the lock at a time.
  Lock rows carry a TTL so a crashed process can never leave the lock
  permanently stuck -- it expires automatically after lock_ttl_seconds.

Keyspace management:
  cassachange never creates keyspaces. All keyspaces (including the history
  keyspace) must exist before running cassachange. Keyspaces should be
  managed via Terraform or equivalent infrastructure tooling.
"""

import datetime
import logging
import socket
import uuid

log = logging.getLogger("cassachange")


class EnterpriseFeatureError(Exception):
    """Raised when a community user tries to use an enterprise feature."""
    _MSG = (
        "\n"
        "  {feature} requires cassachange-enterprise.\n"
        "\n"
        "  Community edition:  pip install cassachange\n"
        "  Enterprise edition: pip install cassachange-enterprise\n"
        "\n"
        "  Contact: enterprise@cassachange.com\n"
    )
    def __init__(self, feature: str):
        super().__init__(self._MSG.format(feature=feature))


# Default TTL for lock rows -- if cassachange crashes mid-deploy the lock
# expires automatically after this many seconds rather than staying stuck.
DEFAULT_LOCK_TTL = 1800  # 30 minutes

CREATE_HISTORY_TABLE_TEMPLATE = """
CREATE TABLE IF NOT EXISTS {keyspace}.{table} (
    installed_on   timestamp,
    script         text,
    script_type    text,
    version        text,
    description    text,
    checksum       text,
    execution_time int,
    status         text,
    installed_by   text,
    keyspace_name  text,
    PRIMARY KEY (script, installed_on)
) WITH CLUSTERING ORDER BY (installed_on DESC)
"""

CREATE_LOCK_TABLE_TEMPLATE = """
CREATE TABLE IF NOT EXISTS {keyspace}.deploy_lock (
    lock_key    text PRIMARY KEY,
    locked_by   text,
    locked_at   timestamp,
    run_id      text
)
"""

CREATE_AUDIT_TABLE_TEMPLATE = """
CREATE TABLE IF NOT EXISTS {keyspace}.audit_log (
    event_time    timestamp,
    run_id        text,
    event_type    text,
    operator      text,
    hostname      text,
    keyspace_name text,
    script        text,
    status        text,
    detail        text,
    PRIMARY KEY (run_id, event_time)
) WITH CLUSTERING ORDER BY (event_time ASC)
"""


class LockError(Exception):
    """Raised when the deploy lock cannot be acquired."""


class HistoryTable:
    def __init__(self, session, keyspace: str, table: str,
                 lock_ttl: int = DEFAULT_LOCK_TTL):
        self.session   = session
        self.keyspace  = keyspace
        self.table     = table
        self.lock_ttl  = lock_ttl
        self._run_id   = str(uuid.uuid4())
        self._identity = f"{socket.gethostname()}:{self._run_id[:8]}"

    # ------------------------------------------------------------------
    # Table setup
    # ------------------------------------------------------------------

    def ensure_table(self):
        """Create the history and lock tables if they do not already exist."""
        self.session.execute(
            CREATE_HISTORY_TABLE_TEMPLATE.format(
                keyspace=self.keyspace, table=self.table
            )
        )
        # Idempotent migration for pre-v1.1.0 tables — add tag + run_id columns.
        # On Cassandra 4+ ALTER TABLE ADD is a no-op if the column already exists.
        # On older clusters it raises InvalidRequest; we catch and ignore that.
        for _col, _ctype in (("tag", "text"), ("run_id", "text")):
            try:
                self.session.execute(
                    f"ALTER TABLE {self.keyspace}.{self.table} "
                    f"ADD {_col} {_ctype}"
                )
            except Exception:
                pass  # column already exists — safe to ignore
        self.session.execute(
            CREATE_LOCK_TABLE_TEMPLATE.format(keyspace=self.keyspace)
        )
        log.debug("History table ready: %s.%s", self.keyspace, self.table)
        log.debug("Lock table ready:    %s.deploy_lock", self.keyspace)

    # ------------------------------------------------------------------
    # Distributed locking
    # ------------------------------------------------------------------

    def acquire_lock(self, lock_key: str = "global"):
        """
        Acquire the deploy lock using a Cassandra LWT (INSERT IF NOT EXISTS).

        If the lock is already held by another process this raises LockError
        with details of who holds it. The lock row has a TTL so a crashed
        process cannot leave it permanently stuck.

        Args:
            lock_key: Scope of the lock. Defaults to "global" (one deploy at
                      a time across all keyspaces). Pass a keyspace name to
                      allow concurrent deploys to different keyspaces.
        """
        log.debug("Acquiring deploy lock (key=%s, run_id=%s)", lock_key, self._run_id)

        result = self.session.execute(
            f"""
            INSERT INTO {self.keyspace}.deploy_lock
                (lock_key, locked_by, locked_at, run_id)
            VALUES (%s, %s, %s, %s)
            IF NOT EXISTS
            USING TTL {self.lock_ttl}
            """,
            (lock_key, self._identity, datetime.datetime.utcnow(), self._run_id),
        )

        row = result.one()
        if not row.applied:
            holder    = getattr(row, "locked_by", "unknown")
            locked_at = getattr(row, "locked_at", "unknown")
            raise LockError(
                f"Deploy lock '{lock_key}' is already held by '{holder}' "
                f"(since {locked_at}). Another cassachange deploy is in progress. "
                f"If this is stale, wait {self.lock_ttl}s for the TTL to expire "
                f"or run: cassachange repair --release-lock"
            )

        log.info("Deploy lock acquired (key=%s, holder=%s)", lock_key, self._identity)

    def release_lock(self, lock_key: str = "global"):
        """
        Release the deploy lock -- only if this process holds it.
        Uses LWT (DELETE IF run_id = ...) so a different process cannot
        accidentally release a lock it does not own.
        """
        result = self.session.execute(
            f"""
            DELETE FROM {self.keyspace}.deploy_lock
            WHERE lock_key = %s
            IF run_id = %s
            """,
            (lock_key, self._run_id),
        )
        row = result.one()
        if row.applied:
            log.info("Deploy lock released (key=%s)", lock_key)
        else:
            log.warning(
                "Could not release lock '%s' -- it is no longer owned by this process. "
                "It may have expired or been released by a repair command.",
                lock_key,
            )

    # ------------------------------------------------------------------
    # Audit log
    # ------------------------------------------------------------------

    def audit(self, event_type: str, operator: str, keyspace_name: str = "",
              script: str = "", status: str = "", detail: str = ""):
        """
        Immutable audit log is an enterprise feature.
        Community edition: silent no-op so internal deploy/rollback/repair
        calls continue to work without errors. Events are not persisted.
        Upgrade to cassachange-enterprise to enable the full audit log.
        """
        pass  # no-op in community edition

    def audit_rows(self, run_id: str = None) -> list:
        """Audit log query is an enterprise feature."""
        from cassachange.history import EnterpriseFeatureError
        raise EnterpriseFeatureError("audit log")

    def force_release_lock(self, lock_key: str = "global"):
        """
        Unconditionally release the lock regardless of who holds it.
        Used by `cassachange repair --release-lock` only.
        """
        self.session.execute(
            f"DELETE FROM {self.keyspace}.deploy_lock WHERE lock_key = %s",
            (lock_key,),
        )
        log.info("Deploy lock '%s' forcibly released.", lock_key)

    def lock_status(self) -> list:
        """Return all current lock rows."""
        return list(self.session.execute(
            f"SELECT lock_key, locked_by, locked_at, run_id "
            f"FROM {self.keyspace}.deploy_lock"
        ))

    # ------------------------------------------------------------------
    # Repair helpers
    # ------------------------------------------------------------------

    def failed_scripts(self, target_keyspace: str = None,
                        script_name: str = None) -> list:
        """
        Return all scripts currently in FAILED status that have not been
        subsequently retried successfully. Used by the repair command.

        Args:
            target_keyspace: filter to a specific keyspace (optional)
            script_name:     filter to a single script filename (optional)
                             enables surgical per-script repair
        """
        cql = (
            f"SELECT script, script_type, version, keyspace_name, installed_on, installed_by "
            f"FROM {self.keyspace}.{self.table} "
            f"WHERE status = 'FAILED' ALLOW FILTERING"
        )
        rows = self.session.execute(cql)
        result = []
        for row in rows:
            if target_keyspace and getattr(row, "keyspace_name", None) != target_keyspace:
                continue
            if script_name and row.script != script_name:
                continue
            result.append(row)
        return result

    def reset_failed_script(self, script_filename: str, target_keyspace: str,
                             repaired_by: str):
        """
        Mark a FAILED script as REPAIRED so cassachange will retry it on
        the next deploy. Inserts a new REPAIRED sentinel row -- the original
        FAILED rows are preserved for audit purposes.
        """
        self.session.execute(
            f"""
            INSERT INTO {self.keyspace}.{self.table}
                (installed_on, script, script_type, version, description,
                 checksum, execution_time, status, installed_by, keyspace_name)
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
            """,
            (
                datetime.datetime.utcnow(),
                script_filename,
                "repair",
                None,
                f"Repaired by {repaired_by}",
                "",
                0,
                "REPAIRED",
                repaired_by,
                target_keyspace,
            ),
        )
        log.info(
            "Script '%s' on keyspace '%s' marked as REPAIRED -- "
            "it will be retried on next deploy.",
            script_filename, target_keyspace,
        )

    # ------------------------------------------------------------------
    # History queries
    # ------------------------------------------------------------------

    def applied_versions(self, target_keyspace: str = None) -> set:
        """Return the set of versions successfully applied (optionally filtered by keyspace)."""
        cql = (
            f"SELECT version, keyspace_name FROM {self.keyspace}.{self.table} "
            f"WHERE script_type = 'versioned' AND status = 'SUCCESS' ALLOW FILTERING"
        )
        rows = self.session.execute(cql)
        result = set()
        for row in rows:
            if not row.version:
                continue
            if target_keyspace and getattr(row, "keyspace_name", None) != target_keyspace:
                continue
            result.add(row.version)
        return result

    def applied_checksums(self, target_keyspace: str = None) -> dict:
        """Return {filename: checksum} for the most recent successful run of each script."""
        cql = (
            f"SELECT script, checksum, keyspace_name FROM {self.keyspace}.{self.table} "
            f"WHERE status = 'SUCCESS' ALLOW FILTERING"
        )
        rows = self.session.execute(cql)
        result = {}
        for row in rows:
            if target_keyspace and getattr(row, "keyspace_name", None) != target_keyspace:
                continue
            if row.script not in result:
                result[row.script] = row.checksum
        return result

    def applied_versions_ordered(self, target_keyspace: str = None) -> list:
        """
        Return list of (version, script_filename) tuples successfully applied,
        sorted descending by version — used for rollback target selection.
        """
        import re
        cql = (
            f"SELECT version, script, keyspace_name FROM {self.keyspace}.{self.table} "
            f"WHERE script_type = 'versioned' AND status = 'SUCCESS' ALLOW FILTERING"
        )
        rows = self.session.execute(cql)
        seen = {}
        for row in rows:
            if not row.version:
                continue
            if target_keyspace and getattr(row, "keyspace_name", None) != target_keyspace:
                continue
            if row.version not in seen:
                seen[row.version] = row.script

        def _vtuple(v):
            return tuple(int(x) for x in re.split(r"[._]", v))

        return sorted(seen.items(), key=lambda t: _vtuple(t[0]), reverse=True)

    def versions_for_tag(self, tag: str, target_keyspace: str = None) -> list:
        """
        Return all version strings that were deployed with the given tag label,
        sorted descending by semver — used by rollback --tag.

        Example: if V1.2.0, V1.3.0, and V1.4.0 were all deployed with
                 tag='release-2.1.0', this returns ['1.4.0','1.3.0','1.2.0'].
        """
        import re
        cql = (
            f"SELECT version, keyspace_name, tag FROM {self.keyspace}.{self.table} "
            f"WHERE script_type = 'versioned' AND status = 'SUCCESS' ALLOW FILTERING"
        )
        rows = self.session.execute(cql)
        seen = set()
        for row in rows:
            if not row.version:
                continue
            if target_keyspace and getattr(row, "keyspace_name", None) != target_keyspace:
                continue
            if (getattr(row, "tag", "") or "") == tag:
                seen.add(row.version)

        def _vtuple(v):
            return tuple(int(x) for x in re.split(r"[._]", v))

        return sorted(seen, key=_vtuple, reverse=True)

    def mark_rolled_back(self, version: str, target_keyspace: str = None,
                          tag: str = "", run_id: str = ""):
        """Insert a ROLLED_BACK sentinel so the version can be re-applied by a future deploy."""
        self.session.execute(
            f"""
            INSERT INTO {self.keyspace}.{self.table}
                (installed_on, script, script_type, version, description,
                 checksum, execution_time, status, installed_by, keyspace_name,
                 tag, run_id)
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
            """,
            (
                datetime.datetime.utcnow(),
                f"ROLLBACK_V{version}",
                "undo",
                version,
                f"Rollback of version {version}",
                "",
                0,
                "ROLLED_BACK",
                "cassachange",
                target_keyspace or "",
                tag or "",
                run_id or "",
            ),
        )

    def all_rows(self, tag_filter: str = None) -> list:
        """Return all history rows, optionally filtered by tag."""
        rows = list(self.session.execute(
            f"SELECT version, script, script_type, status, installed_on, "
            f"execution_time, keyspace_name, tag, run_id "
            f"FROM {self.keyspace}.{self.table}"
        ))
        if tag_filter:
            rows = [r for r in rows
                    if (getattr(r, "tag", "") or "") == tag_filter]
        return rows

    def record(self, script, elapsed_ms: int, status: str,
               installed_by: str, target_keyspace: str = None,
               tag: str = "", run_id: str = ""):
        """
        Insert a row recording the outcome of running a script.

        Args:
            tag:    Release marker stored for filtering/rollback-by-tag.
            run_id: Links this row back to its DEPLOY_START audit event.
        """
        self.session.execute(
            f"""
            INSERT INTO {self.keyspace}.{self.table}
                (installed_on, script, script_type, version, description,
                 checksum, execution_time, status, installed_by, keyspace_name,
                 tag, run_id)
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
            """,
            (
                datetime.datetime.utcnow(),
                script.filename,
                script.script_type.value,
                script.version,
                script.description,
                script.checksum,
                elapsed_ms,
                status,
                installed_by,
                target_keyspace or "",
                tag or "",
                run_id or "",
            ),
        )
