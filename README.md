<p>
  <img src="https://www.cassachange.com/logo3.png" alt="cassachange" width="200"/>
</p>

<p>
Website: https://www.cassachange.com
</p>

### Purpose-built CQL migration tool for Apache Cassandra, DataStax AstraDB, ScyllaDB, Azure Managed Cassandra, and Amazon Keyspaces

## Installation

Versioned scripts, rollback, distributed locking, multi-keyspace deploys, environment profiles, secret manager integration, and native AstraDB auth — no JVM, no XML changelogs, no compromises.

```sh
pip install cassachange
```

## Quick Start

```
$ cassachange deploy --profile dev --tag release-2.1.0
$ cassachange deploy --profile prod --tag release-2.1.0

[secrets] resolved 2 keys via azure-keyvault
[lock]    acquired global (host:a4f3b1)
[RUN]     V1.0.0__create_users.cql        42ms
[RUN]     V1.1.0__add_orders.cql          28ms
[SKIP]    V1.2.0__add_profiles.cql        already applied
[RUN]     R__users_by_email.cql           checksum changed
[RUN]     A__refresh_perms.cql            always
[notify]  Slack → deploy_success
[lock]    released
✓ myapp_prod: run 3 | skip 1 | errors 0 | tag release-2.1.0
```

---

## Table of Contents

1. [Editions — Community vs Enterprise](#1-editions--community-vs-enterprise)
2. [Requirements](#2-requirements)
3. [Installation](#3-installation)
4. [Quick Start](#4-quick-start)
5. [cassachange.yml Reference](#5-cassachangeyml-reference)
6. [Script Types](#6-script-types)
7. [Folder Structure](#7-folder-structure)
8. [Deploy Protocol](#8-deploy-protocol)
9. [Commands](#9-commands)
10. [Config Profiles](#10-config-profiles)
11. [Connection Modes](#11-connection-modes)
12. [Environment Variables](#12-environment-variables)
13. [Distributed Locking](#13-distributed-locking)
14. [Release Tagging](#14-release-tagging)
15. [Dry Run](#15-dry-run)
16. [Notifications](#16-notifications)
17. [CQL Linter](#17-cql-linter)
18. [Baseline Introspection](#18-baseline-introspection)
19. [Repair](#19-repair)
20. [Multi-Keyspace Deploy](#20-multi-keyspace-deploy)
21. [Secret Manager Integration — Enterprise](#21-secret-manager-integration--enterprise)
22. [File Secrets — Enterprise](#22-file-secrets--enterprise)
23. [Audit Log — Enterprise](#23-audit-log--enterprise)
24. [Upgrading Community to Enterprise](#24-upgrading-community-to-enterprise)
25. [Schema Analyser](#25-schema-analyser)
26. [GitHub Actions CI/CD](#26-github-actions-cicd)
27. [Keyspace Management](#27-keyspace-management)
28. [History Tables Reference](#28-history-tables-reference)
29. [Comparison with Other Tools](#29-comparison-with-other-tools)

---

## 1. Editions — Community vs Enterprise

cassachange ships as two separate Python packages. Enterprise installs on top of community and patches itself in at startup via Python entry points — no config changes required when upgrading.

| Feature | Community | Enterprise |
|---|:---:|:---:|
| deploy / rollback / validate / status / repair / baseline | ✔ | ✔ |
| Versioned, repeatable, always, undo scripts | ✔ | ✔ |
| Distributed locking (Cassandra LWT) | ✔ | ✔ |
| CQL syntax linter (offline) | ✔ | ✔ |
| Dry run + JSON plan output | ✔ | ✔ |
| Per-environment config profiles | ✔ | ✔ |
| Release tagging + tag-based rollback | ✔ | ✔ |
| Slack / Teams / webhook notifications | ✔ | ✔ |
| Multi-keyspace deploy | ✔ | ✔ |
| Baseline schema introspection | ✔ | ✔ |
| Per-statement timeout | ✔ | ✔ |
| GitHub Actions workflow included | ✔ | ✔ |
| **Secret manager integration** (Vault / AWS SSM / AWS Secrets Manager / Azure Key Vault) | ✘ | ✔ |
| **File secrets** (SSL certs, AstraDB SCB stored in vault as base64) | ✘ | ✔ |
| **Schema analyser** (`cassachange analyse`) — 23 rules, 5 categories | ✘ | ✔ |
| **Immutable audit log** + `cassachange audit` command | ✘ | ✔ |
| Priority support (additional) | ✘ | ✔ |

If a `secrets` or `secrets_provider` block is present in `cassachange.yml` and the enterprise package is **not** installed, cassachange exits immediately before connecting to Cassandra:

```
ERROR

  Secret manager integration requires cassachange-enterprise.

  Community edition:  pip install cassachange
  Enterprise edition: pip install cassachange-enterprise

  Contact: enterprise@cassachange.com
```

---

## 2. Requirements

- Python 3.8+
- Apache Cassandra 3.x / 4.x /5.x, or DataStax AstraDB or ScyllaDB
- `cassandra-driver >= 3.25`
- `pyyaml >= 6.0`

Secret manager dependencies are optional — install only what you use:

| Provider | Install extra | Packages |
|---|---|---|
| HashiCorp Vault | `cassachange-enterprise[vault]` | `hvac >= 1.0` |
| AWS SSM / Secrets Manager | `cassachange-enterprise[aws]` | `boto3 >= 1.26` |
| Azure Key Vault | `cassachange-enterprise[azure]` | `azure-keyvault-secrets >= 4.7`, `azure-identity >= 1.15` |
| All providers | `cassachange-enterprise[all]` | all of the above |

---

## 3. Installation

### Community (free)

```bash
pip install cassachange
```

### Enterprise

Enterprise must be installed alongside community. Install community first, then enterprise.

```bash
# Base
pip install cassachange
pip install cassachange-enterprise

# With Azure Key Vault
pip install cassachange-enterprise[azure]

# With HashiCorp Vault
pip install cassachange-enterprise[vault]

# With AWS SSM / Secrets Manager
pip install cassachange-enterprise[aws]

# All secret providers
pip install cassachange-enterprise[all]
```

### From wheel files

```bash
# Community
pip install cassachange-1.2.0-py3-none-any.whl

# Enterprise
pip install cassachange-1.2.0-py3-none-any.whl
pip install cassachange_enterprise-1.2.0-py3-none-any.whl

# Enterprise with azure
pip install cassachange_enterprise-1.2.0-py3-none-any.whl[azure]
```

### From source

```bash
cd cassachange/
pip install -e .

cd cassachange-enterprise/
pip install -e .[azure]
```

### Verify

```bash
cassachange --help
pip show cassachange
pip show cassachange-enterprise   # if installed
```

---

## 4. Quick Start

**Step 1 — Create `cassachange.yml` in your project root:**

```yaml
keyspace:         myapp
history_keyspace: myapp_migrations
root_folder:      ./migrations
```

`history_keyspace` is required. It stores the `change_history`, `deploy_lock`, and (enterprise) `audit_log` tables. It must exist before the first deploy — create it via Terraform or cqlsh. cassachange never creates keyspaces.

**Step 2 — Write your first migration:**

```bash
mkdir -p migrations
```

```sql
-- migrations/V1.0.0__create_users_table.cql
CREATE TABLE IF NOT EXISTS myapp.users (
    id         uuid PRIMARY KEY,
    email      text,
    name       text,
    created_at timestamp
);

CREATE INDEX IF NOT EXISTS ON myapp.users (email);
```

**Step 3 — Validate scripts offline:**

```bash
cassachange validate
```

No Cassandra connection needed. Catches naming errors, duplicate versions, CQL syntax problems.

**Step 4 — Deploy:**

```bash
cassachange deploy
```

**Step 5 — Check status:**

```bash
cassachange status
```

```
VERSION  KEYSPACE  SCRIPT                         STATUS   INSTALLED_ON
1.0.0    myapp     V1.0.0__create_users_table.cql SUCCESS  2024-03-15 09:12:00
```

---

## 5. cassachange.yml Reference

Full annotated configuration:

```yaml
# ─── Connection: Standard Cassandra ────────────────────────────────────────
hosts:
  - 10.0.0.1
  - 10.0.0.2
  - 10.0.0.3
port:     9042
username: cassandra
password: secret         # use env var CASSANDRA_PASSWORD in practice

# ─── Connection: AstraDB ───────────────────────────────────────────────────
# Use environment variables for AstraDB credentials — do not commit them.
# ASTRA_SECURE_CONNECT_BUNDLE=/path/to/secure-connect.zip
# ASTRA_TOKEN=AstraCS:xxxx...
#
# Or set in YAML (not recommended for prod):
# secure_connect_bundle: /path/to/secure-connect.zip
# astra_token:           AstraCS:xxxx...

# ─── Keyspaces ─────────────────────────────────────────────────────────────
keyspace:         myapp              # single target keyspace
# keyspaces:                        # or a list for multi-keyspace deploy
#   - myapp
#   - orders
#   - analytics

history_keyspace: myapp_migrations  # REQUIRED — no default
history_table:    change_history    # optional — default: change_history

# ─── Scripts ───────────────────────────────────────────────────────────────
root_folder: ./migrations           # default: ./migrations

# ─── Behaviour ─────────────────────────────────────────────────────────────
timeout: null    # per-CQL-statement timeout in seconds. null = driver default (~10s)
verbose: false

# ─── Notifications ─────────────────────────────────────────────────────────
notifications:
  on_events:
    - deploy_success
    - deploy_failed
    - script_failed
    - rollback_success
    - rollback_failed
  channels:
    - type: slack
      webhook_url_env: SLACK_WEBHOOK_URL    # env var name, not the URL
    - type: teams
      webhook_url_env: TEAMS_WEBHOOK_URL
    - type: webhook
      url: https://ops.example.com/hook     # generic HTTP POST (JSON body)

# ─── Profiles ──────────────────────────────────────────────────────────────
# Each profile deep-merges over the base config.
# Only keys you specify in the profile override the base.
profiles:
  dev:
    hosts:            [127.0.0.1]
    username:         cassandra
    password:         cassandra
    keyspace:         myapp_dev
    history_keyspace: myapp_migrations_dev

  staging:
    hosts:            [staging-cass.internal]
    keyspace:         myapp_staging
    history_keyspace: myapp_migrations_staging
    timeout:          60

  prod:
    hosts:            [cass1.prod, cass2.prod, cass3.prod]
    keyspace:         myapp_prod
    history_keyspace: myapp_migrations_prod
    timeout:          120
    notifications:
      on_events: [deploy_success, deploy_failed, script_failed]
      channels:
        - type: slack
          webhook_url_env: SLACK_WEBHOOK_URL

# ─── Enterprise: Secret Manager ────────────────────────────────────────────
# Requires cassachange-enterprise.
# Community edition exits with a clear error if this block is present.

# secrets_provider: azure-keyvault    # vault | ssm | asm | azure-keyvault
# secrets:
#   password:              akv://my-vault/cassandra-password
#   astra_token:           akv://my-vault/astra-token
#   secure_connect_bundle: akv://my-vault/astra-scb-b64   # file secret
#   ssl_cafile:            akv://my-vault/ca-cert-b64      # file secret
```

---

## 6. Script Types

The **filename is the config**. No XML. No YAML changelogs. Just well-named `.cql` files in whatever folder structure you choose.

### V__ — Versioned

Runs **once**, in strict **semver order**, globally across all subdirectories. Once applied it is permanently recorded in `change_history` and never re-runs.

```
V{version}__{description}.cql

V1.0.0__create_users_table.cql
V1.1.0__add_orders_table.cql
V2.0.0__refactor_payments_schema.cql
```

Version numbers support dots or underscores: `V1_2_0` and `V1.2.0` are equivalent.

### U__ — Undo

Paired rollback script for a versioned migration. Only executes on `cassachange rollback`. The version must exactly match its V__ counterpart.

```
U{version}__{description}.cql

U1.1.0__add_orders_table.cql    ← paired with V1.1.0__add_orders_table.cql
```

### R__ — Repeatable

Reruns on every deploy where its **MD5 checksum has changed** since last apply. Unchanged = skipped. Use for UDFs, materialized views, and lookup table reloads.

```
R__{description}.cql

R__users_by_username.cql
R__orders_by_status_view.cql
```

### A__ — Always

Executes on **every single deploy**, unconditionally. No checksum check, no history lookup. Use for GRANT statements and permission refreshes that must always be current regardless of whether schema has changed.

```
A__{description}.cql

A__refresh_permissions.cql
A__grant_service_account_roles.cql
```

### Dispatch table

| Script type | `deploy` | `rollback` |
|---|---|---|
| `V__` versioned | ✔ pending only | ✔ via paired `U__` |
| `U__` undo | — | ✔ |
| `R__` repeatable | ✔ if checksum changed | — |
| `A__` always | ✔ unconditionally | — |

---

## 7. Folder Structure

Scripts are discovered **recursively**. Version ordering is always global — folder names have no effect on execution order.

**By module:**

```
migrations/
  users/
    V1.0.0__create_users_table.cql
    V1.2.0__add_profile_fields.cql
    U1.2.0__add_profile_fields.cql
    R__users_by_username.cql
  orders/
    V1.1.0__add_orders_table.cql
    V1.3.0__add_order_status.cql
    U1.1.0__add_orders_table.cql
  shared/
    A__refresh_permissions.cql
```

**By release:**

```
migrations/
  release-1.0/
    V1.0.0__initial_schema.cql
  release-1.1/
    V1.1.0__add_orders.cql
    U1.1.0__add_orders.cql
  release-2.0/
    V2.0.0__new_payments_schema.cql
    U2.0.0__new_payments_schema.cql
```

In both layouts the global execution order is identical:

```
V1.0.0 → V1.1.0 → V1.2.0 → V1.3.0 → V2.0.0
```

Duplicate version numbers across subdirectories are caught by `cassachange validate` before any connection is made.

---

## 8. Deploy Protocol

Every `cassachange deploy` follows a deterministic 9-step sequence:

| Step | Action | Notes |
|---|---|---|
| 01 | Validate keyspaces | All target keyspaces + history keyspace must exist. Exits on any missing. |
| 02 | Acquire deploy lock | `INSERT IF NOT EXISTS` (LWT/Paxos). Atomic at cluster level. |
| 03 | Discover scripts | Recursive walk of `root_folder`. Classify by prefix. Sort V__ globally by semver. |
| 04 | Read history | Single query against `change_history` to build applied set + checksums. |
| 05 | Run V__ scripts | Apply pending versions in ascending semver order. Skip already-applied. |
| 06 | Run R__ scripts | Rerun repeatable scripts whose MD5 checksum has changed. Skip unchanged. |
| 07 | Run A__ scripts | Execute all always-scripts unconditionally. |
| 08 | Record history | Write `SUCCESS` or `FAILED` row per script with checksum, tag, run_id, elapsed ms. |
| 09 | Release lock | `DELETE IF run_id = ...` (LWT). Only this process can release its own lock. |

cassachange never creates keyspaces. Keyspace provisioning is an infrastructure concern — use Terraform, cqlsh, or your admin UI.

---

## 9. Commands

All commands accept the same connection flags. These can also be set via environment variables or `cassachange.yml`.

```
--config, -c            Path to cassachange.yml (default: ./cassachange.yml)
--profile               Named profile from cassachange.yml
--hosts                 Comma-separated Cassandra contact points
--port                  Port (default: 9042)
--username, -u          Cassandra username
--password, -p          Cassandra password
--astra-token           AstraDB application token (AstraCS:...)
--secure-connect-bundle Path to AstraDB SCB .zip file
--keyspace, -k          Target keyspace (overrides cassachange.yml)
--keyspaces             Comma-separated list of target keyspaces
--history-keyspace      Keyspace for cassachange internal tables
--history-table         Table name (default: change_history)
--root-folder           Migration scripts folder
--timeout               Per-CQL-statement timeout in seconds
--verbose, -v           Debug logging
```

### deploy

Apply all pending migrations. Acquires distributed lock, runs pending V__ scripts, changed R__ scripts, all A__ scripts, then releases lock.

```bash
# Basic deploy
cassachange deploy

# With profile and release tag
cassachange deploy --profile prod --tag release-2.1.0

# Single keyspace override
cassachange deploy --profile prod --keyspace myapp_prod

# Multiple keyspaces override
cassachange deploy --profile prod --keyspaces myapp_prod,orders_prod,analytics_prod

# Dry run — no lock, no DB writes, preview only
cassachange deploy --profile prod --dry-run

# Dry run with JSON output artifact
cassachange deploy --profile prod --tag release-2.1.0 --dry-run-output plan.json

# With explicit per-statement timeout
cassachange deploy --profile prod --timeout 120
```

### rollback

Roll back versioned migrations using paired U__ undo scripts. Writes `ROLLED_BACK` sentinel rows to `change_history` — rolled-back versions can be re-applied on the next deploy.

```bash
# Roll back the single latest applied version
cassachange rollback --profile prod

# Roll back everything above a specific version (exclusive)
cassachange rollback --profile prod --target-version 1.1.0

# Roll back every version that was deployed under a specific tag
cassachange rollback --profile prod --tag release-2.1.0

# Dry run rollback — shows what would be undone
cassachange rollback --profile prod --tag release-2.1.0 --dry-run
```

Rollback executes U__ scripts in **reverse semver order**. If `V2.0.0` and `V1.2.0` were deployed under `release-2.1.0`, rollback runs `U2.0.0` first, then `U1.2.0`.

### validate

Lint all scripts without connecting to Cassandra. Zero-cost — run on every PR.

```bash
cassachange validate

# Custom folder
cassachange validate --root-folder ./db/migrations
```

Catches: bad filenames, duplicate version numbers, orphaned U__ scripts (no matching V__), empty scripts, CQL syntax errors (see [CQL Linter](#17-cql-linter)).

```
ERRORS:
  ERR   CQL syntax error in V1.2.0__add_login.cql (line ~3):
        Unknown ALTER TABLE sub-command 'MDDADD' (did you mean 'ADD'?).
        Valid: ['ADD', 'ALTER', 'DROP', 'RENAME', 'WITH']
        → ALTER TABLE users MDDADD last_login timestamp

Validated 8 script(s) | 0 warning(s) | 1 error(s)
Validation FAILED.
```

### status

Display migration history from `change_history`.

```bash
cassachange status --profile prod

# Filter to a specific keyspace
cassachange status --profile prod --keyspace myapp_prod

# Filter to a specific release tag
cassachange status --profile prod --tag release-2.1.0
```

Output columns: `VERSION  KEYSPACE  TAG  SCRIPT  STATUS  INSTALLED_BY  INSTALLED_ON  EXEC_MS`

Status values: `SUCCESS`, `FAILED`, `ROLLED_BACK`, `REPAIRED`

### repair

Recover from a failed deploy without touching your data. Operates only on `change_history` and `deploy_lock`.

```bash
# Inspect current state — no changes made
cassachange repair --profile prod --list

# Mark all FAILED scripts in a keyspace for retry
cassachange repair --profile prod --keyspace myapp_prod

# Mark a specific script for retry
cassachange repair --profile prod --script V1.2.0__add_index.cql

# Force-release a stuck deploy lock
# Only use this after confirming no deploy is actually running
cassachange repair --profile prod --release-lock
```

After repair, run `cassachange deploy` to retry the marked scripts. The original `FAILED` row is never deleted — a `REPAIRED` sentinel row is inserted alongside it preserving the full audit chain.

### baseline

Introspect a live keyspace and generate a starter migration file. Captures all tables, UDTs, indexes, UDFs, and UDAs using `IF NOT EXISTS` — safe to re-run on a keyspace that already has those objects.

```bash
# Generate with default version (0.0.0)
cassachange baseline --profile prod --keyspace myapp

# Custom version and output directory
cassachange baseline \
  --profile prod \
  --keyspace myapp \
  --baseline-version 1.0.0 \
  --output ./migrations/baseline

# Generates: V1.0.0__baseline_myapp.cql
```

### analyse

Inspect a live keyspace schema for anti-patterns, hotspot risks, and index problems. Runs 23 rules across 5 categories and produces a scored report per table and overall keyspace. Reads from `system_schema` only — no writes, no side effects.

Reads `keyspace` / `keyspaces` from your profile the same way `deploy` does, including the multi-keyspace list form. Works across all five supported platforms.

```bash
# Analyse all keyspaces defined in your profile
cassachange analyse --profile dev

# Single keyspace override
cassachange analyse --profile prod --keyspace myapp

# With remediation advice per finding
cassachange analyse --profile prod --verbose

# JSON output — pipe into CI gates or custom dashboards
cassachange analyse --profile prod --json

# Suppress specific rules
cassachange analyse --profile prod --skip PK004,IR004
```

**Rule categories:**

| Category | Rules | What it catches |
|---|---|---|
| Partition Key Quality | PK001–PK004 | Low-cardinality keys, timestamp hotspots, missing TTL on time-series |
| Query Safety | QS001–QS004 | ALLOW FILTERING traps, clustering column design |
| Table Health | TH001–TH005 | Missing TTL, unbounded collections, counter column misuse |
| Schema Anti-patterns | SA002–SA006 | Integer IDs, reserved keywords, COMPACT STORAGE |
| Index Recommendations | IR001–IR005 | Write amplification, cardinality mismatches, SAI compatibility |

**Scoring:** Each table scores 0–100. Penalties: CRITICAL −30 · WARNING −10 · INFO −2.

**Exit codes:** `0` = no issues · `1` = warnings · `2` = criticals. Use in CI to gate deploys:

```bash
cassachange analyse --profile prod --json || exit 1
```

---
````

### audit *(Enterprise only)*

View the immutable audit log. Every operation — deploy start/end, script run, failure, rollback, lock acquire/release, repair — is written as an append-only row. Requires `cassachange-enterprise`.

```bash
# Latest 50 events
cassachange audit --profile prod

# Latest 200 events
cassachange audit --profile prod --limit 200

# Single run
cassachange audit --profile prod --run-id a4f3b1c2-...

# Filter by keyspace
cassachange audit --profile prod --keyspace myapp_prod
```

Output columns: `EVENT_TIME  RUN_ID  EVENT_TYPE  OPERATOR  KEYSPACE  DETAIL`

Event types: `DEPLOY_START`, `DEPLOY_END`, `DEPLOY_FAILED`, `ROLLBACK_START`, `ROLLBACK_END`, `ROLLBACK_FAILED`, `SCRIPT_RUN`, `SCRIPT_SKIP`, `SCRIPT_FAILED`, `REPAIR`, `LOCK_ACQUIRE`, `LOCK_RELEASE`, `LOCK_FORCE_RELEASE`

---

## 10. Config Profiles

Profiles let one `cassachange.yml` serve all environments. Each profile **deep-merges** over the base config — only the keys you specify in the profile override the base.

```yaml
# cassachange.yml

# Base config — applies to all profiles unless overridden
history_table: change_history
root_folder:   ./migrations
timeout:       null

profiles:
  dev:
    hosts:            [127.0.0.1]
    port:             9042
    username:         cassandra
    password:         cassandra
    keyspace:         myapp_dev
    history_keyspace: myapp_migrations_dev

  staging:
    hosts:            [staging-node-1.internal, staging-node-2.internal]
    username:         app_staging
    keyspace:         myapp_staging
    history_keyspace: myapp_migrations_staging
    timeout:          60
    notifications:
      on_events: [deploy_failed, script_failed]
      channels:
        - type: slack
          webhook_url_env: SLACK_WEBHOOK_URL

  prod:
    hosts:            [cass1.prod, cass2.prod, cass3.prod]
    username:         app_prod
    keyspaces:
      - myapp_prod
      - orders_prod
      - analytics_prod
    history_keyspace: myapp_migrations_prod
    timeout:          120
    notifications:
      on_events: [deploy_success, deploy_failed, script_failed]
      channels:
        - type: slack
          webhook_url_env: SLACK_WEBHOOK_URL
        - type: teams
          webhook_url_env: TEAMS_WEBHOOK_URL
```

**Selecting a profile:**

```bash
# CLI flag
cassachange deploy --profile prod

# Environment variable (preferred for CI)
export CASSACHANGE_PROFILE=prod
cassachange deploy

# CLI flag takes precedence over env var
cassachange deploy --profile staging
```

**Config priority (highest → lowest):**

```
CLI flags → Environment variables → Profile (profiles.{name}.*) → YAML base → Defaults
```

---

## 11. Connection Modes

Connection mode is auto-detected from config. No mode flag, no manual switching.

### Standard Cassandra

```yaml
# cassachange.yml
hosts:
  - 10.0.0.1
  - 10.0.0.2
port:             9042
username:         cassandra
password:         secret
keyspace:         myapp
history_keyspace: myapp_migrations

# Optional SSL
# ssl:          true
# ssl_cafile:   /path/to/ca.crt
# ssl_certfile: /path/to/client.crt
# ssl_keyfile:  /path/to/client.key
```

### AstraDB

```yaml
# cassachange.yml — non-secret config only
keyspace:         myapp
history_keyspace: myapp_migrations
root_folder:      ./migrations
```

```bash
# Credentials via env vars — never commit to cassachange.yml
export ASTRA_SECURE_CONNECT_BUNDLE=/path/to/secure-connect-mydb.zip
export ASTRA_TOKEN=AstraCS:xxxxxxxxxxxxxxxx...

cassachange deploy
```

````markdown
AstraDB mode activates when both `secure_connect_bundle` and `astra_token` are set (from any source). Protocol v4 is pinned automatically — no deprecation warnings.

### ScyllaDB

cassandra-driver connects natively. Config is identical to Standard Cassandra — no extra driver, no plugin.

```yaml
hosts:
  - scylla-node-1.internal
  - scylla-node-2.internal
port:             9042
username:         app_user
password:         secret
keyspace:         myapp
history_keyspace: myapp_migrations
```

> **LWT note:** ScyllaDB 5.2+ provides production-grade LWT. Pre-5.2 clusters have inconsistent Paxos support — deploy lock is best-effort. Supplement with CI process controls on older clusters.

### Azure Managed Cassandra

Real Apache Cassandra nodes managed by Microsoft. Uses mTLS certificate auth. Pairs naturally with Azure Key Vault (enterprise) — certs are stored as base64 file secrets, decoded to temp files at runtime and deleted on process exit.

```yaml
profiles:
  prod:
    hosts:            [your-cluster.cassandra.cosmos.azure.com]
    port:             9042
    username:         your-username
    ssl:              true
    keyspace:         myapp_prod
    history_keyspace: myapp_migrations_prod
    # enterprise — pull all credentials and certs from Azure Key Vault
    secrets_provider: azure-keyvault
    secrets:
      password:     akv://my-vault/cassandra-password
      ssl_cafile:   akv://my-vault/cassandra-ca-cert-b64
      ssl_certfile: akv://my-vault/cassandra-client-cert-b64
      ssl_keyfile:  akv://my-vault/cassandra-client-key-b64
```

### Amazon Keyspaces

Serverless CQL-compatible service — not Apache Cassandra. The deploy lock is best-effort (no true Paxos). Avoid `DROP TABLE`, `ALTER TABLE DROP COLUMN`, `TRUNCATE`, UDTs, UDFs, and materialized views in migration scripts.

```yaml
profiles:
  prod:
    hosts:            [cassandra.us-east-1.amazonaws.com]
    port:             9142
    ssl:              true
    ssl_cafile:       /path/to/sf-class2-root.crt
    keyspace:         myapp
    history_keyspace: myapp_migrations
    timeout:          30
    # enterprise — pull credentials from AWS Secrets Manager
    secrets_provider: asm
    secrets:
      password: asm://myapp-keyspaces-credentials#password
```

**Supported DDL on Keyspaces:**

| Operation | Supported |
|---|---|
| `CREATE TABLE IF NOT EXISTS` | ✔ |
| `ALTER TABLE ADD column` | ✔ |
| `CREATE INDEX` | ✔ (on supported column types) |
| `DROP TABLE` | ✗ |
| `ALTER TABLE DROP COLUMN` | ✗ |
| `TRUNCATE` | ✗ |
| Materialized views / UDTs / UDFs / UDAs | ✗ |

---

## 12. Environment Variables
````

| Variable | Config key | Notes |
|---|---|---|
| `CASSANDRA_HOSTS` | `hosts` | Comma-separated |
| `CASSANDRA_PORT` | `port` | Default: `9042` |
| `CASSANDRA_KEYSPACE` | `keyspace` | Single keyspace |
| `CASSANDRA_USERNAME` | `username` | |
| `CASSANDRA_PASSWORD` | `password` | |
| `ASTRA_TOKEN` | `astra_token` | `AstraCS:...` |
| `ASTRA_SECURE_CONNECT_BUNDLE` | `secure_connect_bundle` | Path to SCB `.zip` |
| `CASSACHANGE_PROFILE` | *(profile selector)* | e.g. `prod` |
| `CASSACHANGE_HISTORY_KEYSPACE` | `history_keyspace` | **Required — no default** |
| `CASSACHANGE_HISTORY_TABLE` | `history_table` | Default: `change_history` |
| `CASSACHANGE_ROOT_FOLDER` | `root_folder` | Default: `./migrations` |
| `CASSACHANGE_TIMEOUT` | `timeout` | Seconds, integer |
| `CASSACHANGE_ENV` | `environment` | Label in notification payloads |

---

## 13. Distributed Locking

cassachange uses Cassandra **Lightweight Transactions** (Paxos) to guarantee that only one deploy runs at a time — no external coordination service needed.

```
acquire → INSERT INTO deploy_lock (lock_key, locked_by, locked_at, run_id)
          VALUES ('global', 'host:a4f3b1', now(), 'uuid')
          IF NOT EXISTS                         ← atomic at cluster level

release → DELETE FROM deploy_lock
          WHERE lock_key = 'global'
          IF run_id = 'uuid'                    ← only this run releases its own lock

TTL     → lock row has TTL 1800s               ← crashed deploy never permanently blocks
```

If the lock is already held when a deploy starts, cassachange exits immediately:

```
ERROR  Deploy lock already held.
       locked_by=ci-runner:b9c2d4  locked_at=2024-03-15 14:30:01  run_id=b9c2d4...
       Wait for the current deploy to finish, or use:
         cassachange repair --release-lock
```

If a process crashes and leaves the lock behind:

```bash
# Inspect lock state first
cassachange repair --profile prod --list

# Release only after confirming no deploy is actually running
cassachange repair --profile prod --release-lock
```

---

## 14. Release Tagging

Tags stamp every script that runs in a deploy with a label stored in `change_history`. Use them to filter history and to roll back an entire release atomically.

```bash
# Tag a deploy with a semantic version
cassachange deploy --profile prod --tag release-2.1.0

# In CI — use the git tag name automatically
cassachange deploy --profile prod --tag ${{ github.ref_name }}

# See exactly what release-2.1.0 changed
cassachange status --profile prod --tag release-2.1.0

# Roll back the entire release
cassachange rollback --profile prod --tag release-2.1.0
```

History after two tagged deploys:

```
VERSION  SCRIPT                          STATUS   TAG             INSTALLED_ON
1.0.0    V1.0.0__create_users.cql       SUCCESS  release-1.0.0   2024-01-10 09:12:00
1.1.0    V1.1.0__add_orders.cql         SUCCESS  release-1.0.0   2024-01-10 09:12:01
1.2.0    V1.2.0__add_payments.cql       SUCCESS  release-2.1.0   2024-03-15 14:33:22
2.0.0    V2.0.0__new_schema.cql         SUCCESS  release-2.1.0   2024-03-15 14:33:24
```

`cassachange rollback --tag release-2.1.0` undoes `V2.0.0` then `V1.2.0` in reverse order. The `release-1.0.0` scripts are untouched.

**CI convention — auto-tag from git:**

```bash
# Push a git tag and the pipeline picks it up automatically
git tag v2.1.0
git push origin v2.1.0

# In workflow:
cassachange deploy --profile prod --tag ${{ github.ref_name }}
# → cassachange deploy --profile prod --tag v2.1.0
```

---

## 15. Dry Run

Preview exactly what would run without writing anything to the database. No lock is acquired, no history rows are written.

```bash
# Print plan to stdout
cassachange deploy --profile prod --dry-run

# Write structured JSON plan (implies --dry-run)
cassachange deploy --profile prod --tag release-2.1.0 --dry-run-output plan.json
```

`plan.json` structure:

```json
{
  "profile":       "prod",
  "tag":           "release-2.1.0",
  "dry_run":       true,
  "total_actions": 3,
  "actions": [
    {
      "action":   "run",
      "script":   "V1.2.0__add_payments.cql",
      "version":  "1.2.0",
      "type":     "versioned",
      "checksum": "a1b2c3d4e5f6..."
    },
    {
      "action":  "skip",
      "script":  "V1.1.0__add_orders.cql",
      "reason":  "already applied"
    },
    {
      "action":   "run",
      "script":   "R__users_by_username.cql",
      "type":     "repeatable",
      "reason":   "checksum changed"
    }
  ]
}
```

In CI, upload `plan.json` as a GitHub Actions artifact before the real deploy. Reviewers can inspect exactly what will change before approving.

---

## 16. Notifications

Fire-and-forget HTTP notifications to Slack, Microsoft Teams, or any generic webhook. A notification failure logs a `WARNING` and **never blocks a deploy**.

```yaml
notifications:
  on_events:
    - deploy_success     # deploy finished with 0 errors
    - deploy_failed      # deploy finished with ≥ 1 error
    - rollback_success
    - rollback_failed
    - script_failed      # individual script error mid-deploy

  channels:
    # Slack — Block Kit payload
    - type: slack
      webhook_url_env: SLACK_WEBHOOK_URL   # env var name, not the URL

    # Microsoft Teams — Adaptive Card payload
    - type: teams
      webhook_url_env: TEAMS_WEBHOOK_URL

    # Generic HTTP webhook — POST, JSON body
    - type: webhook
      url: https://ops.example.com/migration-events
```

All payloads include: `keyspace`, `environment`, `status`, `tag`, `run_id`, `scripts_run`, `scripts_skipped`, `scripts_failed`, `elapsed_ms`.

---

## 17. CQL Linter

`cassachange validate` runs a built-in CQL linter on every script. No Cassandra connection needed. Run on every PR.

| Error class | Example |
|---|---|
| Misspelled top-level verb | `SELCT * FROM t` → did you mean `SELECT`? |
| Bad `ALTER TABLE` sub-command | `ALTER TABLE t MDDADD col text` → did you mean `ADD`? Valid: `ADD, ALTER, DROP, RENAME, WITH` |
| Bad `CREATE` / `DROP` object type | `CREATE TABEL t (...)` → did you mean `TABLE`? |
| Unbalanced parentheses | `INSERT INTO t (a VALUES (1)` |
| Missing semicolon on last statement | |
| Empty file | no executable statements |
| Duplicate version numbers | `V1.1.0` appears in two files |
| Orphaned undo script | `U1.2.0__...cql` with no matching `V1.2.0__...cql` |

Linter uses Levenshtein distance for suggestions — no external dependencies.

---

## 18. Baseline Introspection

Bring an existing unmanaged keyspace under version control without writing a migration by hand.

```bash
# Generate with default version 0.0.0
cassachange baseline --profile prod --keyspace myapp

# Custom version and output path
cassachange baseline \
  --profile prod \
  --keyspace myapp \
  --baseline-version 1.0.0 \
  --output ./migrations

# Generates: ./migrations/V1.0.0__baseline_myapp.cql
```

The generated file captures: `CREATE TABLE IF NOT EXISTS`, `CREATE TYPE IF NOT EXISTS`, `CREATE INDEX IF NOT EXISTS`, `CREATE FUNCTION IF NOT EXISTS`, `CREATE AGGREGATE IF NOT EXISTS`. All statements are idempotent — safe to run against a keyspace that already has those objects.

**Full onboarding workflow:**

```bash
# 1. Generate baseline from production
cassachange baseline --profile prod --keyspace myapp --baseline-version 1.0.0

# 2. Review the generated file
cat migrations/V1.0.0__baseline_myapp.cql

# 3. Deploy the baseline — stamps it as applied in history
cassachange deploy --profile prod

# 4. Verify status
cassachange status --profile prod
# → VERSION 1.0.0  STATUS SUCCESS

# 5. Start writing V1.1.0__, V1.2.0__ scripts normally
```

---

## 19. Repair

After a failed deploy some scripts are marked `FAILED` in `change_history` and the deploy lock may still be held. Repair fixes both without touching your actual data tables.

```bash
# Step 1 — see what failed and current lock state
cassachange repair --profile prod --list

# Output:
# FAILED scripts in myapp_prod:
#   V1.2.0__add_payments.cql   FAILED  2024-03-15 14:33:22  run_id=a4f3b1
#
# Deploy lock: HELD
#   locked_by=ci-runner:a4f3b1  locked_at=2024-03-15 14:33:21

# Step 2 — release lock (only if certain no deploy is running)
cassachange repair --profile prod --release-lock

# Step 3 — mark failed scripts for retry
cassachange repair --profile prod --keyspace myapp_prod

# Or mark a specific script
cassachange repair --profile prod --script V1.2.0__add_payments.cql

# Step 4 — re-run deploy
cassachange deploy --profile prod
```

The original `FAILED` row is never deleted. A `REPAIRED` sentinel row is inserted alongside it — the full history chain is preserved.

---

## 20. Multi-Keyspace Deploy

Deploy the same migration set across multiple keyspaces in one command. One distributed lock is acquired for the entire run. Each keyspace gets its own `change_history` rows.

```yaml
# cassachange.yml
profiles:
  prod:
    keyspaces:
      - myapp_prod
      - orders_prod
      - analytics_prod
    history_keyspace: myapp_migrations_prod
```

```bash
# Migrates all three keyspaces sequentially
cassachange deploy --profile prod

# Output:
# ✓ myapp_prod:     run 2 | skip 1 | errors 0
# ✓ orders_prod:    run 1 | skip 2 | errors 0
# ✓ analytics_prod: run 0 | skip 3 | errors 0
```

Override via CLI for a surgical single-keyspace run:

```bash
cassachange deploy --profile prod --keyspace orders_prod
```

---

## 21. Secret Manager Integration — Enterprise

Requires `cassachange-enterprise`. Credentials are resolved at runtime from a secret manager, injected into config before the connection opens, and never written to logs or process lists.

Supported providers:

| Provider | URI scheme | Extra |
|---|---|---|
| HashiCorp Vault | `vault://secret/path#key` | `[vault]` |
| AWS SSM Parameter Store | `ssm:///param/path` | `[aws]` |
| AWS Secrets Manager | `asm://secret-name#key` | `[aws]` |
| Azure Key Vault | `akv://vault-name/secret-name` | `[azure]` |

### HashiCorp Vault

```bash
pip install cassachange-enterprise[vault]
```

```yaml
secrets_provider: vault

secrets:
  password:    vault://secret/data/cassandra/prod#password
  astra_token: vault://secret/data/astra/prod#token
```

Auth via `VAULT_TOKEN`, or AppRole with `VAULT_ROLE_ID` + `VAULT_SECRET_ID`:

```bash
export VAULT_ADDR=https://vault.internal:8200
export VAULT_TOKEN=s.xxxxxxxxxxxx
cassachange deploy --profile prod
```

### AWS SSM Parameter Store

```bash
pip install cassachange-enterprise[aws]
```

```yaml
secrets_provider: ssm

secrets:
  password:    ssm:///myapp/prod/cassandra/password
  astra_token: ssm:///myapp/prod/astra/token
```

AWS credentials via the standard boto3 chain — env vars, `~/.aws/credentials`, or IAM role attached to the runner.

### AWS Secrets Manager

```bash
pip install cassachange-enterprise[aws]
```

```yaml
secrets_provider: asm

secrets:
  password:    asm://myapp-prod-cassandra#password
  astra_token: asm://myapp-prod-astra#token
```

### Azure Key Vault

```bash
pip install cassachange-enterprise[azure]
```

```yaml
secrets_provider: azure-keyvault

secrets:
  password:    akv://my-vault-name/cassandra-password
  astra_token: akv://my-vault-name/astra-token
```

Auth via `DefaultAzureCredential` — automatically uses whichever of these is configured: service principal env vars, managed identity, Azure CLI, workload identity.

**Service principal (CI):**

```bash
export AZURE_TENANT_ID=xxxxxxxx-xxxx-xxxx-xxxx-xxxxxxxxxxxx
export AZURE_CLIENT_ID=xxxxxxxx-xxxx-xxxx-xxxx-xxxxxxxxxxxx
export AZURE_CLIENT_SECRET=xxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxx
```

**Managed Identity (production VMs / AKS):** no env vars needed — `DefaultAzureCredential` picks it up automatically.

### Per-profile secrets

```yaml
profiles:
  staging:
    secrets_provider: vault
    secrets:
      password: vault://secret/data/cassandra/staging#password

  prod:
    secrets_provider: azure-keyvault
    secrets:
      password:    akv://prod-vault/cassandra-password
      astra_token: akv://prod-vault/astra-token
```

---

## 22. File Secrets — Enterprise

SSL certificates, client keys, and AstraDB Secure Connect Bundle zip files can be stored as **base64-encoded content** in a secret vault rather than as files on the CI runner.

cassachange resolves the vault URI, base64-decodes the content to a temp file (`chmod 600`), injects the temp file path into config, and deletes the temp file on exit — including on crash.

**Recognised file secret keys:** `secure_connect_bundle`, `ssl_cafile`, `ssl_certfile`, `ssl_keyfile`

**Store a file in Azure Key Vault:**

```bash
# Linux / macOS
base64 -w 0 secure-connect-mydb.zip > /tmp/scb.b64
az keyvault secret set \
  --vault-name my-vault \
  --name astra-scb-b64 \
  --file /tmp/scb.b64
rm /tmp/scb.b64
```

```powershell
# PowerShell — avoids Windows command-line length limit for large files
$bytes = [IO.File]::ReadAllBytes("secure-connect-mydb.zip")
$b64   = [Convert]::ToBase64String($bytes)
$tmp   = [IO.Path]::GetTempFileName()
[IO.File]::WriteAllText($tmp, $b64)
az keyvault secret set --vault-name my-vault --name astra-scb-b64 --file $tmp
Remove-Item $tmp
```

**cassachange.yml:**

```yaml
secrets_provider: azure-keyvault
secrets:
  # String secret — value injected directly into config
  astra_token:           akv://my-vault/astra-token

  # File secrets — base64 content decoded to temp file, path injected into config
  secure_connect_bundle: akv://my-vault/astra-scb-b64
  ssl_cafile:            akv://my-vault/cassandra-ca-cert-b64
  ssl_certfile:          akv://my-vault/cassandra-client-cert-b64
  ssl_keyfile:           akv://my-vault/cassandra-client-key-b64
```

---

## 23. Audit Log — Enterprise

Requires `cassachange-enterprise`. Every operation is written to `{history_keyspace}.audit_log` as an immutable append-only row — never updated, never deleted.

| Column | Type | Description |
|---|---|---|
| `event_time` | timestamp | UTC timestamp of the event |
| `run_id` | text | UUID shared across all events in one deploy/rollback run |
| `event_type` | text | `DEPLOY_START`, `SCRIPT_RUN`, `DEPLOY_END`, etc. |
| `operator` | text | `installed_by` value — hostname or CI identity |
| `hostname` | text | Hostname of the machine running cassachange |
| `keyspace_name` | text | Target keyspace |
| `script` | text | Script filename |
| `status` | text | `SUCCESS`, `FAILED`, etc. |
| `detail` | text | Additional context |

```bash
cassachange audit --profile prod

# Output:
# EVENT_TIME               RUN_ID    EVENT_TYPE    OPERATOR    KEYSPACE      DETAIL
# 2024-03-15 14:33:21.000  a4f3b1    DEPLOY_START  ci-runner   myapp_prod    tag=release-2.1.0
# 2024-03-15 14:33:22.042  a4f3b1    SCRIPT_RUN    ci-runner   myapp_prod    V1.2.0__... SUCCESS 42ms
# 2024-03-15 14:33:24.108  a4f3b1    SCRIPT_RUN    ci-runner   myapp_prod    V2.0.0__... SUCCESS 66ms
# 2024-03-15 14:33:24.210  a4f3b1    DEPLOY_END    ci-runner   myapp_prod    total_errors=0
```

The `audit_log` table is created in `history_keyspace` automatically by enterprise on first deploy.

---

## 24. Upgrading Community to Enterprise

No changes to `cassachange.yml` or your scripts are required. Enterprise uses Python entry points to patch itself into community at startup.

```bash
# Already installed:
pip install cassachange

# Add enterprise
pip install cassachange-enterprise[azure]   # or [vault], [aws], [all]
```

On next run cassachange detects enterprise and enables:

- Real secret resolution (instead of the community error stub)
- Real audit log writes to `audit_log` (instead of the community no-op)
- `cassachange audit` command

**Add a `secrets` block to your profile:**

```yaml
# cassachange.yml
profiles:
  prod:
    hosts:            [cass1.prod, cass2.prod]
    keyspace:         myapp_prod
    history_keyspace: myapp_migrations_prod
    secrets_provider: azure-keyvault
    secrets:
      password:    akv://prod-vault/cassandra-password
      astra_token: akv://prod-vault/astra-token
```

**Community users without a `secrets` block are unaffected.** Community and enterprise can coexist in the same team — developers running community just use profiles that contain no `secrets` block.

```yaml
profiles:
  # Safe for community — no secrets block
  dev:
    hosts:   [127.0.0.1]
    keyspace: myapp_dev
    history_keyspace: myapp_migrations_dev

  # Requires enterprise
  prod:
    secrets_provider: azure-keyvault
    secrets:
      password: akv://prod-vault/cassandra-password
```

```bash
# Dev machine — community, works fine
cassachange deploy --profile dev

# CI — enterprise required, fails cleanly without it
cassachange deploy --profile prod
```

---

## 25. Schema Analyser

The `analyse` command connects to a live cluster, reads `system_schema`, and scores every table in a keyspace against 23 rules across 5 categories.

### Rule reference

| Rule | Severity | Category | What it catches |
|---|---|---|---|
| PK001 | CRITICAL | Partition Key | Boolean / tinyint partition key — near-zero cardinality |
| PK002 | WARNING | Partition Key | Timestamp-only partition key — time-series write hotspot |
| PK003 | WARNING | Partition Key | Date as first composite component — daily write hotspot |
| PK004 | INFO | Partition Key | Time-series table without TTL |
| QS001 | CRITICAL | Query Safety | No clustering columns + many regular columns — ALLOW FILTERING trap |
| QS002 | WARNING | Query Safety | 4+ clustering columns — complex slice query risk |
| QS003 | WARNING | Query Safety | Text / blob clustering column — unbounded sort range |
| QS004 | INFO | Query Safety | No clustering columns — range queries not possible |
| TH001 | CRITICAL | Table Health | Session / cache / token table without TTL |
| TH002 | WARNING | Table Health | Append-oriented table without TTL or deletion strategy |
| TH003 | WARNING | Table Health | 50+ columns — relational model transposed to Cassandra |
| TH004 | WARNING | Table Health | Unbounded list / map collection — partition bloat |
| TH005 | CRITICAL | Table Health | Counter columns mixed with regular columns — writes fail |
| SA002 | WARNING | Schema | Integer id as partition key — sequential write hotspot |
| SA003 | WARNING | Schema | No temporal anchor and no TTL — data lifecycle unclear |
| SA004 | WARNING | Schema | UUID partition with no clustering — isolated row pattern |
| SA005 | INFO | Schema | Column names are CQL reserved keywords |
| SA006 | INFO | Schema | COMPACT STORAGE — deprecated in Cassandra 4.x |
| IR001 | WARNING | Index | 3+ secondary indexes — write amplification per index |
| IR002 | WARNING | Index | Secondary index on UUID / timeuuid — index size ≈ base table |
| IR003 | WARNING | Index | Secondary index on boolean — near-full cluster scan on read |
| IR004 | INFO | Index | 8+ non-key columns with no indexes |
| IR005 | INFO | Index | SAI index — not available on ScyllaDB or Amazon Keyspaces |

### Skipping rules

```bash
cassachange analyse --profile prod --skip PK004,SA003
```

### CI integration

```yaml
- name: Analyse schema
  run: cassachange analyse --profile ${{ inputs.environment }} --json
  # exit 2 on criticals, exit 1 on warnings — set your own threshold
```

---

## 26. GitHub Actions CI/CD

A production-ready workflow ships in the package at `.github/workflows/migrate.yml`. Four jobs. Rollback is manual-only by design — it cannot be triggered by a push event.

```yaml
on:
  push:
    branches: [main]
    tags:     ['v*']          # triggers on v1.0, v2.0, etc.
  workflow_dispatch:
    inputs:
      profile:
        description: Profile (staging | prod)
        default:     prod
      tag:
        description: Release tag
        required:    true
      rollback:
        description: Roll back instead of deploy
        type:        boolean
        default:     false

jobs:

  # Job 01 — offline validation, no DB connection
  validate:
    runs-on: ubuntu-latest
    steps:
      - uses: actions/checkout@v4
      - uses: actions/setup-python@v5
        with: { python-version: '3.11' }
      - run: pip install cassachange
      - run: cassachange validate

  # Job 02 — dry run, uploads plan.json as artifact
  dry-run:
    runs-on: ubuntu-latest
    needs:   validate
    steps:
      - uses: actions/checkout@v4
      - uses: actions/setup-python@v5
        with: { python-version: '3.11' }
      - run: pip install cassachange-enterprise[azure]
      - run: |
          cassachange deploy \
            --profile ${{ github.event.inputs.profile || 'prod' }} \
            --tag     ${{ github.event.inputs.tag     || github.ref_name }} \
            --dry-run-output plan.json
        env:
          AZURE_TENANT_ID:     ${{ secrets.AZURE_TENANT_ID }}
          AZURE_CLIENT_ID:     ${{ secrets.AZURE_CLIENT_ID }}
          AZURE_CLIENT_SECRET: ${{ secrets.AZURE_CLIENT_SECRET }}
      - uses: actions/upload-artifact@v4
        with: { name: migration-plan, path: plan.json }

  # Job 03 — real deploy (push to main or git tag)
  deploy:
    runs-on:     ubuntu-latest
    needs:       dry-run
    environment: production
    if: ${{ !github.event.inputs.rollback }}
    steps:
      - uses: actions/checkout@v4
      - uses: actions/setup-python@v5
        with: { python-version: '3.11' }
      - run: pip install cassachange-enterprise[azure]
      - run: |
          cassachange deploy \
            --profile ${{ github.event.inputs.profile || 'prod' }} \
            --tag     ${{ github.event.inputs.tag     || github.ref_name }}
        env:
          AZURE_TENANT_ID:     ${{ secrets.AZURE_TENANT_ID }}
          AZURE_CLIENT_ID:     ${{ secrets.AZURE_CLIENT_ID }}
          AZURE_CLIENT_SECRET: ${{ secrets.AZURE_CLIENT_SECRET }}
          SLACK_WEBHOOK_URL:   ${{ secrets.SLACK_WEBHOOK_URL }}

  # Job 04 — rollback (manual workflow_dispatch only)
  rollback:
    runs-on:     ubuntu-latest
    environment: production
    if: ${{ github.event_name == 'workflow_dispatch' && github.event.inputs.rollback == 'true' }}
    steps:
      - uses: actions/checkout@v4
      - uses: actions/setup-python@v5
        with: { python-version: '3.11' }
      - run: pip install cassachange-enterprise[azure]
      - run: |
          cassachange rollback \
            --profile ${{ github.event.inputs.profile }} \
            --tag     ${{ github.event.inputs.tag }}
        env:
          AZURE_TENANT_ID:     ${{ secrets.AZURE_TENANT_ID }}
          AZURE_CLIENT_ID:     ${{ secrets.AZURE_CLIENT_ID }}
          AZURE_CLIENT_SECRET: ${{ secrets.AZURE_CLIENT_SECRET }}
          SLACK_WEBHOOK_URL:   ${{ secrets.SLACK_WEBHOOK_URL }}
```

**Repository secrets — standard Cassandra:**

```
CASSANDRA_HOSTS
CASSANDRA_USERNAME
CASSANDRA_PASSWORD
CASSACHANGE_HISTORY_KEYSPACE
SLACK_WEBHOOK_URL
```

**Repository secrets — AstraDB (community):**

```
ASTRA_TOKEN
ASTRA_SECURE_CONNECT_BUNDLE    # base64: base64 -w 0 secure-connect.zip
CASSACHANGE_HISTORY_KEYSPACE
SLACK_WEBHOOK_URL
```

**Repository secrets — enterprise with Azure Key Vault:**

```
AZURE_TENANT_ID
AZURE_CLIENT_ID
AZURE_CLIENT_SECRET
CASSACHANGE_HISTORY_KEYSPACE
SLACK_WEBHOOK_URL
```

With enterprise + Azure Key Vault, all Cassandra credentials live in the vault — the only GitHub secrets needed are the Azure service principal credentials to access the vault.

---

## 27. Keyspace Management

cassachange **never creates keyspaces**. Keyspace creation requires elevated Cassandra permissions (`CREATE` on `ALL KEYSPACES`) that the migration user should not hold.

**Recommended: Terraform**

```hcl
# terraform/cassandra.tf
resource "astra_keyspace" "app" {
  database_id = var.astra_database_id
  name        = "myapp_prod"
}

resource "astra_keyspace" "migrations" {
  database_id = var.astra_database_id
  name        = "myapp_migrations_prod"
}
```

**Alternative: cqlsh / admin UI**

```sql
CREATE KEYSPACE IF NOT EXISTS myapp_prod
  WITH replication = {'class': 'NetworkTopologyStrategy', 'datacenter1': 3};

CREATE KEYSPACE IF NOT EXISTS myapp_migrations_prod
  WITH replication = {'class': 'NetworkTopologyStrategy', 'datacenter1': 3};
```

If any keyspace listed in `cassachange.yml` does not exist at deploy time, cassachange exits with a clear error before acquiring the lock or running any script:

```
ERROR  Keyspace 'myapp_prod' does not exist.
       Create it via your admin UI or cqlsh before running cassachange:
         CREATE KEYSPACE IF NOT EXISTS myapp_prod
           WITH replication = {'class': 'NetworkTopologyStrategy', 'datacenter1': 3};
```

**The boundary:**

| Concern | Tool |
|---|---|
| Keyspace creation and replication config | Terraform (or admin cqlsh) |
| Table / index / type / UDF / view evolution | cassachange `V__` scripts |
| AstraDB collection management | App bootstrap script (`create_collection`) |

---

## 28. History Tables Reference

cassachange creates these tables in `history_keyspace` on first deploy.

### change_history

```sql
CREATE TABLE IF NOT EXISTS {history_keyspace}.change_history (
    installed_on    timestamp,
    script          text,
    script_type     text,        -- versioned | repeatable | always | undo
    version         text,
    description     text,
    checksum        text,        -- MD5 of script content
    execution_time  int,         -- milliseconds
    status          text,        -- SUCCESS | FAILED | ROLLED_BACK | REPAIRED
    installed_by    text,        -- hostname running cassachange
    keyspace_name   text,        -- target keyspace
    tag             text,        -- release tag if supplied
    run_id          text,        -- UUID shared across a deploy run
    PRIMARY KEY (script, installed_on)
) WITH CLUSTERING ORDER BY (installed_on DESC)
```

### deploy_lock

```sql
CREATE TABLE IF NOT EXISTS {history_keyspace}.deploy_lock (
    lock_key   text PRIMARY KEY,
    locked_by  text,             -- "hostname:run_id_prefix"
    locked_at  timestamp,
    run_id     text
)
```

Lock rows have a TTL of 1800 seconds — a crashed deploy can never permanently block future ones.

### audit_log *(Enterprise)*

Created in `history_keyspace` automatically when `cassachange-enterprise` is installed.

```sql
CREATE TABLE IF NOT EXISTS {history_keyspace}.audit_log (
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
```

---

## 29. Comparison with Other Tools

General-purpose SQL migration tools are excellent for relational databases. Their Cassandra support is typically a community plugin bolted on after the fact. cassachange is purpose-built for Cassandra from the ground up.

| Feature | cassachange | SQL-first tool | Generic migrator |
|---|:---:|:---:|:---:|
| Native CQL execution | ✔ cassandra-driver | ⚠ community plugin | ⚠ 3rd-party ext |
| AstraDB SCB + token auth | ✔ built-in | ✗ | ✗ |
| ScyllaDB native support | ✔ | ✗ | ✗ |
| Azure Managed Cassandra | ✔ full + AKV cert fit | ✗ | ✗ |
| Amazon Keyspaces | ✔ CQL subset supported | ✗ | ✗ |
| Schema analyser (CQL-aware, 23 rules) | ✔ | ✗ SQL-only | ✗ |
| Protocol v4 auto-pin | ✔ | ✗ | ✗ |
| Rollback (free) | ✔ U__ scripts | ✗ free / ✔ paid | ⚠ DDL only |
| Rollback on Cassandra DDL | ✔ explicit CQL | ✗ no CQL gen | ✗ no CQL gen |
| Rollback by tag | ✔ | ✗ | ✗ |
| Distributed locking | ✔ Cassandra LWT | ✗ | ✗ |
| Always scripts (A__) | ✔ | ✗ | ✗ |
| Multi-keyspace deploy | ✔ | ✗ | ✗ |
| Offline script validation | ✔ | ✗ | ✗ |
| Dry run to JSON file | ✔ | ⚠ paid only | ⚠ paid only |
| Baseline from live keyspace | ✔ | ✗ (CQL) | ✗ |
| Repair command | ✔ | ✗ | ✗ |
| Config profiles (YAML) | ✔ | ⚠ env files | ✗ |
| Secret manager integration | ✔ enterprise | ✗ | ✗ |
| Slack / Teams notifications | ✔ | ✗ | ✗ |
| Immutable audit log | ✔ enterprise | ⚠ partial | ✗ |
| Never creates keyspaces | ✔ Terraform-safe | ✗ tries CREATE SCHEMA | ✗ tries CREATE SCHEMA |
| Runtime requirement | Python 3.8+ | JVM (Java 8+) | JVM / Node / Ruby |
| GitHub Actions included | ✔ | ⚠ manual | ⚠ manual |

**Use cassachange** if your database is Apache Cassandra, DataStax AstraDB, ScyllaDB, Azure Managed Cassandra, or Amazon Keyspaces.

Use a SQL-first tool or generic migrator if your primary database is relational and Cassandra is a secondary concern. Don't fight your tools.

---

## Contact

Enterprise licensing, workshops, and consulting: **enterprise@cassachange.com**


## License

`cassachange` is released under the [Apache 2.0 License](LICENSE.txt).
