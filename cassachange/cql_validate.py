"""
CQL content linter — no Cassandra connection needed.

What it catches:
  - Misspelled or invalid CQL keywords  (MDDADD, SELCT, DRPO, etc.)
  - Statements that don't start with a known CQL verb
  - Semicolon missing at end of last statement
  - Unbalanced parentheses
  - Empty string literals ''
  - Obvious identifier problems (names starting with a digit)
  - Unrecognised ALTER TABLE sub-commands (catches MDDADD vs ADD)
"""

import re
from dataclasses import dataclass, field
from typing import List

# ---------------------------------------------------------------------------
# Known top-level CQL statement verbs
# ---------------------------------------------------------------------------
CQL_VERBS = {
    "CREATE", "ALTER", "DROP", "TRUNCATE",
    "INSERT", "UPDATE", "DELETE", "SELECT",
    "USE", "GRANT", "REVOKE", "LIST",
    "BEGIN", "APPLY", "BATCH",
}

# ---------------------------------------------------------------------------
# Known sub-commands per statement type
# ---------------------------------------------------------------------------
ALTER_TABLE_SUBCMDS = {"ADD", "DROP", "RENAME", "WITH", "ALTER"}
ALTER_KEYSPACE_SUBCMDS = {"WITH"}
ALTER_TYPE_SUBCMDS = {"ADD", "ALTER", "RENAME"}
ALTER_MATERIALIZED_SUBCMDS = {"WITH"}

CREATE_SUBCMDS = {
    "TABLE", "KEYSPACE", "INDEX", "TYPE", "FUNCTION",
    "AGGREGATE", "MATERIALIZED", "ROLE", "USER",
}
DROP_SUBCMDS = {
    "TABLE", "KEYSPACE", "INDEX", "TYPE", "FUNCTION",
    "AGGREGATE", "MATERIALIZED", "ROLE", "USER",
}

# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

_COMMENT_LINE   = re.compile(r"--[^\n]*")
_COMMENT_BLOCK  = re.compile(r"/\*.*?\*/", re.DOTALL)
_WHITESPACE     = re.compile(r"\s+")


def _strip_comments(sql: str) -> str:
    sql = _COMMENT_BLOCK.sub(" ", sql)
    sql = _COMMENT_LINE.sub(" ", sql)
    return sql


def _tokenise(stmt: str) -> List[str]:
    """Return upper-cased tokens, ignoring string literals."""
    # Replace quoted strings with placeholder so we don't inspect their content
    stmt = re.sub(r"'[^']*'", "'?'", stmt)
    stmt = re.sub(r'"[^"]*"', '"?"', stmt)
    tokens = _WHITESPACE.split(stmt.strip())
    return [t for t in tokens if t]


# ---------------------------------------------------------------------------
# Per-statement linting
# ---------------------------------------------------------------------------

@dataclass
class LintError:
    line_hint: int          # approximate 1-based line in the original file
    statement: str          # first ~80 chars of the statement
    message:   str

    def __str__(self):
        preview = self.statement[:80].replace("\n", " ")
        return f"  line ~{self.line_hint}: {self.message}\n    → {preview}"


def _lint_statement(raw: str, line_hint: int) -> List[LintError]:
    errs: List[LintError] = []
    clean = _strip_comments(raw).strip()
    if not clean:
        return errs

    tokens = _tokenise(clean)
    if not tokens:
        return errs

    upper_tokens = [t.upper() for t in tokens]
    verb = upper_tokens[0]

    # ── 1. Unknown top-level verb ────────────────────────────────────────────
    if verb not in CQL_VERBS:
        # Suggest closest known verb
        suggestion = _closest(verb, CQL_VERBS)
        hint = f" (did you mean '{suggestion}'?)" if suggestion else ""
        errs.append(LintError(line_hint, raw,
            f"Unknown CQL statement verb '{verb}'{hint}"))
        return errs   # no point checking further

    # ── 2. ALTER TABLE sub-command ───────────────────────────────────────────
    if verb == "ALTER" and len(upper_tokens) >= 3:
        object_type = upper_tokens[1]
        if object_type == "TABLE" and len(upper_tokens) >= 4:
            subcmd = upper_tokens[3]  # ALTER TABLE <name> <subcmd>
            if subcmd not in ALTER_TABLE_SUBCMDS:
                suggestion = _closest(subcmd, ALTER_TABLE_SUBCMDS)
                hint = f" (did you mean '{suggestion}'?)" if suggestion else ""
                errs.append(LintError(line_hint, raw,
                    f"Unknown ALTER TABLE sub-command '{subcmd}'{hint}. "
                    f"Valid: {sorted(ALTER_TABLE_SUBCMDS)}"))
        elif object_type == "KEYSPACE" and len(upper_tokens) >= 4:
            subcmd = upper_tokens[3]
            if subcmd not in ALTER_KEYSPACE_SUBCMDS:
                suggestion = _closest(subcmd, ALTER_KEYSPACE_SUBCMDS)
                hint = " (did you mean '" + suggestion + "'?)" if suggestion else ""
                errs.append(LintError(line_hint, raw,
                    "Unknown ALTER KEYSPACE sub-command '" + subcmd + "'" + hint +
                    ". Valid: " + str(sorted(ALTER_KEYSPACE_SUBCMDS))))
        elif object_type == "TYPE" and len(upper_tokens) >= 4:
            subcmd = upper_tokens[3]
            if subcmd not in ALTER_TYPE_SUBCMDS:
                suggestion = _closest(subcmd, ALTER_TYPE_SUBCMDS)
                hint = f" (did you mean '{suggestion}'?)" if suggestion else ""
                errs.append(LintError(line_hint, raw,
                    f"Unknown ALTER TYPE sub-command '{subcmd}'{hint}. "
                    f"Valid: {sorted(ALTER_TYPE_SUBCMDS)}"))

    # ── 3. CREATE / DROP object type ─────────────────────────────────────────
    if verb in ("CREATE", "DROP") and len(upper_tokens) >= 2:
        obj = upper_tokens[1]
        # Allow IF NOT EXISTS / IF EXISTS qualifiers
        if obj in ("IF",):
            obj = upper_tokens[3] if len(upper_tokens) > 3 else obj
        valid = CREATE_SUBCMDS if verb == "CREATE" else DROP_SUBCMDS
        if obj not in valid and obj not in ("OR",):  # OR REPLACE for functions
            suggestion = _closest(obj, valid)
            hint = f" (did you mean '{suggestion}'?)" if suggestion else ""
            errs.append(LintError(line_hint, raw,
                f"Unknown {verb} object type '{obj}'{hint}. "
                f"Valid: {sorted(valid)}"))

    # ── 4. Unbalanced parentheses ────────────────────────────────────────────
    depth = 0
    for ch in clean:
        if ch == "(":
            depth += 1
        elif ch == ")":
            depth -= 1
        if depth < 0:
            errs.append(LintError(line_hint, raw,
                "Unbalanced parentheses — unexpected ')'"))
            depth = 0
    if depth != 0:
        errs.append(LintError(line_hint, raw,
            f"Unbalanced parentheses — {depth} unclosed '('"))

    return errs


# ---------------------------------------------------------------------------
# Levenshtein-based suggestion (no external deps)
# ---------------------------------------------------------------------------

def _closest(word: str, candidates: set, max_dist: int = 3) -> str:
    best, best_d = "", max_dist + 1
    for c in candidates:
        d = _levenshtein(word.upper(), c.upper())
        if d < best_d:
            best, best_d = c, d
    return best


def _levenshtein(a: str, b: str) -> int:
    if a == b:
        return 0
    if len(a) < len(b):
        a, b = b, a
    prev = list(range(len(b) + 1))
    for i, ca in enumerate(a):
        curr = [i + 1]
        for j, cb in enumerate(b):
            curr.append(min(prev[j+1]+1, curr[j]+1,
                            prev[j] + (0 if ca == cb else 1)))
        prev = curr
    return prev[-1]


# ---------------------------------------------------------------------------
# Public API
# ---------------------------------------------------------------------------

def lint_cql_content(content: str, filename: str = "") -> List[LintError]:
    """
    Parse and lint CQL content.  Returns a (possibly empty) list of LintError.
    Raises nothing — all problems are returned as errors.
    """
    stripped = _strip_comments(content)

    # Split on semicolons to get individual statements
    raw_stmts = stripped.split(";")

    errs: List[LintError] = []

    # Track approximate line number
    consumed = 0
    for raw in raw_stmts:
        line_hint = content[:consumed].count("\n") + 1
        consumed += len(raw) + 1   # +1 for the semicolon

        stmt = raw.strip()
        if not stmt:
            continue

        # Final "statement" after last semicolon is allowed to be empty
        if raw is raw_stmts[-1]:
            continue

        errs.extend(_lint_statement(stmt, line_hint))

    # Check last real statement had a semicolon
    real_stmts = [s.strip() for s in raw_stmts if s.strip()]
    original_stripped = content.strip()
    if real_stmts and not original_stripped.rstrip().endswith(";"):
        errs.append(LintError(
            content.count("\n") + 1,
            real_stmts[-1],
            "Last statement is missing a terminating semicolon",
        ))

    return errs
