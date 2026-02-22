"""
Script discovery, parsing, and classification.

Naming conventions:
  V{version}__{description}.cql   Versioned  (runs once, in order)
  U{version}__{description}.cql   Undo       (paired rollback for a versioned script)
  R__{description}.cql            Repeatable (reruns on checksum change)
  A__{description}.cql            Always     (runs every deploy)

Folder structure:
  Scripts are discovered recursively from root_folder. You can organise
  scripts into any subdirectory structure you like -- only the filename
  determines script type and version. Subdirectory names are ignored for
  ordering purposes.

  Example layouts:

    Flat (simple):
      migrations/
        V1.0.0__create_users.cql
        V1.1.0__add_orders.cql

    By module:
      migrations/
        users/
          V1.0.0__create_users_table.cql
          V1.2.0__add_profile_fields.cql
          R__users_by_username.cql
        orders/
          V1.1.0__add_orders_table.cql
          V1.3.0__add_order_status.cql
        undo/
          U1.1.0__add_orders_table.cql
          U1.2.0__add_profile_fields.cql

    By release:
      migrations/
        release-1.0/
          V1.0.0__initial_schema.cql
        release-1.1/
          V1.1.0__add_orders.cql
          U1.1.0__add_orders.cql

  Version ordering is always global across all subdirectories -- V1.1.0 runs
  before V1.2.0 regardless of which subfolder they live in.

  Duplicate versions across subdirectories are detected and reported as errors
  by cassachange validate.
"""

import os
import re
import hashlib
from dataclasses import dataclass, field
from enum import Enum
from typing import Dict, List, Optional, Tuple


VERSIONED_RE  = re.compile(r"^V(\d+(?:[._]\d+)*)__(.+)\.cql$",  re.IGNORECASE)
UNDO_RE       = re.compile(r"^U(\d+(?:[._]\d+)*)__(.+)\.cql$",  re.IGNORECASE)
REPEATABLE_RE = re.compile(r"^R__(.+)\.cql$",                    re.IGNORECASE)
ALWAYS_RE     = re.compile(r"^A__(.+)\.cql$",                    re.IGNORECASE)


class ScriptType(str, Enum):
    VERSIONED  = "versioned"
    UNDO       = "undo"
    REPEATABLE = "repeatable"
    ALWAYS     = "always"


@dataclass
class MigrationScript:
    filename:    str
    filepath:    str
    relative_path: str        # path relative to root_folder, for display/logging
    script_type: ScriptType
    version:     Optional[str]
    description: str
    content:     str
    checksum:    str = field(init=False)

    def __post_init__(self):
        self.checksum = _checksum(self.content)

    @property
    def sort_key(self):
        """
        Versioned scripts sort by numeric version tuple (global across all subdirs).
        Repeatable sorts alphabetically by relative path after all versioned scripts.
        Always sorts alphabetically by relative path after repeatable scripts.
        """
        if self.script_type == ScriptType.VERSIONED:
            return (0, _version_tuple(self.version), "")
        elif self.script_type == ScriptType.REPEATABLE:
            return (1, (), self.relative_path)
        else:  # ALWAYS
            return (2, (), self.relative_path)

    def __str__(self):
        return self.relative_path


def _checksum(content: str) -> str:
    return hashlib.md5(content.encode("utf-8")).hexdigest()


def _version_tuple(version_str: str):
    """Turn '1.2.0' or '1_2_0' into (1, 2, 0) for sorting."""
    return tuple(int(x) for x in re.split(r"[._]", version_str))


def _parse_script(filepath: str, root_folder: str) -> Optional[MigrationScript]:
    filename      = os.path.basename(filepath)
    relative_path = os.path.relpath(filepath, root_folder)

    m = VERSIONED_RE.match(filename)
    if m:
        version     = m.group(1).replace("_", ".")
        description = m.group(2).replace("_", " ")
        content     = _read(filepath)
        return MigrationScript(
            filename=filename, filepath=filepath, relative_path=relative_path,
            script_type=ScriptType.VERSIONED,
            version=version, description=description, content=content,
        )

    m = UNDO_RE.match(filename)
    if m:
        version     = m.group(1).replace("_", ".")
        description = m.group(2).replace("_", " ")
        content     = _read(filepath)
        return MigrationScript(
            filename=filename, filepath=filepath, relative_path=relative_path,
            script_type=ScriptType.UNDO,
            version=version, description=description, content=content,
        )

    m = REPEATABLE_RE.match(filename)
    if m:
        description = m.group(1).replace("_", " ")
        content     = _read(filepath)
        return MigrationScript(
            filename=filename, filepath=filepath, relative_path=relative_path,
            script_type=ScriptType.REPEATABLE,
            version=None, description=description, content=content,
        )

    m = ALWAYS_RE.match(filename)
    if m:
        description = m.group(1).replace("_", " ")
        content     = _read(filepath)
        return MigrationScript(
            filename=filename, filepath=filepath, relative_path=relative_path,
            script_type=ScriptType.ALWAYS,
            version=None, description=description, content=content,
        )

    return None  # Unrecognised filename -- silently ignored


def _read(filepath: str) -> str:
    with open(filepath, encoding="utf-8") as f:
        return f.read()


def _walk_cql_files(root_folder: str) -> List[str]:
    """
    Recursively collect all .cql files under root_folder, sorted by
    full path so discovery order is deterministic across platforms.
    """
    cql_files = []
    for dirpath, dirnames, filenames in os.walk(root_folder):
        # Sort dirnames in-place so os.walk descends in alphabetical order
        dirnames.sort()
        for filename in sorted(filenames):
            if filename.lower().endswith(".cql"):
                cql_files.append(os.path.join(dirpath, filename))
    return cql_files


def discover_scripts(root_folder: str) -> List[MigrationScript]:
    """
    Recursively walk root_folder and return all valid forward migration
    scripts (V, R, A) sorted in execution order.
    """
    scripts = []
    for filepath in _walk_cql_files(root_folder):
        script = _parse_script(filepath, root_folder)
        if script and script.script_type != ScriptType.UNDO:
            scripts.append(script)

    scripts.sort(key=lambda s: s.sort_key)
    return scripts


def discover_undo_scripts(root_folder: str) -> Dict[str, MigrationScript]:
    """
    Recursively walk root_folder and return {version: undo_script} for all
    U__ scripts found. Used by rollback to find the undo script for a version.
    """
    undos: Dict[str, MigrationScript] = {}
    for filepath in _walk_cql_files(root_folder):
        script = _parse_script(filepath, root_folder)
        if script and script.script_type == ScriptType.UNDO:
            undos[script.version] = script
    return undos


def discover_all_scripts(root_folder: str) -> Tuple[List[MigrationScript], Dict[str, MigrationScript]]:
    """
    Recursively walk root_folder and return (forward_scripts, undo_map)
    in a single pass.
    """
    forward = []
    undos: Dict[str, MigrationScript] = {}

    for filepath in _walk_cql_files(root_folder):
        script = _parse_script(filepath, root_folder)
        if not script:
            continue
        if script.script_type == ScriptType.UNDO:
            undos[script.version] = script
        else:
            forward.append(script)

    forward.sort(key=lambda s: s.sort_key)
    return forward, undos
