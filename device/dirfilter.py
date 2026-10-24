"""Directory excludes (`exclude_dirs` in the device config).

A watch root can hold large trees that never contain anything to upload (a MinIO or Postgres
data directory, one directory per object). The suffix filter rejects their files, but the walk
still visits every directory. Excluded directories are pruned before the walk enters them.

Each pattern is a shell-style glob, matched one path component at a time (`*` never crosses `/`):

* no `/`           -- a directory name at any depth: `minio`, `chiron_*`
* relative path    -- a path from the watch root: `chiron_data`, `chiron_backup/sessions/*/minio`
* absolute path    -- a path as the device sees it: `/logging/chiron_data`
"""
import os
from fnmatch import fnmatchcase
from typing import Iterable, List, Tuple

Pattern = Tuple[str, Tuple[str, ...]]   # (kind, components), kind in "name", "rel", "abs"


def compile_patterns(patterns: Iterable) -> List[Pattern]:
    compiled = []
    for p in patterns or []:
        p = str(p).strip()
        if not p:
            continue
        parts = tuple(c for c in p.strip("/").split("/") if c and c != ".")
        if not parts:
            continue
        if p.startswith("/"):
            compiled.append(("abs", parts))
        elif len(parts) == 1:
            compiled.append(("name", parts))
        else:
            compiled.append(("rel", parts))
    return compiled


def _match(parts: Tuple[str, ...], components: List[str]) -> bool:
    return len(parts) == len(components) and all(fnmatchcase(c, p) for c, p in zip(components, parts))


def is_excluded(compiled: List[Pattern], watch_root: str, dirpath: str) -> bool:
    """True if directory `dirpath` (inside `watch_root`) matches an exclude pattern."""
    if not compiled:
        return False
    rel = os.path.relpath(dirpath, watch_root)
    if rel == "." or rel.startswith(".."):
        return False
    rel_parts = rel.split(os.sep)
    abs_parts = os.path.normpath(os.path.join(watch_root, rel)).strip("/").split("/")
    for kind, parts in compiled:
        if kind == "name" and fnmatchcase(rel_parts[-1], parts[0]):
            return True
        if kind == "rel" and _match(parts, rel_parts):
            return True
        if kind == "abs" and _match(parts, abs_parts):
            return True
    return False


def prune(compiled: List[Pattern], watch_root: str, root: str, dirs: List[str]) -> None:
    """Drop excluded entries from `dirs` in place, so a top-down os.walk does not descend into them."""
    if compiled:
        dirs[:] = [d for d in dirs if not is_excluded(compiled, watch_root, os.path.join(root, d))]


def under_excluded(compiled: List[Pattern], watch_root: str, path: str) -> bool:
    """True if any directory between `watch_root` and `path` is excluded."""
    if not compiled:
        return False
    rel = os.path.relpath(path, watch_root)
    if rel == "." or rel.startswith(".."):
        return False
    parts = rel.split(os.sep)
    cur = watch_root
    for c in parts[:-1]:
        cur = os.path.join(cur, c)
        if is_excluded(compiled, watch_root, cur):
            return True
    return False
