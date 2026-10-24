"""Folder bundles (`bundle_dirs` in the device config): a folder whose name matches a pattern is
handled as one file, `<folder>.tar`.

The scan does not enter such a folder; everything inside it, MCAPs included, is plain content of
the tar. Example: tmux pane logs, written as dozens of small files per run into
`tmux/<YYYY_MM_DD_HH_MM_SS>.tmux/`, become one upload per run.

The tar is never written to disk. It is generated as a stream whose bytes depend only on the
folder's contents (sorted members, uid/gid 0, no user names, whole-second mtimes, GNU format for
long names), so the hash computed during the scan and the bytes sent during the upload agree, and
an unchanged folder always has the same content id. Robots are short of disk; a stored copy of
every bundle would double the space their logs take.

A folder has *changed* when its fingerprint changes: the (path, size, mtime_ns) of every member.
The fingerprint is kept in the folder's sidecars `<folder>.tar.metadata` / `<folder>.tar.md5`
(beside the folder); a different fingerprint means a new hash, hence a new upload. A folder is
only bundled once nothing in it has changed for `settle_s` seconds, so a run that is still writing
is not captured half way.

The uploaded name carries the version: `<folder>.<first 8 hex of the content hash>.tar`. Each
version of a folder is then its own file on the server; without it, a changed folder would land on
the path of the earlier tar, which the server reports as already present and refuses to overwrite.
"""
import hashlib
import json
import os
import re
import shutil
import tarfile
import time
from datetime import datetime, timezone

import pytz

from device.debug_print import debug_print

SUFFIX = ".tar"
BLOCK = tarfile.BLOCKSIZE          # 512
RECORD = tarfile.RECORDSIZE        # 10240
DEFAULT_SETTLE_S = 600


def compile_patterns(patterns):
    """(compiled patterns, [(pattern, error), ...]). Each pattern must match a whole folder name."""
    compiled, errors = [], []
    for p in patterns or []:
        p = str(p)
        if not p.strip():
            continue
        try:
            compiled.append(re.compile(p))
        except re.error as e:
            errors.append((p, str(e)))
    return compiled, errors


def matches(compiled, name: str) -> bool:
    return any(p.fullmatch(name) for p in compiled)


_VERSION = re.compile(r"(.+)\.[0-9a-f]{8}")


def versioned(tar_rel: str, content_hash: str) -> str:
    """`<folder>.tar` -> `<folder>.<hash8>.tar`, the name a bundle is uploaded under."""
    return tar_rel[:-len(SUFFIX)] + "." + content_hash[:8] + SUFFIX


def bundle_dir(fullpath: str):
    """The folder behind a `<folder>.tar` or `<folder>.<hash8>.tar` path, or None."""
    if not fullpath.endswith(SUFFIX):
        return None
    base = fullpath[:-len(SUFFIX)]
    for d in (base, (_VERSION.fullmatch(base) or [None, None])[1]):
        if d and os.path.isdir(d) and not os.path.islink(d):
            return d
    return None


def _sidecar(folder: str, kind: str) -> str:
    """The device's own cache files for a bundle, beside the folder: one per folder, not per version."""
    return folder.rstrip("/") + SUFFIX + kind


def members(folder: str):
    """[(TarInfo, absolute path, mtime_ns)] in a fixed order: the folder itself, then its contents
    walked in sorted order. Regular files and directories only; symlinks and special files are
    left out (and logged)."""
    folder = folder.rstrip("/")
    top = os.path.basename(folder)
    out = []

    def info(path, arcname, st, kind):
        ti = tarfile.TarInfo(arcname)
        ti.type = kind
        ti.mode = st.st_mode & 0o7777
        ti.mtime = int(st.st_mtime)
        ti.uid = ti.gid = 0
        ti.uname = ti.gname = ""
        ti.size = st.st_size if kind == tarfile.REGTYPE else 0
        return (ti, path, st.st_mtime_ns)

    out.append(info(folder, top, os.stat(folder), tarfile.DIRTYPE))
    for root, dirs, files in os.walk(folder):
        dirs.sort()
        rel_root = os.path.relpath(root, folder)
        for name in sorted(dirs) + sorted(files):
            path = os.path.join(root, name)
            arcname = os.path.normpath(os.path.join(top, rel_root, name))
            try:
                st = os.lstat(path)
            except OSError:
                continue
            if os.path.isdir(path) and not os.path.islink(path):
                out.append(info(path, arcname, st, tarfile.DIRTYPE))
            elif os.path.isfile(path) and not os.path.islink(path):
                out.append(info(path, arcname, st, tarfile.REGTYPE))
            else:
                debug_print(f"bundle {folder}: leaving out {path} (not a regular file or directory)")
        # os.walk descends into `dirs` in the order given; the sort above keeps it fixed.
    return out


def _header(ti) -> bytes:
    return ti.tobuf(tarfile.GNU_FORMAT, "utf-8", "surrogateescape")


def _padded(n: int) -> int:
    return (n + BLOCK - 1) // BLOCK * BLOCK


def tar_size(mem) -> int:
    """Exact length of iter_tar(mem), computed from the headers and sizes alone."""
    n = sum(len(_header(ti)) + _padded(ti.size) for ti, _, _ in mem) + 2 * BLOCK
    return (n + RECORD - 1) // RECORD * RECORD


def iter_tar(mem, chunk_size: int = 1024 * 1024):
    """The tar as a stream of byte chunks, identical to what tarfile writes for the same members.
    A member is read for exactly the size recorded in its header (zero-filled if it shrank)."""
    total = 0
    for ti, path, _ in mem:
        hdr = _header(ti)
        total += len(hdr)
        yield hdr
        if ti.type != tarfile.REGTYPE:
            continue
        left = ti.size
        with open(path, "rb") as fh:
            while left > 0:
                data = fh.read(min(chunk_size, left))
                if not data:
                    debug_print(f"bundle: {path} shrank while being read; zero-filled")
                    data = b"\0" * min(chunk_size, left)
                left -= len(data)
                total += len(data)
                yield data
        pad = _padded(ti.size) - ti.size
        if pad:
            total += pad
            yield b"\0" * pad
    end = 2 * BLOCK
    total += end
    tail = (RECORD - total % RECORD) % RECORD
    yield b"\0" * (end + tail)


class BundleReader:
    """A read-only, forward-only file object over iter_tar, for the upload worker."""

    def __init__(self, folder: str, chunk_size: int = 1024 * 1024):
        self._it = iter_tar(members(folder), chunk_size)
        self._buf = b""

    def read(self, n: int = -1) -> bytes:
        while n < 0 or len(self._buf) < n:
            try:
                self._buf += next(self._it)
            except StopIteration:
                break
        if n < 0:
            data, self._buf = self._buf, b""
        else:
            data, self._buf = self._buf[:n], self._buf[n:]
        return data

    def seek(self, offset: int, whence: int = 0):
        """Only a skip forward from the start is supported: resuming an interrupted upload."""
        assert whence == 0
        while offset > 0:
            skipped = self.read(min(offset, 8 * 1024 * 1024))
            if not skipped:
                break
            offset -= len(skipped)

    def close(self):
        self._it = iter(())

    def __enter__(self):
        return self

    def __exit__(self, *exc):
        self.close()


def fingerprint(mem) -> str:
    h = hashlib.sha1()
    for ti, _, mtime_ns in mem:
        h.update(json.dumps([ti.name, ti.type.decode(), ti.size, mtime_ns]).encode("utf-8", "surrogateescape"))
    return h.hexdigest()


def _local(ts: float, local_tz: str) -> str:
    return datetime.fromtimestamp(ts, tz=timezone.utc).astimezone(pytz.timezone(local_tz)).strftime("%Y-%m-%d %H:%M:%S")


def describe(folder: str, local_tz: str):
    """(members, fingerprint, metadata) for a folder. start/end are the oldest and newest member
    mtimes in local_tz; size is the tar's size."""
    mem = members(folder)
    files = [ti for ti, _, _ in mem if ti.type == tarfile.REGTYPE]
    times = [ns / 1e9 for _, _, ns in mem]
    meta = {
        "start_time": _local(min(times), local_tz),
        "end_time": _local(max(times), local_tz),
        "size": tar_size(mem),
        "topics": {},
        "bundle": {"files": len(files), "bytes": sum(ti.size for ti in files)},
    }
    return mem, fingerprint(mem), meta


def newest_age_s(mem) -> float:
    return time.time() - max(ns for _, _, ns in mem) / 1e9


def metadata_worker(args):
    """Catalog entry for one bundle folder, or None (unreadable, or still changing).
    The sidecar is reused while the fingerprint is unchanged."""
    message_queue, dirroot, filename, folder, robot_name, local_tz, updates, settle_s = args
    try:
        mem = members(folder)
        if not mem:
            return None
        if newest_age_s(mem) < settle_s:
            debug_print(f"bundle {folder}: changed in the last {settle_s}s; not bundled this scan")
            return None
        fp = fingerprint(mem)
        sidecar = _sidecar(folder, ".metadata")
        entry = None
        try:
            with open(sidecar) as fh:
                cached = json.load(fh)
            if cached.get("fingerprint") == fp and cached.get("time_zone") == local_tz:
                entry = cached
        except (OSError, json.JSONDecodeError):
            pass
        if entry is None:
            _, _, meta = describe(folder, local_tz)
            entry = {"site": "default", "md5": None}
            entry.update(meta)
        entry.update({"dirroot": dirroot, "filename": filename, "robot_name": robot_name,
                      "fingerprint": fp, "time_zone": local_tz, "is_bundle": True})
        if filename in updates:
            entry.update(updates[filename])
        try:
            with open(sidecar, "w") as fh:
                json.dump(entry, fh, indent=True)
        except OSError as e:
            debug_print(f"Error writing [{sidecar}]: {e}")
        message_queue.put({"main_pbar": entry["size"]})
        return entry
    except Exception as e:
        debug_print(f"Skipping bundle {folder}: {type(e).__name__}: {e}")
        return None


def hash_entry(entry, message_queue, chunk_size: int):
    """Set entry["md5"] (xxh128 of the tar stream, like every other file) and the exact size.
    Reuses `<folder>.tar.md5` while the fingerprint is unchanged. None on failure."""
    import xxhash
    tar_path = os.path.join(entry["dirroot"], entry["filename"])
    folder = bundle_dir(tar_path)
    if folder is None:
        debug_print(f"bundle folder for {tar_path} is gone")
        return None
    tar_rel = os.path.relpath(folder, entry["dirroot"]) + SUFFIX
    mem = members(folder)
    fp = fingerprint(mem)
    size = tar_size(mem)
    cache = _sidecar(folder, ".md5")
    try:
        with open(cache) as fh:
            cached = json.load(fh)
        if isinstance(cached, dict) and cached.get("fingerprint") == fp and cached.get("size") == size:
            entry.update({"md5": cached["md5"], "size": size, "fingerprint": fp,
                          "filename": versioned(tar_rel, cached["md5"])})
            message_queue.put({"main_pbar": size})
            return entry
    except (OSError, json.JSONDecodeError, KeyError):
        pass

    x = xxhash.xxh128()
    sent = 0
    try:
        for data in iter_tar(mem, chunk_size):
            x.update(data)
            sent += len(data)
            message_queue.put({"main_pbar": len(data)})
    except OSError as e:
        debug_print(f"Hashing bundle {folder} failed: {e}")
        return None
    if sent != size:
        debug_print(f"bundle {folder}: stream was {sent} bytes, expected {size}; not cached")
        return None
    if fingerprint(members(folder)) != fp:
        debug_print(f"bundle {folder} changed while it was hashed; next scan")
        return None
    entry.update({"md5": x.hexdigest(), "size": size, "fingerprint": fp,
                  "filename": versioned(tar_rel, x.hexdigest())})
    try:
        with open(cache, "w") as fh:
            json.dump({"md5": entry["md5"], "fingerprint": fp, "size": size}, fh)
    except OSError as e:
        debug_print(f"Error writing [{cache}]: {e}")
    return entry


def remove(tar_path: str, compiled) -> bool:
    """Delete a bundle folder after the server has its tar, and the folder's sidecars.

    Only if the folder name still matches a pattern and its fingerprint is still the one that was
    hashed (`<folder>.tar.md5`): a folder that changed since has content the server does not have.
    """
    folder = bundle_dir(tar_path)
    if folder is None or not matches(compiled, os.path.basename(folder)):
        return False
    try:
        with open(_sidecar(folder, ".md5")) as fh:
            hashed = json.load(fh)
    except (OSError, json.JSONDecodeError):
        hashed = None
    if not isinstance(hashed, dict) or hashed.get("fingerprint") != fingerprint(members(folder)):
        debug_print(f"Not deleting bundle {folder}: changed since it was hashed")
        return False
    version = _VERSION.fullmatch(tar_path[:-len(SUFFIX)])
    if version and os.path.basename(tar_path) != os.path.basename(versioned(folder + SUFFIX, hashed.get("md5", ""))):
        debug_print(f"Not deleting bundle {folder}: the server asked about another version ({tar_path})")
        return False
    debug_print(f"Removing bundle folder {folder}")
    shutil.rmtree(folder)
    for side in (_sidecar(folder, ".md5"), _sidecar(folder, ".metadata")):
        if os.path.exists(side):
            os.remove(side)
    return True
