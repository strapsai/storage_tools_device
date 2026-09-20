"""A stable `source` name for this device.

The source `DEV-<robot_name>-<id>` is the device's room on the server and part of every
upload id the server assigns to its files. The `<id>` used to be a hash of the MAC addresses of
every interface that was up, so plugging in a dongle, a link that was down at boot, or (on a host
that runs containers) a restarted container with new random `veth` MACs renamed the device, and
every file it had uploaded became a new identity on the server.

The id is now decided once and remembered:

  1. `source_id` in the device config, if set, wins. Letters, digits and dots only.
  2. Otherwise an identity file next to the config (`<config>.identity`, or `IDENTITY_FILE`)
     holds it. It is created on first start, seeded from the current MAC-derived hash so an
     upgraded device keeps the name it happens to have, and never rewritten.

If the id cannot be persisted the device refuses to start rather than silently falling back to
a name that changes.
"""
import os
import re
from typing import Optional

from device.debug_print import debug_print
from device.utils import get_source_by_mac_address

_ID_RE = re.compile(r"^[A-Za-z0-9.]{1,64}$")


def identity_file_for(config_filename: str) -> str:
    """`IDENTITY_FILE` if set, else `<config without extension>.identity` next to the config."""
    explicit = os.environ.get("IDENTITY_FILE")
    if explicit:
        return explicit
    base, _ext = os.path.splitext(os.path.abspath(config_filename))
    return base + ".identity"


def _validate(source_id: str, origin: str) -> str:
    source_id = (source_id or "").strip()
    if not _ID_RE.match(source_id):
        raise SystemExit(f"invalid source id {source_id!r} from {origin}: use 1-64 letters, digits or dots "
                         f"(no spaces, '-' or '_', they delimit the source name)")
    return source_id


def _read(path: str) -> Optional[str]:
    try:
        with open(path, "r") as fh:
            value = fh.read().strip()
    except FileNotFoundError:
        return None
    if not value:
        raise SystemExit(f"identity file {path} is empty; delete it to have a new id generated, or write an id into it")
    return _validate(value, path)


def _create(path: str, value: str) -> bool:
    """Write `value` unless the file exists. True if this call created it."""
    try:
        fd = os.open(path, os.O_WRONLY | os.O_CREAT | os.O_EXCL, 0o644)
    except FileExistsError:
        return False
    except OSError as e:
        raise SystemExit(f"cannot create identity file {path}: {e}. Set source_id in the config, point IDENTITY_FILE "
                         f"at a writable location, or fix the permissions of the config directory.")
    with os.fdopen(fd, "w") as fh:
        fh.write(value + "\n")
    return True


def stable_source_id(config: dict, config_filename: str) -> str:
    """The persistent id part of this device's source name (see module docstring)."""
    configured = config.get("source_id")
    if configured is not None and str(configured).strip():
        return _validate(str(configured), f"source_id in {config_filename}")

    path = identity_file_for(config_filename)
    existing = _read(path)
    if existing:
        return existing

    # First start on this config: keep the name the device has right now, and remember it.
    seed = get_source_by_mac_address("x").rsplit("-", 1)[-1]
    if _create(path, seed):
        debug_print(f"created identity file {path} with id {seed}")
        return seed
    existing = _read(path)          # created by someone else a moment ago
    if not existing:
        raise SystemExit(f"identity file {path} appeared but is unreadable")
    return existing


def stable_source(config: dict, config_filename: str, salt=None) -> str:
    """`DEV-<robot_name>-<id>` (+ salt), identical on every start."""
    robot_name = config.get("robot_name", "robot")
    source = f"DEV-{robot_name}-{stable_source_id(config, config_filename)}"
    if salt:
        source += str(salt)
    return source
