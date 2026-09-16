# storage_tools_device

Agent-side (“device”) half of AIRLab Storage Tools. It watches directories on a machine that
produces data, builds a catalog of what it finds, advertises that catalog to one or more
**servers**, and uploads files on request.

Its counterpart is `storage_tools_server`. The two are separate repos that share a wire protocol
(Socket.IO events + an HTTP upload endpoint) and an on-disk metadata convention; changing an event
name, an entry field, or the sidecar format on one side requires a matching change on the other.

## Mental model

A device is a **client**. It never listens for servers; it dials out, and every transfer is
initiated by a request that arrives over that outbound socket. A device may be connected to
several servers at once and will publish its catalog to all of them, but each server pulls
independently.

Servers form a tree (a server can itself act as a “node” client of an upstream server). The device
only ever sees the leaf-most server it connects to and is unaware of the rest of the tree.

## Layout

```
device/
  app.py            Flask app factory: routes, socket handlers, starts Device.run()
  Device.py         Everything. Connection management, scan pipeline, upload orchestration
  workers.py        multiprocessing.Pool worker functions (send/hash/metadata/reindex)
  utils.py          Identity, per-format metadata extraction, progress-bar thread, address helpers
  reindexMCAP.py    Wraps the `mcap` CLI to test/recover damaged MCAP files
  SocketIOTQDM.py   tqdm subclass that mirrors progress to N Socket.IO targets
  static/           Local dashboard (jQuery + Bootstrap)
config/config.yaml  Device configuration (see below)
```

`Device` is a large single class; `app.py` is only wiring. New behavior almost always belongs in
`Device.py`, with any CPU/IO-heavy per-file work in `workers.py` so it can be pooled.

## Identity

`get_source_by_mac_address(robot_name)` (`device/utils.py`) hashes the MAC addresses of every
non-loopback interface that is currently **up** and returns `DEV-<robot_name>-<hash8>`. This string
is the device's `source` — its room name on the server, and part of every `upload_id`.

The server computes `upload_id = md5(f"{source}_{project}_{content_hash}")` from the xxh128 digest
this device reports as `md5` (protocol 2, since 1.1.0). The id does not depend on the file's path,
so a folder moved together with its `.metadata`/`.md5` sidecars keeps its ids and is not uploaded
again. Before 1.1.0 the path was part of the id; see the server's `docs/Migration-1.1.md`.

Consequences worth knowing:

- The name changes if the set of up interfaces changes (plugging in a dongle, a down link at boot).
  A device that reappears under a new name looks like a brand-new device to the server.
- Changing `robot_name` changes `source`, so `save_config` deliberately tears down and rebuilds all
  connections when it sees that key change.
- An optional `salt` argument (CLI `-s`) appends to the source, for running two devices on one host.

## Connection lifecycle

Before opening the socket the device fetches `GET /name` from the server, which since server
1.1.0 also returns `version` and `protocol`. If the server's protocol differs from
`device/__version__.py:PROTOCOL_VERSION` (missing = 1, i.e. a pre-1.1.0 server) the device does
**not** connect; it records the reason (`_report_server_error`) and the local page shows a red
banner naming the server. The `join` message carries `version` and `protocol`; a server that
refuses them answers `incompatible_version` and disconnects, which is shown the same way and
stops retries for that address (`server_should_run`).

Servers come from two places: the static `servers:` list in the config, and zeroconf discovery
(service type `_http._tcp.local.`, name `Airlab_storage`, whose TXT record carries the server's
`source`). Discovered addresses land in `config["zero_conf"]` and are skipped if they resolve to
something already in `servers:` (`address_in_list` compares resolved IP **and** port).

Each server address gets its own thread looping over `test_connection()`:

1. TCP connect probe, then `GET http://<server>/name` with header `X-Api-Key: <API_KEY_TOKEN>`.
   A non-200 aborts this attempt — this is where a bad API key is caught, before any socket.
2. Read the server's `source` from that response. If that same source is already reachable via a
   *different* address, mark this one duplicate and stop trying it. This is how the static list and
   zeroconf are prevented from double-connecting to one server.
3. `sio.connect(...)`, then `emit('join', {room: <our source>, type: "device", session_token: ...})`.
4. **The handshake is not complete until the server replies `dashboard_info`.** Only in that
   callback is the socket registered in `self.server_sio` and a scan kicked off. Anything that
   emits before this silently goes nowhere; `_emit_to_all_servers` logs when it has no live sockets.

Two flags gate the loops: `server_can_run[addr]` (should this address be managed at all) and
`server_should_run[addr]` (stay in the connected busy-wait). Clearing the latter drops the
connection and lets the manager thread reconnect.

Zeroconf servers are managed by a single long-lived thread that re-reads `config["zero_conf"]` on
every pass (`start_zero_config_servers` only re-arms it, never spawns a second one) — **it connects
to at most one zeroconf server at a time**, unlike the static list which gets a thread each.

## Scan pipeline

`_background_scan()` starts a chain; each stage claims an “already running” flag under `scan_lock`,
runs its `_impl` inside `try/finally` so the flag is always released (an exception in one stage is
logged and the chain continues), does a pooled pass, and calls the next:

1. **`_background_reindex`** — walk `watch:` dirs, collect `.mcap`, test-open each, and run
   `reindexMCAP.recover_mcap` on the ones that fail. Damaged MCAPs (killed recorder, power loss)
   are common and would otherwise yield no metadata.
2. **`_background_metadata`** — `metadata_worker` per file. Writes `<file>.metadata` (JSON) beside
   each file and reuses it when it is newer than the file. Only what is intrinsic to the file is
   taken from the cache; `dirroot`, `filename` and `size` are always set from where the scan found
   the file, so moved folders report their new location (a stale cached path used to make the hash
   stage report the file as missing). Extraction is per-format
   (`utils.getMetaData`): MCAP via `mcap.reader` (also counts messages per topic), ROS1 bags via
   `rosbags`, MP4 via `ffmpeg.probe`, JPEG via EXIF, everything else falls back to filename date
   patterns (`getDateFromFilename`) and then mtime.
3. **`_background_hash`** — `hash_worker` per file, **xxhash‑128** cached in `<file>.md5`. Note the
   field is named `md5` throughout both repos for historical reasons; it is not MD5. Cache is
   trusted when newer than the file.
4. **`emitFiles` → `send_device_data`** — one `device_data` message (project, robot name, free-space
   per filesystem, and the number of blocks to expect) followed by `device_data_block` messages of
   100 entries each. The server reassembles by counting block ids.

`_include()` decides membership: files starting with `.` or `_` are always skipped; otherwise
`include_suffix` (allow-list, if present) wins over `exclude_suffix` (deny-list).

`self.m_updates` holds server-pushed corrections (`update_entry` event, e.g. an operator fixing a
robot name or site in the dashboard); `metadata_worker` merges them into the sidecar so the change
survives the next scan.

## Upload

The server sends `device_send` with a list of
`(dirroot, relative_path, upload_id, offset_b, file_size)` — the server chose the `upload_id` and
told us where to resume from (`offset_b` is the size of its `.tmp` partial).

`send_worker` (`workers.py`) streams `POST http://<server>/file/<source>/<upload_id>` with query
params `offset`, `cid`, `splits`. A file larger than `split_size_gb` is sent as multiple sequential
POSTs; `cid == splits` tells the server this is the final piece and it should verify size and
rename `.tmp` into place. Reads are `chunk_size_mb` at a time.

Cancellation is a `Manager().Event()` per server (`m_signal[server]`), set by the
`device_cancel_transfer` event and polled before every chunk read and every split. The Event only
lives as long as that transfer's `Manager`; it is dropped from `m_signal` when the transfer ends,
and a late cancel is ignored. `m_send_threads[server]` is the in-flight guard, so a second
`device_send` for the same server while one is running is refused.

Progress from every stage is mirrored through `MultiTargetSocketIOTQDM` to the local dashboard
**and** every connected server, so a transfer is visible from either end.

## Deletion

`device_remove` deletes files from the device after the server has them. `_removeFiles` resolves
the final path (`realpath`, so symlinks and `..` count) and refuses anything not under a configured
`watch:` root via `_path_in_watch` — keep that guard; `_on_device_send` applies the same check
before reading anything for upload. It removes the file plus its `.md5` and `.metadata` sidecars,
then rescans.

## Configuration (`config/config.yaml`)

| Key | Meaning |
|---|---|
| `robot_name` | Part of `source`; changing it forces a reconnect |
| `project` | Optional; if unset the server prompts an operator to assign one |
| `API_KEY_TOKEN` | Sent as `X-Api-Key`; must exist in the server's keys file |
| `watch` | List of roots to scan; also the delete allow-list |
| `servers` | Static `host:port` list; zeroconf is used in addition |
| `local_tz` | Timezone for converting recorded UTC timestamps; validated at startup |
| `include_suffix` / `exclude_suffix` | File filter |
| `threads` | Pool size for every stage, and the upload concurrency |
| `wait_s` | Reconnect/poll interval |
| `split_size_gb`, `chunk_size_mb` | Upload chunking (defaults 1 GB / 1 MB; the shipped `config.yaml` does not set them) |
| `chunk_size` | Read size in bytes for hashing (default 8 MiB); separate from `chunk_size_mb` |

The local dashboard (`/`, port from `STORAGE_TOOL_DEVICE_CONFIG_PORT`, default 8811) edits this file
in place via `POST /save_config`. The posted fields are merged into the running config and the
*merged* config (minus runtime keys `source`/`zero_conf`) is written back, so keys the dashboard has
no widget for survive a save. It then reacts: rescan if `watch` changed, full reconnect if
`robot_name` changed, and start/stop threads for added/removed servers.

## Running

`python -m device.app` reads `STORAGE_TOOL_DEVICE_CONFIG_FILE` (default for `-c/--config`, which
overrides it), `SALT` (default for `-s`) and `STORAGE_TOOL_DEVICE_CONFIG_PORT`. Under gunicorn the
env var is required. The bundled `docker-compose.yaml` uses host networking and bind-mounts the data
dir plus `./config`.

Gunicorn is deliberately **not** used: `entrypoint.sh` notes it does not play well with
`multiprocessing.Pool`, which every scan stage depends on.

## Gotchas

- Emitting before `dashboard_info` arrives is a no-op. Trace connection bugs by looking for that
  event first.
- Every `requests` call and the TCP probe carry timeouts; a wedged server fails the attempt instead
  of pinning a thread or pool worker.
- `zero_conf` is rebuilt (not appended to) on every zeroconf resolution.
- The `md5` field is xxhash-128.
- Sidecar caches are validated by mtime only; touching a file forces a full re-hash, and editing a
  sidecar without touching it will be silently kept.
- `_include` returns `None` (falsy) when neither suffix list is configured — effectively excluding
  everything. Always configure one.
- Metadata and hash stages both walk the tree independently; a file appearing between them is
  handled on the next scan, not this one.
