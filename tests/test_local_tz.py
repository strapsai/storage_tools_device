"""Files dated by modification time get local_tz dates, like MCAPs, whatever zone the process runs in.

The device container runs in UTC. A recording that stopped at 21:30 EDT had its MCAP filed under
the 23rd (message times are converted to local_tz) but its metadata.yaml, images and logs under
the 24th (modification time in the process's zone).
"""
import json
import os
import queue
import tempfile
import time
import unittest
from datetime import datetime, timezone
from unittest import mock

from device import utils, workers

NY = "America/New_York"
# 2026-10-24 01:30:00 UTC = 2026-10-23 21:30:00 EDT
EVENING = datetime(2026, 10, 24, 1, 30, tzinfo=timezone.utc).timestamp()
# 2026-01-15 03:00:00 UTC = 2026-01-14 22:00:00 EST (winter offset)
WINTER = datetime(2026, 1, 15, 3, 0, tzinfo=timezone.utc).timestamp()


class InUTC(unittest.TestCase):
    """Run as the container does: the process's own zone is UTC."""

    def setUp(self):
        self._tz = os.environ.get("TZ")
        os.environ["TZ"] = "UTC"
        time.tzset()
        self.tmp = tempfile.TemporaryDirectory()

    def tearDown(self):
        if self._tz is None:
            os.environ.pop("TZ", None)
        else:
            os.environ["TZ"] = self._tz
        time.tzset()
        self.tmp.cleanup()

    def make(self, name, mtime=EVENING, content="x"):
        path = os.path.join(self.tmp.name, name)
        os.makedirs(os.path.dirname(path), exist_ok=True)
        with open(path, "w") as fh:
            fh.write(content)
        os.utime(path, (mtime, mtime))
        return path


class MtimeDates(InUTC):
    def test_every_mtime_dated_type_uses_local_tz(self):
        for name in ["metadata.yaml", "notes.txt", "a.md", "b.csv", "pane_%3.log", "c.ass", "d.png"]:
            md = utils.getMetaData(self.make("rec/" + name), NY)
            self.assertEqual(md["start_time"], "2026-10-23 21:30:00", name)
            self.assertEqual(md["end_time"], "2026-10-23 21:30:00", name)

    def test_winter_offset(self):
        self.assertEqual(utils.mtime_as_local(self.make("w.txt", WINTER), NY), "2026-01-14 22:00:00")

    def test_date_in_the_file_name_wins_and_is_not_shifted(self):
        md = utils.getMetaData(self.make("cam_20261023_233000.txt"), NY)
        self.assertEqual(md["start_time"], "2026-10-23 23:30:00")

    def test_mp4_creation_time_is_converted(self):
        path = self.make("v.mp4")
        probe = {"streams": [{"tags": {"creation_time": "2026-10-24T01:30:00.000000Z"}}],
                 "format": {"duration": "60"}}
        with mock.patch.object(utils.ffmpeg, "probe", return_value=probe):
            md = utils.getMetaData(path, NY)
        self.assertEqual((md["start_time"], md["end_time"]), ("2026-10-23 21:30:00", "2026-10-23 21:31:00"))

    def test_new_entry_records_its_zone(self):
        path = self.make("rec/notes.txt")
        entry = workers.create_device_entry(path, "rec/notes.txt", self.tmp.name, 1, "r", NY)
        self.assertEqual((entry["start_time"], entry["time_zone"]), ("2026-10-23 21:30:00", NY))


class OldSidecars(InUTC):
    def run_worker(self, path, rel):
        q = queue.Queue()
        return workers.metadata_worker((q, self.tmp.name, rel, path, "r", NY, {}))

    def write_sidecar(self, path, entry):
        with open(path + ".metadata", "w") as fh:
            json.dump(entry, fh)
        later = os.path.getmtime(path) + 60
        os.utime(path + ".metadata", (later, later))   # newer than the file: used as cache

    def test_old_utc_dated_sidecar_is_redated_once(self):
        path = self.make("rec/metadata.yaml")
        self.write_sidecar(path, {"dirroot": self.tmp.name, "filename": "rec/metadata.yaml", "size": 1,
                                  "start_time": "2026-10-24 01:30:00", "end_time": "2026-10-24 01:30:00",
                                  "site": "field", "robot_name": "r", "md5": None})
        entry = self.run_worker(path, "rec/metadata.yaml")
        self.assertEqual((entry["start_time"], entry["time_zone"]), ("2026-10-23 21:30:00", NY))
        self.assertEqual(entry["site"], "field")                       # other cached fields kept
        with open(path + ".metadata") as fh:
            self.assertEqual(json.load(fh)["time_zone"], NY)            # written back: next scan is cached
        with mock.patch.object(workers, "create_device_entry", side_effect=AssertionError("recomputed")):
            self.assertEqual(self.run_worker(path, "rec/metadata.yaml")["start_time"], "2026-10-23 21:30:00")

    def test_old_mcap_sidecar_is_stamped_not_reread(self):
        path = self.make("rec/r_0.mcap")
        self.write_sidecar(path, {"dirroot": self.tmp.name, "filename": "rec/r_0.mcap", "size": 1,
                                  "start_time": "2026-10-23 21:00:00", "end_time": "2026-10-23 21:30:00",
                                  "site": "default", "robot_name": "r", "md5": None, "topics": {"/a": 5}})
        with mock.patch.object(workers, "create_device_entry", side_effect=AssertionError("re-read the MCAP")):
            entry = self.run_worker(path, "rec/r_0.mcap")
        self.assertEqual((entry["start_time"], entry["topics"], entry["time_zone"]),
                         ("2026-10-23 21:00:00", {"/a": 5}, NY))


if __name__ == "__main__":
    unittest.main()
