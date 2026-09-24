"""A moved folder must report its new location even when it carries a cached .metadata sidecar."""
import json
import os
import queue
import shutil
import tempfile
import unittest

from device import workers


class Location(unittest.TestCase):
    def test_cached_sidecar_gets_current_location(self):
        root = tempfile.mkdtemp()
        old = os.path.join(root, "recA")
        os.makedirs(old)
        f = os.path.join(old, "notes.txt")
        with open(f, "w") as fh:
            fh.write("hello")
        # A sidecar written when the file lived under recA/, with the intrinsic fields cached.
        stale = {"dirroot": root, "filename": "recA/notes.txt", "size": 5, "start_time": "2026-09-15 10:00:00",
                 "end_time": "2026-09-15 10:00:00", "site": "default", "robot_name": "old", "md5": None, "topics": {}}
        with open(f + ".metadata", "w") as fh:
            json.dump(stale, fh)
        os.utime(f + ".metadata", None)   # newer than the file, so it is used as cache
        new = os.path.join(root, "archive", "2026", "recA_moved")
        os.makedirs(os.path.dirname(new))
        shutil.move(old, new)
        moved = os.path.join(new, "notes.txt")
        q = queue.Queue()
        entry = workers.metadata_worker((q, root, "archive/2026/recA_moved/notes.txt", moved, "bot", "UTC", {}))
        self.assertEqual(entry["filename"], "archive/2026/recA_moved/notes.txt")
        self.assertEqual(entry["dirroot"], root)
        self.assertEqual(entry["robot_name"], "bot")
        self.assertEqual(entry["start_time"], "2026-09-15 10:00:00")     # cached metadata kept
        with open(moved + ".metadata") as fh:
            self.assertEqual(json.load(fh)["filename"], "archive/2026/recA_moved/notes.txt")   # sidecar rewritten

    def test_protocol_constant(self):
        from device.__version__ import PROTOCOL_VERSION, __version__
        self.assertEqual(PROTOCOL_VERSION, 2)
        self.assertGreaterEqual(tuple(int(x) for x in __version__.split(".")), (1, 1, 0))


if __name__ == "__main__":
    unittest.main()
