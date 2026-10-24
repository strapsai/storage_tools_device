"""exclude_dirs: excluded trees are never walked, and the device refuses to send or delete inside them."""
import os
import tempfile
import unittest

from device import dirfilter


def touch(path):
    os.makedirs(os.path.dirname(path), exist_ok=True)
    with open(path, "w") as fh:
        fh.write("x")


def walk(root, compiled):
    found = []
    for r, dirs, files in os.walk(root):
        dirfilter.prune(compiled, root, r, dirs)
        found += [os.path.relpath(os.path.join(r, f), root) for f in files]
    return sorted(found)


class Patterns(unittest.TestCase):
    def setUp(self):
        self.tmp = tempfile.TemporaryDirectory()
        self.root = os.path.join(self.tmp.name, "logging")
        for rel in ["basestation_20261023_151538/a.mcap",
                    "chiron_data/minio/chiron-bucket/x.jpg/xl.meta",
                    "chiron_data/postgres/base/1/2.yaml",
                    "chiron_backup/sessions/s1/minio/b/xl.meta",
                    "chiron_backup/sessions/s1/db.yaml",
                    "test_data/minio_notes/readme.md",
                    "deep/a/chiron_data/c.mcap"]:
            touch(os.path.join(self.root, rel))

    def tearDown(self):
        self.tmp.cleanup()

    def test_no_patterns_walks_everything(self):
        self.assertEqual(len(walk(self.root, dirfilter.compile_patterns(None))), 7)

    def test_bare_name_matches_at_any_depth(self):
        got = walk(self.root, dirfilter.compile_patterns(["chiron_data"]))
        self.assertNotIn("chiron_data/postgres/base/1/2.yaml", got)
        self.assertNotIn("deep/a/chiron_data/c.mcap", got)
        self.assertIn("chiron_backup/sessions/s1/minio/b/xl.meta", got)

    def test_relative_path_is_anchored_at_the_watch_root(self):
        got = walk(self.root, dirfilter.compile_patterns(["chiron_data/minio", "chiron_backup/sessions/*/minio"]))
        self.assertEqual(got, ["basestation_20261023_151538/a.mcap",
                               "chiron_backup/sessions/s1/db.yaml",
                               "chiron_data/postgres/base/1/2.yaml",
                               "deep/a/chiron_data/c.mcap",
                               "test_data/minio_notes/readme.md"])

    def test_star_does_not_cross_a_slash(self):
        got = walk(self.root, dirfilter.compile_patterns(["chiron_backup/*/minio"]))
        self.assertIn("chiron_backup/sessions/s1/minio/b/xl.meta", got)

    def test_absolute_path(self):
        got = walk(self.root, dirfilter.compile_patterns([self.root + "/chiron_data/"]))
        self.assertNotIn("chiron_data/postgres/base/1/2.yaml", got)
        self.assertIn("deep/a/chiron_data/c.mcap", got)

    def test_glob_on_a_name(self):
        got = walk(self.root, dirfilter.compile_patterns(["minio*"]))
        self.assertNotIn("test_data/minio_notes/readme.md", got)
        self.assertNotIn("chiron_data/minio/chiron-bucket/x.jpg/xl.meta", got)

    def test_the_watch_root_itself_and_empty_patterns_never_exclude(self):
        compiled = dirfilter.compile_patterns(["", "/", ".", "logging"])
        self.assertFalse(dirfilter.is_excluded(compiled, self.root, self.root))
        self.assertEqual(len(walk(self.root, dirfilter.compile_patterns(["", "/", "."]))), 7)

    def test_under_excluded(self):
        compiled = dirfilter.compile_patterns(["chiron_data"])
        self.assertTrue(dirfilter.under_excluded(compiled, self.root, os.path.join(self.root, "chiron_data/minio/x")))
        self.assertFalse(dirfilter.under_excluded(compiled, self.root, os.path.join(self.root, "test_data/chiron_data.mcap")))
        self.assertFalse(dirfilter.under_excluded(compiled, self.root, os.path.join(self.root, "a.mcap")))


class PathInWatch(unittest.TestCase):
    """Send/delete requests from a server must not reach into an excluded tree either."""

    def test_guard(self):
        from device.Device import Device
        with tempfile.TemporaryDirectory() as tmp:
            root = os.path.join(tmp, "logging")
            touch(os.path.join(root, "chiron_data/minio/x"))
            touch(os.path.join(root, "rec/a.mcap"))
            os.symlink(os.path.join(root, "chiron_data"), os.path.join(root, "rec/link"))
            dev = Device.__new__(Device)
            dev.m_config = {"watch": [root]}
            dev.m_exclude_dirs = dirfilter.compile_patterns(["chiron_data"])
            self.assertTrue(dev._path_in_watch(os.path.join(root, "rec/a.mcap")))
            self.assertFalse(dev._path_in_watch(os.path.join(root, "chiron_data/minio/x")))
            self.assertFalse(dev._path_in_watch(os.path.join(root, "rec/link/minio/x")))
            self.assertFalse(dev._path_in_watch(os.path.join(tmp, "elsewhere")))


if __name__ == "__main__":
    unittest.main()
