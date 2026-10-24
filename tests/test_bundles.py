"""Folder bundles: a matching folder is one deterministic, streamed .tar."""
import io
import json
import os
import queue
import tarfile
import tempfile
import time
import unittest

import xxhash

from device import bundles

PATTERN = [r"\d{4}(_\d{2}){5}\.tmux"]
# Real pane-log names: '%', spaces, ':', '()', a newline, and 251 bytes.
LONG = ("session-profile_session-profile: 2 windows (created Sat Oct 10 06:56:21 2026) (attached)\n0_session_profile__"
        "2_windows__created_Sat_Oct_10_06_56_21_2026___attached__server_session-profile: 2 windows (created Sat Oct 10 "
        "06:56:21 2026) (attached)\n%2.log")
OLD = time.time() - 3600


def make_run(root, name="2026_10_24_03_10_58.tmux"):
    folder = os.path.join(root, "tmux", name)
    os.makedirs(os.path.join(folder, "sub", "empty"))
    files = {
        "basestation-drivers_18_Health_Republisher_%57.log": b"health\n" * 1000,
        LONG: b"profile\n",
        "sub/inner_0.mcap": b"\x89MCAP0\r\n" + b"x" * 700,     # plain bytes inside a bundle
        "zero.log": b"",
    }
    for rel, data in files.items():
        with open(os.path.join(folder, rel), "wb") as fh:
            fh.write(data)
    for r, ds, fs in os.walk(folder):
        for n in ds + fs:
            os.utime(os.path.join(r, n), (OLD, OLD))
    os.utime(folder, (OLD, OLD))
    return folder, files


class Stream(unittest.TestCase):
    def setUp(self):
        self.tmp = tempfile.TemporaryDirectory()
        self.folder, self.files = make_run(self.tmp.name)

    def tearDown(self):
        self.tmp.cleanup()

    def stream(self):
        return b"".join(bundles.iter_tar(bundles.members(self.folder), chunk_size=97))

    def test_bytes_equal_what_tarfile_writes_and_size_is_exact(self):
        mem = bundles.members(self.folder)
        ours = self.stream()
        ref = io.BytesIO()
        with tarfile.open(fileobj=ref, mode="w", format=tarfile.GNU_FORMAT, encoding="utf-8", errors="surrogateescape") as tf:
            for ti, path, _ in mem:
                if ti.type == tarfile.REGTYPE:
                    with open(path, "rb") as fh:
                        tf.addfile(ti, fh)
                else:
                    tf.addfile(ti)
        self.assertEqual(ours, ref.getvalue())
        self.assertEqual(len(ours), bundles.tar_size(mem))

    def test_extracts_to_the_same_names_and_contents(self):
        with tarfile.open(fileobj=io.BytesIO(self.stream()), errors="surrogateescape") as tf:
            names = tf.getnames()
            for rel, data in self.files.items():
                arc = "2026_10_24_03_10_58.tmux/" + rel
                self.assertIn(arc, names)
                self.assertEqual(tf.extractfile(arc).read(), data)
            self.assertIn("2026_10_24_03_10_58.tmux/sub/empty", names)

    def test_deterministic_and_change_sensitive(self):
        a, fa = self.stream(), bundles.fingerprint(bundles.members(self.folder))
        self.assertEqual(a, self.stream())
        with open(os.path.join(self.folder, "zero.log"), "ab") as fh:
            fh.write(b"late line\n")
        self.assertNotEqual(fa, bundles.fingerprint(bundles.members(self.folder)))
        self.assertNotEqual(a, self.stream())

    def test_reader_resumes_at_an_offset(self):
        full = self.stream()
        for offset in (0, 1, 511, 512, 5000, len(full) - 3):
            with bundles.BundleReader(self.folder, chunk_size=64) as r:
                r.seek(offset)
                got = b""
                while chunk := r.read(1000):
                    got += chunk
            self.assertEqual(got, full[offset:], offset)


class Patterns(unittest.TestCase):
    def test_whole_name_match_and_errors(self):
        compiled, errors = bundles.compile_patterns(PATTERN + ["(", ""])
        self.assertEqual(len(compiled), 1)
        self.assertEqual([p for p, _ in errors], ["("])
        self.assertTrue(bundles.matches(compiled, "2026_10_24_03_10_58.tmux"))
        self.assertFalse(bundles.matches(compiled, "2026_10_24_03_10_58"))
        self.assertFalse(bundles.matches(compiled, "x2026_10_24_03_10_58.tmux"))
        self.assertFalse(bundles.matches(compiled, "2026_10_24_03_10_58.tmux.old"))


class Catalog(unittest.TestCase):
    def setUp(self):
        self.tmp = tempfile.TemporaryDirectory()
        self.folder, _ = make_run(self.tmp.name)
        self.rel = "tmux/2026_10_24_03_10_58.tmux.tar"
        self.q = queue.Queue()

    def tearDown(self):
        self.tmp.cleanup()

    def meta(self, settle=600):
        return bundles.metadata_worker((self.q, self.tmp.name, self.rel, self.folder, "g-force-2", "America/New_York", {}, settle))

    def test_entry_hash_and_cache(self):
        entry = self.meta()
        self.assertTrue(entry["is_bundle"])
        self.assertEqual(entry["bundle"]["files"], 4)
        self.assertEqual(entry["filename"], self.rel)
        stream = b"".join(bundles.iter_tar(bundles.members(self.folder)))
        hashed = bundles.hash_entry(dict(entry), self.q, 1 << 20)
        self.assertEqual(hashed["md5"], xxhash.xxh128(stream).hexdigest())
        self.assertEqual(hashed["size"], len(stream))
        with open(os.path.join(self.tmp.name, self.rel) + ".md5") as fh:
            self.assertEqual(json.load(fh)["fingerprint"], hashed["fingerprint"])
        # unchanged: the cached hash is used without reading the folder again
        orig = bundles.iter_tar
        bundles.iter_tar = lambda *a, **k: (_ for _ in ()).throw(AssertionError("re-read"))
        try:
            self.assertEqual(bundles.hash_entry(dict(entry), self.q, 1 << 20)["md5"], hashed["md5"])
        finally:
            bundles.iter_tar = orig

    def test_uploaded_name_carries_the_version(self):
        hashed = bundles.hash_entry(self.meta(), self.q, 1 << 20)
        self.assertEqual(hashed["filename"], "tmux/2026_10_24_03_10_58.tmux.%s.tar" % hashed["md5"][:8])
        versioned_path = os.path.join(self.tmp.name, hashed["filename"])
        self.assertEqual(bundles.bundle_dir(versioned_path), self.folder)
        self.assertEqual(bundles.bundle_dir(os.path.join(self.tmp.name, self.rel)), self.folder)
        self.assertIsNone(bundles.bundle_dir(os.path.join(self.tmp.name, "tmux/2026_10_24_03_10_58.tmux.nothex12.tar")))
        compiled, _ = bundles.compile_patterns(PATTERN)
        other = os.path.join(self.tmp.name, "tmux/2026_10_24_03_10_58.tmux.00000000.tar")
        self.assertFalse(bundles.remove(other, compiled))                      # not the version hashed here
        self.assertTrue(os.path.isdir(self.folder))
        self.assertTrue(bundles.remove(versioned_path, compiled))

    def test_a_folder_still_being_written_waits(self):
        os.utime(os.path.join(self.folder, "zero.log"), None)     # touched now
        self.assertIsNone(self.meta(settle=600))
        self.assertIsNotNone(self.meta(settle=0))

    def test_changed_folder_gets_a_new_hash(self):
        first = bundles.hash_entry(self.meta(), self.q, 1 << 20)
        with open(os.path.join(self.folder, "late.log"), "w") as fh:
            fh.write("more\n")
        os.utime(os.path.join(self.folder, "late.log"), (OLD, OLD))
        os.utime(self.folder, (OLD, OLD))                         # adding a file touched the folder too
        second = bundles.hash_entry(self.meta(), self.q, 1 << 20)
        self.assertNotEqual(first["md5"], second["md5"])

    def test_remove_only_what_was_hashed(self):
        compiled, _ = bundles.compile_patterns(PATTERN)
        tar_path = os.path.join(self.tmp.name, self.rel)
        self.assertFalse(bundles.remove(tar_path, compiled))                 # never hashed
        bundles.hash_entry(self.meta(), self.q, 1 << 20)
        with open(os.path.join(self.folder, "late.log"), "w") as fh:
            fh.write("more\n")
        self.assertFalse(bundles.remove(tar_path, compiled))                 # changed since
        self.assertTrue(os.path.isdir(self.folder))
        bundles.hash_entry(self.meta(settle=0), self.q, 1 << 20)
        self.assertFalse(bundles.remove(tar_path, bundles.compile_patterns([r"other"])[0]))   # pattern gone
        self.assertTrue(bundles.remove(tar_path, compiled))
        self.assertFalse(os.path.exists(self.folder))
        self.assertFalse(os.path.exists(tar_path + ".md5"))
        self.assertFalse(os.path.exists(tar_path + ".metadata"))


if __name__ == "__main__":
    unittest.main()
