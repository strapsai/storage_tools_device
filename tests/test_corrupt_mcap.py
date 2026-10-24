"""One damaged MCAP must not empty the device's file list."""
import os
import queue
import tempfile
import unittest
from unittest import mock

from mcap.writer import CompressionType, Writer
from mcap.reader import make_reader

from device import utils, workers


def write_mcap(path, n=2000):
    with open(path, "wb") as fh:
        w = Writer(fh, compression=CompressionType.ZSTD, chunk_size=4096)
        w.start()
        schema = w.register_schema(name="raw", encoding="", data=b"")
        a = w.register_channel(topic="/a", message_encoding="raw", schema_id=schema)
        b = w.register_channel(topic="/b", message_encoding="raw", schema_id=schema)
        for i in range(n):
            t = 1_760_000_000_000_000_000 + i * 1_000_000
            w.add_message(a if i % 4 else b, log_time=t, publish_time=t, data=b"x" * 64 + i.to_bytes(4, "little"))
        w.finish()


def corrupt_first_chunk(path):
    with open(path, "rb") as fh:
        summary = make_reader(fh).get_summary()
    idx = summary.chunk_indexes[0]
    # flip bytes in the middle of the compressed records of the first chunk
    off = idx.chunk_start_offset + idx.chunk_length // 2
    with open(path, "r+b") as fh:
        fh.seek(off)
        data = bytearray(fh.read(16))
        fh.seek(off)
        fh.write(bytes(x ^ 0xFF for x in data))


class CorruptMcap(unittest.TestCase):
    def setUp(self):
        self.tmp = tempfile.TemporaryDirectory()
        self.path = os.path.join(self.tmp.name, "rec_20261021_190147_1.mcap")
        write_mcap(self.path)

    def tearDown(self):
        self.tmp.cleanup()

    def test_intact_file_counts_messages(self):
        md = utils._getMetaDataMCAP(self.path, "America/New_York")
        self.assertEqual(md["topics"], {"/a": 1500, "/b": 500})

    def test_damaged_chunk_falls_back_to_summary_counts(self):
        corrupt_first_chunk(self.path)
        with open(self.path, "rb") as fh:
            r = make_reader(fh)
            with self.assertRaises(Exception):
                for _ in r.iter_messages():
                    pass
        md = utils._getMetaDataMCAP(self.path, "America/New_York")
        self.assertEqual(md["topics"], {"/a": 1500, "/b": 500})

    def test_worker_returns_an_entry_for_a_damaged_file(self):
        corrupt_first_chunk(self.path)
        q = queue.Queue()
        entry = workers.metadata_worker((q, self.tmp.name, os.path.basename(self.path), self.path, "r", "America/New_York", {}))
        self.assertIsNotNone(entry)
        self.assertEqual(entry["filename"], os.path.basename(self.path))

    def test_worker_never_raises(self):
        q = queue.Queue()
        with mock.patch.object(workers, "create_device_entry", side_effect=RuntimeError("boom")):
            entry = workers.metadata_worker((q, self.tmp.name, os.path.basename(self.path), self.path, "r", "America/New_York", {}))
        self.assertIsNone(entry)
        self.assertEqual(q.get_nowait(), {"main_pbar": os.path.getsize(self.path)})


if __name__ == "__main__":
    unittest.main()
