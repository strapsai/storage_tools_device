"""The device's source name must not change between starts or when the set of up interfaces changes."""
import os
import tempfile
import unittest
from unittest import mock

from device import identity


class StableSource(unittest.TestCase):
    def setUp(self):
        self.tmp = tempfile.TemporaryDirectory()
        self.config_file = os.path.join(self.tmp.name, "config.yaml")
        with open(self.config_file, "w") as fh:
            fh.write("robot_name: basestation-device\n")
        os.environ.pop("IDENTITY_FILE", None)

    def tearDown(self):
        self.tmp.cleanup()

    def test_first_start_keeps_the_current_mac_name_and_remembers_it(self):
        cfg = {"robot_name": "basestation-device"}
        with mock.patch.object(identity, "get_source_by_mac_address", return_value="DEV-x-1d8cf137"):
            self.assertEqual(identity.stable_source(cfg, self.config_file), "DEV-basestation-device-1d8cf137")
        with open(os.path.join(self.tmp.name, "config.identity")) as fh:
            self.assertEqual(fh.read().strip(), "1d8cf137")
        # A dongle was plugged in / a container restarted: the interfaces changed, the name must not.
        with mock.patch.object(identity, "get_source_by_mac_address", return_value="DEV-x-30da9822"):
            self.assertEqual(identity.stable_source(cfg, self.config_file), "DEV-basestation-device-1d8cf137")

    def test_robot_name_change_keeps_the_id(self):
        with open(os.path.join(self.tmp.name, "config.identity"), "w") as fh:
            fh.write("1d8cf137\n")
        self.assertEqual(identity.stable_source({"robot_name": "spot1-auto"}, self.config_file), "DEV-spot1-auto-1d8cf137")

    def test_salt_is_appended(self):
        self.assertEqual(identity.stable_source({"robot_name": "r", "source_id": "abc"}, self.config_file, salt="2"),
                         "DEV-r-abc2")

    def test_config_source_id_wins_and_writes_nothing(self):
        with mock.patch.object(identity, "get_source_by_mac_address", side_effect=AssertionError("must not be called")):
            self.assertEqual(identity.stable_source({"robot_name": "r", "source_id": "bs01"}, self.config_file), "DEV-r-bs01")
        self.assertFalse(os.path.exists(os.path.join(self.tmp.name, "config.identity")))

    def test_identity_file_env_override(self):
        path = os.path.join(self.tmp.name, "elsewhere", "id")
        os.makedirs(os.path.dirname(path))
        with open(path, "w") as fh:
            fh.write("abc123\n")
        with mock.patch.dict(os.environ, {"IDENTITY_FILE": path}):
            self.assertEqual(identity.stable_source({"robot_name": "r"}, self.config_file), "DEV-r-abc123")

    def test_losing_the_create_race_reads_the_winners_value(self):
        def other_process_wins(p, value):
            with open(p, "w") as fh:
                fh.write("winner01\n")
            return False

        with mock.patch.object(identity, "get_source_by_mac_address", return_value="DEV-x-loser001"), \
                mock.patch.object(identity, "_create", side_effect=other_process_wins):
            self.assertEqual(identity.stable_source_id({}, self.config_file), "winner01")

    def test_bad_ids_are_refused(self):
        for bad in ("has space", "with-dash", "with_underscore", "", "x" * 65):
            with self.assertRaises(SystemExit, msg=repr(bad)):
                identity._validate(bad, "test")
        with open(os.path.join(self.tmp.name, "config.identity"), "w") as fh:
            fh.write("\n")
        with self.assertRaises(SystemExit):
            identity.stable_source_id({}, self.config_file)

    def test_unwritable_config_dir_fails_loudly_instead_of_drifting(self):
        ro = os.path.join(self.tmp.name, "ro")
        os.makedirs(ro)
        cfg = os.path.join(ro, "c.yaml")
        with open(cfg, "w") as fh:
            fh.write("robot_name: r\n")
        os.chmod(ro, 0o555)
        try:
            if os.access(ro, os.W_OK):      # running as root: the permission bit is not enforced
                self.skipTest("directory is writable for this user")
            with mock.patch.object(identity, "get_source_by_mac_address", return_value="DEV-x-deadbeef"):
                with self.assertRaises(SystemExit):
                    identity.stable_source_id({}, cfg)
        finally:
            os.chmod(ro, 0o755)


if __name__ == "__main__":
    unittest.main()
