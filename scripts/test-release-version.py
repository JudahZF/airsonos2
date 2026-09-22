#!/usr/bin/env python3
"""Exercise publication gates in an isolated Git repository."""
import subprocess
import tempfile
import unittest
from pathlib import Path

SCRIPT = Path(__file__).with_name("check-release-version.py").resolve()


class ReleaseGateTest(unittest.TestCase):
    def test_publication_requires_matching_versions_and_tagged_commit(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            (root / "airsonos2").mkdir()
            (root / "Cargo.toml").write_text('[workspace.package]\nversion = "1.2.3"\n')
            (root / "airsonos2/config.yaml").write_text('version: "1.2.3"\n')

            def git(*args):
                subprocess.run(["git", *args], cwd=root, check=True, capture_output=True)

            def gate(version="v1.2.3"):
                return subprocess.run(["python3", str(SCRIPT), version], cwd=root, capture_output=True, text=True)

            git("init")
            git("config", "commit.gpgsign", "false")
            git("config", "tag.gpgsign", "false")
            git("config", "user.email", "fixture@example.invalid")
            git("config", "user.name", "Release fixture")
            git("add", ".")
            git("commit", "-m", "fixture")
            self.assertNotEqual(gate().returncode, 0, "untagged commits must fail")
            git("tag", "v1.2.3")
            self.assertEqual(gate().returncode, 0)
            self.assertEqual(gate("1.2.3").returncode, 0)
            self.assertNotEqual(gate("v1.2.4").returncode, 0)
            (root / "airsonos2/config.yaml").write_text('version: "1.2.2"\n')
            self.assertNotEqual(gate().returncode, 0, "HA version mismatch must fail")
            (root / "airsonos2/config.yaml").write_text('version: "1.2.3"\n')
            git("commit", "--allow-empty", "-m", "new commit")
            self.assertNotEqual(gate().returncode, 0, "old tags must not authorize a new commit")


if __name__ == "__main__":
    unittest.main()
