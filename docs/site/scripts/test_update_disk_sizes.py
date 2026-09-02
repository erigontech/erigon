"""Unit tests for update-disk-sizes.py.

Run from repo root:
  python3 -m unittest discover docs/site/scripts -v
  python3 docs/site/scripts/test_update_disk_sizes.py    # direct invocation also works
"""

import importlib.util
import json
import tempfile
import unittest
from pathlib import Path

_HERE = Path(__file__).parent
_spec = importlib.util.spec_from_file_location("update_disk_sizes", _HERE / "update-disk-sizes.py")
u = importlib.util.module_from_spec(_spec)
_spec.loader.exec_module(u)


class FormatBytesTests(unittest.TestCase):
    """Test the format_bytes() function for correct SI unit formatting."""

    def test_terabytes_with_precision(self):
        # 1.5 TB
        self.assertEqual(u.format_bytes(1_500_000_000_000), "1.50 TB")

    def test_terabytes_boundary(self):
        # Exactly 1 TB
        self.assertEqual(u.format_bytes(1_000_000_000_000), "1.00 TB")

    def test_gigabytes_with_precision(self):
        # 500.75 GB
        self.assertEqual(u.format_bytes(500_750_000_000), "500.75 GB")

    def test_gigabytes_near_terabyte_boundary(self):
        # 999.9 GB should not round to "1000 GB"
        result = u.format_bytes(999_900_000_000)
        self.assertEqual(result, "999.90 GB")
        self.assertNotIn("1000", result)

    def test_gigabytes_boundary(self):
        # Exactly 1 GB
        self.assertEqual(u.format_bytes(1_000_000_000), "1.00 GB")

    def test_megabytes_rounded(self):
        # 500.5 MB -> rounds to 500 MB (banker's rounding)
        self.assertEqual(u.format_bytes(500_500_000), "500 MB")

    def test_megabytes_promote_to_gigabytes_at_rounded_boundary(self):
        # Avoid formatting just-below-1 GB values as "1000 MB"
        self.assertEqual(u.format_bytes(999_900_000), "1.00 GB")

    def test_megabytes_small(self):
        # 50 MB
        self.assertEqual(u.format_bytes(50_000_000), "50 MB")

    def test_zero_bytes(self):
        # Edge case: 0 bytes
        self.assertEqual(u.format_bytes(0), "0 MB")

    def test_sub_megabyte(self):
        # 500 KB -> rounds to 0 MB (0.5 rounds down with banker's rounding)
        self.assertEqual(u.format_bytes(500_000), "0 MB")
    
    def test_megabytes_rounds_up(self):
        # 1.5 MB -> rounds to 2 MB
        self.assertEqual(u.format_bytes(1_500_000), "2 MB")


class ArtifactParsingTests(unittest.TestCase):
    """Test artifact filename parsing and JSON update logic."""

    def test_mainnet_full_artifact(self):
        """Test parsing disk-usage-mainnet-full.txt"""
        with tempfile.TemporaryDirectory() as tmpdir:
            artifacts_dir = Path(tmpdir) / "artifacts"
            artifacts_dir.mkdir()
            
            # Create artifact file
            artifact = artifacts_dir / "disk-usage-mainnet-full.txt"
            artifact.write_text("1234567890123")
            
            # Create initial JSON
            json_path = Path(tmpdir) / "disk-sizes.json"
            initial_data = {
                "networks": {
                    "mainnet": {
                        "full": {"bytes": 0, "display": "0 MB", "measured_at": "2025-01-01", "source": "manual"}
                    }
                }
            }
            json_path.write_text(json.dumps(initial_data, indent=2))
            
            # Run the main logic (simulate command line args)
            import sys
            old_argv = sys.argv
            try:
                sys.argv = ["update-disk-sizes.py", str(artifacts_dir), str(json_path), "full"]
                u.main()
            except SystemExit:
                pass
            finally:
                sys.argv = old_argv
            
            # Verify JSON was updated
            with open(json_path) as f:
                result = json.load(f)
            
            self.assertEqual(result["networks"]["mainnet"]["full"]["bytes"], 1234567890123)
            self.assertEqual(result["networks"]["mainnet"]["full"]["display"], "1.23 TB")
            self.assertEqual(result["networks"]["mainnet"]["full"]["source"], "ci")
            self.assertIn("ci_last_updated", result)

    def test_gnosis_minimal_artifact(self):
        """Test parsing disk-usage-gnosis-minimal.txt"""
        with tempfile.TemporaryDirectory() as tmpdir:
            artifacts_dir = Path(tmpdir) / "artifacts"
            artifacts_dir.mkdir()
            
            # Create artifact file
            artifact = artifacts_dir / "disk-usage-gnosis-minimal.txt"
            artifact.write_text("234567890123")
            
            # Create initial JSON
            json_path = Path(tmpdir) / "disk-sizes.json"
            initial_data = {
                "networks": {
                    "gnosis": {
                        "minimal": {"bytes": 0, "display": "0 MB", "measured_at": "2025-01-01", "source": "manual"}
                    }
                }
            }
            json_path.write_text(json.dumps(initial_data, indent=2))
            
            # Run the main logic
            import sys
            old_argv = sys.argv
            try:
                sys.argv = ["update-disk-sizes.py", str(artifacts_dir), str(json_path), "minimal"]
                u.main()
            except SystemExit:
                pass
            finally:
                sys.argv = old_argv
            
            # Verify JSON was updated
            with open(json_path) as f:
                result = json.load(f)
            
            self.assertEqual(result["networks"]["gnosis"]["minimal"]["bytes"], 234567890123)
            self.assertEqual(result["networks"]["gnosis"]["minimal"]["display"], "234.57 GB")
            self.assertEqual(result["networks"]["gnosis"]["minimal"]["source"], "ci")

    def test_unknown_chain_skipped(self):
        """Test that unknown chains are skipped without error"""
        with tempfile.TemporaryDirectory() as tmpdir:
            artifacts_dir = Path(tmpdir) / "artifacts"
            artifacts_dir.mkdir()
            
            # Create artifact for unknown chain
            artifact = artifacts_dir / "disk-usage-unknown-chain-full.txt"
            artifact.write_text("123456789")
            
            # Create initial JSON without unknown-chain
            json_path = Path(tmpdir) / "disk-sizes.json"
            initial_data = {
                "networks": {
                    "mainnet": {
                        "full": {"bytes": 0, "display": "0 MB", "measured_at": "2025-01-01", "source": "manual"}
                    }
                }
            }
            json_path.write_text(json.dumps(initial_data, indent=2))
            
            # Run the main logic
            import sys
            old_argv = sys.argv
            exit_code = 0
            try:
                sys.argv = ["update-disk-sizes.py", str(artifacts_dir), str(json_path), "full"]
                u.main()
            except SystemExit as e:
                exit_code = e.code
            finally:
                sys.argv = old_argv
            self.assertEqual(exit_code, 1)
            
            # Verify JSON was NOT updated (no ci_last_updated field added)
            with open(json_path) as f:
                result = json.load(f)
            
            self.assertNotIn("ci_last_updated", result)
            self.assertNotIn("unknown-chain", result["networks"])

    def test_invalid_bytes_skipped(self):
        """Test that artifacts with invalid byte values are skipped"""
        with tempfile.TemporaryDirectory() as tmpdir:
            artifacts_dir = Path(tmpdir) / "artifacts"
            artifacts_dir.mkdir()
            
            # Create artifact with invalid content
            artifact = artifacts_dir / "disk-usage-mainnet-full.txt"
            artifact.write_text("not-a-number")
            
            # Create initial JSON
            json_path = Path(tmpdir) / "disk-sizes.json"
            initial_data = {
                "networks": {
                    "mainnet": {
                        "full": {"bytes": 999, "display": "999 MB", "measured_at": "2025-01-01", "source": "manual"}
                    }
                }
            }
            json_path.write_text(json.dumps(initial_data, indent=2))
            
            # Run the main logic
            import sys
            old_argv = sys.argv
            exit_code = 0
            try:
                sys.argv = ["update-disk-sizes.py", str(artifacts_dir), str(json_path), "full"]
                u.main()
            except SystemExit as e:
                exit_code = e.code
            finally:
                sys.argv = old_argv
            self.assertEqual(exit_code, 1)
            
            # Verify JSON was NOT updated (original value preserved)
            with open(json_path) as f:
                result = json.load(f)
            
            self.assertEqual(result["networks"]["mainnet"]["full"]["bytes"], 999)
            self.assertNotIn("ci_last_updated", result)

    def test_multiple_artifacts_same_mode(self):
        """Test processing multiple chains in the same prune mode"""
        with tempfile.TemporaryDirectory() as tmpdir:
            artifacts_dir = Path(tmpdir) / "artifacts"
            artifacts_dir.mkdir()
            
            # Create multiple artifacts
            (artifacts_dir / "disk-usage-mainnet-full.txt").write_text("1000000000000")
            (artifacts_dir / "disk-usage-gnosis-full.txt").write_text("500000000000")
            
            # Create initial JSON
            json_path = Path(tmpdir) / "disk-sizes.json"
            initial_data = {
                "networks": {
                    "mainnet": {
                        "full": {"bytes": 0, "display": "0 MB", "measured_at": "2025-01-01", "source": "manual"}
                    },
                    "gnosis": {
                        "full": {"bytes": 0, "display": "0 MB", "measured_at": "2025-01-01", "source": "manual"}
                    }
                }
            }
            json_path.write_text(json.dumps(initial_data, indent=2))
            
            # Run the main logic
            import sys
            old_argv = sys.argv
            try:
                sys.argv = ["update-disk-sizes.py", str(artifacts_dir), str(json_path), "full"]
                u.main()
            except SystemExit:
                pass
            finally:
                sys.argv = old_argv
            
            # Verify both chains were updated
            with open(json_path) as f:
                result = json.load(f)
            
            self.assertEqual(result["networks"]["mainnet"]["full"]["bytes"], 1000000000000)
            self.assertEqual(result["networks"]["gnosis"]["full"]["bytes"], 500000000000)
            self.assertIn("ci_last_updated", result)

    def test_wrong_mode_artifacts_ignored(self):
        """Test that artifacts for different prune modes are ignored"""
        with tempfile.TemporaryDirectory() as tmpdir:
            artifacts_dir = Path(tmpdir) / "artifacts"
            artifacts_dir.mkdir()
            
            # Create artifacts for both modes
            (artifacts_dir / "disk-usage-mainnet-full.txt").write_text("1000000000000")
            (artifacts_dir / "disk-usage-mainnet-minimal.txt").write_text("500000000000")
            
            # Create initial JSON
            json_path = Path(tmpdir) / "disk-sizes.json"
            initial_data = {
                "networks": {
                    "mainnet": {
                        "full": {"bytes": 0, "display": "0 MB", "measured_at": "2025-01-01", "source": "manual"},
                        "minimal": {"bytes": 0, "display": "0 MB", "measured_at": "2025-01-01", "source": "manual"}
                    }
                }
            }
            json_path.write_text(json.dumps(initial_data, indent=2))
            
            # Run with "full" mode
            import sys
            old_argv = sys.argv
            try:
                sys.argv = ["update-disk-sizes.py", str(artifacts_dir), str(json_path), "full"]
                u.main()
            except SystemExit:
                pass
            finally:
                sys.argv = old_argv
            
            # Verify only "full" was updated
            with open(json_path) as f:
                result = json.load(f)
            
            self.assertEqual(result["networks"]["mainnet"]["full"]["bytes"], 1000000000000)
            self.assertEqual(result["networks"]["mainnet"]["minimal"]["bytes"], 0)  # unchanged


if __name__ == "__main__":
    unittest.main()
