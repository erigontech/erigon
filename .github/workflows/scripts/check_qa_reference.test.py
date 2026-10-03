#!/usr/bin/env python3
"""Run with: python3 .github/workflows/scripts/check_qa_reference.test.py."""

import os
from pathlib import Path
import re
import subprocess
import sys
import tempfile
import textwrap
import unittest


ROOT = Path(__file__).resolve().parents[3]
WORKFLOWS = (
    ("qa-rpc-integration-tests.yml", "mainnet"),
    ("qa-rpc-integration-tests-gnosis.yml", "gnosis"),
    ("qa-rpc-integration-tests-latest.yml", "mainnet"),
    ("qa-rpc-performance-tests.yml", "mainnet"),
)


def reference_step(workflow):
    lines = (ROOT / ".github/workflows" / workflow).read_text().splitlines(True)
    start = next(i for i, line in enumerate(lines) if line.strip() in (
        "- name: Set reference data dir based on branch",
        "- name: Check QA reference data",
    ))
    run = next(i for i in range(start, len(lines)) if lines[i].strip() == "run: |")
    end = run + 1
    while end < len(lines) and (not lines[end].strip() or lines[end].startswith("          ")):
        end += 1
    return textwrap.dedent("".join(lines[run + 1:end]))


class ReferenceWorkflowTests(unittest.TestCase):
    def test_missing_release_reference_fails_before_export(self):
        for workflow, chain in WORKFLOWS:
            with self.subTest(workflow=workflow), tempfile.TemporaryDirectory() as tmp:
                env_file = Path(tmp) / "github_env"
                env_file.touch()
                env = dict(os.environ, GITHUB_BASE_REF="release/3.7",
                           GITHUB_REF="refs/pull/1/merge", GITHUB_ENV=str(env_file),
                           ERIGON_QA_VERSIONS_DIR=tmp, CHAIN=chain)
                result = subprocess.run(["bash", "-e", "-c", reference_step(workflow)],
                                        cwd=ROOT, env=env, text=True, capture_output=True,
                                        timeout=10)
                self.assertNotEqual(result.returncode, 0, "missing QA data was accepted")
                self.assertIn("QA reference data", result.stderr + result.stdout)
                self.assertEqual(env_file.read_text(), "")

    def test_reference_selection_for_each_event(self):
        for workflow, chain in WORKFLOWS:
            for base, ref, suffix in (
                ("release/3.7", "refs/pull/1/merge", "-3.7"),
                ("", "refs/heads/release/3.7", "-3.7"),
                ("main", "refs/pull/1/merge", ""),
                ("", "refs/heads/main", ""),
                ("", "refs/heads/fix/qa", ""),
            ):
                with self.subTest(workflow=workflow, base=base, ref=ref), tempfile.TemporaryDirectory() as tmp:
                    prefix = "gnosis-reference-version" if chain == "gnosis" else "reference-version"
                    reference = make_reference(Path(tmp) / (prefix + suffix))
                    env_file = Path(tmp) / "github_env"
                    env = dict(os.environ, GITHUB_BASE_REF=base, GITHUB_REF=ref,
                               GITHUB_ENV=str(env_file), ERIGON_QA_VERSIONS_DIR=tmp, CHAIN=chain)
                    result = subprocess.run(["bash", "-e", "-c", reference_step(workflow)],
                                            cwd=ROOT, env=env, text=True, capture_output=True, timeout=10)
                    self.assertEqual(result.returncode, 0, result.stderr)
                    self.assertEqual(env_file.read_text(),
                                     f"ERIGON_REFERENCE_DIR={reference}\n"
                                     f"ERIGON_REFERENCE_DATA_DIR={reference / 'datadir'}\n")

    def test_release_creation_requires_every_qa_pool(self):
        workflow = (ROOT / ".github/workflows/create-release-branch.yml").read_text()
        self.assertRegex(workflow, r"(?m)^  create-release-branch:\n    needs: \[qa-reference-data\]")
        preflight = workflow.split("  qa-reference-data:\n", 1)[1].split("\n  create-release-branch:", 1)[0]
        self.assertIn("fail-fast: false", preflight)
        self.assertIn("check_qa_reference.py", preflight)
        self.assertIn('RELEASE_BRANCH: ${{ inputs.release_branch }}', preflight)
        for name, _ in WORKFLOWS:
            job = (ROOT / ".github/workflows" / name).read_text()
            labels = re.search(r"runs-on: \[\s*(self-hosted, qa, [^\]]+)\]", job)
            self.assertIsNotNone(labels, name)
            self.assertIn(labels.group(1).strip(), preflight)

    def test_rpc_preflight_runs_before_build_and_database_work(self):
        for workflow, _ in WORKFLOWS:
            with self.subTest(workflow=workflow):
                content = (ROOT / ".github/workflows" / workflow).read_text()
                self.assertLess(content.index("name: Check QA reference data"),
                                content.index("name: Clean Erigon Build Directory"))
                self.assertIn("id: pause_production", content)
                resume = content.split("- name: Resume the Erigon instance dedicated to db maintenance", 1)[1]
                self.assertIn("steps.pause_production.outcome == 'success'", resume.split("run: |", 1)[0])


def make_reference(reference):
    (reference / "datadir/chaindata").mkdir(parents=True)
    (reference / "datadir/snapshots").mkdir()
    (reference / "datadir/chaindata/mdbx.dat").write_bytes(b"fixture database")
    (reference / "production.ini").write_text("[production]\nerigon_repo_commit = abc123\n")
    return reference


class ReferenceDataTests(unittest.TestCase):
    def setUp(self):
        self.tmp = tempfile.TemporaryDirectory(prefix="qa reference ")
        self.addCleanup(self.tmp.cleanup)
        self.root = Path(self.tmp.name)
        self.reference = make_reference(self.root / "reference-version-3.7")
        self.env_file = self.root / "github_env"

    def run_check(self, branch="release/3.7"):
        return subprocess.run([
            sys.executable, str(ROOT / ".github/workflows/scripts/check_qa_reference.py"),
            "--branch", branch, "--chain", "mainnet", "--versions-dir", str(self.root),
            "--github-env", str(self.env_file),
        ], text=True, capture_output=True, timeout=10)

    def assert_unready(self, missing):
        result = self.run_check()
        self.assertNotEqual(result.returncode, 0)
        self.assertIn("QA reference data", result.stderr)
        self.assertIn(missing, result.stderr)
        self.assertFalse(self.env_file.exists(), "unready references must not be exported")

    def test_accepts_complete_reference(self):
        result = self.run_check()
        self.assertEqual(result.returncode, 0, result.stderr)
        self.assertIn(str(self.reference / "datadir"), self.env_file.read_text())

    def test_rejects_missing_database(self):
        (self.reference / "datadir/chaindata/mdbx.dat").unlink()
        self.assert_unready("mdbx.dat")

    def test_rejects_empty_database(self):
        (self.reference / "datadir/chaindata/mdbx.dat").write_bytes(b"")
        self.assert_unready("mdbx.dat")

    def test_rejects_missing_snapshots(self):
        (self.reference / "datadir/snapshots").rmdir()
        self.assert_unready("snapshots")

    def test_rejects_missing_metadata(self):
        (self.reference / "production.ini").unlink()
        self.assert_unready("production.ini")

    def test_rejects_invalid_metadata(self):
        (self.reference / "production.ini").write_text("not an INI file")
        self.assert_unready("production.ini")

    def test_rejects_missing_commit(self):
        (self.reference / "production.ini").write_text("[production]\nerigon_repo_commit =\n")
        self.assert_unready("erigon_repo_commit")

    def test_does_not_fall_back_to_main(self):
        self.reference.rename(self.root / "reference-version")
        self.assert_unready("reference-version-3.7")

    def test_rejects_invalid_release_branch(self):
        for branch in ("release/", "release/../3.7", "release/3.7\ninjected=value"):
            with self.subTest(branch=branch):
                result = self.run_check(branch)
                self.assertNotEqual(result.returncode, 0)
                self.assertIn("release/N.N", result.stderr)
                self.assertFalse(self.env_file.exists())


if __name__ == "__main__":
    unittest.main()
