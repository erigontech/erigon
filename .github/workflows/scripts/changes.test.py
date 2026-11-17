#!/usr/bin/env python3
import os
from pathlib import Path
import subprocess
import tempfile
import unittest

import yaml


WORKFLOWS = Path(__file__).resolve().parents[1]


class ChangesTest(unittest.TestCase):
    def setUp(self):
        self.work = tempfile.TemporaryDirectory()
        self.addCleanup(self.work.cleanup)
        self.repo = Path(self.work.name)
        self.git("init", "-q")
        (self.repo / "code.go").write_text("package main\n")
        self.git("add", ".")
        self.git("commit", "-qm", "base")
        self.base = self.git("rev-parse", "HEAD")

    def git(self, *args):
        return subprocess.check_output(
            ["git", "-c", "user.name=Test", "-c", "user.email=test@example.com",
             "-c", "commit.gpgsign=false", "-c", "core.hooksPath=/dev/null", *args],
            cwd=self.repo, text=True,
        ).strip()

    def commit_file(self, name):
        path = self.repo / name
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_text("changed\n")
        self.git("add", ".")
        self.git("commit", "-qm", "change")

    def detect(self, event="merge_group"):
        workflow = yaml.safe_load((WORKFLOWS / "changes.yml").read_text())
        step = next(s for s in workflow["jobs"]["check"]["steps"] if s.get("id") == "check")
        context = {
            "github.event_name": event,
            "github.repository": "test/repo",
            "github.event.pull_request.number": "1",
            "github.event.merge_group.base_sha": self.base,
            "github.event.merge_group.head_sha": self.git("rev-parse", "HEAD"),
            "github.token": "",
        }

        def render(value):
            for key, replacement in context.items():
                value = value.replace("${{ " + key + " }}", replacement)
            return value

        output = self.repo / "output"
        env = dict(os.environ, GITHUB_EVENT_NAME=event, GITHUB_OUTPUT=str(output),
                   PATH=str(self.repo) + os.pathsep + os.environ["PATH"])
        env.update({key: render(value) for key, value in step.get("env", {}).items()})
        subprocess.run(["bash", "-e", "-o", "pipefail", "-c", render(step["run"])],
                       cwd=self.repo, env=env, check=True, capture_output=True, text=True)
        return dict(line.split("=", 1) for line in output.read_text().splitlines())

    def test_docs_only_merge_group(self):
        self.commit_file("docs/site/docs/intro.md")
        self.assertEqual(self.detect(), {"code": "false", "docs_site": "true"})

    def test_non_ascii_docs_path(self):
        self.git("config", "core.quotePath", "true")
        self.commit_file("docs/site/docs/café.md")
        self.assertEqual(self.detect(), {"code": "false", "docs_site": "true"})

    def test_docs_outside_site(self):
        for name in ("docs/design.md", "README.md", "llms-full.txt"):
            self.commit_file(name)
        self.assertEqual(self.detect(), {"code": "false", "docs_site": "false"})

    def test_mixed_merge_group_includes_earlier_code_change(self):
        self.commit_file("execution/code.go")
        self.commit_file("docs/site/docs/intro.md")
        self.assertEqual(self.detect(), {"code": "true", "docs_site": "true"})

    def test_nested_markdown_is_code(self):
        self.commit_file("execution/README.md")
        self.assertEqual(self.detect(), {"code": "true", "docs_site": "false"})

    def test_deleted_code_is_code(self):
        self.git("rm", "code.go")
        self.git("commit", "-qm", "delete")
        self.assertEqual(self.detect(), {"code": "true", "docs_site": "false"})

    def test_code_renamed_into_docs_is_code(self):
        (self.repo / "docs/site").mkdir(parents=True)
        self.git("mv", "code.go", "docs/site/example.md")
        self.git("commit", "-qm", "rename")
        self.assertEqual(self.detect(), {"code": "true", "docs_site": "true"})

    def test_empty_diff_keeps_code_checks(self):
        self.assertEqual(self.detect(), {"code": "true", "docs_site": "false"})

    def test_missing_base_fails(self):
        self.base = "0" * 40
        with self.assertRaises(subprocess.CalledProcessError):
            self.detect()

    def test_other_events_keep_full_coverage(self):
        for event in ("push", "schedule", "workflow_dispatch"):
            with self.subTest(event=event):
                self.assertEqual(self.detect(event), {"code": "true", "docs_site": "true"})

    def test_pull_request_still_uses_file_api(self):
        gh = self.repo / "gh"
        gh.write_text("#!/usr/bin/env bash\nprintf '%s\\n' docs/site/docs/intro.md\n")
        gh.chmod(0o755)
        self.assertEqual(self.detect("pull_request"), {"code": "false", "docs_site": "true"})

    def test_pull_request_api_failure_fails(self):
        gh = self.repo / "gh"
        gh.write_text("#!/usr/bin/env bash\nexit 1\n")
        gh.chmod(0o755)
        with self.assertRaises(subprocess.CalledProcessError):
            self.detect("pull_request")

    def test_hive_eest_uses_change_detection(self):
        workflow = yaml.safe_load((WORKFLOWS / "ci-gate.yml").read_text())
        job = workflow["jobs"]["hive-eest"]
        self.assertIn("changes", job.get("needs", []))
        self.assertIn("needs.changes.outputs.code != 'false'", job["if"])


if __name__ == "__main__":
    unittest.main()
