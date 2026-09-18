#!/usr/bin/env python3
"""Check Pectra's validator fixtures and required consensus postconditions."""

import json
import os
from pathlib import Path
import shutil
import subprocess
import tempfile
import unittest


ROOT = Path(__file__).resolve().parents[3]
FIXTURES = ROOT / ".github/workflows/kurtosis"


def load_yaml(path):
    return json.loads(subprocess.check_output(["yq", "-o=json", ".", str(path)]))


def load(name):
    return load_yaml(FIXTURES / name)


def tasks(value):
    if isinstance(value, dict):
        if "name" in value:
            yield value
        for child in value.values():
            yield from tasks(child)
    elif isinstance(value, list):
        for child in value:
            yield from tasks(child)


class PectraFixtures(unittest.TestCase):
    def test_request_tests_share_genesis_keys_but_run_independently(self):
        suite = load("pectra.io")
        mnemonic = suite["network_params"]["preregistered_validator_keys_mnemonic"]
        for test in suite["assertoor_params"]["tests"]:
            if "/el-triggered-" not in test["file"]:
                continue
            with self.subTest(playbook=test["file"]):
                self.assertEqual(test["config"]["validatorMnemonic"], mnemonic)
                self.assertEqual(test["schedule"], {"startup": True, "skipQueue": True})

    def test_ci_loads_playbooks_from_the_tested_revision(self):
        workflow = load_yaml(ROOT / ".github/workflows/test-kurtosis-assertoor.yml")
        steps = [step for job in workflow["jobs"].values() for step in job.get("steps", [])]
        rewrite = next((step for step in steps if step.get("id") == "pectra-test-revision"), None)
        self.assertIsNotNone(rewrite, "CI must not run main's playbooks when testing a PR")
        with tempfile.TemporaryDirectory() as directory:
            path = Path(directory) / ".github/workflows/kurtosis/pectra.io"
            path.parent.mkdir(parents=True)
            shutil.copyfile(FIXTURES / "pectra.io", path)
            env = dict(os.environ, TEST_REPOSITORY="example/erigon", TEST_REVISION="a" * 40)
            subprocess.run(["bash", "-e", "-c", rewrite["run"]], cwd=directory, env=env, check=True)
            for test in load_yaml(path)["assertoor_params"]["tests"]:
                self.assertTrue(test["file"].startswith(f"https://raw.githubusercontent.com/example/erigon/{'a' * 40}/"))

    def test_genesis_validators_exist_and_tests_do_not_share_them(self):
        suite = load("pectra.io")
        network = suite["network_params"]
        count = network.get("num_validator_keys_per_node", 64) * len(suite["participants_matrix"]["cl"])
        used = set()
        for name in ("el-triggered-consolidations-test.io", "el-triggered-withdrawal.io", "el-triggered-exit.io"):
            config = load(name)["config"]
            indices = [value for key, value in config.items() if key.endswith("ValidatorIndex") or key == "validatorIndex"]
            with self.subTest(playbook=name):
                self.assertTrue(indices)
                self.assertTrue(all(0 <= index < count for index in indices), (indices, count))
                self.assertFalse(used.intersection(indices), "independent tests must use different validators")
            used.update(indices)

    def test_consolidation_has_churn_capacity(self):
        suite = load("pectra.io")
        network = suite["network_params"]
        count = network.get("num_validator_keys_per_node", 64) * len(suite["participants_matrix"]["cl"])
        # Electra reserves up to 256 ETH of churn for activation and exit.
        self.assertGreater(count * 32 // network["churn_limit_quotient"] - 256, 32)

    def test_requests_have_consensus_postconditions(self):
        for name in ("el-triggered-consolidations-test.io", "el-triggered-withdrawal.io", "el-triggered-exit.io"):
            playbook = load(name)
            all_tasks = list(tasks(playbook["tasks"]))
            with self.subTest(playbook=name):
                self.assertTrue(any(task["name"] == "generate_bls_changes" for task in all_tasks))
                self.assertTrue(any(task["name"] == "check_consensus_slot_range" for task in all_tasks))
                if name == "el-triggered-consolidations-test.io":
                    self.assertTrue(any(task.get("config", {}).get("maxValidatorBalance") == 0 for task in all_tasks))
                    self.assertTrue(any(task.get("config", {}).get("minValidatorBalance", 0) > 32_000_000_000 for task in all_tasks))
                else:
                    self.assertTrue(any("expectWithdrawals" in task.get("configVars", {}) for task in all_tasks))


if __name__ == "__main__":
    unittest.main()
