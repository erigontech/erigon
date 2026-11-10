import importlib.util
import http.client
import io
import json
from contextlib import redirect_stdout
from pathlib import Path
import unittest
from unittest.mock import patch


spec = importlib.util.spec_from_file_location("reorg", Path(__file__).with_name("test-reorg-devnet.py"))
reorg = importlib.util.module_from_spec(spec)
spec.loader.exec_module(reorg)


class ReorgWaitTests(unittest.TestCase):
    def test_wait_retries_partial_responses(self):
        for error in (json.JSONDecodeError("partial JSON", "{", 1), http.client.IncompleteRead(b"{")):
            with self.subTest(error=type(error).__name__), patch.object(reorg.time, "sleep"):
                with patch.object(reorg, "request", side_effect=[error, {"result": "ready"}]) as request:
                    self.assertEqual("ready", reorg.wait("RPC recovery", lambda: reorg.rpc("url", "method")))
                    self.assertEqual(2, request.call_count)

    def test_wait_retries_rpc_error(self):
        responses = [{"error": {"code": -32000, "message": "node is starting"}}, {"result": "ready"}]
        with patch.object(reorg, "request", side_effect=responses), patch.object(reorg.time, "sleep"):
            self.assertEqual("ready", reorg.wait("RPC recovery", lambda: reorg.rpc("url", "method")))

    def test_wait_does_not_hide_programming_errors(self):
        with patch.object(reorg, "rpc", side_effect=TypeError("invalid argument")):
            with self.assertRaisesRegex(TypeError, "invalid argument"):
                reorg.wait("RPC recovery", lambda: reorg.rpc("url", "method"))


class ReorgRecoveryTests(unittest.TestCase):
    def run_recovery(self, next_heads):
        branches = ["A"] * 6
        partitioned = healed = restarted = False
        finalized_reads = 0
        elapsed = 0
        checked_heights = []

        def cli(command, **kwargs):
            nonlocal restarted
            if command[1:3] == ["port", "print"]:
                return "http://" + command[4]
            self.assertEqual("service", command[1])
            if command[2] == "start" and command[4].startswith("el-"):
                restarted = True
            return ""

        def request(url, data=None, method=None):
            nonlocal partitioned, healed, finalized_reads
            if url.endswith("/v1/state"):
                partitioned = True
                for group in data["partitions"][0]["groups"]:
                    nodes = group["node-index"]
                    for node in nodes:
                        branches[node - 1] = "A" if 1 in nodes else "B"
                return {}
            if url.endswith("/v1/state/clear"):
                healed = True
                branches[:] = ["A"] * 6
                return {}

            node = int(url.split("-")[1]) - 1
            if url.endswith("/finality_checkpoints"):
                epoch = 2
                if restarted:
                    epoch = [5, 6, 7, 5, 6, 5][node] if finalized_reads < 6 else 8
                    finalized_reads += 1
                return {"data": {"finalized": {"epoch": str(epoch)}}}

            method, params = data["method"], data["params"]
            if method == "eth_getBlockByNumber":
                latest = 110 if healed else 30 if partitioned else 20
                if restarted:
                    # The witness first lags its peers; only later reads can show new production.
                    latest = next_heads[node] if elapsed else (100 if node == 4 else 110)
                height = latest if params[0] == "latest" else int(params[0], 16)
                if height > latest:
                    return {"result": None}
                return {"result": {
                    "number": hex(height), "hash": f"{branches[node]}-{height}",
                    "stateRoot": f"root-{height}", "transactions": [f"tx-{branches[node]}-{height}"],
                    "gasUsed": "0x1",
                }}
            if method == "eth_getBlockReceipts":
                if restarted:
                    checked_heights.append(int(params[0], 16))
                return {"result": [{"from": "sender", "to": "contract", "contractAddress": None}]}
            if method == "eth_getProof":
                return {"result": {"accountProof": ["proof"]}}
            if method == "eth_getTransactionReceipt":
                return {"result": None}
            self.fail(f"unexpected RPC method: {method}")

        def sleep(seconds):
            nonlocal elapsed
            elapsed += seconds

        output = io.StringIO()
        with (patch.object(reorg.subprocess, "check_output", side_effect=cli),
              patch.object(reorg, "request", side_effect=request),
              patch.object(reorg.time, "monotonic", side_effect=lambda: elapsed),
              patch.object(reorg.time, "sleep", side_effect=sleep),
              redirect_stdout(output)):
            reorg.run("test")
        return json.loads(output.getvalue().splitlines()[-1]), checked_heights

    def test_recovery_rejects_only_catching_up_to_existing_blocks(self):
        with self.assertRaisesRegex(AssertionError, "Timed out: .*execution"):
            self.run_recovery([110] * 6)

    def test_recovery_checks_new_execution_state(self):
        result, checked_heights = self.run_recovery([114] * 6)
        self.assertEqual("recovered", result["phase"])
        self.assertGreater(int(result["height"], 16), 110)
        self.assertEqual(int(result["height"], 16), checked_heights[-1])


if __name__ == "__main__":
    unittest.main()
