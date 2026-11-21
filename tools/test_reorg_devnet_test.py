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

# The devnet requests no storage slots. Use the trie fixture's account proof at its original state root.
PROOF_FIXTURE = Path(__file__).resolve().parents[1] / "execution/commitment/trie/proofResponse.json"
ACCOUNT_PROOF = dict(json.loads(PROOF_FIXTURE.read_text()), storageProof=[])
STATE_ROOT = "0x21054df428c339643677a514d1df8414d114b5a858aeb45b7dc9698b189c5ddf"


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

    def test_retry_accepts_empty_responses(self):
        for response in (None, {}, []):
            with self.subTest(response=response):
                self.assertEqual(response, reorg.retry("read", lambda: response))


class ReorgRecoveryTests(unittest.TestCase):
    def run_recovery(self, next_heads, transient_failures=(), *, rpc_overrides=None):
        pending_failures = set(transient_failures)
        branches = ["A"] * 6
        partitioned = healed = restarted = False
        finalized_reads = 0
        elapsed = 0
        checked_heights = []
        running_beacons = set(range(1, 7))
        votes_ready = False
        restarted_at = None
        # One transaction from B's split block returns at another height and index on A.
        split_height = 28
        remined_height = split_height + 1
        remined_tx = f"tx-A-{remined_height}-1"

        def block_at(node, height):
            block_hash = f"{branches[node]}-{height}"
            transactions = [f"tx-{block_hash}-0", f"tx-{block_hash}-1"]
            if branches[node] == "B" and height == split_height:
                transactions[0] = remined_tx
            return {
                "number": hex(height), "hash": block_hash, "stateRoot": STATE_ROOT,
                "transactions": transactions, "gasUsed": "0x1",
            }

        def receipts_at(node, height):
            block = block_at(node, height)
            return [{
                "blockHash": block["hash"], "blockNumber": block["number"],
                "transactionHash": tx, "transactionIndex": hex(i),
                "from": ACCOUNT_PROOF["address"], "to": ACCOUNT_PROOF["address"], "contractAddress": None,
            } for i, tx in enumerate(block["transactions"])]

        def cli(command, **kwargs):
            nonlocal restarted, restarted_at
            if command[0] == "docker":
                if command[1] == "ps":
                    return "\n".join(f"beacon-{node}" for node in range(1, 7))
                node = int(command[2].split("-")[1])
                if command[1] == "pause":
                    running_beacons.remove(node)
                elif command[1] == "unpause":
                    running_beacons.add(node)
                else:
                    self.fail(f"unexpected Docker command: {command}")
                return ""
            if command[1:3] == ["port", "print"]:
                return "http://" + command[4]
            self.assertEqual("service", command[1])
            if command[2] == "start" and command[4].startswith("el-"):
                restarted = True
                restarted_at = elapsed
            return ""

        def request(url, data=None, method=None):
            nonlocal partitioned, healed, finalized_reads
            operation = data.get("method") if data and "method" in data else url.rsplit("/", 1)[-1]
            if operation in pending_failures:
                pending_failures.remove(operation)
                raise ConnectionResetError("interrupted response")
            if url.endswith("/v1/state"):
                returning = partitioned and branches[4] == "B" and 5 in data["partitions"][0]["groups"][0]["node-index"]
                if partitioned and (running_beacons or (returning and not votes_ready)):
                    # Rule replacement briefly reconnects the network; B has more votes.
                    branches[:] = ["B"] * 6
                previous = branches[:]
                partitioned = True
                for group in data["partitions"][0]["groups"]:
                    nodes = group["node-index"]
                    for node in nodes:
                        branches[node - 1] = previous[0] if 1 in nodes else "B"
                return {}
            if url.endswith("/v1/state/clear"):
                healed = True
                branches[:] = ["A"] * 6
                return {}

            node = int(url.split("-")[1]) - 1
            if url.endswith("/debug/fork_choice"):
                return {"fork_choice_nodes": [{
                    "execution_block_hash": f"{branches[node]}-{split_height}",
                    "weight": "3" if node == 0 and votes_ready else "2" if node == 4 else "1",
                }]}
            if url.endswith("/finality_checkpoints"):
                epoch = 2
                if restarted:
                    epoch = [5, 6, 7, 5, 6, 5][node] if finalized_reads < 6 else 8
                    finalized_reads += 1
                return {"data": {"finalized": {"epoch": str(epoch)}}}

            method, params = data["method"], data["params"]
            if method == "eth_getBlockByNumber":
                latest = 110 if healed else split_height + 2 if partitioned else 20
                if restarted:
                    # The witness first lags its peers; only later reads can show new production.
                    latest = next_heads[node] if elapsed > restarted_at else (100 if node == 4 else 110)
                height = latest if params[0] == "latest" else int(params[0], 16)
                result = block_at(node, height) if height <= latest else None
            elif method == "eth_getBlockReceipts":
                height = int(params[0], 16)
                if restarted:
                    checked_heights.append(height)
                result = receipts_at(node, height)
            elif method == "eth_getProof":
                result = ACCOUNT_PROOF
            elif method == "eth_getTransactionReceipt":
                result = receipts_at(node, remined_height)[1] if params[0] == remined_tx else None
            else:
                self.fail(f"unexpected RPC method: {method}")
            if rpc_overrides and method in rpc_overrides:
                result = rpc_overrides[method](result)
            return {"result": result}

        def sleep(seconds):
            nonlocal elapsed, votes_ready
            elapsed += seconds
            votes_ready = True

        output = io.StringIO()
        with (patch.object(reorg.subprocess, "check_output", side_effect=cli),
              patch.object(reorg, "request", side_effect=request),
              patch.object(reorg.time, "monotonic", side_effect=lambda: elapsed),
              patch.object(reorg.time, "sleep", side_effect=sleep),
              redirect_stdout(output)):
            reorg.run("test")
        self.assertEqual(set(range(1, 7)), running_beacons)
        self.assertFalse(pending_failures)
        return json.loads(output.getvalue().splitlines()[-1]), checked_heights

    def test_recovery_rejects_only_catching_up_to_existing_blocks(self):
        with self.assertRaisesRegex(AssertionError, "Timed out: .*execution"):
            self.run_recovery([110] * 6)

    def test_recovery_checks_new_execution_state(self):
        result, checked_heights = self.run_recovery([114] * 6)
        self.assertEqual("recovered", result["phase"])
        self.assertGreater(int(result["height"], 16), 110)
        self.assertEqual(int(result["height"], 16), checked_heights[-1])

    def test_recovery_rejects_receipts_that_do_not_match_the_block(self):
        for name, change in {
            "discarded block": lambda rows: [dict(row, blockHash="discarded") for row in rows],
            "wrong transaction order": lambda rows: rows[::-1],
            "missing transaction": lambda rows: rows[:1],
            "wrong index": lambda rows: [dict(row, transactionIndex="0x0") for row in rows],
        }.items():
            with self.subTest(name=name), self.assertRaisesRegex(AssertionError, "receipt"):
                self.run_recovery([114] * 6, rpc_overrides={"eth_getBlockReceipts": change})

    def test_recovery_rejects_wrong_receipt_for_discarded_transaction(self):
        for name, fields in {
            "another transaction": {"transactionHash": "some-other-tx"},
            "another transaction's index": {"transactionIndex": "0x0"},
            "index past end of block": {"transactionIndex": "0x2"},
            "negative index": {"transactionIndex": "-0x1"},
            "discarded block": {"blockHash": "discarded"},
        }.items():
            with self.subTest(name=name), self.assertRaisesRegex(AssertionError, "receipt"):
                self.run_recovery([114] * 6, rpc_overrides={
                    "eth_getTransactionReceipt": lambda row: dict(row, **fields) if row is not None else None,
                })

    def test_recovery_rejects_proofs_that_do_not_match_the_block(self):
        for name, method, change in [
            ("null proof", "eth_getProof", lambda proof: None),
            ("wrong balance", "eth_getProof", lambda proof: dict(proof, balance="0x1")),
            ("wrong root", "eth_getBlockByNumber", lambda block: dict(block, stateRoot="0x" + "ff" * 32)),
            ("wrong address", "eth_getBlockReceipts", lambda rows: [dict(row, to="0x" + "22" * 20) for row in rows]),
        ]:
            with self.subTest(name=name), self.assertRaisesRegex(AssertionError, "proof"):
                self.run_recovery([114] * 6, rpc_overrides={method: change})

    def test_recovery_retries_transient_reads_and_partition_updates(self):
        result, _ = self.run_recovery([114] * 6, ("state", "eth_getBlockReceipts", "eth_getProof", "clear"))
        self.assertEqual("recovered", result["phase"])


if __name__ == "__main__":
    unittest.main()
