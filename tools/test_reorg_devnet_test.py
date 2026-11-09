import importlib.util
import http.client
import json
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


if __name__ == "__main__":
    unittest.main()
