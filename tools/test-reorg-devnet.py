#!/usr/bin/env python3
"""Bounce a witness between two live CL branches, then verify recovery."""

import argparse
import json
import http.client
import os
import subprocess
import time
import urllib.error
import urllib.request


class RPCUnavailable(RuntimeError):
    pass


def request(url, data=None, method=None):
    body = None if data is None else json.dumps(data).encode()
    req = urllib.request.Request(url, body, {"Content-Type": "application/json"}, method=method)
    with urllib.request.urlopen(req, timeout=10) as response:
        return json.load(response)


def rpc(url, method, *params):
    response = request(url, {"jsonrpc": "2.0", "id": 1, "method": method, "params": params})
    if "error" in response:
        raise RPCUnavailable(f"{url} {method}: {response['error']}")
    return response["result"]


def wait(description, check, timeout=600):
    deadline = time.monotonic() + timeout
    last = None
    while time.monotonic() < deadline:
        try:
            result = check()
            if result:
                return result
        except (urllib.error.URLError, TimeoutError, ConnectionError,
                json.JSONDecodeError, http.client.IncompleteRead, RPCUnavailable) as error:
            last = error
        time.sleep(2)
    raise AssertionError(f"Timed out: {description}; last response error: {last}")


def same_block(blocks):
    return all(block is not None for block in blocks) and len({block["hash"] for block in blocks}) == 1


def run(enclave):
    kurtosis = os.environ.get("KURTOSIS", "kurtosis")

    def cli(*args):
        return subprocess.check_output([kurtosis, *args], text=True, timeout=180).strip()

    def endpoint(service, port):
        address = cli("port", "print", enclave, service, port)
        return "http://" + address.removeprefix("http://").removeprefix("tcp://")

    clients = ["lighthouse", "lighthouse", "teku", "teku", "lighthouse", "lighthouse"]
    execution = [endpoint(f"el-{i}-erigon-{client}", "ws-rpc")
                 for i, client in enumerate(clients, 1)]
    consensus = [endpoint(f"cl-{i}-{client}-erigon", "http") for i, client in enumerate(clients, 1)]
    disruptoor = endpoint("disruptoor", "http")

    def block(node, number="latest"):
        result = rpc(execution[node], "eth_getBlockByNumber", number, False)
        if result is None:
            raise RPCUnavailable(f"node {node+1}: block {number} is not available")
        return result

    def finality():
        return [int(request(url + "/eth/v1/beacon/states/head/finality_checkpoints")
                    ["data"]["finalized"]["epoch"]) for url in consensus]

    def converged(minimum):
        height = min(int(block(i)["number"], 16) for i in range(len(clients))) - 2
        if height < minimum:
            return None
        blocks = [block(i, hex(height)) for i in range(len(clients))]
        return blocks if same_block(blocks) and blocks[0]["transactions"] else None

    def check_state(blocks):
        height = blocks[0]["number"]
        assert len({b["stateRoot"] for b in blocks}) == 1, blocks
        receipts = [rpc(url, "eth_getBlockReceipts", height) for url in execution]
        assert receipts[0], f"no transaction workload at {height}"
        assert all(rows == receipts[0] for rows in receipts), f"receipt disagreement at {height}"
        addresses = {row["from"] for row in receipts[0]}
        addresses.update(row["to"] for row in receipts[0] if row["to"])
        addresses.update(row["contractAddress"] for row in receipts[0] if row["contractAddress"])
        for address in sorted(addresses):
            proofs = [rpc(url, "eth_getProof", address, [], height) for url in execution]
            assert all(proof == proofs[0] for proof in proofs), f"state disagreement for {address} at {height}"

    wait("initial finality on every CL", lambda: min(finality()) >= 2, timeout=1200)
    def partition(groups):
        request(disruptoor + "/v1/state", {"partitions": [{
            "name": "reorg-test", "groups": [
                {"node-index": group, "client-type": ["execution", "beacon"]} for group in groups
            ], "scope": ["el_p2p", "cl_p2p"], "symmetric": True,
        }]}, "PUT")

    stopped_validators = set()

    def validators(node, running):
        cli("service", "start" if running else "stop", enclave, f"vc-{node}-erigon-{clients[node-1]}")
        if running:
            stopped_validators.discard(node)
        else:
            stopped_validators.add(node)

    common = wait("common canonical chain", lambda: converged(1))
    start = int(common[0]["number"], 16)
    # Six equal validator groups: A starts with one, B with two, and node 5
    # only observes. Activating A's two reserves makes A heavier than B without
    # either isolated branch having the two-thirds needed for finality.
    try:
        for node in (2, 5, 6):
            validators(node, False)
        left, right = [1, 2, 5, 6], [3, 4]
        partition([left, right])
        print(json.dumps({"phase": "partition", "groups": [left, right], "split_at": start}), flush=True)

        def split_heads():
            height = min(int(block(i)["number"], 16) for i in range(len(clients))) - 2
            if height < start + 3:
                return None
            heads = [block(i, hex(height)) for i in range(len(clients))]
            if not all(same_block([heads[i-1] for i in group]) for group in (left, right)):
                return None
            if heads[0]["hash"] == heads[2]["hash"]:
                return None
            if any(int(heads[group[0]-1]["gasUsed"], 16) == 0 for group in (left, right)):
                return None
            return heads

        before = wait("two live branches with transactions", split_heads, timeout=1200)
        height = before[0]["number"]
        branch_a, branch_b = before[0]["hash"], before[2]["hash"]
        partition([[1, 2, 6], [3, 4, 5]])
        wait("witness switches from A to B", lambda: block(4, height)["hash"] == branch_b)
        print(json.dumps({"phase": "A_to_B", "height": height, "old": branch_a, "new": branch_b}), flush=True)

        for node in (2, 6):
            validators(node, True)
        partition([[1, 2, 5, 6], [3, 4]])
        wait("witness returns from B to A", lambda: block(4, height)["hash"] == branch_a)
        assert block(0, height)["hash"] == branch_a, "A's original branch was lost"
        assert block(2, height)["hash"] == branch_b, "B's original branch was lost"
        print(json.dumps({"phase": "B_to_A", "height": height, "old": branch_b, "new": branch_a}), flush=True)
    finally:
        request(disruptoor + "/v1/state/clear", {}, "POST")
        for node in list(stopped_validators):
            validators(node, True)

    wait("healed chain advances", lambda: converged(int(height, 16) + 6))
    after = [block(i, height) for i in range(len(clients))]
    assert same_block(after) and after[0]["hash"] == branch_a, "healing lost the restored branch"
    check_state(after)
    for tx_hash in before[2]["transactions"]:
        receipt = rpc(execution[4], "eth_getTransactionReceipt", tx_hash)
        if receipt:
            canonical = block(4, receipt["blockNumber"])
            assert receipt["blockHash"] == canonical["hash"], f"stale receipt for {tx_hash}"

    service = "el-5-erigon-lighthouse"
    cli("service", "stop", enclave, service)
    cli("service", "start", enclave, service)
    execution[4] = endpoint(service, "ws-rpc")
    recovered = wait("restarted witness EL catches up", lambda: converged(int(height, 16) + 12))
    check_state(recovered)
    finalized_after_recovery = max(finality())
    wait("new finalized epoch after recovery", lambda: min(finality()) > finalized_after_recovery, timeout=1200)
    print(json.dumps({"phase": "recovered", "height": recovered[0]["number"], "finalized": finality()}), flush=True)


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("enclave")
    run(parser.parse_args().enclave)
