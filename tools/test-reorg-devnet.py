#!/usr/bin/env python3
"""Partition and heal a four-node Kurtosis devnet; require observable EL reorgs."""

import argparse
import json
import os
import subprocess
import time
import urllib.error
import urllib.request


def request(url, data=None, method=None):
    body = None if data is None else json.dumps(data).encode()
    req = urllib.request.Request(url, body, {"Content-Type": "application/json"}, method=method)
    with urllib.request.urlopen(req, timeout=10) as response:
        return json.load(response)


def rpc(url, method, *params):
    response = request(url, {"jsonrpc": "2.0", "id": 1, "method": method, "params": params})
    if "error" in response:
        raise RuntimeError(f"{url} {method}: {response['error']}")
    return response["result"]


def wait(description, check, timeout=600):
    deadline = time.monotonic() + timeout
    last = None
    while time.monotonic() < deadline:
        try:
            result = check()
            if result:
                return result
        except (urllib.error.URLError, TimeoutError, ConnectionError) as error:
            last = error
        time.sleep(2)
    raise AssertionError(f"Timed out: {description}; last network error: {last}")


def same_block(blocks):
    return all(block is not None for block in blocks) and len({block["hash"] for block in blocks}) == 1


def run(enclave):
    kurtosis = os.environ.get("KURTOSIS", "kurtosis")

    def cli(*args):
        return subprocess.check_output([kurtosis, *args], text=True, timeout=180).strip()

    def endpoint(service, port):
        address = cli("port", "print", enclave, service, port)
        return "http://" + address.removeprefix("http://").removeprefix("tcp://")

    clients = ["lighthouse", "lighthouse", "teku", "teku"]
    execution = [endpoint(f"el-{i}-erigon-{client}", "ws-rpc")
                 for i, client in enumerate(clients, 1)]
    consensus = [endpoint(f"cl-{i}-{client}-erigon", "http") for i, client in enumerate(clients, 1)]
    disruptoor = endpoint("disruptoor", "http")

    def block(node, number="latest"):
        return rpc(execution[node], "eth_getBlockByNumber", number, False)

    def finality():
        return [int(request(url + "/eth/v1/beacon/states/head/finality_checkpoints")
                    ["data"]["finalized"]["epoch"]) for url in consensus]

    def converged(minimum):
        height = min(int(block(i)["number"], 16) for i in range(4)) - 2
        if height < minimum:
            return None
        blocks = [block(i, hex(height)) for i in range(4)]
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
    # Each half owns half the validators, so neither competing branch can finalize.
    for round_number, left in enumerate(([1, 2], [1, 3]), 1):
        common = wait("common canonical chain", lambda: converged(1))
        start = int(common[0]["number"], 16)
        finalized_before = min(finality())
        right = sorted(set(range(1, 5)) - set(left))
        print(json.dumps({"round": round_number, "split_at": start, "groups": [left, right]}), flush=True)
        partition = {"partitions": [{
            "name": "reorg-test", "groups": [
                {"node-index": group, "client-type": ["execution", "beacon"]} for group in (left, right)
            ], "scope": ["el_p2p", "cl_p2p"], "symmetric": True,
        }]}
        try:
            request(disruptoor + "/v1/state", partition, "PUT")

            def split_heads():
                height = min(int(block(i)["number"], 16) for i in range(4)) - 2
                if height < start + 6:
                    return None
                heads = [block(i, hex(height)) for i in range(4)]
                if not all(same_block([heads[i-1] for i in group]) for group in (left, right)):
                    return None
                if heads[left[0]-1]["hash"] == heads[right[0]-1]["hash"]:
                    return None
                if any(int(heads[group[0]-1]["gasUsed"], 16) == 0 for group in (left, right)):
                    return None
                return heads

            before = wait("two distinct canonical branches with transactions", split_heads, timeout=300)
        finally:
            request(disruptoor + "/v1/state/clear", {}, "POST")

        height = before[0]["number"]
        wait("healed chain advances", lambda: converged(int(height, 16) + 6))
        after = [block(i, height) for i in range(4)]
        assert same_block(after), f"canonical disagreement at healed height {height}"
        losers = [i for i in range(4) if before[i]["hash"] != after[i]["hash"]]
        assert losers, "convergence without a witnessed reorg is not a successful test"
        check_state(after)
        for i in losers:
            for tx_hash in before[i]["transactions"]:
                receipt = rpc(execution[i], "eth_getTransactionReceipt", tx_hash)
                if receipt:
                    canonical = block(i, receipt["blockNumber"])
                    assert receipt["blockHash"] == canonical["hash"], f"stale receipt for {tx_hash}"
        print(json.dumps({"round": round_number, "height": height,
                          "old_hashes": [b["hash"] for b in before],
                          "canonical": after[0]["hash"], "reorged_nodes": [i+1 for i in losers]}), flush=True)

        node = losers[0]
        service = f"el-{node+1}-erigon-{clients[node]}"
        cli("service", "stop", enclave, service)
        cli("service", "start", enclave, service)
        execution[node] = endpoint(service, "ws-rpc")
        recovered = wait("restarted EL catches up", lambda: converged(int(height, 16) + 12))
        check_state(recovered)
        wait("fresh finality after recovery", lambda: min(finality()) > finalized_before, timeout=1200)
    print("Reorgs, state agreement, restart and fresh finality verified", flush=True)


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("enclave")
    run(parser.parse_args().enclave)
