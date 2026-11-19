#!/usr/bin/env python3
"""Bounce a witness between two live CL branches, then verify recovery."""

import argparse
from contextlib import ExitStack
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


def retry(description, operation):
    # Empty or null responses can be valid; only transport/RPC errors need a retry.
    return wait(description, lambda: (operation(),), timeout=60)[0]


def same_block(blocks):
    return all(block is not None for block in blocks) and len({block["hash"] for block in blocks}) == 1


def run(enclave):
    kurtosis = os.environ.get("KURTOSIS", "kurtosis")

    def cli(*args):
        return subprocess.check_output([kurtosis, *args], text=True, timeout=180).strip()

    def docker(*args):
        return subprocess.check_output(["docker", *args], text=True, timeout=180).strip()

    def endpoint(service, port):
        address = cli("port", "print", enclave, service, port)
        return "http://" + address.removeprefix("http://").removeprefix("tcp://")

    clients = ["lighthouse", "lighthouse", "teku", "teku", "lighthouse", "lighthouse"]
    node_ids = range(1, len(clients) + 1)
    branch_a_node, branch_b_node, witness = 1, 3, 5
    execution = [endpoint(f"el-{i}-erigon-{client}", "ws-rpc")
                 for i, client in enumerate(clients, 1)]
    consensus = [endpoint(f"cl-{i}-{client}-erigon", "http") for i, client in enumerate(clients, 1)]
    disruptoor = endpoint("disruptoor", "http")

    def block(node, number="latest"):
        result = rpc(execution[node-1], "eth_getBlockByNumber", number, False)
        if result is None:
            raise RPCUnavailable(f"node {node}: block {number} is not available")
        return result

    def finality():
        return [int(request(url + "/eth/v1/beacon/states/head/finality_checkpoints")
                    ["data"]["finalized"]["epoch"]) for url in consensus]

    def branch_weight(node, block_hash):
        nodes = request(consensus[node-1] + "/eth/v1/debug/fork_choice")["fork_choice_nodes"]
        for entry in nodes:
            if entry["execution_block_hash"] == block_hash:
                return int(entry["weight"])
        raise RPCUnavailable(f"node {node}: fork choice has no block {block_hash}")

    def converged(minimum):
        height = min(int(block(node)["number"], 16) for node in node_ids) - 2
        if height < minimum:
            return None
        blocks = [block(node, hex(height)) for node in node_ids]
        return blocks if same_block(blocks) and blocks[0]["transactions"] else None

    def check_state(blocks):
        height = blocks[0]["number"]
        assert len({b["stateRoot"] for b in blocks}) == 1, blocks
        receipts = retry(f"receipts at {height}", lambda: [rpc(url, "eth_getBlockReceipts", height) for url in execution])
        assert receipts[0], f"no transaction workload at {height}"
        assert all(rows == receipts[0] for rows in receipts), f"receipt disagreement at {height}"
        addresses = {row["from"] for row in receipts[0]}
        addresses.update(row["to"] for row in receipts[0] if row["to"])
        addresses.update(row["contractAddress"] for row in receipts[0] if row["contractAddress"])
        for address in sorted(addresses):
            proofs = retry(f"proofs for {address} at {height}",
                           lambda: [rpc(url, "eth_getProof", address, [], height) for url in execution])
            assert all(proof == proofs[0] for proof in proofs), f"state disagreement for {address} at {height}"

    wait("initial finality on every CL", lambda: min(finality()) >= 2, timeout=1200)
    partitioned = False

    def partition(groups):
        nonlocal partitioned
        with ExitStack() as resume:
            # Disruptoor clears old rules before installing replacements. Pause
            # beacon clients during that gap, keeping their network namespaces alive.
            if partitioned:
                containers = docker("ps", "-q", "--filter", f"label=com.kurtosistech.enclave-name={enclave}",
                                    "--filter", "label=com.kurtosistech.custom.ethereum-package.client-type=beacon").split()
                assert len(containers) == len(clients), f"expected {len(clients)} beacon containers, found {containers}"
                for container in containers:
                    docker("pause", container)
                    resume.callback(docker, "unpause", container)
            retry("install network partition", lambda: request(disruptoor + "/v1/state", {"partitions": [{
                "name": "reorg-test", "groups": [
                    {"node-index": group, "client-type": ["execution", "beacon"]} for group in groups
                ], "scope": ["el_p2p", "cl_p2p"], "symmetric": True,
            }]}, "PUT"))
            partitioned = True

    stopped_validators = set()

    def validators(node, running):
        cli("service", "start" if running else "stop", enclave, f"vc-{node}-erigon-{clients[node-1]}")
        if running:
            stopped_validators.discard(node)
        else:
            stopped_validators.add(node)

    common = wait("common canonical chain", lambda: converged(1))
    start = int(common[0]["number"], 16)
    # Six equal validator groups: A starts with one, B with two, and the witness
    # only observes. Activating A's two reserves makes A heavier than B without
    # either isolated branch having the two-thirds needed for finality.
    try:
        for node in (2, witness, 6):
            validators(node, False)
        left, right = [branch_a_node, 2, witness, 6], [branch_b_node, 4]
        partition([left, right])
        print(json.dumps({"phase": "partition", "groups": [left, right], "split_at": start}), flush=True)

        def split_heads():
            height = min(int(block(node)["number"], 16) for node in node_ids) - 2
            if height < start + 3:
                return None
            heads = [block(node, hex(height)) for node in node_ids]
            if not all(same_block([heads[node-1] for node in group]) for group in (left, right)):
                return None
            if heads[branch_a_node-1]["hash"] == heads[branch_b_node-1]["hash"]:
                return None
            if any(int(heads[group[0]-1]["gasUsed"], 16) == 0 for group in (left, right)):
                return None
            return heads

        before = wait("two live branches with transactions", split_heads, timeout=1200)
        height = before[0]["number"]
        branch_a, branch_b = before[branch_a_node-1]["hash"], before[branch_b_node-1]["hash"]
        partition([[branch_a_node, 2, 6], [branch_b_node, 4, witness]])
        wait("witness switches from A to B", lambda: block(witness, height)["hash"] == branch_b)
        print(json.dumps({"phase": "A_to_B", "height": height, "old": branch_a, "new": branch_b}), flush=True)

        for node in (2, 6):
            validators(node, True)
        wait("A has greater fork-choice weight than B", lambda: branch_weight(branch_a_node, branch_a) > branch_weight(witness, branch_b))
        partition([left, right])
        wait("witness returns from B to A", lambda: block(witness, height)["hash"] == branch_a)
        assert retry("read branch A", lambda: block(branch_a_node, height))["hash"] == branch_a, "A's original branch was lost"
        assert retry("read branch B", lambda: block(branch_b_node, height))["hash"] == branch_b, "B's original branch was lost"
        print(json.dumps({"phase": "B_to_A", "height": height, "old": branch_b, "new": branch_a}), flush=True)
    finally:
        retry("clear network partition", lambda: request(disruptoor + "/v1/state/clear", {}, "POST"))
        for node in list(stopped_validators):
            validators(node, True)

    wait("healed chain advances", lambda: converged(int(height, 16) + 6))
    after = retry("read healed branches", lambda: [block(node, height) for node in node_ids])
    assert same_block(after) and after[branch_a_node-1]["hash"] == branch_a, "healing lost the restored branch"
    check_state(after)
    for tx_hash in before[branch_b_node-1]["transactions"]:
        receipt = retry(f"receipt for {tx_hash}", lambda: rpc(execution[witness-1], "eth_getTransactionReceipt", tx_hash))
        if receipt:
            canonical = retry("receipt's canonical block", lambda: block(witness, receipt["blockNumber"]))
            assert receipt["blockHash"] == canonical["hash"], f"stale receipt for {tx_hash}"

    service = f"el-{witness}-erigon-{clients[witness-1]}"
    cli("service", "stop", enclave, service)
    cli("service", "start", enclave, service)
    execution[witness-1] = endpoint(service, "ws-rpc")
    recovered = wait("restarted witness EL catches up", lambda: converged(int(height, 16) + 12))
    check_state(recovered)
    # Convergence trails the slowest node; require a block beyond every current EL head.
    head_after_recovery = retry("heads after recovery", lambda: max(int(block(node)["number"], 16) for node in node_ids))
    finalized_after_recovery = max(retry("finality after recovery", finality))
    recovered = wait("new execution blocks after recovery", lambda: converged(head_after_recovery + 1))
    check_state(recovered)

    def new_finality():
        epochs = finality()
        return epochs if min(epochs) > finalized_after_recovery else None

    finalized = wait("new finalized epoch after recovery", new_finality, timeout=1200)
    print(json.dumps({"phase": "recovered", "height": recovered[0]["number"], "finalized": finalized}), flush=True)


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("enclave")
    run(parser.parse_args().enclave)
