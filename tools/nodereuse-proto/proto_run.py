#!/usr/bin/env python3
"""Node-reuse prototype for the hive consume-rlp simulator, part 2: run both arms.

  before: one docker container per test — create, start, wait for the RPC
          endpoint, two getBlockByNumber calls, stop+remove (the current
          single_test_client architecture).
  after:  one long-lived container; per test a docker exec resets it
          (kill node, wipe datadir, map genesis, erigon init, import blocks,
          boot node) and the same RPC calls run against the reused container.

Writes one JSON line per test to results-<arm>.jsonl and a summary to
summary-<arm>.json.
"""
import json
import os
import statistics
import subprocess
import sys
import time
import urllib.request
from pathlib import Path

HERE = Path(__file__).resolve().parent
PORT = 18545
RPC = f"http://127.0.0.1:{PORT}"
RUNNER = "rlp-proto-runner"
RESET_SCRIPT = os.environ.get("RESET_SCRIPT", "reset_run.sh")

BASE_ENV = {
    "HIVE_NETWORK_ID": "1337",
    "HIVE_CHAIN_ID": "1337",
    "HIVE_NODETYPE": "full",
    "HIVE_CHECK_LIVE_PORT": "8545",
    "HIVE_KZG_FAST_SETUP": "1",
    "HIVE_FORK_DAO_VOTE": "1",
}

FLAGS = (
    "--fakepow --datadir /erigon-hive-datadir --db.size.limit 2GB"
    " --prune.include-commitment-history --networkid 1337"
    " --txpool.blobslots=1000 --txpool.totalblobpoollimit=10000"
    " --sync.parallel-state-flushing=false --externalcl"
    " --http --http.addr=0.0.0.0 --http.api=admin,debug,trace,eth,net,txpool,web3,testing"
    " --ws --ws.port=8546"
    " --authrpc.addr=0.0.0.0 --authrpc.jwtsecret=/jwt.secret"
)


def sh(*args: str) -> subprocess.CompletedProcess:
    return subprocess.run(args, capture_output=True, text=True)


def rpc(method: str, params: list) -> float:
    body = json.dumps(
        {"jsonrpc": "2.0", "id": 1, "method": method, "params": params}
    ).encode()
    t0 = time.perf_counter()
    req = urllib.request.Request(
        RPC, data=body, headers={"Content-Type": "application/json"}
    )
    with urllib.request.urlopen(req, timeout=10) as resp:
        json.load(resp)
    return time.perf_counter() - t0


def wait_ready(deadline: float = 90.0) -> bool:
    end = time.perf_counter() + deadline
    while time.perf_counter() < end:
        try:
            rpc("eth_chainId", [])
            return True
        except Exception:
            time.sleep(0.05)
    return False


def env_for(test: dict, rulesets: dict, with_flags: bool = False) -> list:
    env = dict(BASE_ENV)
    env.update(rulesets.get(test["fork"], {}))
    if with_flags:
        env["FLAGS"] = FLAGS
    return [f"-e{k}={v}" for k, v in env.items()]


def run_before(test: dict, rulesets: dict, image: str) -> dict:
    seg = {}
    t0 = time.perf_counter()
    c = sh(
        "docker", "create", "--name", RUNNER,
        "-p", f"127.0.0.1:{PORT}:8545",
        *env_for(test, rulesets),
        image,
    )
    if c.returncode != 0:
        return {"error": "create", "detail": c.stderr[-300:]}
    cp = sh(
        "docker", "cp", f"{test['dir']}/genesis-input.json", f"{RUNNER}:/genesis.json"
    )
    cpb = sh("docker", "cp", f"{test['dir']}/blocks", f"{RUNNER}:/blocks")
    if cp.returncode != 0 or cpb.returncode != 0:
        sh("docker", "rm", "-f", RUNNER)
        return {"error": "cp", "detail": (cp.stderr + cpb.stderr)[-300:]}
    seg["create"] = time.perf_counter() - t0

    t0 = time.perf_counter()
    s = sh("docker", "start", RUNNER)
    if s.returncode != 0:
        sh("docker", "rm", "-f", RUNNER)
        return {"error": "start", "detail": s.stderr[-300:]}
    ok = wait_ready()
    seg["start_ready"] = time.perf_counter() - t0

    t0 = time.perf_counter()
    if ok:
        rpc("eth_getBlockByNumber", ["0", False])
        rpc("eth_getBlockByNumber", ["latest", False])
    seg["rpc"] = time.perf_counter() - t0

    t0 = time.perf_counter()
    sh("docker", "rm", "-f", RUNNER)
    seg["teardown"] = time.perf_counter() - t0
    seg["total"] = sum(v for k, v in seg.items() if isinstance(v, float))
    return seg


def start_runner(image: str, rulesets: dict) -> None:
    sh("docker", "rm", "-f", RUNNER)
    c = sh(
        "docker", "run", "-d", "--name", RUNNER,
        "-p", f"127.0.0.1:{PORT}:8545",
        "-v", f"{HERE}:/proto",
        "--entrypoint", "/bin/bash",
        image, "-c", "sleep infinity",
    )
    if c.returncode != 0:
        raise RuntimeError(f"runner start failed: {c.stderr[-300:]}")
    time.sleep(1)


def run_after(test: dict, rulesets: dict) -> dict:
    seg = {}
    env = env_for(test, rulesets, with_flags=True)
    t0 = time.perf_counter()
    e = sh(
        "docker", "exec", *env, RUNNER,
        "/bin/bash", f"/proto/{RESET_SCRIPT}", "/proto/" + Path(test["dir"]).name,
    )
    seg["reset_boot"] = time.perf_counter() - t0
    ok = e.returncode == 0

    t0 = time.perf_counter()
    if ok:
        rpc("eth_getBlockByNumber", ["0", False])
        rpc("eth_getBlockByNumber", ["latest", False])
    seg["rpc"] = time.perf_counter() - t0
    if not ok:
        seg["error"] = "exec"
        seg["detail"] = e.stderr[-300:]
    seg["total"] = seg["reset_boot"] + seg["rpc"]
    return seg


def main() -> None:
    arm, manifest_path, rulesets_path, image = sys.argv[1:5]
    manifest = json.load(open(manifest_path))
    rulesets = json.load(open(rulesets_path))
    out = open(HERE / f"results-{arm}.jsonl", "w")
    times = []
    for i, test in enumerate(manifest):
        if arm == "before":
            seg = run_before(test, rulesets, image)
        else:
            if i == 0:
                start_runner(image, rulesets)
                subprocess.run(
                    ["docker", "cp", str(HERE / "reset_run.sh"),
                     f"{RUNNER}:/proto/reset_run.sh"],
                    capture_output=True,
                )
            seg = run_after(test, rulesets)
        out.write(json.dumps({"i": i, **seg}) + "\n")
        out.flush()
        if "total" in seg:
            times.append(seg["total"])
        print(f"[{i + 1}/{len(manifest)}] {seg.get('total', 'ERR'):>7.2f}s {test['fork']}")
    summary = {
        "tests": len(times),
        "errors": len(manifest) - len(times),
        "total_s": sum(times),
        "mean_s": statistics.mean(times) if times else None,
        "median_s": statistics.median(times) if times else None,
        "per_16263_tests_scaled_min": (statistics.mean(times) * 16263 / 60) if times else None,
    }
    (HERE / f"summary-{arm}.json").write_text(json.dumps(summary, indent=1))
    print(json.dumps(summary, indent=1))


if __name__ == "__main__":
    main()
