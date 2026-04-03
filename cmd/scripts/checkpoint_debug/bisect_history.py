#!/usr/bin/env python3
"""
Binary search for the exact block where Erigon's historical account nonce
diverges from a reference RPC.

Usage:
    python3 bisect_history.py [--rpc URL] [--address ADDR] [--from BLOCK] [--to BLOCK] [--ef-dump FILE]

If --ef-dump is provided, Erigon nonces are extracted from the dump-slot-history
JSON file. Otherwise, you must provide Erigon nonces externally.
"""

import argparse
import json
import struct
import sys
import time
import urllib.request


RPC_ENDPOINTS = [
    "https://sepolia.drpc.org",
    "https://ethereum-sepolia-rpc.publicnode.com",
    "https://rpc.sepolia.org",
    "https://rpc2.sepolia.org",
]


def rpc_nonce_at_block(rpc_url, address, block_num, retries=3):
    """Query eth_getTransactionCount at a specific block."""
    endpoints = [rpc_url] if rpc_url not in RPC_ENDPOINTS else RPC_ENDPOINTS
    payload = json.dumps({
        "jsonrpc": "2.0",
        "method": "eth_getTransactionCount",
        "params": [address, hex(block_num)],
        "id": 1,
    }).encode()

    last_err = None
    for endpoint in endpoints:
        for attempt in range(retries):
            try:
                req = urllib.request.Request(endpoint, data=payload,
                                             headers={
                                                 "Content-Type": "application/json",
                                                 "User-Agent": "Mozilla/5.0",
                                             })
                with urllib.request.urlopen(req, timeout=30) as resp:
                    data = json.loads(resp.read())
                if "error" in data:
                    last_err = RuntimeError(f"RPC error from {endpoint}: {data['error']}")
                    break  # try next endpoint
                return int(data["result"], 16)
            except Exception as e:
                last_err = e
                time.sleep(1 * (attempt + 1))
    raise last_err


def decode_nonce_v3(hex_value):
    """Decode nonce from Erigon V3 account serialization.

    Format: each field is length_byte + big-endian bytes.
    Fields in order: nonce, balance, codeHash, incarnation.
    length_byte=0 means field is zero/empty.
    """
    b = bytes.fromhex(hex_value.replace("0x", ""))
    if len(b) == 0:
        return 0
    nonce_len = b[0]
    if nonce_len == 0:
        return 0
    nonce_bytes = b[1:1 + nonce_len]
    return int.from_bytes(nonce_bytes, "big")


def load_ef_nonces_by_block(ef_dump_path, from_block, to_block):
    """Parse EF dump JSON and extract the last nonce (valueHex) per block in range.

    valueHex = GetAsOf(txNum+1) = state AFTER this write.
    For each block, the last EF entry's valueHex is the post-block state.
    """
    block_nonces = {}
    with open(ef_dump_path) as f:
        data = json.load(f)

    for entry in data.get("efEntries", []):
        bn = entry.get("blockNum", 0)
        if bn < from_block or bn > to_block:
            continue
        val = entry.get("valueHex", "0x0")
        if val in ("0x0", "<not_found>"):
            nonce = 0
        else:
            nonce = decode_nonce_v3(val)
        # Last entry per block wins (post-block state)
        block_nonces[bn] = nonce

    return block_nonces


def bisect(rpc_url, address, from_block, to_block, ef_nonces=None):
    """Binary search for first block where Erigon nonce != RPC nonce."""
    lo, hi = from_block, to_block
    first_bad = None

    print(f"Binary search: blocks {lo} → {hi}")
    print(f"Address: {address}")
    print()

    iteration = 0
    while lo <= hi:
        mid = (lo + hi) // 2
        iteration += 1

        # Get RPC nonce
        rpc_nonce = rpc_nonce_at_block(rpc_url, address, mid)
        time.sleep(0.2)  # rate limit

        # Get Erigon nonce
        erigon_nonce = None
        if ef_nonces:
            # Find closest block <= mid that has an EF entry
            # The EF entry gives the post-block nonce for that block
            closest = None
            for bn in sorted(ef_nonces.keys()):
                if bn <= mid:
                    closest = bn
                else:
                    break
            if closest is not None:
                erigon_nonce = ef_nonces[closest]

        match = erigon_nonce == rpc_nonce if erigon_nonce is not None else None
        status = "MATCH" if match else ("MISMATCH" if match is not None else "NO_DATA")

        print(f"  [{iteration:2d}] block {mid}: rpc_nonce={rpc_nonce}, erigon_nonce={erigon_nonce}, {status}")

        if match is None:
            # No Erigon data for this block, try to move towards blocks with data
            hi = mid - 1
            continue

        if not match:
            first_bad = mid
            hi = mid - 1
        else:
            lo = mid + 1

    return first_bad


def main():
    parser = argparse.ArgumentParser(description="Binary search for history divergence block")
    parser.add_argument("--rpc", default="https://sepolia.drpc.org", help="RPC endpoint")
    parser.add_argument("--address", default="0x32ae87a8176c6910b5e70569dd60bbd9627830a4")
    parser.add_argument("--from", dest="from_block", type=int, default=2679324)
    parser.add_argument("--to", dest="to_block", type=int, default=2693198)
    parser.add_argument("--ef-dump", dest="ef_dump", help="Path to dump-slot-history JSON file")
    args = parser.parse_args()

    ef_nonces = None
    if args.ef_dump:
        print(f"Loading EF dump from {args.ef_dump}...")
        ef_nonces = load_ef_nonces_by_block(args.ef_dump, args.from_block, args.to_block)
        print(f"  Loaded nonces for {len(ef_nonces)} blocks")
        print()

    first_bad = bisect(args.rpc, args.address, args.from_block, args.to_block, ef_nonces)

    print()
    if first_bad:
        print(f"FIRST DIVERGENCE at block {first_bad}")

        # Show context around the divergence
        if ef_nonces:
            rpc_nonce = rpc_nonce_at_block(args.rpc, args.address, first_bad)
            erigon_nonce = ef_nonces.get(first_bad)
            if first_bad - 1 in ef_nonces:
                prev_erigon = ef_nonces[first_bad - 1]
                prev_rpc = rpc_nonce_at_block(args.rpc, args.address, first_bad - 1)
                print(f"  block {first_bad - 1}: erigon={prev_erigon}, rpc={prev_rpc} (last good)")
            print(f"  block {first_bad}: erigon={erigon_nonce}, rpc={rpc_nonce} (first bad)")
            if erigon_nonce is not None and rpc_nonce is not None:
                print(f"  offset: {rpc_nonce - erigon_nonce}")
    else:
        print("NO DIVERGENCE FOUND in range")


if __name__ == "__main__":
    main()
