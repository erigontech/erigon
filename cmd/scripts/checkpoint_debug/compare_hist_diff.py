#!/usr/bin/env python3
"""
Compare Erigon's dump-hist-at-blk JSON output against prestateTracer diff JSON
from an RPC provider (e.g. Alchemy).

Usage:
    python3 compare_hist_diff.py <erigon_dump.json> <rpc_diff.json>

erigon_dump.json: output of `erigon seg dump-hist-at-blk --block N --output dump.json`
rpc_diff.json:    output of debug_traceBlockByNumber with prestateTracer + diffMode=true
"""

import json
import sys


def accumulate_post_state(rpc_data):
    """Accumulate final post-state from per-tx prestate diffs.

    For each account, the last tx that touches it gives the final post value.
    We merge pre (initial state at start of block) and post (changes) to get
    the full picture of what the state looks like after the block.
    """
    # Track the latest pre and post values per account
    accounts = {}   # addr -> {balance, nonce, codeHash, storage: {slot: val}}

    for tx_entry in rpc_data["result"]:
        result = tx_entry.get("result", {})
        pre = result.get("pre", {})
        post = result.get("post", {})

        # First pass: record pre-state for accounts we haven't seen yet
        for addr, fields in pre.items():
            addr_lower = addr.lower()
            if addr_lower not in accounts:
                accounts[addr_lower] = {
                    "balance": fields.get("balance", "0x0"),
                    "nonce": int(fields.get("nonce", "0x0"), 16) if isinstance(fields.get("nonce"), str) else fields.get("nonce", 0),
                    "codeHash": fields.get("codeHash"),
                    "storage": dict(fields.get("storage", {})),
                }

        # Second pass: apply post-state updates
        for addr, fields in post.items():
            addr_lower = addr.lower()
            if addr_lower not in accounts:
                accounts[addr_lower] = {
                    "balance": "0x0",
                    "nonce": 0,
                    "codeHash": None,
                    "storage": {},
                }
            acc = accounts[addr_lower]
            if "balance" in fields:
                acc["balance"] = fields["balance"]
            if "nonce" in fields:
                n = fields["nonce"]
                acc["nonce"] = int(n, 16) if isinstance(n, str) else n
            if "codeHash" in fields:
                acc["codeHash"] = fields["codeHash"]
            if "storage" in fields:
                for slot, val in fields["storage"].items():
                    acc["storage"][slot.lower()] = val.lower()

    return accounts


def normalize_hex(h):
    """Normalize hex value: lowercase, strip leading zeros after 0x."""
    if h is None:
        return None
    h = h.lower().strip()
    if h.startswith("0x"):
        h = h[2:]
    h = h.lstrip("0") or "0"
    return "0x" + h


def compare(erigon_dump, rpc_post_state):
    mismatches = []

    # Compare accounts
    erigon_accounts = erigon_dump.get("accounts", {})
    for addr_raw, erigon_acc in erigon_accounts.items():
        addr = addr_raw.lower()
        rpc_acc = rpc_post_state.get(addr)
        if rpc_acc is None:
            # Account touched in erigon but not in RPC diff — could be just
            # that the RPC diff doesn't include accounts whose state didn't change
            continue

        # Compare balance
        e_bal = normalize_hex(erigon_acc.get("balance", "0x0"))
        r_bal = normalize_hex(rpc_acc.get("balance", "0x0"))
        if e_bal != r_bal:
            mismatches.append({
                "type": "account_balance",
                "address": addr,
                "erigon": e_bal,
                "reference": r_bal,
            })

        # Compare nonce
        e_nonce = erigon_acc.get("nonce", 0)
        r_nonce = rpc_acc.get("nonce", 0)
        if e_nonce != r_nonce:
            mismatches.append({
                "type": "account_nonce",
                "address": addr,
                "erigon": e_nonce,
                "reference": r_nonce,
            })

    # Compare storage
    erigon_storage = erigon_dump.get("storage", {})
    for composite_key, erigon_entry in erigon_storage.items():
        parts = composite_key.lower().split(":")
        if len(parts) != 2:
            continue
        addr, slot = parts[0], parts[1]

        rpc_acc = rpc_post_state.get(addr)
        if rpc_acc is None:
            continue

        e_val = normalize_hex(erigon_entry.get("value", "0x0"))
        r_val = normalize_hex(rpc_acc.get("storage", {}).get(slot, None))
        if r_val is None:
            # Slot touched in erigon but not in RPC diff — may not have changed
            continue
        if e_val != r_val:
            mismatches.append({
                "type": "storage",
                "address": addr,
                "slot": slot,
                "erigon": e_val,
                "reference": r_val,
            })

    return mismatches


def main():
    if len(sys.argv) != 3:
        print(__doc__.strip())
        sys.exit(1)

    erigon_path = sys.argv[1]
    rpc_path = sys.argv[2]

    with open(erigon_path) as f:
        erigon_dump = json.load(f)
    with open(rpc_path) as f:
        rpc_data = json.load(f)

    print(f"Erigon dump: block {erigon_dump['blockNum']}, toTxNum {erigon_dump['toTxNum']}")
    print(f"  accounts: {len(erigon_dump.get('accounts', {}))}")
    print(f"  storage:  {len(erigon_dump.get('storage', {}))}")
    print(f"  code:     {len(erigon_dump.get('code', {}))}")
    print(f"RPC diff: {len(rpc_data['result'])} transactions")

    rpc_post_state = accumulate_post_state(rpc_data)
    print(f"RPC accumulated accounts: {len(rpc_post_state)}")
    print()

    mismatches = compare(erigon_dump, rpc_post_state)

    if not mismatches:
        print("NO MISMATCHES FOUND")
        print("(Note: only keys touched in both sources are compared)")
    else:
        print(f"FOUND {len(mismatches)} MISMATCHES:")
        for m in mismatches:
            if m["type"] == "storage":
                print(f"  STORAGE {m['address']} slot {m['slot']}:")
                print(f"    erigon:    {m['erigon']}")
                print(f"    reference: {m['reference']}")
            elif m["type"] == "account_balance":
                print(f"  BALANCE {m['address']}:")
                print(f"    erigon:    {m['erigon']}")
                print(f"    reference: {m['reference']}")
            elif m["type"] == "account_nonce":
                print(f"  NONCE {m['address']}:")
                print(f"    erigon:    {m['erigon']}")
                print(f"    reference: {m['reference']}")


if __name__ == "__main__":
    main()
