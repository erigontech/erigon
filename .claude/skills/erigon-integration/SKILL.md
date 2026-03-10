# Erigon Integration Tool

The `integration` binary (`./build/bin/integration`) is a developer tool for running Erigon
stages in custom ways: single-stage execution, unwind/re-org loops, state inspection,
commitment debugging, and DB diagnostics.

Build it with:
```bash
make integration
```

## All Commands

### Stage Execution

| Command | Purpose |
|---------|---------|
| `stage_exec` | Run the execution stage forward (or unwind/reset it) |
| `stage_headers` | Run headers stage |
| `stage_bodies` | Run bodies stage |
| `stage_senders` | Run senders stage |
| `stage_snapshots` | Run snapshots stage |
| `stage_tx_lookup` | Run tx lookup stage |
| `stage_custom_trace` | Re-generate optional domains (receipts, traces, etc.) |
| `state_stages` | Run all post-senders stages in a forward/unwind loop |
| `loop_exec` | Simple infinite execution loop |
| `read_domains` | Run block execution+commitment with Domains |

### Commitment

| Command | Purpose |
|---------|---------|
| `commitment rebuild` | Rebuild the commitment (trie) domain from scratch |
| `commitment branch` | Read branch data from the commitment domain |
| `commitment print` | Print commitment domain entries |
| `commitment visualize` | Generate HTML visualization of commitment .kv files |
| `commitment bench-lookup` | Benchmark trie node lookup times |
| `commitment bench-history-lookup` | Benchmark GetAsOf() across history files |

### DB Inspection & Maintenance

| Command | Purpose |
|---------|---------|
| `print_stages` | Show current stage progress heights |
| `print_table_sizes` | Show size of each MDBX table |
| `print_migrations` | List all DB migrations |
| `run_migrations` | Run pending DB migrations |
| `remove_migration` | Remove a migration record |
| `reset_state` | Reset stages 5–10 (Execution and beyond) + their DB tables |
| `clear_bad_blocks` | Clear bad-block markers so those blocks are re-processed |
| `compare_bucket` | Compare a bucket to `--chaindata.reference` |
| `compare_states` | Compare all state buckets to `--chaindata.reference` |
| `compact_domains` | Rewrite .kv files removing duplicate/old keys |
| `write_amplification` | Calculate write amplification ratio for domain .kv files |
| `mdbx_to_mdbx` | Copy chaindata between two MDBX databases |
| `f_to_mdbx` | Copy data from flat files to MDBX |
| `history` | History domain inspection |
| `trigger_fcu` | Manually trigger a forkChoiceUpdate for testing |
| `alloc` | Memory allocation diagnostics |

---

## Key Use Cases

### 1. Reproduce / Debug a Gas Mismatch

A gas mismatch at block N means the execution output doesn't match the block header.
Use `stage_exec` with env trace vars to isolate it:

```bash
# Re-execute from block N (unwind N blocks then re-exec):
ERIGON_TRACE_GAS=true \
TRACE_BLOCKS=<N> \
./build/bin/integration stage_exec \
  --datadir=<datadir> --chain=mainnet \
  --unwind=<blocks_to_unwind> \
  --block=<N>

# Or just re-exec without committing (safe, non-destructive):
./build/bin/integration stage_exec \
  --datadir=<datadir> --chain=mainnet \
  --block=<N> --no-commit
```

Trace environment variables (set to `true` to enable):

| Var | What it traces |
|-----|---------------|
| `ERIGON_TRACE_GAS` | Per-tx fees + refunds |
| `ERIGON_TRACE_DYNAMIC_GAS` | Dynamic gas (memory expansion, etc.) |
| `ERIGON_TRACE_INSTRUCTIONS` | EVM opcode-level trace |
| `ERIGON_TRACE_APPLY` | State writes (balance/nonce/storage) |
| `ERIGON_TRACE_TRANSACTION_IO` | Tx-level reads/writes |
| `ERIGON_TRACE_DOMAIN_IO` | Domain-level I/O |
| `LOG_HASH_MISMATCH_REASON=true` | Log receipt/bloom/gas hash mismatch details |

Scope reduction (critical for large blocks):

| Var | Example |
|-----|---------|
| `TRACE_BLOCKS=N[,M]` | `TRACE_BLOCKS=24578939` |
| `TRACE_TXINDEXES=I[,J]` | `TRACE_TXINDEXES=241,270` |
| `TRACE_ACCOUNTS=0xAddr` | `TRACE_ACCOUNTS=0xa0b8...` |

### 2. Test Unwind Correctness

Run forward N blocks then unwind M blocks in a loop. Use `--chaindata.reference` to
diff final state against a known-good DB:

```bash
# 10 forward, 1 back — tight re-org stress test
./build/bin/integration state_stages \
  --datadir=<datadir> --chain=mainnet \
  --unwind=1 --unwind.every=10 \
  --integrity.fast --integrity.slow

# 1 forward, 10 back — deep unwind stress test
./build/bin/integration state_stages \
  --datadir=<datadir> --chain=mainnet \
  --unwind=10 --unwind.every=1

# Unwind 100 blocks then stop
./build/bin/integration state_stages \
  --datadir=<datadir> --chain=mainnet \
  --unwind=100

# Run to block 2M with 100-block unwinds every 100k blocks, compare to reference DB
./build/bin/integration state_stages \
  --datadir=<datadir> --chain=mainnet \
  --unwind=100 --unwind.every=100000 --block=2000000 \
  --chaindata.reference=<path-to-reference-chaindata>
```

### 3. Reset and Re-execute the Execution Stage

After a bug fix, re-execute blocks from the point of divergence:

```bash
# Reset execution stage (drops all state above snapshots)
./build/bin/integration stage_exec --datadir=<datadir> --chain=mainnet --reset

# Then run forward (will pick up from snapshot frontier)
./build/bin/integration stage_exec --datadir=<datadir> --chain=mainnet
```

Or to avoid snapshot re-download:
```bash
erigon snapshots rm-all-state-state  # removes state snapshots only
./build/bin/integration stage_exec --datadir=<datadir> --chain=mainnet --reset
./build/bin/integration stage_exec --datadir=<datadir> --chain=mainnet
```

### 4. Rebuild Commitment / Fix Wrong State Root

If execution succeeds but state root is wrong after a storage bug fix:

```bash
./build/bin/integration commitment rebuild --datadir=<datadir> --chain=mainnet
```

### 5. Re-allow a Bad Block

If a block was incorrectly marked invalid (e.g. after fixing a gas bug):

```bash
./build/bin/integration clear_bad_blocks --datadir=<datadir>
```

### 6. Compare State Between Two Datadirs

After a fix, verify that two datadirs agree at a block:

```bash
./build/bin/integration compare_states \
  --datadir=<fixed-datadir> \
  --chaindata.reference=<reference-chaindata>
```

### 7. Re-generate Optional Domains (Receipts, Traces)

```bash
erigon snapshots rm-state-snapshots --domain=receipt,rcache,logtopics,logaddrs,tracesfrom,tracesto
./build/bin/integration stage_custom_trace \
  --domain=receipt,rcache,logtopics,logaddrs,tracesfrom,tracesto \
  --datadir=<datadir> --chain=mainnet --reset
./build/bin/integration stage_custom_trace \
  --domain=receipt,rcache,logtopics,logaddrs,tracesfrom,tracesto \
  --datadir=<datadir> --chain=mainnet
```

### 8. Chaintip Mode (Per-block Commit)

Simulate what a live node does — commit every block, generate diffs/changesets:

```bash
./build/bin/integration stage_exec \
  --datadir=<datadir> --chain=mainnet \
  --sync.mode.chaintip
```

### 9. Inspect Stage Heights

```bash
./build/bin/integration print_stages --datadir=<datadir> --chain=mainnet
```

### 10. Check DB Table Sizes

```bash
./build/bin/integration print_table_sizes --datadir=<datadir> --chain=mainnet
```

---

## Common Flags (stage_exec / state_stages)

| Flag | Default | Purpose |
|------|---------|---------|
| `--datadir` | — | Path to erigon datadir |
| `--chain` | — | Chain name: `mainnet`, `sepolia`, etc. |
| `--block` | 0 | Stop at this block number |
| `--unwind` | 0 | How many blocks to unwind per iteration |
| `--unwind.every` | 0 | How many blocks forward before each unwind |
| `--no-commit` | false | Run without committing (safe read-only replay) |
| `--reset` | false | Drop stage data and restart from scratch |
| `--exec.workers` | 6 | Parallel execution workers |
| `--batchSize` | 512M | Batch size for execution stage |
| `--integrity.fast` | false | Fast DB integrity check each step |
| `--integrity.slow` | false | Slow DB integrity check each step |
| `--chaindata.reference` | — | Path to reference chaindata for comparison |
| `--txtrace` | false | Enable transaction tracing |
| `--pprof` | false | Enable pprof HTTP server on :6060 |
| `--sync.mode.chaintip` | false | Per-block commit + diff generation |

---

## Workflow: Gas Mismatch Debugging with Integration

1. **Identify block N** from the erigon error log: `gas used by execution: X, in header: Y`

2. **Stop erigon** if running.

3. **Unwind to just before block N** (or use `--no-commit` for safe replay):
   ```bash
   ./build/bin/integration stage_exec \
     --datadir=<datadir> --chain=mainnet \
     --block=<N-1>
   ```

4. **Re-execute block N with gas tracing**:
   ```bash
   ERIGON_TRACE_GAS=true TRACE_BLOCKS=<N> \
   ./build/bin/integration stage_exec \
     --datadir=<datadir> --chain=mainnet \
     --block=<N> --no-commit
   ```

5. **Compare per-tx gas** against a trusted source (`eth_getTransactionReceipt`).

6. **Drill into a specific tx** with full opcode trace:
   ```bash
   ERIGON_TRACE_INSTRUCTIONS=true \
   ERIGON_TRACE_DYNAMIC_GAS=true \
   TRACE_BLOCKS=<N> TRACE_TXINDEXES=<txIdx> \
   ./build/bin/integration stage_exec \
     --datadir=<datadir> --chain=mainnet \
     --block=<N> --no-commit
   ```
