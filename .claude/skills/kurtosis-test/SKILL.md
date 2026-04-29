---
name: kurtosis-test
description: Run a local Kurtosis Ethereum testnet against a locally-built erigon image, monitor EL/CL/assertoor/spamoor health, triage failures with a cross-client comparison methodology, and auto-iterate fix → rebuild → rerun. Use when the user wants to reproduce, debug, or validate erigon against an `ethereum-package` config locally — equivalent to the `test-kurtosis-assertoor` CI workflow but interactive. Handles image build, enclave lifecycle, block-progress + assertoor + log watching, log dumping on failure, and the erigon-source fix loop.
argument-hint: "<config-yaml-path> [enclave-name] [duration=Nm] [auto=true|false] [max-attempts=N]"
allowed-tools: Bash, Read, Write, Edit, Glob, Grep, WebFetch, Skill
---

# Run a local Kurtosis Ethereum testnet against erigon

This skill mirrors the CI workflow at `.github/workflows/test-kurtosis-assertoor.yml`
but runs **locally** via the raw `kurtosis` CLI. The CI uses the
`ethpandaops/kurtosis-assertoor-github-action@v1` wrapper, which is not portable outside
GitHub Actions; this skill drives `kurtosis run`, `kurtosis enclave inspect`,
`kurtosis service logs`, and `kurtosis enclave dump` directly.

The skill takes an `ethereum-package` YAML config, builds the local
`test/erigon:current` Docker image, starts a Kurtosis enclave, monitors
EL/CL/assertoor/spamoor health, triages failures (challenging peer clients against
erigon to identify the offender), and iterates a fix → rebuild → rerun loop until the
testnet is stable or `max-attempts` is reached.

## Inputs

The model parses these arguments and binds them to the shell variables used in the
bash blocks below: `$1`/`$2` are positional; `duration=Nm` → `duration_secs`,
`auto=true|false` → `auto`, `max-attempts=N` → `max_attempts`.

| Argument | Default | Notes |
|---|---|---|
| `$1` config path | required | Path to an `ethereum-package` args YAML. The reference set lives in `.github/workflows/kurtosis/`, but that directory also contains assertoor playbooks (`id:` / `tasks:` schema) — only the files whose top-level keys are `participants:` or `participants_matrix:` are valid here. To list candidates: `grep -lE '^participants(_matrix)?:' .github/workflows/kurtosis/*.io`. |
| `$2` enclave name | `kurtosis-test-<unix-ts>` | Used for `kurtosis run --enclave`. Each rerun gets a fresh timestamp. |
| `duration=Nm` | `20m` | Wall-clock window the monitor watches before declaring "stable" if no failures trip. |
| `auto=true\|false` | `true` | If `true`, the fix-rebuild-rerun loop runs autonomously up to `max-attempts`. If `false`, pause for user approval before each fix. |
| `max-attempts=N` | `5` | Cap on fix-loop iterations. After hitting the cap, halt and surface the per-attempt triage history. |

## Prerequisites

1. **Docker** running: `docker info >/dev/null` should succeed.
2. **Kurtosis CLI** installed: `kurtosis version`. Install from
   https://docs.kurtosis.com/install if missing. CLI **≥ 1.18.1** is only needed when
   the `--package@branch` you run includes the `GpuConfig` Starlark built-in (i.e.
   `ethereum-package` `main` post commit `835dd9b`). The pinned branches in the
   mapping table below — including `glamsterdam`'s `6.1.0` — predate that change, so
   they work on older CLIs. See Troubleshooting if you hit a `GpuConfig` Starlark
   error.
3. **Erigon source tree** at the cwd: `Makefile` exists and `go.mod` contains
   `module github.com/erigontech/erigon`.
4. **`curl` and `jq`** on `$PATH` — used by the monitor / triage snippets below to
   poll the EL JSON-RPC endpoint and the assertoor API.
5. **Fork detection** — skim the YAML for `_fork_epoch` keys. The repo's configs use
   the CL-side fork names: `deneb_fork_epoch` (Cancun on EL), `electra_fork_epoch`
   (Prague), `fulu_fork_epoch` (Osaka), `gloas_fork_epoch` (Amsterdam). Future fork
   keys will follow the same CL-naming convention. Feeds the spec-lookup section below.

## Spec lookup (when debugging unfamiliar forks/EIPs)

If the YAML enables a fork under development, invoke `/erigon-implement-eip` Steps 2–4
to fetch:

- **Step 2** — referenced/dependent EIPs.
- **Step 3** — the meta EIP enumerating which EIPs the fork includes (CFI/SFI/PFI/DFI lists).
- **Step 4** — the latest devnet specification at `https://notes.ethereum.org/@ethpandaops/<devnet>`.

Use these as ground truth when triaging. For specific opcodes / state transitions,
also pull the EIP body via Step 1. If anything in the spec is contradictory or
ambiguous, **stop and ask the user** rather than guessing — the same rule the EIP skill
enforces.

## Build the erigon docker image

The image tag must be **exactly** `test/erigon:current` because every
`.github/workflows/kurtosis/*.io` config references that tag.

```bash
docker build -t test/erigon:current --build-arg BINARIES="erigon caplin" .
```

`caplin` is required in `BINARIES` because some configs (e.g.
`caplin-minimal-assertoor.io`) use erigon as the CL via the same image.

Always rebuild before each run — the same approach the CI uses. BuildKit's layer cache
makes the no-op rebuild fast, and the fix → rebuild → rerun loop necessarily picks up
uncommitted source edits this way (a freshness check against `git log` would miss them
and silently run a stale image).

If the user asks for a from-scratch binary build instead of docker, point them at
`/erigon-build`; this skill itself uses docker because the kurtosis configs reference a
docker image tag.

## Suite → ethereum-package branch mapping (from CI)

The CI matrix pins different package branches per suite. Use the same pinning when the
config matches a known CI file; for unknown configs, default to `5.0.1` and ask the
user to confirm.

| Config file | `--package@branch` |
|---|---|
| `regular-assertoor.io` | `github.com/ethpandaops/ethereum-package@5.0.1` |
| `pectra.io` | `github.com/ethpandaops/ethereum-package@5.0.1` |
| `glamsterdam.io` | `github.com/ethpandaops/ethereum-package@6.1.0` |
| `caplin-assertoor.io` | `github.com/erigontech/ethereum-package@erigontech/fix-caplin-launcher` |
| `caplin-minimal-assertoor.io` | `github.com/erigontech/ethereum-package@erigontech/fix-caplin-launcher` |
| (other / user-supplied) | default `5.0.1`, prompt user if unsure |

Note: `glamsterdam` is pinned to `6.1.0` rather than `main` because `main` introduced
the `GpuConfig` Starlark built-in which requires kurtosis CLI ≥ 1.18.1. Caplin suites
(`caplin-assertoor.io`, `caplin-minimal-assertoor.io`) require the `erigontech` fork —
do not let them fall back to the default `5.0.1`.

## Start the testnet

```bash
ENCLAVE="${2:-kurtosis-test-$(date +%s)}"
CONFIG="$1"

# Map config basename → ethereum-package branch (mirrors the table above).
case "$(basename "$CONFIG")" in
  glamsterdam.io)
    PACKAGE_REF="github.com/ethpandaops/ethereum-package@6.1.0" ;;
  caplin-assertoor.io|caplin-minimal-assertoor.io)
    PACKAGE_REF="github.com/erigontech/ethereum-package@erigontech/fix-caplin-launcher" ;;
  regular-assertoor.io|pectra.io)
    PACKAGE_REF="github.com/ethpandaops/ethereum-package@5.0.1" ;;
  *)
    PACKAGE_REF="github.com/ethpandaops/ethereum-package@5.0.1" ;;
esac

kurtosis run \
  "$PACKAGE_REF" \
  --enclave "$ENCLAVE" \
  --args-file "$CONFIG" \
  --verbosity detailed --cli-log-level trace
```

Once `kurtosis run` returns, capture service names and host-mapped ports:

```bash
kurtosis enclave inspect "$ENCLAVE" --full-uuids

# Pick the first erigon EL service. `kurtosis enclave inspect` prints columnar
# rows (UUID first, then service name), so we match against field 2.
EL_SERVICE=$(kurtosis enclave inspect "$ENCLAVE" --full-uuids 2>/dev/null \
  | awk '$2 ~ /^el-[0-9]+-erigon-[a-z]+$/ {print $2; exit}')
EL_RPC_PORT=$(kurtosis port print "$ENCLAVE" "$EL_SERVICE" rpc 2>/dev/null \
  | sed -E 's|.*:([0-9]+).*|\1|')

# CL endpoint (whichever client paired with that EL)
CL_SERVICE=$(kurtosis enclave inspect "$ENCLAVE" --full-uuids 2>/dev/null \
  | awk '$2 ~ /^cl-[0-9]+-[a-z]+-erigon$/ {print $2; exit}')
CL_HTTP_PORT=$(kurtosis port print "$ENCLAVE" "$CL_SERVICE" http 2>/dev/null \
  | sed -E 's|.*:([0-9]+).*|\1|')

# Optional services
ASSERTOOR_PORT=$(kurtosis port print "$ENCLAVE" assertoor http 2>/dev/null | sed -E 's|.*:([0-9]+).*|\1|')
DORA_PORT=$(kurtosis port print "$ENCLAVE" dora http 2>/dev/null | sed -E 's|.*:([0-9]+).*|\1|')
SPAMOOR_PORT=$(kurtosis port print "$ENCLAVE" spamoor http 2>/dev/null | sed -E 's|.*:([0-9]+).*|\1|')
```

Print the assertoor / dora URLs so the user can open the dashboards in a browser.

## Monitor

Three checks run on a polling loop until either (a) `duration` elapses, or (b) any
failure trips. All three are captured in the run history.

### Check A — Block height progress

`duration_secs` is the parsed `duration=Nm` input (default 1200). Three terminal
outcomes:

- **STABLE** — chain produced blocks and the duration elapsed without a stall.
- **STALL** — chain produced at least one block, then stopped advancing for
  `>3 × seconds_per_slot`.
- **NO_PROGRESS** — chain never produced a block within `duration_secs` (e.g.
  validators didn't start).

```bash
duration_secs=${duration_secs:-1200}
prev=0
slot_secs=$(grep -E '^\s*seconds_per_slot:' "$CONFIG" | awk '{print $2}'); slot_secs=${slot_secs:-12}
poll_interval=$(( slot_secs * 2 ))
stall_window=$(( slot_secs * 3 ))
start=$(date +%s)
end=$(( start + duration_secs ))
stall_deadline=$(( start + stall_window ))
outcome=""

while [ "$(date +%s)" -lt "$end" ]; do
  height_hex=$(curl -s --max-time 5 "http://127.0.0.1:${EL_RPC_PORT}" \
    -H 'Content-Type: application/json' \
    -d '{"jsonrpc":"2.0","method":"eth_blockNumber","params":[],"id":1}' \
    | jq -r '.result // empty')
  if [[ "$height_hex" =~ ^0x[0-9a-fA-F]+$ ]]; then
    height=$(printf '%d\n' "$height_hex")
    echo "[$(date -u +%H:%M:%S)] height=$height"
    if [ "$height" -gt "$prev" ]; then
      prev=$height
      stall_deadline=$(( $(date +%s) + stall_window ))
    fi
  else
    echo "[$(date -u +%H:%M:%S)] RPC unreachable or invalid response — retrying"
  fi
  # Only declare a stall after seeing at least one block. Many configs set
  # genesis_delay > stall_window (e.g. glamsterdam.io: 20s delay vs 18s window
  # at 6s slots), so the pre-genesis gap would otherwise trip a false stall.
  if [ "$prev" -gt 0 ] && [ "$(date +%s)" -gt "$stall_deadline" ]; then
    outcome="STALL: chain not progressing for >${stall_window}s (last height=$prev)"
    break
  fi
  sleep "$poll_interval"
done

if [ -z "$outcome" ]; then
  if [ "$prev" -eq 0 ]; then
    outcome="NO_PROGRESS: chain never produced a block within ${duration_secs}s"
  else
    outcome="STABLE: chain progressed for full ${duration_secs}s window (final height=$prev)"
  fi
fi
echo "$outcome"
```

Pass: height advances ≥1 within every `3 × seconds_per_slot` (the stall window),
sustained for the full `duration_secs` (poll cadence is `2 × seconds_per_slot`).
Fail: STALL or NO_PROGRESS.

### Check B — Assertoor results

```bash
curl -s "http://127.0.0.1:${ASSERTOOR_PORT}/api/v1/test_runs" \
  | jq '.data[] | {name, status, result}'
```

Pass: every test_run has `result=success`. Fail: any `result=failure`, or any test
stuck `pending` / `running` past 3× its expected duration. The assertoor web UI at
`http://127.0.0.1:${ASSERTOOR_PORT}/` shows per-step trees; use it for deep dives.

### Check C — Erigon-focused log scan

```bash
kurtosis service logs "$ENCLAVE" "$EL_SERVICE" 2>&1 \
  | grep -iE 'panic|fatal|^ERROR|"lvl"="error"|consensus failure|invalid block' \
  | tail -200
```

For cross-client comparison (used by the triage section), run the same scan across
every EL/CL service:

```bash
for svc in $(kurtosis enclave inspect "$ENCLAVE" --full-uuids \
             | awk '$2 ~ /^(el|cl|vc)-[0-9]+-[a-z]+-[a-z]+$/ {print $2}'); do
  echo "=== $svc ==="
  kurtosis service logs "$ENCLAVE" "$svc" 2>&1 \
    | grep -iE 'error|panic|fatal' | tail -30
done
```

If `snooper-engine-*` services exist (when `snooper_enabled: true` in the YAML), pull
their logs too — they capture the full Engine API request/response trace, invaluable
when an EL bug is suspected.

## Issue detection criteria

| User check | Pass | Fail |
|---|---|---|
| 1. Block production / height progress | `eth_blockNumber` advances ≥1 every `2 × seconds_per_slot`, sustained for `duration` | No advance for `>3 × seconds_per_slot`, OR explicit chain reorg / fork-choice loop in CL logs |
| 2. EL/CL log errors (focus erigon) | No `panic`, `fatal`, or error-level lines in any erigon service | Any erigon-side panic/fatal/consensus failure. Non-erigon errors recorded but informational unless they crash the peer. |
| 3. Assertoor test failures | All assertoor `test_runs` reach `result=success` | Any `result=failure`, OR a test stuck `pending`/`running` past 3× expected duration |

A single failed check trips the triage section. Block-stall + erigon panic + assertoor
fail are independent signals — record all three in the run history; do not stop at the
first.

## Debugging methodology — triage erigon vs peer-client vs network/config

Decision tree:

1. **Reproduce.** A single one-shot failure gets one re-run before triaging. Truly
   intermittent failures still get triaged, but flag them as flaky.
2. **Classify the symptom.** One of: block-stall, EL panic, EL invalid-payload, CL
   fork-choice mismatch, assertoor opcode/EIP test failure, spamoor tx-submission
   failure.
3. **Cross-client comparison.** For each erigon-side error, find the equivalent moment
   in the peer-client log at the same slot/block. Three outcomes:
   - **Erigon wrong**: erigon rejects/panics; peer client + assertoor accept the
     block → erigon bug, fix locally.
   - **Peer wrong**: erigon accepts; peer rejects → check peer-client image tag
     against the fork's expected tag (often a stale image). Surface to user; do not
     fix erigon.
   - **Both disagree with spec**: clients produce different "valid" answers from what
     the EIP spec says → escalate to the user. Likely spec ambiguity or a misread.
4. **Cross-reference the spec.** Pull the relevant EIP (`/erigon-implement-eip` Step 1)
   and the devnet spec (`/erigon-implement-eip` Step 4) for the failing block, opcode,
   or state transition.
5. **Rule out config drift.** Diff the YAML's `el_extra_params`, `network_params`, and
   fork epochs against the equivalent CI suite under `.github/workflows/kurtosis/`.
   Mismatches there are config bugs, not erigon bugs.
6. **Rule out enclave plumbing.** `kurtosis service exec <enclave> <svc> "ping
   <other_svc>"` to verify network reachability; check JWT mounting via
   `kurtosis service exec <enclave> <el-svc> "ls -la /jwt/"`. The CLI takes the
   command as a single positional arg (multi-word commands must be quoted) — there
   is no `--` separator.

### Triage table

| Symptom | Likely owner | Next action |
|---|---|---|
| Erigon panic with stack trace inside `execution/...` | Erigon | Capture stack, find offending call in repo, propose fix |
| `eth_newPayloadV4` returns INVALID; CL logs say block is valid; assertoor passes elsewhere | Erigon (likely block-validation divergence) | Replay the payload via `debug_traceBlockByNumber` / `debug_traceBlockByHash`; check fork activation timestamp |
| All EL clients stop progressing after a specific slot | Config (fork epoch wrong) or shared dep | Diff YAML against working CI suite; check ethereum-package branch |
| Assertoor `block-proposal-check` fails on slot N for `vc-N-erigon-…` | Erigon block builder | Fetch block N body via RPC; replay locally |
| Assertoor `synchronized-check` fails | Network plumbing | Inspect peer counts; `kurtosis service exec` connectivity test |
| `caplin` panics but `lighthouse` runs fine on the same EL | Caplin | Edit YAML to swap CL to lighthouse for bisection; report to user |
| Spamoor reports persistent "insufficient funds" / "nonce too low" | Spamoor config | Increase `funding_gas_limit`; lower `throughput`; check prefunded keys |
| Snooper shows malformed Engine API request | Erigon RPC layer | Capture the request from snooper logs; inspect erigon engine handler |
| Erigon accepts a payload that lighthouse + teku both reject | Erigon (single-client divergence) | Almost always an erigon bug — fix locally |
| `eth_blockNumber` stays at 0x0 after >2 epochs | Validators not running | Check `vc-*` service logs; verify keystore mounting |

## Fix-rebuild-rerun loop

Auto-iterates by default (`auto=true`), capped at `max-attempts=5`. Per attempt:

1. **Tear down**: `kurtosis enclave rm -f "$ENCLAVE"`.
2. **Apply the fix** to erigon source via `Edit`. Only auto-apply when the triage
   classified the issue as "Erigon wrong" with high confidence. If ambiguous (peer
   could be wrong, or spec interpretation unclear), pause and surface to the user
   even before the cap — that overrides `auto=true`.
3. **Rebuild image**: `docker build -t test/erigon:current --build-arg BINARIES="erigon caplin" .`.
4. **Re-launch**: same config, fresh timestamped enclave name (so each attempt's dump
   stays separate).
5. **Re-run monitor**: same three checks.
6. **Record**: per attempt — symptom, hypothesis, fix applied, outcome.

After `max-attempts` consecutive failures, halt and print the per-attempt history. Do
not auto-apply more fixes once the cap is hit. Reference: `/autoresearch` follows the
same iterate-and-record pattern.

If `auto=false`, pause for user approval before steps 2–4 of every attempt.

## Cleanup (always run)

Run as the final step of every iteration regardless of outcome (success, failure,
or user interrupt):

```bash
DUMP_DIR="/tmp/kurtosis-dump-${ENCLAVE}"
# `kurtosis enclave dump` refuses if the destination already exists — never pre-create it.
kurtosis enclave dump "$ENCLAVE" "$DUMP_DIR" || true
kurtosis enclave rm -f "$ENCLAVE" || true
echo "Logs dumped to: $DUMP_DIR"
```

The dump contains per-service logs (`el-*`, `cl-*`, `vc-*`, `assertoor`, `spamoor`,
`dora`, `snooper-*`) — keep this directory until triage is complete; GitHub blob
storage is not in play here so the logs are only on disk locally.

After multiple iterations, also prune dangling docker images:

```bash
docker image prune -f
```

## Troubleshooting

| Problem | Solution |
|---|---|
| `kurtosis run` fails with Starlark error mentioning `GpuConfig` | Use one of the pinned `--package@branch` values from the mapping table (e.g. `6.1.0` for glamsterdam, `5.0.1` for regular/pectra) — they predate the `GpuConfig` built-in. OR upgrade kurtosis CLI to ≥ 1.18.1 if you need an `ethereum-package` branch that includes it. |
| `kurtosis enclave dump` errors "destination exists" | Use a fresh dir name or `rm -rf` it first |
| All `el-*-erigon-*` services missing from `enclave inspect` | Image build failed; check `docker images \| grep test/erigon` and rerun docker build |
| `eth_blockNumber` returns `0x0` forever | Validators didn't start; check `vc-*` service logs; verify keystore mounting |
| Connection refused on assertoor port | Service still booting; or `assertoor` not in `additional_services` in the YAML |
| `kurtosis service logs` truncates very long logs | Use `kurtosis enclave dump` for the full per-service log files |
| `caplin-minimal` config fails with "binary not found" | Confirm `BINARIES="erigon caplin"` in the docker build args |
| `eth_blockNumber` advances but assertoor reports timeout | Slot time / preset mismatch; check `seconds_per_slot` and `preset` in YAML |
| Erigon image stale despite rebuild | `docker image rm test/erigon:current && docker build ...` to force; check BuildKit cache scope |
| Port already allocated | Another enclave is running — `kurtosis enclave ls` then `kurtosis enclave rm -f <old>` |
| Engine API JWT mismatch in EL logs | Check `kurtosis service exec <enclave> el-1-erigon-… "ls -la /jwt/"`; restart enclave if missing |
