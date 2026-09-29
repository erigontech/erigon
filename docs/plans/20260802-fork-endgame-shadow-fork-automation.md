# Fork endgame: shadow-fork stack — step-wise primitives AND single-command automation

**Date:** 2026-08-02
**Status:** North-star target for the fork stack. All in-flight fork work maps to closing gaps that this doc enumerates.

## Design principle: both tiers are first-class

The shadow-fork stack must support **both** modes of consumption, always:

- **Tier A — manual step-wise (expert-friendly, available as each primitive lands):** an operator composes the pieces by hand — start a node with the appropriate flags, mint a chain.toml + trust-root via `integration set_fork`, drive a mode-B unwind to the fork boundary, deploy contracts via ordinary `eth_sendTransaction`, spin up validators from separate launcher scripts, run mempool-mirror as a side-car. Each step is a distinct explicit action; each piece is verifiable in isolation. **This is what today's fork-test-suite exercises.**
- **Tier B — automated single-command (novice-friendly, the endgame):** two commands (or one RPC + one command) wrap the entire Tier A sequence into hands-off automation.

Both tiers use exactly the same underlying primitives. Neither is downstream of the other — each is a legitimate first-class consumer.

Concretely, every gap closed below strengthens **both** tiers:

- The step-wise operator gains a working primitive they can compose immediately.
- The single-command automation gains a building block it can wrap.

If a gap-fix lands but only works under the single-command wrapper (or only works step-wise), it is incomplete. Design each primitive so that it is directly usable via CLI / RPC / integration-command by an expert operator, then design the automation wrapper on top of that same primitive.

## The single-command target

Two commands (or one RPC + one command) take a fresh box from zero to a running shadow-fork validator cluster. Everything else is an implementation detail:

```
erigon shadow-fork create --from=<live-chain> --at-block=<N> --fork-name=<name>
erigon shadow-fork spawn-validators --fork=<name> --count=<K>
```

Or equivalently as JSON-RPC (`erigon_shadowForkCreate`, `erigon_shadowForkSpawnValidators`).

Behind those calls, the client:

1. **Fetches only the state needed to reproduce block N** — no full archive sync. `--snapshot.trim-below-block=N` etc.
2. **Mints the fork identity** — chain.toml + trust-root UCAN + genesis derived from parent's state root at N.
3. **Isolates from the live P2P layer** — bootnode swap, chain-ID rotation, all architectural (not manual config edits).
4. **Auto-provisions K validator keys** with their own datadirs, wires them to a private bootnode (the shadow-fork parent), starts them.
5. **(Optional)** Enables the mempool-mirror so the fork replays live-network traffic for stress testing.

Zero manual `genesis.json` edits. Zero manual key management. Zero manual bootnode wiring.

## Today's manual 5-step process (what we're replacing)

The current shadow-fork process across GoQuorum / Besu / Geth ecosystems:

1. **Spin up an archive node on the live network.** Wait full-history sync (~TB storage, days). Node must sync to the target block N.
2. **Isolate the node.** Kill P2P peers, remove bootnodes from config, edit `genesis.json` chain-ID / network-ID by hand to a unique custom value to prevent broadcasting to live network.
3. **Apply the network-upgrade code.** Inject new protocol rules; edit config to activate the fork at block N+1.
4. **Deploy custom validators + bootnodes.** Manually generate validator keys, spin up private bootnode, wire validators to bootnode by hand.
5. **Replay live traffic.** Set up transaction relayer / mempool mirror as separate scripting to spy on live txpool and re-send transactions into the shadow fork.

Every step is manual per-fork today. We want each step to disappear into an implementation detail of one RPC/command.

## What we already have

The 2026 fork-test track has been building the primitives for this, largely without framing them as "shadow forks":

- **`Controller` in-process chain swap** ([node/components/fork/controller.go](../../node/components/fork/controller.go)) — replaces step 2's "restart with new chain-ID" with a live process swap. Tier 3b test (fork-rpc-transition) validates it end-to-end.
- **`debug_setFork` RPC** ([docs/plans/20260728-debug-setFork-design.md](20260728-debug-setFork-design.md), [20260729-debug-setfork-ucan-auth.md](20260729-debug-setfork-ucan-auth.md)) — the single-RPC entry point that would front `shadow-fork create`. Already routes fork identity + UCAN + trust-root through Controller.
- **chain.toml v2/v3 + UCAN Authority** ([20260729-chaintoml-v3-fork-identity.md](20260729-chaintoml-v3-fork-identity.md)) — replaces step 2's manual chain-ID edit with a manifest-driven fork identity that carries its own trust chain.
- **Snapshot flow** (`--snap.bootstrap-from-preverified` + `--snap.p2p-manifest`) — replaces step 1's full-archive sync with a snapshot-set download. The 2026-08-02 downloader self-deadlock fix ([db/downloader/downloader.go](../../db/downloader/downloader.go) commit `5120b9ea08`) was blocking this exact path.
- **Fork-parent + fork-child launcher pattern** ([scripts/erigon-launch-hoodi-fork-parent.sh](../../scripts/erigon-launch-hoodi-fork-parent.sh), [scripts/erigon-launch-hoodi-fork-child.sh](../../scripts/erigon-launch-hoodi-fork-child.sh)) — the two-role split that maps to "shadow-fork bootnode" (parent) + "shadow-fork validators" (children).
- **`fork-test-suite.sh`** — end-to-end automation of the transition sequence, catches ordering bugs that would otherwise silently trip up any single-command flow.
- **Fresh sync completes on the merged branch** (verified 2026-08-02, post-downloader-fix) — the "minimal disk" path works end-to-end for the first time.
- **Fork-testing docs baseline** ([20260630-fork-testing-scenarios.md](20260630-fork-testing-scenarios.md), [20260718-fork-testing-decisions.md](20260718-fork-testing-decisions.md), [20260728-fork-test-reshape.md](20260728-fork-test-reshape.md), [20260731-fork-test-scope-and-leaks.md](20260731-fork-test-scope-and-leaks.md)) — scenario coverage + phased approach.

## Gaps to close

Each gap maps back to a specific piece of the manual 5-step process it eliminates.

### Gap 1: auto-mint chain.toml + UCAN + trust-root on `shadow-fork create`
*(Replaces step 2's manual chain-ID edit + step 3's manual config-file edit.)*

Today the fork-parent launcher generates its own trust-root key; the operator has to know that. `shadow-fork create` should do it invisibly and hand back the fork-name.

### Gap 2: auto-derive genesis + fork-activation block from parent state at N
*(Replaces step 3's manual `genesis.json` edit.)*

Today we hand-edit `genesis.json`. The client should snapshot the parent's state root at N and write the child's genesis from it. This is derivation, not edit — the fork identity is determined by (parent chain, N, upgrade rules), so given those three inputs the genesis is a pure function.

### Gap 3: automate parent → child bootnode wiring
*(Replaces step 4's manual `--staticnodes` / `--bootnodes` config.)*

Today `--staticnodes` / `--bootnodes` is manually set. On `shadow-fork spawn-validators`, the fork-parent's ENR should be discovered from local state (we own the parent process) and injected into each child's config.

### Gap 4: automate validator key provisioning
*(Replaces step 4's manual key generation + startup.)*

`--count=K` should mint K validator keys under `<datadir>/shadow-forks/<name>/validators/<i>/` and start them without operator interaction. Each validator should be a proper UCAN-delegated publisher of the fork's chain.toml.

### Gap 5: test-harness ordering / re-entrancy fixes
*(Prerequisite for any hands-off flow that survives multiple invocations.)*

The 2026-08-02 Tier 3c fail (Tier 3b left chain at `hoodi-fork-rpc-*`, Tier 3c tried `hoodi → hoodi-fork-restart-*` — sibling forks correctly rejected by `debug_setFork`) is exactly the class of bug that would show up in a `shadow-fork create → create → create` sequence. The Controller has to know how to peel back to the original parent (or handle "fork of a fork") for hands-off flow to survive multiple invocations.

### Gap 6: snapshot-set trimming for minimal disk + re-download-on-demand
*(Replaces step 1's TB-of-archive-sync requirement with a working set of GB.)*

Today's snapshot download pulls everything preverified.toml lists (~1.6 GB for hoodi, much more for mainnet). A shadow-fork only needs state up to N + block bodies for the fork boundary + some working history for CL re-anchor. Wiring `--snapshot.trim-below-block=N` to skip pre-N historical block snapshots would drop the shadow-fork disk requirement by an order of magnitude — and is what makes "empty directory" → "running shadow-fork" achievable on a developer laptop.

**Shared primitive**: the "trim" side and the "re-download-on-demand" side are the same mechanism — fetch specific snapshot files from the manifest / webseed / P2P on demand, verified against preverified.toml hashes. That primitive is also needed by G5 (mode-B block download-on-demand) and by G7's fix D (mode-B state-domain symmetric removal under `--prune.mode=minimal`). One implementation covers all three call sites. See [prune-gap-redownload-principle-2026-08-02] in project memory for the architectural principle.

### Gap 7 (optional / stage 5): mempool-mirror as an in-process component
*(Replaces step 5's separate relayer scripting.)*

An `erigon shadow-fork enable-mempool-mirror --source=<live-rpc>` command that spies on the live txpool over RPC and injects those transactions into the fork's mempool. Stage-5 stress-testing without operator scripting.

### Gap 8: Cocoon shadow-mode as a first-class consumer of the shadow-fork infra
*(The shadow-fork isn't only for erigon-team protocol testing — it's the substrate any componentized service can boot against.)*

Cocoon (the erigon componentization track) should be able to boot in **shadow mode**: point its components at a running shadow-fork's RPC endpoint, deploy the component's own contracts into that shadow via ordinary `eth_sendTransaction` calls, then integration-test the component's behaviour against **real on-chain contracts inherited from the parent chain's state at fork-block N**. No further integration wiring should be required beyond "here is the shadow-fork RPC URL and its chain-id."

**Tier A (step-wise, available as primitives land):**

An operator stitches the pieces together:

```
# 1. Start an erigon parent, drive it to the fork-block N
./erigon --datadir=<...> --chain=mainnet ...
# ... wait for sync to N ...

# 2. Mint the fork identity (via integration + debug_setFork)
./integration set_fork --chain=cocoon-integration --parent=mainnet --from-block=N ...

# 3. Spawn N validators from separate launcher(s), each pointed at parent's ENR
./scripts/erigon-launch-<forkname>-validator.sh --index=0 ...
./scripts/erigon-launch-<forkname>-validator.sh --index=1 ...

# 4. Point Cocoon at the shadow RPC and let it deploy + test
cocoon --shadow-mode --chain-rpc=http://127.0.0.1:<port> --chain-id=<id>
```

Every step is a distinct explicit action a Cocoon developer can run and verify. This is available today as each individual primitive lands.

**Tier B (single-command, endgame wrap):**

```
erigon shadow-fork create --from=mainnet --at-block=20000000 --fork-name=cocoon-integration
erigon shadow-fork spawn-validators --fork=cocoon-integration --count=1
cocoon --shadow-mode --chain-rpc=http://127.0.0.1:<shadow-fork-http-port>
```

The last command boots the Cocoon component, which (a) submits its contract-deployment transactions to the shadow, (b) waits for confirmation, (c) starts serving requests against a chain that has both its own contracts AND the inherited mainnet state (real DEX pools, real oracle contracts, real bridges, whatever the component depends on).

**What this requires from the shadow-fork side** (largely already there or trivial extensions of existing gaps):

1. `shadow-fork create` / `spawn-validators` must expose a **stable, predictable RPC endpoint** for the shadow — same shape as the fork-parent launcher exposes today (`--http.port=…`, discoverable via a status file under `<datadir>/shadow-forks/<name>/rpc.json` or similar).
2. The chain-id + genesis hash of the shadow must be **queryable via a status command** (`erigon shadow-fork status --fork=<name>`) so Cocoon can configure its signing without hand-editing config.
3. Shadow-fork chain-ids and identity must be **stable across `shadow-fork create` reruns with the same `--from + --at-block + --fork-name`** — Cocoon's deployment scripts assume deterministic contract addresses derived from `(chain-id, deployer-nonce)`; a shadow that changes chain-id on every rebuild breaks that assumption.
4. Shadow-fork must accept **externally-submitted transactions immediately after `spawn-validators` returns** — no post-spawn warm-up window during which Cocoon's contract-deployment txns get rejected. Validators must be already producing blocks when the command returns.

**What this requires from the Cocoon side** (out of scope for this doc — belongs on the componentization roadmap):

- A `--shadow-mode` boot flag that swaps Cocoon's normal chain-endpoint configuration for the shadow's RPC.
- Idempotent contract-deployment on shadow-mode startup (if contracts are already deployed at expected addresses, skip; else deploy).
- No assumption that the underlying chain has any particular history beyond what's inherited from the parent's state at N.

Cross-refs: [component-actor-model-refactor], [componentization-push-plan], [three-legs-stable] on the componentization side; `erigon-documents/cocoon/` for Cocoon's own docs.

## Ordering — what to do next

**In flight** (August 2026):

- **G6 diagnosis** — unblocks mode-B unwind reliability, which is what enables `debug_setFork` on already-synced nodes without leaving them wedged. Directly on the shadow-fork critical path.
- **Local hoodi master publisher** ([memory/local-master-publisher-architecture-2026-08-02.md]) — the architecture doubles as the shadow-fork infrastructure. A local master IS a shadow-fork parent that other tests consume; the whole test suite validating the ENR/UCAN stack IS the same code path a single-command shadow-fork would exercise.
- **L10 followup 3× runs** — proves the fresh-sync path (which shadow-fork bootstrap piggybacks on) is deterministic.

**Then, in order:**

1. Gap 5 (test-harness re-entrancy) — because it's a prerequisite for any single-command flow that survives being called more than once.
2. Gap 1 (auto-mint chain.toml + UCAN + trust-root) — turns the current fork-parent launcher's manual key handling into an implementation detail.
3. Gap 3 (bootnode auto-wiring) + Gap 4 (validator provisioning) — bundle: the "spawn-validators" command needs both.
4. Gap 2 (genesis derivation) — the client-side of `shadow-fork create` that closes the "manual `genesis.json` edit" step.
5. Gap 6 (snapshot trimming) — the "minimal disk" enabler. Landing this last because it's an optimization over a working end-to-end flow, not a blocker.
6. Gap 7 (mempool-mirror) — optional stage-5 feature, separately scheduled.

## Success criteria

**Two independent criteria — both must pass.** Fixing one gap must advance BOTH criteria.

### Criterion 1 (Tier A — step-wise)

An expert operator, using only documented CLI flags + `integration` subcommands + already-shipped launcher scripts, can produce a running validator cluster on a private shadow fork of mainnet at a chosen block N. Each step is a distinct explicit action they run; each step's output is verifiable before proceeding to the next. Nothing requires undocumented tribal knowledge or manual `genesis.json` editing.

The operator can also drive individual sub-flows in isolation:
- "Mint a fork identity without spawning validators."
- "Spawn validators against an existing fork parent."
- "Drive a mode-B unwind on an existing node to prepare it as a fork boundary."
- "Deploy Cocoon contracts against an existing shadow-fork RPC endpoint."

Each of those is a legitimate use-case that composes the primitives differently.

### Criterion 2 (Tier B — single-command)

On an empty directory on a developer laptop, the following two-command flow produces the same running validator cluster, without any manual file editing, key management, or bootnode configuration:

```
$ erigon shadow-fork create --from=mainnet --at-block=20000000 --fork-name=my-shadow
[shadow-fork] snapshot download: X GB fetched in Y min
[shadow-fork] fork identity minted: chain-id=... trust-root=...
[shadow-fork] fork-parent running on p2p port 30303, RPC 8545
$ erigon shadow-fork spawn-validators --fork=my-shadow --count=5
[shadow-fork] spawned 5 validators, all attesting on my-shadow
$
```

The flow is reproducible / idempotent — running the same commands on the same box does not require manual cleanup or state-directory management.

**Both flows produce the identical end state.** Tier B is Tier A's composition, wrapped and defaulted.
