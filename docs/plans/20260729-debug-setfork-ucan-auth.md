# debug_setFork UCAN Authority

**Date:** 2026-07-29
**Depends on:** the fork componentization landings (`89175ee888`…),
snapshotauth's `Verifier` + `ForkAuthorityAccept` + capability model.

## Problem

`debug_setFork` today is protected only by the RPC namespace (needs
`--http.api=debug`). Anyone who can reach that RPC can transition the
node onto any chain the datadir + chain.json can resolve. That's
weaker access control than an offline `snapshots fork-from` — that
tool requires signing keys + trust-root configuration to produce a
usable fork datadir.

Symmetry with the offline path: the operator needs to prove they hold
the fork's authority before the node acts.

## Design

**New capability:** `fork:transition:<fork-chain-name>` in
`node/components/snapshotauth/capabilities.go`. Format mirrors
`fork:from:<pubkey>` — dynamic, parsed prefix-wise. Bound to a
specific fork chain name so a UCAN granted for one fork can't
authorize transitions to another.

**RPC signature change:**

```go
// Before
func (s *Ethereum) SetFork(ctx, targetChainName string) (*SetForkResult, error)

// After
func (s *Ethereum) SetFork(ctx, targetChainName, authorityUCAN string) (*SetForkResult, error)
```

`authorityUCAN` is the base64-encoded CBOR UCAN the caller presents.
Empty → immediate rejection. Same shape flows through
`rpchelper.ForkController`, `DebugAPIImpl.SetFork`, and the
`integration set_fork` client (new `--authority-ucan-file` flag).

**Node-side verification (in `Ethereum.SetFork`, before delegating to
`fork.Controller`):**

1. Decode `authorityUCAN` (base64 → CBOR).
2. Verify signature + time-window via a `snapshotauth.Verifier` built
   from the node's operator-configured trust roots (same
   `--snapshot.trust-roots` set used for snapshot advertise UCAN
   verification — reuse rather than introduce a parallel accept-set).
3. Require capability `fork:transition:<targetChainName>` present in
   the leaf UCAN (matched exactly — not a delegated cascade prefix).
4. Optional but recommended: audience match. If the node has an
   operator pubkey configured, the leaf UCAN's audience must equal
   it. Without this, a UCAN intended for operator A could be replayed
   against operator B's node.
5. Verifier failure → return an actionable RPC error (kind:
   `"authority_rejected: <reason>"`), no state changes, no
   `fork.Controller.Transition` invocation.

**Fork.Controller stays authority-agnostic.** Verification is
security-perimeter concern at the RPC edge; the Controller remains a
pure state-transition primitive. Same design decision as
`SwapChainConfig` vs `SetChainConfig` — the wrapper orchestrates
policy; the Controller executes mechanics.

**Minting tool:** operators produce transition-caps via a new
`MintForkTransitionUCAN` helper in `node/components/snapshotauth`
(alongside `MintForkAuthorityUCAN`). Caps embed
`fork:transition:<fork-chain-name>`; issuer is one of the fork's
trust roots; audience is the target operator's pubkey; expiry is
short (hours to days, not 90d like the long-lived authority).

## Rollout

- **Phase 1 (this PR):** capability, verifier hook, RPC signature,
  client, error path, unit tests.
- **Phase 2 (follow-up):** `MintForkTransitionUCAN` minting helper
  + `integration mint_fork_transition` CLI so operators can produce
  UCANs from their trust-root keys without going through
  `snapshots fork-from`.
- **Phase 3:** `fork-rpc-transition.sh` + `fork-soak-until-stopped.sh`
  drivers wire in the minting + UCAN-passing steps so the E2E soak
  covers the authenticated flow, not the bare RPC.

## What this closes

- The gap the user flagged: "you can't ask a node to fork unless you
  have authority over it."
- Reduces the blast radius of an exposed `--http.api=debug` port —
  reaching the RPC no longer implies control over the chain identity.
- Provides the crypto anchor the chain.toml integration follow-up
  needs: the fork's authority UCAN is the identity that would sign
  the extended chain.toml carrying CL config + parent-cut hashes.

## Non-goals for Phase 1

- No changes to `snapshots fork-from` — offline path already requires
  the operator to hold signing keys directly.
- No revocation / rotation flow — short UCAN lifetime + operator
  reissuance is the mechanism; explicit revocation lists are
  out of scope until we see the need.
- No delegation-cascade caps for fork transition (unlike snapshot
  advertise which walks through `ForkAncestryResolver`). A transition
  cap must be direct-signed by an accepted trust root.
