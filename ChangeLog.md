ChangeLog
---------

## v3.0.0-beta1 (in development)

### Breaking changes

- `--prune.mode` default is `full`. For compatibility with `geth` and other clients. Plz set explicit
  `--prune.mode` flag to your Erigon3 setups - to simplify future upgrade/downgrade.
- Bor chains: enable our internal Consensus Layer by default (name: Astrid)
- Released artifacts for Linux are now dynamically linked (
  Issues: https://github.com/erigontech/erigon/issues/12570, https://github.com/erigontech/erigon/issues/12556 )

### Changes

- Decided to fix snapshots format and go for `beta1`. Main focus: fast bug-reports fixes, chain-tip/rpc perf, validator
  mode.
- Erigon3 book: https://development.erigon-documentation-preview.pages.dev/
- Known problem:
    - external CL support
    - `erigon_getLatestLogs` not emplimented

## v3.0.0-alpha5

- Breaking change: Caplin changed snapshots format
- RPC-compatibility tests passed
- Caplin eating 1Gb less RAM. And Erigon3 works on 16gb machine.
- time-limit for pruning on chain-tip: https://github.com/erigontech/erigon/pull/12535

### TODO

Acknowledgements:

New features:

Fixes:
