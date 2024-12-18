ChangeLog
---------

## v3.0.0-beta1 (in development)

### Breaking changes

- `--prune.mode` default is `full`. For compatibility with `geth` and other clients. Plz set explicit
  `--prune.mode` flag to your Erigon3 setups - to simplify future upgrade/downgrade.
- Bor chains: enable our internal Consensus Layer by default (name: Astrid)
- Released binaries for Linux are now dynamically linked (
  Issues: https://github.com/erigontech/erigon/issues/12570, https://github.com/erigontech/erigon/issues/12556 )

### New features:

- Decided to fix snapshots format and go for `beta1`. Main focus: fast bug-reports fixes, chain-tip/rpc perf, validator
  mode.
- Erigon3 book: https://development.erigon-documentation-preview.pages.dev/

### TODO

- milestone: https://github.com/erigontech/erigon/milestone/5
- Known problem:
    - external CL support
    - `erigon_getLatestLogs` not implemented

### Acknowledgements:

## v3.0.0-alpha7

### Improvements:

- Faster eth_getTransactionReceipt with "txn-granularity cache" in https://github.com/erigontech/erigon/pull/13134 and "executing only 1 txn"  https://github.com/erigontech/erigon/pull/12424
- Return PrunedError when trying to read unavailable historical data in https://github.com/erigontech/erigon/pull/13014

### Fixes:

- Fix trace_block returning "insufficient funds" (Issues #12525 and similar) with standalone rpcdaemon in https://github.com/erigontech/erigon/pull/13129


### Acknowledgements:

## v3.0.0-alpha6

### Breaking changes

- `--prune.mode` default is `full`. For compatibility with `geth` and other clients. Plz set explicit
  `--prune.mode` flag to your Erigon3 setups - to simplify future upgrade/downgrade.

### New features:

- Reduced `.idx` and `.efi` files size by 25% (require re-sync)
- Support: `debug_getRawReceipts`
- debian packages
- `--externalcl` support
- bor-mainnet can work on 32G machine
- Erigon3 book: https://development.erigon-documentation-preview.pages.dev/

### Fixes:

- `eth_syncing` works on Bor chains
- support upper-bounds at: `eth_accRange` https://github.com/erigontech/erigon/pull/12609 ,
  `erigon_getBalanceChangesInBlock` https://github.com/erigontech/erigon/pull/12642,
  `debug_getModifiedAccountsByNumber` https://github.com/erigontech/erigon/pull/12634
- `eth_getLogs` fix `fee cap less than block` https://github.com/erigontech/erigon/pull/12640

### Acknowledgements:

## v3.0.0-alpha5

- Breaking change: Caplin changed snapshots format
- RPC-compatibility tests passed
- Caplin eating 1Gb less RAM. And Erigon3 works on 16gb machine.
- time-limit for pruning on chain-tip: https://github.com/erigontech/erigon/pull/12535
