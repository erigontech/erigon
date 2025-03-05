ChangeLog
---------

## v3.1.0 (in development)

**Improvements:**

TODO

**Bugfixes:**

TODO

### TODO

- milestones:
https://github.com/erigontech/erigon/milestone/31


## v3.0.0 (in development)


### Milestone

https://github.com/erigontech/erigon/milestone/30

## v3.0.0-rc2

**Bugfixes:**

- Caplin: error on aggregation_bit merge by @domiwei in https://github.com/erigontech/erigon/pull/14063
- Pectra: fix bad deposit contract deposit unmarshalling by @Giulio2002 in https://github.com/erigontech/erigon/pull/14068

### Milestone

https://github.com/erigontech/erigon/milestone/36

## v3.0.0-rc1

**Improvements:**

- Schedule Pectra for Chiado by @yperbasis in https://github.com/erigontech/erigon/pull/13898
- stagedsync: dbg option to log receipts on receipts hash mismatch (#13905) by @taratorio in https://github.com/erigontech/erigon/pull/13940
- Introduces a new method for estimating transaction gas that targets the maximum gas a contract could use (#13913). Fixes eth_estimateGas for historical blocks (#13903) by @somnathb1 in https://github.com/erigontech/erigon/pull/13916

**Bugfixes:**

- rpcdaemon: Show state sync transactions in eth_getLogs (#13924) by @shohamc1 in https://github.com/erigontech/erigon/pull/13951
- polygon/heimdall: fix snapshot store last entity to check in snapshots too (#13845) by @taratorio in https://github.com/erigontech/erigon/pull/13938
- Implemented wait if heimdall is not synced to the chain (#13807) by @taratorio in https://github.com/erigontech/erigon/pull/13939

**Known Problems:**

- polygon: `eth_getLogs` if search by filters - doesn't return state-sync (state-sync events are not indexed yet). Without filter can see state-sync events. In `eth_getReceipts` also can see. [Will](https://github.com/erigontech/erigon/issues/14003) release fixed files in E3.1
- polygon: `eth_getLogs` state-sync events have incorrect `index` field. [Will](https://github.com/erigontech/erigon/issues/14003) release fixed files in E3.1

### Milestone

https://github.com/erigontech/erigon/milestone/34

## v3.0.0-beta2

### Breaking changes
- Reverts Optimize gas by default in eth_createAccessList #8337  

### Improvements:

- `eth_estimateGas`: StateOverrides and HistoricalBlocks support
- fixes a number of issues on Polygon with the new default flow (Astrid)
  - `nonsequential block in bridge processing` - should be fixed
  - `pos sync failed: fork choice update failure: status=5, validationErr=''` - should be fixed
  - `external rpc daemon getting stuck` - should be fixed
  - `process not exiting in a clean way (getting stuck) upon astrid errs` - should be fixed
  - `very rare chance of bridge deadlock while at chain tip due to forking` - should be fixed

### TODO

- milestone: https://github.com/erigontech/erigon/milestone/28
- Known problem:
    - external CL support
    - `erigon_getLatestLogs` not implemented

### Acknowledgements:

## v3.0.0-beta1

### Breaking changes

- Bor chains: enable our internal Consensus Layer by default (name: Astrid)
    - The process should auto upgrade - in which case you may find that it starts creating new snapshots for checkpoints
      and milestones.
    - This may however fail, as there are a number of potential edge cases. If this happens the process will likely stop
      with a failure message.
    - In this situation you will need to do a clean sync, in which case the complete snapshot set will be downloaded and
      astrid will sync.
    - If you want to prevent this and retain the old behaviour start erigon with --polygon.sync=false

### Acknowledgements:

## v3.0.0-alpha7

### Improvements:

- Faster eth_getTransactionReceipt with "txn-granularity cache" in https://github.com/erigontech/erigon/pull/13134 and "
  executing only 1 txn"  https://github.com/erigontech/erigon/pull/12424
- Return PrunedError when trying to read unavailable historical data in https://github.com/erigontech/erigon/pull/13014

### Fixes:

- Fix trace_block returning "insufficient funds" (Issues #12525 and similar) with standalone rpcdaemon
  in https://github.com/erigontech/erigon/pull/13129

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
