ChangeLog
---------

## v3.0.11

### Milestone

https://github.com/erigontech/erigon/milestone/50

The default gas limit for Ethereum Mainnet is increased to 45M in line with other clients.

**Improvements:**
- Added chain-specific gas limit defaults by @Giulio2002 in https://github.com/erigontech/erigon/pull/15802

**Bugfixes:**

**Full Changelog**: https://github.com/erigontech/erigon/compare/v3.0.10...v3.0.11

## v3.0.5

### Milestone

https://github.com/erigontech/erigon/milestone/44

## v3.0.4

### Milestone

https://github.com/erigontech/erigon/milestone/43

**Improvements:**

- disable diagnostics by default by @yperbasis in https://github.com/erigontech/erigon/pull/14953
- less disk IO during files merge by @AskAlexSharov in https://github.com/erigontech/erigon/pull/14901
- stage_custom_trace: to produce indices by @AskAlexSharov in https://github.com/erigontech/erigon/pull/14879
- persist receipts - external rpcd support by @AskAlexSharov in https://github.com/erigontech/erigon/pull/15004
- support `NO_PRUNE` env var by @AskAlexSharov in https://github.com/erigontech/erigon/pull/15131
- cmd: Increase default `db.size.limit` by @AskAlexSharov in https://github.com/erigontech/erigon/pull/15170
- notify rpcd when e3 files change by @sudeepdino008 in https://github.com/erigontech/erigon/pull/15044
- mdbx v0.13.6 by @JkLondon in https://github.com/erigontech/erigon/pull/15112

**Bugfixes:**

- Fix issues reported in snapshot processing to fix sync issues due to bugs in sync event and checkpoint snapshot production by @eastorski in https://github.com/erigontech/erigon/pull/14887, https://github.com/erigontech/erigon/pull/14947,  https://github.com/erigontech/erigon/pull/14951
- Update go-libutp for AUR build error by @anacrolix in https://github.com/erigontech/erigon/pull/14892
- Caplin: fix occassional mev-boost bug by @Giulio2002 in https://github.com/erigontech/erigon/pull/14991
- Erigon: optimistic inclusion for deep reorgs #14875 by @Giulio2002 in https://github.com/erigontech/erigon/pull/14876
- Caplin: Fix misc issues after electra (#14910) by @domiwei in https://github.com/erigontech/erigon/pull/14935
- fix erigon seg retire to handle incomplete merges by @sudeepdino008 in https://github.com/erigontech/erigon/pull/15003
- fast failing if version string contains "." (v1.0- v2.23 for ex) by @JkLondon in https://github.com/erigontech/erigon/pull/15048
- prune mode flag parsing and `String()`-ing by @awskii in https://github.com/erigontech/erigon/pull/14882
- rpcdaemon: fix txNum at GetReceipt call in getLogsV3 by @lupin012 in https://github.com/erigontech/erigon/pull/14986

**Full Changelog**: https://github.com/erigontech/erigon/compare/v3.0.3...v3.0.4

## v3.0.3

### Milestone

https://github.com/erigontech/erigon/milestone/42

**RPC fixes:**

Polygon users who have previously run migration steps to fix incorrect logIndex related to state sync transactions released in 3.0.2 are advised to run the migration steps again and add the --polygon.logindex flag to their flags for a complete mitigation.

**Improvements:**

- consensus: Add syscall failure scenarios (#14403) by @somnathb1 in https://github.com/erigontech/erigon/pull/14818
- consensus: validate fixed lengths in abi decoding EIP-6110 deposit requests by @somnathb1 in https://github.com/erigontech/erigon/pull/14823
- historical receipts persistency (optional) by @AskAlexSharov in https://github.com/erigontech/erigon/pull/14781
- reduce dependency on github; download snapshot hashes from R2 by @wmitsuda in https://github.com/erigontech/erigon/pull/14849

**Bugfixes:**

- eth, execution: Use block level gasPool in serial execution (#14761) by @somnathb1 in https://github.com/erigontech/erigon/pull/14820
- no greedy prune on chain-tip  (node did fall behind periodically) by @AskAlexSharov in https://github.com/erigontech/erigon/pull/14782
- fixed performance and ordering issues with the [RPC fix from 3.0.2](https://github.com/erigontech/erigon/releases/tag/v3.0.2) by @mh0lt and @shohamc1 in https://github.com/erigontech/erigon/pull/14842 https://github.com/erigontech/erigon/pull/14785 https://github.com/erigontech/erigon/pull/14790

**Full Changelog**: https://github.com/erigontech/erigon/compare/v3.0.2...v3.0.3

## v3.0.2

### Milestone

https://github.com/erigontech/erigon/milestone/41

**Gnosis users: this is a required update for the upcoming Pectra hardfork scheduled for 30 April 2025**

**RPC fixes:**

Previous versions of Erigon 3 have two bugs regarding handling of state sync events on Polygon chains:
- Incorrect `logIndex` on all state sync transaction logs
- Missing log events when using `eth_getLogs` with filters

A proper fix has been implemented and will be progressively rolled out in Erigon 3.1 and 3.2 (track issue [here](https://github.com/erigontech/erigon/issues/14003)). A temporary workaround has been introduced if these issues are critical for your use-case. This requires regenerating receipts for all transactions on the chain. The procedure is as follows:
1. Shutdown Erigon and all rpcdaemon processes
2. Update binaries Erigon release
3. `erigon seg rm-state-snapshots --domain=receipt --datadir <your-datadir>`
4. `integration stage_custom_trace --datadir <your-datadir> --chain <amoy|bor-mainnet> --bor.heimdall <heimdall-url>`
	- For machines with many cores, you can add ` --exec.workers=<num>` to improve performance (default is `7`)
5. Once complete, `rm -rf <your-datadir>/chaindata`
6. Start Erigon

**Improvements:**

- Schedule Pectra hard fork for Gnosis Mainnet (#14521) by @somnathb1 in https://github.com/erigontech/erigon/pull/14523
- params: Use no padding in minor version (#14588) by @somnathb1 in https://github.com/erigontech/erigon/pull/14594
- recent receipts persistence: to reduce RPC latency  by @AskAlexSharov in https://github.com/erigontech/erigon/pull/14532
- historical `eth_getProof` experimental flag by @Giulio2002 in https://github.com/erigontech/erigon/pull/14568
- Enhance efficiency of attestation selection (#14624) by @Giulio2002 in https://github.com/erigontech/erigon/pull/14633
- EngineAPI: recover from missing chain segments (#14579) by @Giulio2002 in https://github.com/erigontech/erigon/pull/14589
- remove unsafe closing of subsumed files in mergeLoopStep by @sudeepdino008 in https://github.com/erigontech/erigon/pull/14557

**Bugfixes:**

- execution: fix missing log notifications when flushing extending fork… by @Giulio2002 in https://github.com/erigontech/erigon/pull/14578
- EthereumExecution: fix canonical chain routine (#14580) by @Giulio2002 in https://github.com/erigontech/erigon/pull/14592
- caplin: fix parsing topics (#14543) by @Giulio2002 in https://github.com/erigontech/erigon/pull/14593
- Fix `trace_transaction` for Polygon chains (#14470) by @shohamc1 in https://github.com/erigontech/erigon/pull/14530

**Full Changelog**: https://github.com/erigontech/erigon/compare/v3.0.1...v3.0.2

## v3.0.1 

**Improvements:**

- receipts gen: dedup parallel re-exec of same block by @AskAlexSharov in https://github.com/erigontech/erigon/pull/14377
- Increase maximum open files limit on MacOSX by @AlexeyAkhunov in https://github.com/erigontech/erigon/pull/14427
- engineeapi, txpool: Implement GetBlobsV1 (#13975) by @somnathb1 in https://github.com/erigontech/erigon/pull/14380
- cmd: Set default EL extradata to erigon-version (#14419) by @somnathb1 in https://github.com/erigontech/erigon/pull/14435
- Schedule Pectra hard fork for Ethereum Mainnet (#14424) by @somnathb1 in https://github.com/erigontech/erigon/pull/14436
- engineapi: Add requests nil check (#14421) by @somnathb1 in https://github.com/erigontech/erigon/pull/14499

**Bugfixes:**

- Include execution requests in produced block (#14326) by @domiwei in https://github.com/erigontech/erigon/pull/14395
- txpool: Fix 7702 signature parsing and simplify auth handling (#14486) by @somnathb1 in https://github.com/erigontech/erigon/pull/14495

### Milestone

https://github.com/erigontech/erigon/milestone/40

## v3.0.0 

### Milestone

https://github.com/erigontech/erigon/milestone/30


## v3.0.0-rc3

**Bugfixes:**

- fixed txPool panics by @somnathb1 in https://github.com/erigontech/erigon/pull/14096
- fixed sendRawTransaction and eth_getTransactionByHash by @shohamc1 in https://github.com/erigontech/erigon/pull/14077

### Milestone

https://github.com/erigontech/erigon/milestone/37

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

**Bugfixes:**

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
