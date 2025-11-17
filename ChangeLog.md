# Changelog

https://keepachangelog.com/en/1.1.0/


## [3.3.0] – 2025-11-17

### Added

- Support of historical `eth_getProof` (https://github.com/erigontech/erigon/issues/12984). It requires `--prune.experimental.include-commitment-history` flag.
- 

#### RPC Endpoints 
- `eth_simulateV1`: Complete implementation of Ethereum simulation API with support for state overrides, blob transactions, block overrides, and historical state roots (#15771)
- `eth_createAccessList`: StateOverrides parameter support (#17653)
- Support for `eth_call` with blockOverrides (#17261)
- `trace_filter`: Block tags support (#17238)

#### Consensus & Execution
- Fusaka scheduling enabled on Ethereum mainnet (#17734)
- EIP-7928: BlockAccessList type support (#17544)
- EIP-7934: EstimateGas capped by MaxTxnGasLimit in Osaka (#17251)
- EIP-7825: Gas limit enforcement in Osaka (#17251)
- EIP-7702 transaction support (#17412)
- Rewrite bytecode support for post-Merge blocks (#17770)

#### Caplin (Consensus Layer)
- Get blobs support (Fusaka compatibility) (#17829)
- Faster Data Availability recovery (#17392)
- Better waiting heuristic for snapshot downloader (#17204)
- Improved block downloader (#15255)

#### Monitoring
- Block serial execution metric (#17353)
- BlockRangeUpdate publishing every 32 blocks (#17567)
- Client-level trackers implementation (#17765)

#### Error Handling & Validation
- Self-destruct operation validation (EIP 6780) (#17728)
- Gossip manager with rate limiting (#17606)
- CODEOWNERS configuration (#17644)
- Filters returning empty arrays instead of null (#17615)

### Changed

#### Performance Optimizations
- Zero allocation interpreter call loop in VM (#17573)
- Reduced memory copies in changeset operations (#17659)
- BTree seek optimization: reduced disk reads and allocations (#17693)
- VM memory resize refactoring (#17730)
- Improved string sorting with slices.Sort (#17744)
- Reduced allocations in ETL pre-allocation and sort buffer (#17695)
- Improved BTree index Get operation performance (#17707)

#### RPC Improvements
- `eth_getProof`: Support for proofs on non-existent keys and extension nodes (#17760, #17802, #17551)
- `eth_getTransactionReceipt`: Pre-Byzantium transaction handling (#17479, #17509)
- `eth_estimateGas`: Improved handling with StateOverrides (#17914, #17295)
- `debug_traceCall`: System contract execution support (#17339)
- Filter operations: Better error handling for missing filters (#17327)
- State override handling throughout RPC layer (#17460, #17581, #17313, #17390)
- RPC storage slot validation (#17340)
- Blob transaction and blob base fee override support (#17313)
- Hive compatibility improvements (#17584, #17520, #17509)
- Nonce validation in eth_simulateV1 (#17390)

#### Execution Engine
- Block building: Deadlock prevention with single builder (#17213)
- Newpayload processing: Optimized to avoid background file building (#17436)
- ChainTip pruning: 500ms timeout enforcement (#17889)
- E3 file collation: Reorg protection (#17469)
- Block limit exhausted handling in parallel flows (#17683, #17679, #17455)
- E3 file collation reorg protection (#17469)
- Parallel ExecV3 processing (#16922)
- Execution engine → rules engine rename (#17807)

#### State & Commitment
- Historical state query improvements (#17413)
- Proof verification for non-existent accounts (#17802)
- Storage slot validation enhancements (#17340)
- CachedWriter: Incarnation=1 conflict prevention (#17498)
- Domain compaction: State root key protection (#17380)
- Final commitment calculation support (#17361)
- Commitment extraction and interface improvements (#17531, #17530)
- GetAsOf() dereferencing support (#17687)

#### Consensus Layer
- Fork schedule: Fixed initial previous_version handling (#17263)
- Blob request support after Fusaka (#17492)
- Electra transition: Demoted verbose logging (#15835)
- Custody columns revision (#17247)
- Data column parameters and constraints (#17239, #17248, #17241)
- Pending epoch deletion order fix (#17416)
- MAX_PENDING_PARTIALS_PER_WITHDRAWALS_SWEEP for Gnosis (#17501)

#### Network & P2P
- Gossip manager refactoring with rate limiting (#17606)
- RequestMore: Switched to channel-based communication (#17881)
- Context handling: Cancellable context for batch signature verification (#17400)
- NAT mapping error propagation (#16761)
- P2P PRNG seed improvement (#17410)
- Error string caching in p2p discovery (#17781)

#### Data Handling
- Column data validation before marking as seen (#17241)
- EIP-7594: wrapper_version validation for blob transactions (#17481)
- BlockUpdateRange packet validation (#17464)
- Legacy transaction decoding: Catch trailing bits (#17555)
- Transaction commitment count validation (#17553)

#### Code Organization
- Directories reorganized: `core` integrated into `execution` (#17384, #17812)
- VM integrated into execution (#17367)
- `ethstats` moved to `node` package (#17442)
- `spectest` moved to `cl/spectest` (#17579)
- Interfaces moved from erigon-lib to node (#17223)
- Top-level `eth` split across appropriate packages (#17393)
- Kill erigon-lib go module (#17337)
- Trie moved into commitment (#17526)

#### Docker & Build
- Updated to Go 1.25-trixie (#17837)
- Debian 13 "trixie" for Docker images (#17774)
- Customized Docker build process (#17524)

#### Dependencies & Build
- golangci-lint: Updated to v2.5.0 (#17516)
- roaring bitmap: Version upgrade (#17587)
- gnark-crypto: Updated to 0.19.1 (#17721)
- quic-go: Updated to 0.49.1 (#17420)
- mdbx: Updated to 0.13.7 with fixes (#17517)
- Default MDBX flags set (#17621)
- Context cancellation improvements (#17623)

#### Mining
- Removed PoW mining code organization (#17813)

#### Blockchain Parameters
- Ethereum mainnet default block gas limit to 60M (#16949)
- Rio Hard Fork Block for bor-mainnet (#17254)

### Deprecated

- Deprecated DB API stub removed in favor of typed interfaces (#17863)
- Legacy .ef conversion tools deprecated and removed (#17536)
- DiagFlags removed (#17671)
- SkipAnalysis VM optimization removed (#17217)

### Removed

- PoW mining code removed (#17813)
- Holesky network support removed (#17685)
- eth/67 protocol support removed (#17318)
- Rclone downloader removed (#17433)
- MDBX store backend removed in favor of current implementations (#17434)
- BadHashes legacy code removed (#17733)
- Unmaintained Control Flow Analysis/Graph removed (#17853)
- Unused `IterateChangedKeys` method from InvIdx (#17570)
- Unused convertToParityTrace stub removed (#17754)
- Unused fields and accessors from downloader (WebSeeds) (#17439)
- Unused ticker in buildFiles (#17425)
- Unused utils.c from sais library (#17443)
- Unused splitOntoHexNibbles helper (#17708)
- Old block downloader flow (#17333)
- BaseAPI.genesis helper (#17722)
- Unused print stages cmd flags (#17514)
- Redundant variable declarations in for loops (#17657)
- Obsolete //+build tag (#17577)
- Unused useDefaultValue from BlockAmount and Distance (#17489)
- Unused RetryableHttpLogger adapter (#17072)
- Time.Tick in trackRemovedFiles preventing timer leak (#17283)
- Unnecessary CopyBytes in cl/rpc (#17792)
- Dead loadSkipFunc and unused stepBytes (#17569)
- Over-allocation in ETL Prealloc (#17695)
- NextBlockProposers usage (#17576)
- Redundant boolean comparison in tracing assertion (#17885)
- math/rand fallback for subscription IDs (now uses crypto/rand) (#17199)

### Fixed

#### RPC
- `eth_estimateGas`: StateOverrides handling (#17914)
- `eth_getTransactionReceipt`: Pre-Byzantium support and post-state caching (#17509, #17520)
- `eth_getFilter`: No changes and filter not found error handling (#17327)
- `eth_simulateV1`: State root, blob gas, nonce validation, and cache hit issues (#17276, #17401, #17390, #17520)
- `debug_traceCall`: System contract execution (#17339)
- `debug_traceBlockByNumber`: Clear error messaging for missing blocks (#17663)
- `eth_getProof`: Key quantity and root hash verification (#17301, #17374, #17687)
- `eth_syncing`: Initial highest block calculation (#17409)
- `eth_call`: Block override support (#17261)
- Hive simulatev1: Error code compatibility (#17584)
- HTTP client timeout: Prevent infinite hangs (#16788)
- GetModifiedAccountsByNumber/Hash semantics (#17513)
- Error handling in OnOpcode memory copy (#15075)
- Modified account access list order (#17291)

#### Execution
- Block building deadlock with single builder (#17213)
- ChainTip mode in stage_exec (#17482)
- Block limit exhausted in parallel flow (#17683, #17679)
- Newpayload/block building race conditions (#17436)
- E3 file collation reorg protection (#17469)
- Initial cycle flag on first FCU with empty DB (#17515)
- Error in stage_exec with --no-commit (#17311)
- Deadlock in TestWebsocketLargeCall (#17706, #17762)

#### State & Commitment
- Historical state queries from commitment (#17413)
- Proof verification for non-existent accounts (#17802)
- GetAsOf() for commitment dereferencing (#17687)
- Shortened key accessor check in findShortenedKey (#17850)
- CachedWriter incarnation handling (#17498)
- Domain compaction commitment key protection (#17380)
- Data race in HexPatriciaHashed parallel tests (#17423)

#### Consensus Layer
- Fork schedule initial previous_version (#17263)
- Initial cycle flag on first FCU with empty DB (#17515)
- Block downloader behavior in various modes (#17897)
- Blob epoch ordering (#17493)
- EpochData SSZ marshalling (#17495)
- Validator block production on Hoodi (#17499)
- Electra state transition logging (#15835)

#### Data Handling
- Column data validation and length constraints (#17241, #17248)
- EIP-7594 wrapper_version validation (#17481)
- Legacy transaction decoding: trailing bits (#17555)
- BlockAccessList packet validation before send/receive (#17464)
- EEST error returns (#17300)
- DeleteNewerEpochs deletion order (#17416)
- Blob requests after Fusaka (#17492)

#### Testing & QA
- Tip-tracking under memory constraints re-enabled (#17497)
- Sync with external CL test reliability (#17640)
- HexPatriciaHashed data race in parallel tests (#17423)
- TestWebsocketLargeCall deadlock (#17706, #17762)
- TestMergedFileGet flakiness (#17594, #17545, #17668)
- Bash test pipeline (#17895)
- Test filtering improvements (#17852)
- Race conditions in exec3 metrics (#17293)
- Tip event channel tests flakiness (#17200)
- DUMP_RESPONSE in RPC test runner (#17719, #17275)
- ERIGON_PID assignment in remote tests (#17761)

#### Monitoring & Diagnostics
- Diag goroutines appearing without diag flags (#17888)
- ReadStorageBodyRLP incorrect log message (#17825)
- Broken links in documentation (#17900, #17903)
- Parameter expansion in shell code (#17224)
- Log directory permissions (0764 → 0755) (#17860)
- Brittleness in integrity check (#17698)
- Rebuild commitment when datadir name contains commitment (#17749)
- Download execution history ETA calculation (#17691)

#### Infrastructure
- Bash test pipeline (#17895)
- CI workflow for remote RPC testing (#17699)
- Docker login condition in GitHub Actions (#17598, #17639)
- Shutter validator registration URL (#17865)
- Broken Gitbook links and layout (#17875, #17900)

#### Cryptography & Security
- Hash length validation in no-cgo Sign (#17831)
- GasPrice check when NoBaseFee (#17196)

### Security

- Switched to `crypto/rand` for subscription ID generation; removed math/rand fallback (#17199)
- NAT mapping: Propagated internalAddress error (#16761)
- Improved error propagation in database operations (#17265, #17854, #17565)
- User input sanitization in shell and test scripts (#17228, #17207, #17212, #17224)
- Large file detection in git history (#17655)
- Hash length validation in no-cgo Sign (#17831)
- Max RLP block size enforcement from MEV relays (#17620)

### Documentation

- Gitbook improvements and restructuring (#17858, #17847, #17875)
- Broken links fixes (#17900, #17903)
- Shutter validator registration URL fix (#17865)
- KV interface documentation (#17738)
- Blockchain FAQ: Historical blocks data availability (#17478)
- Docker customization documentation (#17524)
- Help section folder organization (#17896)
- Integration Readme GC table documentation (#17463)

### Tests & CI

#### Benchmark & Performance Tests
- Improved BTree index benchmark tests (#17486)
- Improved benchmarks for bt index (#17583)
- Benchmark tests using b.Loop() (#17427, #17456, #17457, #17465, #17468, #17470, #17471)
- Switch CL/DB benchmarks to b.Loop() (#17826)
- Decompressor heap-alloc vs buf-reuse bench (#17542)
- P2P nodedb bench sync thresholds (#17405)
- Exec comparison bench script (#17309)
- Block indexes bench tool (#17614)
- Executor bench script update (#17395)

#### Hive & Compatibility Tests
- Update EEST to v5.3.0 (#17507, #17418)
- Change simulation target from eest to eels (#17599)
- Hive rpc-compat suite zero failures (#17608)
- Hive execution auth and rpc compat tests (#17222)
- Hive fork to enable commitment history (#17504)

#### QA & Integration Tests
- Tip-tracking with load test improvements (#17355, #17446, #17697, #17839)
- Re-enable "tip-tracking under memory constraints" test (#17497)
- Sync-with-externalcl test improvements (#17640, #17582, #17848)
- Tip-tracking tests scheduling update (#17714)
- Tip-tracking log improvements (#17328)
- Plot upload task to sync-from-scratch workflows (#17312)
- Fix DUMP_RESPONSE in RPC test runner (#17275, #17719)
- Remote RPC testing workflow (#17699)
- Attach logs to RPC tests (#17240)
- Profiling data upload to tip-tracking (#17697)
- Hoodi test report addition (#17198)

#### Unit & Integration Tests
- Add unit tests (#17206)
- Fuzzers moved into native packages (#17688)
- Large files catch in git history (#17655)
- TestSetupGenesis less concurrency (#17873)
- Improve EpochData SSZ test (#17495)
- EIP-7702 transaction case (#17412)
- Tidy up test filtering (#17852)
- Disable eth_blobBaseFee in RPC latest tests (#17307)
- ETH config spec adherence (#17552)
- RPC-tests version bump to 1.94.0 (#17389)

#### Workflow & CI
- Race tests on go-1.25 (#17512)
- Re-enable docker login for tests with secrets (#17205)
- Cleanup step in test-all-erigon workflow (#17758)
- Conditional docker login in workflows (#17639, #17598)
- User input sanitization in test scripts (#17212, #17221)
- Temporary MergedFileGet disable (#17593)
- Git history large file detection (#17655)

#### Flaky Test Fixes
- TestMergedFileGet flakiness (#17545, #17594, #17668)
- TestWebsocketLargeCall deadlock (#17706, #17762)
- Tip event channel tests (#17200)
- HexPatriciaHashed data race (#17423)

### Dependencies

- build(deps): bump actions/upload-artifact from 4 to 5 (#17670, #17752)
- build(deps): bump actions/download-artifact from 5 to 6 (#17669)
- build(deps): bump actions/setup-node from 5 to 6 (#17543)
- build(deps): bump golangci/golangci-lint-action from 8 to 9 (#17834)
- build(deps): bump gacts/run-and-post-run from 1.4.2 to 1.4.3 (#17833)
- build(deps): bump al-cheb/configure-pagefile-action from 1.4 to 1.5 (#17835)
- build(deps): bump github.com/quic-go/quic-go from 0.48.2 to 0.49.1 (#17420)
- build(deps): bump github.com/consensys/gnark-crypto from 0.19.0 to 0.19.1 (#17721)
- Lighthouse client version updates in external CL tests (#17373, #17582, #17848)
- Gnosis rpc-tests version bump (#17591)

**Full Changelog**: https://github.com/erigontech/erigon/compare/v3.2.0...v3.3.0

Milestone: https://github.com/erigontech/erigon/milestone/58

Total commits between v3.2.0 and v3.3.0: **381 commits**

---

## [3.2.0] "Quirky Quests" – 2025-10-02

Erigon 3.2.0 has a complete implementation of [Fusaka](https://eips.ethereum.org/EIPS/eip-7607) and schedules it on the test nets (#17197):
- Holesky on  Wednesday, 1 October 2025 08:48:00 UTC
- Sepolia on Tuesday, 14 October 2025 07:36:00 UTC
- Hoodi on Tuesday, 28 October 2025 18:53:12 UTC

**Fixes**

- Re-org/unwind fixes (#17105, #17165) by @taratorio
- RPC: Fixes to eth_getProof (#16220, #16251, #16564, #16606, #16687) by @awskii
- tracer: fix prestates for EIP7702 transactions (#16497) by @nebojsa94

**Improvements**

- New EL block downloader (#16270, #16673) by @taratorio
- Caplin p2p improvements (#16719, #16995) by @Giulio2002
- EVM: MODEXP precompile performance improvements (#16579, #16583, #16396, #17151) by @chfast & @yperbasis
- execution: more accurate bad block responses (#16994) by @taratorio
- Block builder: improve txpool polling (#16412) by @taratorio
- execution/stagedsync: handle sync loop block limit exhaustion (#16268) by @taratorio
- RPC: Apply batch limit to WebSocket/IPC connections (#16255) by @grandchildrice
- RPC: Estimate gas align to geth (#16397) by @lupin012
- snapshotsync: add support for `--snap.download.to.block` (#16938) by @taratorio

**New features**

- Complete Fusaka implementation (#16183, #16185, #16186, #16187, #16184, #16391, #16401, #16207, #16420, #16428, #16494, #16457, #16644, #16928, #16945, #17060, #16989, #17076, #17169) by @taratorio, @yperbasis, @Giulio2002 and @domiwei
- Implement eth/69 (#15279, #17186, #17171) by @shohamc1
- RPC: implement new eth_config spec (#16218, #16410) by @taratorio
- RPC: impl admin_RemovePeer (#16292) by @lupin012

**Full Changelog**: https://github.com/erigontech/erigon/compare/v3.1.0...v3.2.0
 
-----

