#!/bin/bash

set +e # Disable exit on error

# Array of disabled tests
disabled_tests=(
    # Erigon2 and Erigon3 never supported this api methods
    trace_rawTransaction
    # remove these lines after https://github.com/erigontech/rpc-tests/pull/284 and https://github.com/erigontech/erigon/pull/12655
    eth_feeHistory/test_12.json
    eth_feeHistory/test_13.json
    eth_feeHistory/test_15.json
    eth_feeHistory/test_16.json
    eth_feeHistory/test_17.json
    eth_feeHistory/test_18.json
    eth_feeHistory/test_19.json
    eth_feeHistory/test_20.json
    # total difficulty field was removed, then added back
    # remove this line after https://github.com/erigontech/rpc-tests/pull/282
    eth_getBlockByHash/test_10.json
    eth_getBlockByNumber/test_12.json
    # Erigon bugs: https://github.com/erigontech/erigon/pull/12609
    debug_accountRange,debug_storageRangeAt
    # need update rpc-test - because Erigon is correct (@AskAlexSharov will do after https://github.com/erigontech/erigon/pull/12634)
    # remove this line after https://github.com/erigontech/rpc-tests/pull/273
    debug_getModifiedAccountsByHash,debug_getModifiedAccountsByNumber
    # Erigon bug https://github.com/erigontech/erigon/issues/12603
    erigon_getLatestLogs,erigon_getLogsByHash/test_04.json
    # Erigon bug https://github.com/erigontech/erigon/issues/12637
    debug_traceBlockByNumber/test_05.tar
    debug_traceBlockByNumber/test_08.tar
    debug_traceBlockByNumber/test_09.tar
    debug_traceBlockByNumber/test_10.tar
    debug_traceBlockByNumber/test_11.tar
    debug_traceBlockByNumber/test_12.tar
    # remove this line after https://github.com/erigontech/rpc-tests/pull/281
    parity_getBlockReceipts
    parity_listStorageKeys/test_12.json
    # to investigate
    debug_traceCallMany/test_02.tar
    debug_traceCallMany/test_04.tar
    debug_traceCallMany/test_05.tar
    debug_traceCallMany/test_06.tar
    debug_traceCallMany/test_07.tar
    debug_traceCallMany/test_09.json
    debug_traceCallMany/test_10.tar
    engine_exchangeCapabilities/test_1.json
    engine_exchangeTransitionConfigurationV1/test_01.json
    engine_getClientVersionV1/test_1.json
    erigon_getBalanceChangesInBlock
    trace_replayBlockTransactions/test_29.tar
    # do these perhaps require Erigon up?
    admin_nodeInfo/test_01.json
    admin_peers/test_01.json
    erigon_nodeInfo/test_1.json
    eth_coinbase/test_01.json
    eth_createAccessList/test_16.json
    eth_getTransactionByHash/test_02.json
    eth_getWork/test_01.json
    eth_mining/test_01.json
    eth_protocolVersion/test_1.json
    eth_submitHashrate/test_1.json
    eth_submitWork/test_1.json
    net_peerCount/test_1.json
    net_version/test_1.json
    txpool_status/test_1.json
    web3_clientVersion/test_1.json)

# Transform the array into a comma-separated string
disabled_test_list=$(IFS=,; echo "${disabled_tests[*]}")

python3 ./run_tests.py -p 8545 --continue -f --json-diff -x "$disabled_test_list"

exit $?
