#!/bin/bash

set +e # Disable exit on error

# Array of disabled tests
disabled_tests=(
    # Erigon2 and Erigon3 never supported this api methods
    trace_rawTransaction
    # Erigon bug https://github.com/erigontech/erigon/issues/12603
    erigon_getLatestLogs
    # created task https://github.com/erigontech/erigon/issues/12668 is merged
    debug_traceCallMany/test_02.tar
    debug_traceCallMany/test_04.tar
    debug_traceCallMany/test_05.tar
    debug_traceCallMany/test_06.tar
    debug_traceCallMany/test_07.tar
    debug_traceCallMany/test_09.json
    debug_traceCallMany/test_10.tar
    # to investigate
    engine_exchangeCapabilities/test_1.json
    engine_exchangeTransitionConfigurationV1/test_01.json
    engine_getClientVersionV1/test_1.json
    # waiting PR https://github.com/erigontech/rpc-tests/pull/298 to be merged
    trace_replayBlockTransactions/test_29.tar
    # these tests requires Erigon active
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
