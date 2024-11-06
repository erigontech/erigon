#!/bin/bash

set +e # Disable exit on error

python3 ./run_tests.py -p 8545 --continue -f --json-diff -x \
# Erigon2 and Erigon3 never supported this api methods
trace_rawTransaction,\
# false positives: Erigon return expected response. but rpc-test somehow doesn\'t see 1 field.
erigon_getHeaderByHash,erigon_getHeaderByNumber,eth_feeHistory,\
# total difficulty field was removed, then added back
eth_getBlockByHash,eth_getBlockByNumber,\
# Erigon bugs
debug_accountRange,debug_storageRangeAt,\
# need update rpc-test - because Erigon is correct \(@AskAlexSharov will do after https://github.com/erigontech/erigon/pull/12634\)
debug_getModifiedAccountsByHash,debug_getModifiedAccountsByNumber,\
# Erigon bug https://github.com/erigontech/erigon/issues/12603
erigon_getLatestLogs,erigon_getLogsByHash/test_04.json,\
# Erigon bug https://github.com/erigontech/erigon/issues/12637
debug_traceBlockByNumber/test_05.tar,debug_traceBlockByNumber/test_08.tar,debug_traceBlockByNumber/test_09.tar,debug_traceBlockByNumber/test_10.tar,debug_traceBlockByNumber/test_11.tar,debug_traceBlockByNumber/test_12.tar,\
# remove this line after https://github.com/erigontech/rpc-tests/pull/281
parity_getBlockReceipts,\
# to investigate
debug_traceBlockByHash,\
debug_traceCallMany/test_02.tar,debug_traceCallMany/test_04.tar,debug_traceCallMany/test_05.tar,debug_traceCallMany/test_06.tar,debug_traceCallMany/test_07.tar,debug_traceCallMany/test_09.json,debug_traceCallMany/test_10.tar,\
engine_exchangeCapabilities/test_1.json,\
engine_exchangeTransitionConfigurationV1/test_01.json,\
engine_getClientVersionV1/test_1.json,\
erigon_getBalanceChangesInBlock,\
eth_createAccessList/test_16.json,\
admin_nodeInfo/test_01.json,\
admin_peers/test_01.json,\
erigon_nodeInfo/test_1.json,\
eth_coinbase/test_01.json,\
eth_getTransactionByHash/test_02.json,\
eth_getWork/test_01.json,\
eth_mining/test_01.json,\
eth_protocolVersion/test_1.json,\
eth_submitHashrate/test_1.json,\
eth_submitWork/test_1.json,\
net_peerCount/test_1.json,\
net_version/test_1.json,\
txpool_content/test_01.json,\
txpool_status/test_1.json,\
web3_clientVersion/test_1.json,\
eth_estimateGas/test_14.json,\
trace_replayBlockTransactions/test_29.tar

exit $?