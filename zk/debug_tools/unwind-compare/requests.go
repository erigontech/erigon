package main

var zkevm_getBatchByNumber = `{"jsonrpc":"2.0", "method":"zkevm_getBatchByNumber", "params":["%v", true], "id":"1" }`

var zkevm_getFullBlockByHash = `{"jsonrpc": "2.0", "method": "zkevm_getFullBlockByHash", "params": [ "%s", true ], "id": "1" }`
