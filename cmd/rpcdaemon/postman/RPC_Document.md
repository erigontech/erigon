# RPC_Testing Copy

----------------

A collection holding all the Ethereum JSON RPC API calls

----------------

## clientVersion

```
POST {{HOST}}
```

Returns the current client version.

**Parameters**

none

**Returns**

`String` - The current client version

----------------

### Request

> 
> **Header**
> 
> |Key|Value|Description|
> |---|---|---|
> |Content-Type|application/json||
> 
> **Body**
> 
> ```
> {
>     "jsonrpc": "2.0",
>     "method": "web3_clientVersion",
>     "params": [],
>     "id": 1
> }
> ```
> 

### Examples:

> 

----------------

## sha3

```
POST {{HOST}}
```

Returns Keccak-256 (not the standardized SHA3-256) of the given data.

**Parameters**

`DATA` - The data to convert into a SHA3 hash

```
params: [
  "0x68656c6c6f20776f726c64"
]
```

**Returns**

`DATA` - The SHA3 result of the given string

----------------

### Request

> 
> **Header**
> 
> |Key|Value|Description|
> |---|---|---|
> |Content-Type|application/json||
> 
> **Body**
> 
> ```
> {
> 	"jsonrpc":"2.0",
> 	"method":"web3_sha3",
> 	"params":["0x68656c6c6f20776f726c64"],
> 	"id":64
> }
> ```
> 

### Examples:

> 

----------------

## listening

```
POST {{HOST}}
```

Returns `true` if client is actively listening for network connections.

**TODO** Remove hard coded value

**Parameters**

none

**Returns**

`Boolean` - `true` when listening, `false` otherwise

----------------

### Request

> 
> **Header**
> 
> |Key|Value|Description|
> |---|---|---|
> |Content-Type|application/json||
> 
> **Body**
> 
> ```
> {
> 	"jsonrpc":"2.0",
> 	"method":"net_listening",
> 	"params":[],
> 	"id":67
> }
> ```
> 

### Examples:

> 

----------------

## version

```
POST {{HOST}}
```

Returns the current network id.

**Parameters**

none

**Returns**

`String` - The current network id

`"1"`: Ethereum Mainnet

`"2"`: Morden Testnet (deprecated)

`"3"`: Ropsten Testnet

`"4"`: Rinkeby Testnet

`"42"`: Kovan Testnet

----------------

### Request

> 
> **Header**
> 
> |Key|Value|Description|
> |---|---|---|
> |Content-Type|application/json||
> 
> **Body**
> 
> ```
> {
> 	"jsonrpc":"2.0",
> 	"method":"net_version",
> 	"params":[],
> 	"id":67
> }
> ```
> 

### Examples:

> 

----------------

## peerCount

```
POST {{HOST}}
```

Returns number of peers currently connected to the client.

**TODO** This routine currently returns a hard coded value of '25'

**Parameters**

none

**Returns**

`QUANTITY` - Integer of the number of connected peers

----------------

### Request

> 
> **Header**
> 
> |Key|Value|Description|
> |---|---|---|
> |Content-Type|application/json||
> 
> **Body**
> 
> ```
> {
> 	"jsonrpc":"2.0",
> 	"method":"net_peerCount",
> 	"params":[],
> 	"id":74
> }
> ```
> 

### Examples:

> 

----------------

## getBlockByNumber

```
POST {{HOST}}
```

Returns information about a block given the block's number.

**Parameters**

`QUANTITY|TAG` - Integer of a block number, or the string `"earliest"`, `"latest"` or `"pending"`, as in the default block parameter

`Boolean` - If `true` it returns the full transaction objects, if `false` only the hashes of the transactions

```
params: [
   '0x1b4', // 436
   true
]
```

**Returns**

`Object` - A block object, or null when no block was found. See `eth_getBlockByHash`

----------------

### Request

> 
> **Header**
> 
> |Key|Value|Description|
> |---|---|---|
> |Content-Type|application/json||
> 
> **Body**
> 
> ```
> {
> 	"jsonrpc":"2.0",
> 	"method":"eth_getBlockByNumber",
> 	"params":[
> 		"0xf4629", 
> 		false
> 	],
> 	"id":1
> }
> ```
> 

### Examples:

> 

----------------

## getBlockByHash

```
POST {{HOST}}
```

Returns information about a block given the block's hash.

**Parameters**

`DATA`, 32 Bytes - Hash of a block

`Boolean` - If true it returns the full transaction objects, if false only the hashes of the transactions

```
params: [
   '0xe670ec64341771606e55d6b4ca35a1a6b75ee3d5145a99d05921026d1527331',
   true
]
```

**Returns**

`Object` - A block object, or null when no block was found

`number`: `QUANTITY` - The block number. null when its pending block

`hash`: `DATA`, 32 Bytes - hash of the block. `null` when its pending block

`parentHash`: `DATA`, 32 Bytes - hash of the parent block

`nonce`: `DATA`, 8 Bytes - hash of the generated proof-of-work. `null` when its pending block

`sha3Uncles`: `DATA`, 32 Bytes - SHA3 of the uncles data in the block

`logsBloom`: `DATA`, 256 Bytes - The bloom filter for the logs of the block. `null` when its pending block

`transactionsRoot`: `DATA`, 32 Bytes - The root of the transaction trie of the block

`stateRoot`: `DATA`, 32 Bytes - The root of the final state trie of the block

`receiptsRoot`: `DATA`, 32 Bytes - The root of the receipts trie of the block

`miner`: `DATA`, 20 Bytes - The address of the beneficiary to whom the mining rewards were given

`difficulty`: `QUANTITY` - Integer of the difficulty for this block

`totalDifficulty`: `QUANTITY` - Integer of the total difficulty of the chain until this block

`extraData`: `DATA` - The "extra data" field of this block

`size`: `QUANTITY` - integer the size of this block in bytes

`gasLimit`: `QUANTITY` - The maximum gas allowed in this block

`gasUsed`: `QUANTITY` - The total used gas by all transactions in this block

`timestamp`: `QUANTITY` - The unix timestamp for when the block was collated

`transactions`: `Array` - Array of transaction objects, or 32 Bytes transaction hashes depending on the last given parameter

`uncles`: `Array` - Array of uncle hashes

----------------

### Request

> 
> **Header**
> 
> |Key|Value|Description|
> |---|---|---|
> |Content-Type|application/json||
> 
> **Body**
> 
> ```
> {
> 	"jsonrpc":"2.0",
> 	"method":"eth_getBlockByHash",
> 	"params":[
> 		"0x0b4c6fb75ded4b90218cf0346b0885e442878f104e1b60bf75d5b6860eeacd53", 
> 		false
> 	],
> 	"id":1
> }
> ```
> 

### Examples:

> 

----------------

## getBlockTransactionCountByNumber

```
POST {{HOST}}
```

Returns the number of transactions in a block given the block's block number.

**Parameters**

`QUANTITY|TAG` - Integer of a block number, or the string `"earliest"`, `"latest"` or `"pending"`, as in the default block parameter

```
params: [
   '0xe8', // 232
]
```

**Returns**

`QUANTITY` - Integer of the number of transactions in this block

----------------

### Request

> 
> **Header**
> 
> |Key|Value|Description|
> |---|---|---|
> |Content-Type|application/json||
> 
> **Body**
> 
> ```
> {
> 	"jsonrpc":"2.0",
> 	"method":"eth_getBlockTransactionCountByNumber",
> 	"params":[
> 		"0xf4629"
> 	],
> 	"id":1
> }
> ```
> 

### Examples:

> 

----------------

## getBlockTransactionCountByHash

```
POST {{HOST}}
```

Returns the number of transactions in a block given the block's block hash.

**Parameters**

`DATA`, 32 Bytes - hash of a block

```
params: [
   '0xb903239f8543d04b5dc1ba6579132b143087c68db1b2168786408fcbce568238'
]
```

**Returns**

`QUANTITY` - Integer of the number of transactions in this block

----------------

### Request

> 
> **Header**
> 
> |Key|Value|Description|
> |---|---|---|
> |Content-Type|application/json||
> 
> **Body**
> 
> ```
> {
> 	"jsonrpc":"2.0",
> 	"method":"eth_getBlockTransactionCountByHash",
> 	"params":[
> 		"0x0b4c6fb75ded4b90218cf0346b0885e442878f104e1b60bf75d5b6860eeacd53"
> 	],
> 	"id":1
> }
> ```
> 

### Examples:

> 

----------------

## getTransactionByHash

```
POST {{HOST}}
```

Returns information about a transaction given the transaction's hash.

**Parameters**

`DATA`, 32 Bytes - hash of a transaction

```
params: [
   "0xb903239f8543d04b5dc1ba6579132b143087c68db1b2168786408fcbce568238"
]
```

**Returns**

`Object` - A transaction object, or null when no transaction was found

`hash`: `DATA`, 32 Bytes - hash of the transaction

`nonce`: `QUANTITY` - The number of transactions made by the sender prior to this one

`blockHash`: `DATA`, 32 Bytes - hash of the block where this transaction was in. `null` when its pending

`blockNumber`: `QUANTITY` - block number where this transaction was in. `null` when its pending

`transactionIndex`: `QUANTITY` - Integer of the transactions index position in the block. `null` when its pending

`from`: `DATA`, 20 Bytes - address of the sender

`to`: `DATA`, 20 Bytes - address of the receiver. `null` when its a contract creation transaction

`value`: `QUANTITY` - value transferred in Wei

`gasPrice`: `QUANTITY` - gas price provided by the sender in Wei

`gas`: `QUANTITY` - gas provided by the sender

`input`: `DATA` - The data send along with the transaction

----------------

### Request

> 
> **Header**
> 
> |Key|Value|Description|
> |---|---|---|
> |Content-Type|application/json||
> 
> **Body**
> 
> ```
> {
> 	"jsonrpc":"2.0",
> 	"method":"eth_getTransactionByHash",
> 	"params":[
> 		"0xb2fea9c4b24775af6990237aa90228e5e092c56bdaee74496992a53c208da1ee"
> 	],
> 	"id":1
> }
> ```
> 

### Examples:

> 

----------------

## getTransactionByBlockHashAndIndex

```
POST {{HOST}}
```

Returns information about a transaction given the block's hash and a transaction index.

**Parameters**

`DATA`, 32 Bytes - hash of a block

`QUANTITY` - Integer of the transaction index position

```
params: [
   '0xe670ec64341771606e55d6b4ca35a1a6b75ee3d5145a99d05921026d1527331',
   '0x0' // 0
]
```

**Returns**

`Object` - A transaction object, or null when no transaction was found. See `eth_getTransactionByHash`

----------------

### Request

> 
> **Header**
> 
> |Key|Value|Description|
> |---|---|---|
> |Content-Type|application/json||
> 
> **Body**
> 
> ```
> {
> 	"jsonrpc":"2.0",
> 	"method":"eth_getTransactionByBlockHashAndIndex",
> 	"params":[
> 		"0x785b221ec95c66579d5ae14eebe16284a769e948359615d580f02e646e93f1d5", 
> 		"0x25"
> 	],
> 	"id":1
> }
> ```
> 

### Examples:

> 

----------------

## getTransactionByBlockNumberAndIndex

```
POST {{HOST}}
```

Returns information about a transaction given a block number and transaction index.

**Parameters**

`QUANTITY|TAG` - a block number, or the string `"earliest"`, `"latest"` or `"pending"`, as in the default block parameter

`QUANTITY` - The transaction index position

```
params: [
   '0x29c', // 668
   '0x0' // 0
]
```

**Returns**

`Object` - A transaction object, or null when no transaction was found. See `eth_getTransactionByHash`

----------------

### Request

> 
> **Header**
> 
> |Key|Value|Description|
> |---|---|---|
> |Content-Type|application/json||
> 
> **Body**
> 
> ```
> {
> 	"jsonrpc":"2.0",
> 	"method":"eth_getTransactionByBlockNumberAndIndex",
> 	"params":[
> 		"0x52a90b", 
> 		"0x25"
> 	],
> 	"id":1
> }
> 
> ```
> 

### Examples:

> 

----------------

## getTransactionReceipt

```
POST {{HOST}}
```

Returns the receipt of a transaction given the transaction's hash.

**Note** That the receipt is not available for pending transactions.

**Parameters**

`DATA`, 32 Bytes - hash of a transaction

```
params: [
   '0xb903239f8543d04b5dc1ba6579132b143087c68db1b2168786408fcbce568238'
]
```

**Returns**

`Object` - A transaction receipt object, or `null` when no receipt was found

`transactionHash`: `DATA`, 32 Bytes - hash of the transaction

`transactionIndex`: `QUANTITY` - Integer of the transactions index position in the block

`blockHash`: `DATA`, 32 Bytes - hash of the block where this transaction was in

`blockNumber`: `QUANTITY` - block number where this transaction was in

`cumulativeGasUsed`: `QUANTITY` - The total amount of gas used when this transaction was executed in the block

`gasUsed`: `QUANTITY` - The amount of gas used by this specific transaction alone

`contractAddress`: `DATA`, 20 Bytes - The contract address created, if the transaction was a contract creation, `null` otherwise

`logs`: `Array` - Array of log objects, which this transaction generated

`logsBloom`: `DATA`, 256 Bytes - Bloom filter for light clients to quickly retrieve related logs

It also returns either

`root` : `DATA` 32 bytes of post-transaction stateroot (if the block is pre-Byzantium)

`status`: `QUANTITY` either `1` (success) or `0` (failure)

----------------

### Request

> 
> **Header**
> 
> |Key|Value|Description|
> |---|---|---|
> |Content-Type|application/json||
> 
> **Body**
> 
> ```
> {
> 	"jsonrpc":"2.0",
> 	"method":"eth_getTransactionReceipt",
> 	"params":[
> 		"0xa3ece39ae137617669c6933b7578b94e705e765683f260fcfe30eaa41932610f"
> 	],
> 	"id":1
> }
> ```
> 

### Examples:

> 

----------------

## getUncleByBlockNumberAndIndex

```
POST {{HOST}}
```

Returns information about an uncle given a block's number and the index of the uncle.

**Parameters**

`QUANTITY|TAG` - a block number, or the string `"earliest"`, `"latest"` or `"pending"`, as in the default block parameter

`QUANTITY` - The uncle's index position

```
params: [
   '0x29c', // 668
   '0x0' // 0
]
```

**Returns**

`Object` - A block object (with zero transactions), or null when no uncle was found. See `eth_getBlockByHash`

----------------

### Request

> 
> **Header**
> 
> |Key|Value|Description|
> |---|---|---|
> |Content-Type|application/json||
> 
> **Body**
> 
> ```
> {
> 	"jsonrpc":"2.0",
> 	"method":"eth_getUncleByBlockNumberAndIndex",
> 	"params":[
> 		"0x3",
> 		"0x0"
> 	],
> 	"id":1
> }
> ```
> 

### Examples:

> 

----------------

## getUncleByBlockHashAndIndex

```
POST {{HOST}}
```

Returns information about an uncle given a block's hash and the index of the uncle.

**Parameters**

`DATA`, 32 Bytes - hash a block

`QUANTITY` - The uncle's index position

```
params: [
   '0xc6ef2fc5426d6ad6fd9e2a26abeab0aa2411b7ab17f30a99d3cb96aed1d1055b',
   '0x0' // 0
]
```

**Returns**

`Object` - A block object (with zero transactions), or null when no uncle was found. See `eth_getBlockByHash`

----------------

### Request

> 
> **Header**
> 
> |Key|Value|Description|
> |---|---|---|
> |Content-Type|application/json||
> 
> **Body**
> 
> ```
> {
> 	"jsonrpc":"2.0",
> 	"method":"eth_getUncleByBlockHashAndIndex",
> 	"params":[
> 		"0x3d6122660cc824376f11ee842f83addc3525e2dd6756b9bcf0affa6aa88cf741", 
> 		"0x0"
> 	],
> 	"id":1
> }
> ```
> 

### Examples:

> 

----------------

## getUncleCountByBlockNumber

```
POST {{HOST}}
```

Returns the number of uncles in the block, if any.

**Parameters**

`QUANTITY|TAG` - a block number, or the string `"earliest"`, `"latest"` or `"pending"`, as in the default block parameter

```
params: [
   '0x29c' // 668
]
```

**Returns**

`QUANTITY` - The number of uncles in the block, if any

----------------

### Request

> 
> **Header**
> 
> |Key|Value|Description|
> |---|---|---|
> |Content-Type|application/json||
> 
> **Body**
> 
> ```
> {
> 	"jsonrpc":"2.0",
> 	"method":"eth_getUncleByBlockNumberAndIndex",
> 	"params":[
> 		"0x3",
> 		"0x0"
> 	],
> 	"id":1
> }
> ```
> 

### Examples:

> 

----------------

## getUncleCountByBlockHash

```
POST {{HOST}}
```

Returns the number of uncles in the block, if any.

**Parameters**

`DATA`, 32 Bytes - hash a block

```
params: [
   '0xc6ef2fc5426d6ad6fd9e2a26abeab0aa2411b7ab17f30a99d3cb96aed1d1055b',
   '0x0' // 0
]
```

**Returns**

`QUANTITY` - The number of uncles in the block, if any

----------------

### Request

> 
> **Header**
> 
> |Key|Value|Description|
> |---|---|---|
> |Content-Type|application/json||
> 
> **Body**
> 
> ```
> {
> 	"jsonrpc":"2.0",
> 	"method":"eth_getUncleByBlockHashAndIndex",
> 	"params":[
> 		"0x3d6122660cc824376f11ee842f83addc3525e2dd6756b9bcf0affa6aa88cf741", 
> 		"0x0"
> 	],
> 	"id":1
> }
> ```
> 

### Examples:

> 

----------------

## newPendingTransactionFilter

```
POST {{HOST}}
```

Creates a pending transaction filter in the node. To check if the state has changed, call `eth_getFilterChanges`.

**Parameters**

None

**Returns**

`QUANTITY` - A filter id

----------------

### Request

> 
> **Header**
> 
> |Key|Value|Description|
> |---|---|---|
> |Content-Type|application/json||
> 
> **Body**
> 
> ```
> {
> 	"jsonrpc":"2.0",
> 	"method":"eth_newPendingTransactionFilter",
> 	"params":[],
> 	"id":73
> }
> ```
> 

### Examples:

> 

----------------

## newBlockFilter

```
POST {{HOST}}
```

Creates a block filter in the node, to notify when a new block arrives. To check if the state has changed, call `eth_getFilterChanges`.

**Parameters**

None

**Returns**

`QUANTITY` - A filter id

----------------

### Request

> 
> **Header**
> 
> |Key|Value|Description|
> |---|---|---|
> |Content-Type|application/json||
> 
> **Body**
> 
> ```
> {
> 	"jsonrpc":"2.0",
> 	"method":"eth_newBlockFilter",
> 	"params":[],
> 	"id":73
> }
> ```
> 

### Examples:

> 

----------------

## newFilter

```
POST {{HOST}}
```

Creates an arbitrary filter object, based on filter options, to notify when the state changes (logs). To check if the state has changed, call `eth_getFilterChanges`.

**Example** A note on specifying topic filters

Topics are order-dependent. A transaction with a log with topics [A, B] will be matched by the following topic filters

`[]` "anything"

`[A]` "A in first position (and anything after)"

`[null, B]` "anything in first position AND B in second position (and anything after)"

`[A, B]` "A in first position AND B in second position (and anything after)"

`[[A, B], [A, B]]` "(A OR B) in first position AND (A OR B) in second position (and anything after)"

**Parameters**

`Object` - The filter options

`fromBlock`: `QUANTITY|TAG` - (optional, default `"latest"`) Integer block number, or `"latest"` for the last mined block or `"pending"`, `"earliest"` for not yet mined transactions

`toBlock`: `QUANTITY|TAG` - (optional, default `"latest"`) Integer block number, or `"latest"` for the last mined block or `"pending"`, `"earliest"` for not yet mined transactions

`address`: `DATA|Array` of DATA, 20 Bytes - (optional) Contract address or a list of addresses from which logs should originate

`topics`: `Array of DATA`, - (optional) Array of 32 Bytes DATA topics. Topics are order-dependent. Each topic can also be an array of DATA with "or" options

```
params: [{
  "fromBlock": "0x1",
  "toBlock": "0x2",
  "address": " 0x8888f1f195afa192cfee860698584c030f4c9db1",
  "topics": ["0x000000000000000000000000a94f5374fce5edbc8e2a8697c15331677e6ebf0b", null, ["0x000000000000000000000000a94f5374fce5edbc8e2a8697c15331677e6ebf0b", "0x0000000000000000000000000aff3454fce5edbc8cca8697c15331677e6ebccc"]]
}]
```

**Returns**

`QUANTITY` - A filter id

----------------

### Request

> 
> **Header**
> 
> |Key|Value|Description|
> |---|---|---|
> |Content-Type|application/json||
> 
> **Body**
> 
> ```
> {
> 	"jsonrpc":"2.0",
> 	"method":"eth_newFilter",
> 	"params":[
> 		{
> 			"topics":["0x12341234"]
> 		}
> 	],
> 	"id":73
> }
> ```
> 

### Examples:

> 

----------------

## uninstallFilter

```
POST {{HOST}}
```

Uninstalls a previously-created filter given the filter's id. Always uninstall filters when no longer needed.

**Note** Filters timeout when they are not requested with eth_getFilterChanges for a period of time.

**Parameters**

`QUANTITY` - The filter id

```
params: [
  "0xb" // 11
]
```

**Returns**

`Boolean` - `true` if the filter was successfully uninstalled, `false` otherwise

----------------

### Request

> 
> **Header**
> 
> |Key|Value|Description|
> |---|---|---|
> |Content-Type|application/json||
> 
> **Body**
> 
> ```
> {
> 	"jsonrpc":"2.0",
> 	"method":"eth_uninstallFilter",
> 	"params":[
> 		"0xb"
> 	],
> 	"id":73
> }
> ```
> 

### Examples:

> 

----------------

## getFilterChanges

```
POST {{HOST}}
```

Polling method for a previously-created filter, which returns an array of logs which occurred since last poll.

**Parameters**

`QUANTITY` - The filter id

```
params: [
  "0x16" // 22
]
```

**Returns**

`Array` - Array of log objects, or an empty array if nothing has changed since last poll

For filters created with `eth_newBlockFilter` the return are block hashes (`DATA`, 32 Bytes), e.g. `["0x3454645634534..."]`

For filters created with `eth_newPendingTransactionFilter` the return are transaction hashes (`DATA`, 32 Bytes), e.g. `["0x6345343454645..."]`

For filters created with `eth_newFilter` logs are objects with following params

`removed`: `TAG` - true when the log was removed, due to a chain reorganization. false if its a valid log

`logIndex`: `QUANTITY` - Integer of the log index position in the block. `null` when its pending log

`transactionIndex`: `QUANTITY` - Integer of the transactions index position log was created from. `null` when its pending log

`transactionHash`: `DATA`, 32 Bytes - hash of the transactions this log was created from. `null` when its pending log

`blockHash`: `DATA`, 32 Bytes - hash of the block where this log was in. `null` when its pending. `null` when its pending log

`blockNumber`: `QUANTITY` - The block number where this log was in. `null` when its pending. `null` when its pending log

`address`: `DATA`, 20 Bytes - address from which this log originated

`data`: `DATA` - contains one or more 32 Bytes non-indexed arguments of the log

`topics`: `Array of DATA` - Array of 0 to 4 32 Bytes DATA of indexed log arguments. (In solidity: The first topic is the hash of the signature of the event (e.g. `Deposit(address,bytes32,uint256)`), except you declared the event with the anonymous specifier.)

----------------

### Request

> 
> **Header**
> 
> |Key|Value|Description|
> |---|---|---|
> |Content-Type|application/json||
> 
> **Body**
> 
> ```
> {
> 	"jsonrpc":"2.0",
> 	"method":"eth_getFilterChanges",
> 	"params":[
> 		"0x16"
> 	],
> 	"id":73
> }
> ```
> 

### Examples:

> 

----------------

## getLogs

```
POST {{HOST}}
```

Returns an array of logs matching a given filter object.

**Parameters**

`Object` - The filter object, see eth_newFilter parameters

```
params: [{
  "topics": ["0x000000000000000000000000a94f5374fce5edbc8e2a8697c15331677e6ebf0b"]
}]
```

**Returns**

`Array` - Array of log objects, or an empty array if nothing has changed since last poll. See `eth_getFilterChanges`

----------------

### Request

> 
> **Header**
> 
> |Key|Value|Description|
> |---|---|---|
> |Content-Type|application/json||
> 
> **Body**
> 
> ```
> {
> 	"jsonrpc":"2.0",
> 	"method":"eth_getLogs",
> 	"params":[{
> 		"topics":[
> 			"0x000000000000000000000000a94f5374fce5edbc8e2a8697c15331677e6ebf0b"
> 		]
> 	}],
> 	"id":74
> }
> ```
> 

### Examples:

> 

----------------

## accounts (deprecated)

```
POST {{HOST}}
```

Returns a list of addresses owned by the client.

**Deprecated** This function will be removed in the future.

**Parameters**

none

**Returns**

`Array of DATA`, 20 Bytes - addresses owned by the client

----------------

### Request

> 
> **Header**
> 
> |Key|Value|Description|
> |---|---|---|
> |Content-Type|application/json||
> 
> **Body**
> 
> ```
> {
> 	"jsonrpc":"2.0",
> 	"method":"eth_accounts",
> 	"params":[],
> 	"id":1
> }
> ```
> 

### Examples:

> 

----------------

## getBalance

```
POST {{HOST}}
```

Returns the balance of an account for a given address.

**Parameters**

`DATA`, 20 Bytes - address to check for balance

`QUANTITY|TAG` - Integer block number, or the string "latest", "earliest" or "pending", see the default block parameter

```
params: [
   ' 0x407d73d8a49eeb85d32cf465507dd71d507100c1',
   'latest'
]
```

**Returns**

`QUANTITY` - Integer of the current balance in wei

----------------

### Request

> 
> **Header**
> 
> |Key|Value|Description|
> |---|---|---|
> |Content-Type|application/json||
> 
> **Body**
> 
> ```
> {
> 	"jsonrpc":"2.0",
> 	"method":"eth_getBalance",
> 	"params":[
> 		"0x5df9b87991262f6ba471f09758cde1c0fc1de734", 
> 		"0xb443"
> 	],
> 	"id":1
> }
> ```
> 

### Examples:

> 

----------------

## getTransactionCount

```
POST {{HOST}}
```

Returns the number of transactions sent from an address (the nonce).

**Parameters**

`DATA`, 20 Bytes - address

`QUANTITY|TAG` - Integer block number, or the string `"latest"`, `"earliest"` or `"pending"`, see the default block parameter

```
params: [
   '0x407d73d8a49eeb85d32cf465507dd71d507100c1',
   'latest' // state at the latest block
]
```

**Returns**

`QUANTITY` - Integer of the number of transactions sent from this address

----------------

### Request

> 
> **Header**
> 
> |Key|Value|Description|
> |---|---|---|
> |Content-Type|application/json||
> 
> **Body**
> 
> ```
> {
> 	"jsonrpc":"2.0",
> 	"method":"eth_getTransactionCount",
> 	"params":[
> 		"0xfd2605a2bf58fdbb90db1da55df61628b47f9e8c", 
> 		"0xc443"
> 	],
> 	"id":1
> }
> 
> ```
> 

### Examples:

> 

----------------

## getCode

```
POST {{HOST}}
```

Returns the byte code at a given address (if it's a smart contract).

**Parameters**

`DATA`, 20 Bytes - Address from which to retreive byte code

`QUANTITY|TAG` - Integer block number, or the string `"latest"`, `"earliest"` or `"pending"`, see the default block parameter

```
params: [
   ' 0xa94f5374fce5edbc8e2a8697c15331677e6ebf0b',
   '0x2'  // 2
]
```

**Returns**

`DATA` - The byte code (if any) found at the given address

----------------

### Request

> 
> **Header**
> 
> |Key|Value|Description|
> |---|---|---|
> |Content-Type|application/json||
> 
> **Body**
> 
> ```
> {
> 	"jsonrpc":"2.0",
> 	"method":"eth_getCode",
> 	"params":[
> 		"0x109c4f2ccc82c4d77bde15f306707320294aea3f", 
> 		"0xc443"
> 	],
> 	"id":1
> }
> ```
> 

### Examples:

> 

----------------

## getStorageAt

```
POST {{HOST}}
```

Returns the value from a storage position at a given address.

**Parameters**

`DATA`, 20 Bytes - Address of the contract whose storage to retreive

`QUANTITY` - Integer of the position in the storage

`QUANTITY|TAG` - Integer block number, or the string `"latest"`, `"earliest"` or `"pending"`, see the default block parameter

**Returns**

`DATA` - The value at this storage position

**Example**

Calculating the correct position depends on the storage to retrieve. Consider the following contract deployed at  0x295a70b2de5e3953354a6a8344e616ed314d7251 by address 0x391694e7e0b0cce554cb130d723a9d27458f9298.

```
contract Storage {
    uint pos0;
    mapping(address => uint) pos1;
    
    function Storage() {
        pos0 = 1234;
        pos1[msg.sender] = 5678;
    }
}
```

Retrieving the value of pos0 is straight forward:

```
curl -X POST --data '{"jsonrpc":"2.0", "method": "eth_getStorageAt", "params": ["0x295a70b2de5e3953354a6a8344e616ed314d7251", "0x0", "latest"], "id": 1}' {{HOST}}

{"jsonrpc":"2.0","id":1,"result":"0x00000000000000000000000000000000000000000000000000000000000004d2"}
```

Retrieving an element of the map is harder. The position of an element in the map is calculated with:

```
keccack(LeftPad32(key, 0), LeftPad32(map position, 0))
```

This means to retrieve the storage on `pos1["0x391694e7e0b0cce554cb130d723a9d27458f9298"]` we need to calculate the position with:

```
keccak(decodeHex("000000000000000000000000391694e7e0b0cce554cb130d723a9d27458f9298" + "0000000000000000000000000000000000000000000000000000000000000001"))
```

The geth console which comes with the web3 library can be used to make the calculation:

```
> var key = "000000000000000000000000391694e7e0b0cce554cb130d723a9d27458f9298" + "0000000000000000000000000000000000000000000000000000000000000001"
undefined
> web3.sha3(key, {"encoding": "hex"})
"0x6661e9d6d8b923d5bbaab1b96e1dd51ff6ea2a93520fdc9eb75d059238b8c5e9"
```

----------------

### Request

> 
> **Header**
> 
> |Key|Value|Description|
> |---|---|---|
> |Content-Type|application/json||
> 
> **Body**
> 
> ```
> {
> 	"jsonrpc":"2.0", 
> 	"method": "eth_getStorageAt", 
> 	"params": [
> 		"0x109c4f2ccc82c4d77bde15f306707320294aea3f", 
> 		"0x0",
> 		"0xc443"
> 	], 
> 	"id": 1
> }
> ```
> 

### Examples:

> 

----------------

## blockNumber

```
POST {{HOST}}
```

Returns the block number of most recent block.

**Parameters**

none

**Returns**

`QUANTITY` - Integer of the current highest block number the client is on

----------------

### Request

> 
> **Header**
> 
> |Key|Value|Description|
> |---|---|---|
> |Content-Type|application/json||
> 
> **Body**
> 
> ```
> {
> 	"jsonrpc":"2.0",
> 	"method":"eth_blockNumber",
> 	"params":[],
> 	"id":83
> }
> ```
> 

### Examples:

> 

----------------

## syncing

```
POST {{HOST}}
```

Returns a data object detaling the status of the sync process or false if not syncing.

**Parameters**

none

**Returns**

`Object / Boolean`, An object with sync status data or `false`, when not syncing

`startingBlock`: `QUANTITY` - The block at which the import started (will only be reset, after the sync reached his head)

`currentBlock`: `QUANTITY` - The current block, same as eth_blockNumber

`highestBlock`: `QUANTITY` - The estimated highest block

----------------

### Request

> 
> **Header**
> 
> |Key|Value|Description|
> |---|---|---|
> |Content-Type|application/json||
> 
> **Body**
> 
> ```
> {
> 	"jsonrpc":"2.0",
> 	"method":"eth_syncing",
> 	"params":[],
> 	"id":1
> }
> ```
> 

### Examples:

> 

----------------

## chainId

```
POST {{HOST}}
```

Returns the current ethereum chainId.

**Parameters**

none

**Returns**

`QUANTITY` - The current chainId

----------------

### Request

> 
> **Header**
> 
> |Key|Value|Description|
> |---|---|---|
> |Content-Type|application/json||
> 
> **Body**
> 
> ```
> {
> 	"jsonrpc":"2.0",
> 	"method":"eth_chainId",
> 	"params":[],
> 	"id":67
> }
> ```
> 

### Examples:

> 

----------------

## protocolVersion

```
POST {{HOST}}
```

Returns the current ethereum protocol version.

**Parameters**

none

**Returns**

`QUANTITY` - The current ethereum protocol version

----------------

### Request

> 
> **Header**
> 
> |Key|Value|Description|
> |---|---|---|
> |Content-Type|application/json||
> 
> **Body**
> 
> ```
> {
> 	"jsonrpc":"2.0",
> 	"method":"eth_protocolVersion",
> 	"params":[],
> 	"id":67
> }
> ```
> 

### Examples:

> 

----------------

## gasPrice

```
POST {{HOST}}
```

Returns the current price per gas in wei.

**Parameters**

none

**Returns**

`QUANTITY` - Integer of the current gas price in wei

----------------

### Request

> 
> **Header**
> 
> |Key|Value|Description|
> |---|---|---|
> |Content-Type|application/json||
> 
> **Body**
> 
> ```
> {
> 	"jsonrpc":"2.0",
> 	"method":"eth_gasPrice",
> 	"params":[],
> 	"id":73
> }
> ```
> 

### Examples:

> 

----------------

## call

```
POST {{HOST}}
```

Executes a new message call immediately without creating a transaction on the block chain.

**Parameters**

`Object` - The transaction call object

`from`: `DATA`, 20 Bytes - (optional) The address the transaction is sent from

`to`: `DATA`, 20 Bytes - The address the transaction is directed to

`gas`: `QUANTITY` - (optional) Integer of the gas provided for the transaction execution. eth_call consumes zero gas, but this parameter may be needed by some executions

`gasPrice`: `QUANTITY` - (optional) Integer of the gasPrice used for each paid gas

`value`: `QUANTITY` - (optional) Integer of the value sent with this transaction

`data`: `DATA` - (optional) Hash of the method signature and encoded parameters. For details see Ethereum Contract ABI

`QUANTITY|TAG` - Integer block number, or the string `"latest"`, `"earliest"` or `"pending"`, see the default block parameter

**Returns**

`DATA` - The return value of executed contract

----------------

### Request

> 
> **Header**
> 
> |Key|Value|Description|
> |---|---|---|
> |Content-Type|application/json||
> 
> **Body**
> 
> ```
> {
> 	"jsonrpc":"2.0",
> 	"method":"eth_call",
> 	"params":[
>     {
>       "to": "0x08a2e41fb99a7599725190b9c970ad3893fa33cf",
>       "data": "0x18160ddd"
>     },
>     "0xa2f2e0"
>   ],
> 	"id":1
> }
> ```
> 

### Examples:

> 

----------------

## estimateGas

```
POST {{HOST}}
```

Returns an estimate of how much gas is necessary to allow the transaction to complete. The transaction will not be added to the blockchain.

**Note** The estimate may be significantly more than the amount of gas actually used by the transaction for a variety of reasons including EVM mechanics and node performance.

**Note** If no gas limit is specified geth uses the block gas limit from the pending block as an upper bound. As a result the returned estimate might not be enough to executed the call/transaction when the amount of gas is higher than the pending block gas limit.

**Parameters**

`Object` - The transaction call object. See `eth_call` parameters, expect that all properties are optional

**Returns**

`QUANTITY` - The estimated amount of gas needed for the call

----------------

### Request

> 
> **Header**
> 
> |Key|Value|Description|
> |---|---|---|
> |Content-Type|application/json||
> 
> **Body**
> 
> ```
> {
> 	"jsonrpc":"2.0",
> 	"method":"eth_estimateGas",
> 	"params":[
>     {
>       "to": "0x3d597789ea16054a084ac84ce87f50df9198f415",
>       "from": "0x3d597789ea16054a084ac84ce87f50df9198f415",
>       "value": "0x1"
>     }
>   ],
> 	"id":1
> }
> ```
> 

### Examples:

> 

----------------

## sendTransaction

```
POST {{HOST}}
```

Creates new message call transaction or a contract creation if the data field contains code.

**Note** Use `eth_getTransactionReceipt` to get the contract address, after the transaction was mined, when you created a contract

**Parameters**

`Object` - The transaction object

`from`: `DATA`, 20 Bytes - The address the transaction is send from

`to`: `DATA`, 20 Bytes - (optional when creating new contract) The address the transaction is directed to

`gas`: `QUANTITY` - (optional, default 90000) Integer of the gas provided for the transaction execution. It will return unused gas

`gasPrice`: `QUANTITY` - (optional, default To-Be-Determined) Integer of the gasPrice used for each paid gas

`value`: `QUANTITY` - (optional) Integer of the value sent with this transaction

`data`: `DATA` - The compiled code of a contract OR the hash of the invoked method signature and encoded parameters. For details see Ethereum Contract ABI

`nonce`: `QUANTITY` - (optional) Integer of a nonce. This allows to overwrite your own pending transactions that use the same nonce

```
params: [{
  "from": " 0xb60e8dd61c5d32be8058bb8eb970870f07233155",
  "to": " 0xd46e8dd67c5d32be8058bb8eb970870f07244567",
  "gas": "0x76c0", // 30400
  "gasPrice": "0x9184e72a000", // 10000000000000
  "value": "0x9184e72a", // 2441406250
  "data": "0xd46e8dd67c5d32be8d46e8dd67c5d32be8058bb8eb970870f072445675058bb8eb970870f072445675"
}]
```

**Returns**

`DATA`, 32 Bytes - The transaction hash, or the zero hash if the transaction is not yet available

----------------

### Request

> 
> **Header**
> 
> |Key|Value|Description|
> |---|---|---|
> |Content-Type|application/json||
> 
> **Body**
> 
> ```
> {
> 	"jsonrpc":"2.0",
> 	"method":"eth_sendTransaction",
> 	"params":[{
> 		"from": "0xb60e8dd61c5d32be8058bb8eb970870f07233155",
> 		"to": "0xd46e8dd67c5d32be8058bb8eb970870f07244567_hangs_parity",
> 		"gas": "0x76c0",
> 		"gasPrice": "0x9184e72a000",
> 		"value": "0x9184e72a",
> 		"data": "0xd46e8dd67c5d32be8d46e8dd67c5d32be8058bb8eb970870f072445675058bb8eb970870f072445675"
> 	}],
> 	"id":1
> }
> ```
> 

### Examples:

> 

----------------

## sendRawTransaction

```
POST {{HOST}}
```

Creates new message call transaction or a contract creation for previously-signed transactions.

**Note** Use `eth_getTransactionReceipt` to get the contract address, after the transaction was mined, when you created a contract.

**Parameters**

`DATA`, The signed transaction data

```
params: ["0xd46e8dd67c5d32be8d46e8dd67c5d32be8058bb8eb970870f072445675058bb8eb970870f072445675"]
```

**Returns**

`DATA`, 32 Bytes - The transaction hash, or the zero hash if the transaction is not yet available

----------------

### Request

> 
> **Header**
> 
> |Key|Value|Description|
> |---|---|---|
> |Content-Type|application/json||
> 
> **Body**
> 
> ```
> {
> 	"jsonrpc":"2.0",
> 	"method":"eth_sendRawTransaction",
> 	"params":["0xd46e8dd67c5d32be8d46e8dd67c5d32be8058bb8eb970870f072445675058bb8eb970870f072445675"],
> 	"id":1
> }
> ```
> 

### Examples:

> 

----------------

## coinbase

```
POST {{HOST}}
```

Returns the current client coinbase address.

**Parameters**

none

**Returns**

`DATA`, 20 bytes - The current coinbase address

----------------

### Request

> 
> **Header**
> 
> |Key|Value|Description|
> |---|---|---|
> |Content-Type|application/json||
> 
> **Body**
> 
> ```
> {
> 	"jsonrpc":"2.0",
> 	"method":"eth_coinbase",
> 	"params":[],
> 	"id":64
> }
> ```
> 

### Examples:

> 

----------------

## hashrate

```
POST {{HOST}}
```

Returns the number of hashes per second that the node is mining with.

**Parameters**

none

**Returns**

`QUANTITY` - Number of hashes per second

----------------

### Request

> 
> **Header**
> 
> |Key|Value|Description|
> |---|---|---|
> |Content-Type|application/json||
> 
> **Body**
> 
> ```
> {
> 	"jsonrpc":"2.0",
> 	"method":"eth_hashrate",
> 	"params":[],
> 	"id":71
> }
> ```
> 

### Examples:

> 

----------------

## mining

```
POST {{HOST}}
```

Returns `true` if client is actively mining new blocks.

**Parameters**

none

**Returns**

`Boolean` - `true` if the client is mining, `false` otherwise

----------------

### Request

> 
> **Header**
> 
> |Key|Value|Description|
> |---|---|---|
> |Content-Type|application/json||
> 
> **Body**
> 
> ```
> {
> 	"jsonrpc":"2.0",
> 	"method":"eth_mining",
> 	"params":[],
> 	"id":71
> }
> ```
> 

### Examples:

> 

----------------

## getWork

```
POST {{HOST}}
```

Returns the hash of the current block, the seedHash, and the boundary condition to be met ("target").

**Parameters**

none

**Returns**

`Array` of DATA, 32 Bytes - Array of three hashes representing block header pow-hash, seed hash and boundary condition

`DATA`, 32 Bytes - current block header pow-hash

`DATA`, 32 Bytes - The seed hash used for the DAG

`DATA`, 32 Bytes - The boundary condition ("target"), 2^256 / difficulty

----------------

### Request

> 
> **Header**
> 
> |Key|Value|Description|
> |---|---|---|
> |Content-Type|application/json||
> 
> **Body**
> 
> ```
> {
> 	"jsonrpc":"2.0",
> 	"method":"eth_getWork",
> 	"params":[],
> 	"id":73
> }
> ```
> 

### Examples:

> 

----------------

## submitWork

```
POST {{HOST}}
```

Submits a proof-of-work solution to the blockchain.

**Parameters**

`DATA`, 8 Bytes - The nonce found (64 bits)

`DATA`, 32 Bytes - The header's pow-hash (256 bits)

`DATA`, 32 Bytes - The mix digest (256 bits)

```
params: [
  "0x0000000000000001",
  "0x1234567890abcdef1234567890abcdef1234567890abcdef1234567890abcdef",
  "0xD1FE5700000000000000000000000000D1FE5700000000000000000000000000"
]
```

**Returns**

`Boolean` - `true` if the provided solution is valid, `false` otherwise

----------------

### Request

> 
> **Header**
> 
> |Key|Value|Description|
> |---|---|---|
> |Content-Type|application/json||
> 
> **Body**
> 
> ```
> {
> 	"jsonrpc":"2.0", 
> 	"method":"eth_submitWork", 
> 	"params":[
> 		"0x1", 
> 		"0x1234567890abcdef1234567890abcdef1234567890abcdef1234567890abcdef", 
> 		"0xD16E5700000000000000000000000000D16E5700000000000000000000000000"
> 	],
> 	"id":73
> }
> ```
> 

### Examples:

> 

----------------

## submitHashrate

```
POST {{HOST}}
```

Submit the mining hashrate to the blockchain.

**Parameters**

`DATA`, 32 Bytes - a hexadecimal string representation (32 bytes) of the hash rate

`ID`, String - A random hexadecimal(32 bytes) ID identifying the client

```
params: [
  "0x0000000000000000000000000000000000000000000000000000000000500000",
  "0x59daa26581d0acd1fce254fb7e85952f4c09d0915afd33d3886cd914bc7d283c"
]
```

**Returns**

`Boolean` - `true` if submitting went through succesfully, `false` otherwise

----------------

### Request

> 
> **Header**
> 
> |Key|Value|Description|
> |---|---|---|
> |Content-Type|application/json||
> 
> **Body**
> 
> ```
> {
> 	"jsonrpc":"2.0", 
> 	"method":"eth_submitHashrate", 
> 	"params":[
> 		"0x0000000000000000000000000000000000000000000000000000000000500000", 
> 		"0x59daa26581d0acd1fce254fb7e85952f4c09d0915afd33d3886cd914bc7d283c"
> 	],
> 	"id":73
> }
> ```
> 

### Examples:

> 

----------------

## call

```
POST {{HOST}}
```

Call(ctx context.Context, call CallParam, blockNr rpc.BlockNumber) ([]interface{}, error)

----------------

### Request

> 
> **Header**
> 
> |Key|Value|Description|
> |---|---|---|
> |Content-Type|application/json||
> 
> **Body**
> 
> ```
> {
> 	"jsonrpc":"2.0",
> 	"method":"trace_call",
> 	"params":[],
> 	"id":1
> }
> ```
> 

### Examples:

> 

----------------

## callMany

```
POST {{HOST}}
```

CallMany(ctx context.Context, calls CallParams) ([]interface{}, error)

----------------

### Request

> 
> **Header**
> 
> |Key|Value|Description|
> |---|---|---|
> |Content-Type|application/json||
> 
> **Body**
> 
> ```
> {
> 	"jsonrpc":"2.0",
> 	"method":"trace_callMany",
> 	"params":[],
> 	"id":1
> }
> ```
> 

### Examples:

> 

----------------

## rawTransaction

```
POST {{HOST}}
```

RawTransaction(ctx context.Context, txHash common.Hash, traceTypes []string) ([]interface{}, error)

----------------

### Request

> 
> **Header**
> 
> |Key|Value|Description|
> |---|---|---|
> |Content-Type|application/json||
> 
> **Body**
> 
> ```
> {
> 	"jsonrpc":"2.0",
> 	"method":"trace_rawTransaction",
> 	"params":[],
> 	"id":1
> }
> ```
> 

### Examples:

> 

----------------

## replayBlockTransactions

```
POST {{HOST}}
```

ReplayBlockTransactions(ctx context.Context, blockNr rpc.BlockNumber, traceTypes []string) ([]interface{}, error)

----------------

### Request

> 
> **Header**
> 
> |Key|Value|Description|
> |---|---|---|
> |Content-Type|application/json||
> 
> **Body**
> 
> ```
> {
> 	"jsonrpc":"2.0",
> 	"method":"trace_replayBlockTransactions",
> 	"params":[],
> 	"id":1
> }
> ```
> 

### Examples:

> 

----------------

## replayTransaction

```
POST {{HOST}}
```

ReplayTransaction(ctx context.Context, txHash common.Hash, traceTypes []string) ([]interface{}, error)

----------------

### Request

> 
> **Header**
> 
> |Key|Value|Description|
> |---|---|---|
> |Content-Type|application/json||
> 
> **Body**
> 
> ```
> {
> 	"jsonrpc":"2.0",
> 	"method":"trace_replayTransaction",
> 	"params":[],
> 	"id":1
> }
> ```
> 

### Examples:

> 

----------------

## transaction

```
POST {{HOST}}
```

Transaction(ctx context.Context, txHash common.Hash) (ParityTraces, error)

----------------

### Request

> 
> **Header**
> 
> |Key|Value|Description|
> |---|---|---|
> |Content-Type|application/json||
> 
> **Body**
> 
> ```
> {
> 	"jsonrpc":"2.0",
> 	"method":"trace_transaction",
> 	"params":[],
> 	"id":1
> }
> ```
> 

### Examples:

> 

----------------

## get

```
POST {{HOST}}
```

Get(ctx context.Context, txHash common.Hash, txIndicies []hexutil.Uint64) (*ParityTrace, error)

----------------

### Request

> 
> **Header**
> 
> |Key|Value|Description|
> |---|---|---|
> |Content-Type|application/json||
> 
> **Body**
> 
> ```
> {
> 	"jsonrpc":"2.0",
> 	"method":"trace_get",
> 	"params":[],
> 	"id":1
> }
> ```
> 

### Examples:

> 

----------------

## block

```
POST {{HOST}}
```

Block(ctx context.Context, blockNr rpc.BlockNumber) (ParityTraces, error)

----------------

### Request

> 
> **Header**
> 
> |Key|Value|Description|
> |---|---|---|
> |Content-Type|application/json||
> 
> **Body**
> 
> ```
> {
> 	"jsonrpc":"2.0",
> 	"method":"trace_block",
> 	"params":[],
> 	"id":1
> }
> ```
> 

### Examples:

> 

----------------

## filter

```
POST {{HOST}}
```

Filter(ctx context.Context, req TraceFilterRequest) (ParityTraces, error)

----------------

### Request

> 
> **Header**
> 
> |Key|Value|Description|
> |---|---|---|
> |Content-Type|application/json||
> 
> **Body**
> 
> ```
> {
> 	"jsonrpc":"2.0",
> 	"method":"trace_filter",
> 	"params":[],
> 	"id":1
> }
> ```
> 

### Examples:

> 

----------------

## forks

```
POST {{HOST}}
```

Returns the genesis block hash and a sorted list of already passed fork block numbers as well as the next fork block (if applicable)

**Parameters**

`QUANTITY|TAG or DATA` - Integer of a block number, the string `"earliest"`, `"latest"` or `"pending"`, or a 32 byte block hash

**Returns**

A structure of the type Forks

```
type Forks struct {
	genesis common.Hash // the hash of the genesis block
	passed []uint64 // array of block numbers passed by this client
	next *uint64 // the next fork block (may be empty)
}
```

----------------

### Request

> 
> **Header**
> 
> |Key|Value|Description|
> |---|---|---|
> |Content-Type|application/json||
> 
> **Body**
> 
> ```
> {
> 	"jsonrpc":"2.0",
> 	"method":"tg_forks",
> 	"params":["latest"],
> 	"id":1
> }
> ```
> 

### Examples:

> 

----------------

## getHeaderByNumber

```
POST {{HOST}}
```

Returns a block's header given a block number ignoring the block's transaction and uncle list (may be faster).

**Parameters**

`QUANTITY|TAG` - Integer of a block number, or the string `"earliest"`, `"latest"` or `"pending"`

**Returns**

`Object` - A block object, or null when no block was found. See `eth_getBlockByHash`

----------------

### Request

> 
> **Header**
> 
> |Key|Value|Description|
> |---|---|---|
> |Content-Type|application/json||
> 
> **Body**
> 
> ```
> {
> 	"jsonrpc":"2.0",
> 	"method":"tg_getHeaderByNumber",
> 	"params":[
> 		"0x3"
> 	],
> 	"id":1
> }
> ```
> 

### Examples:

> 

----------------

## getHeaderByHash

```
POST {{HOST}}
```

Returns a block's header given a block's hash.

**Parameters**

`DATA`, 32 Bytes - Hash of a block

```
params: [
   '0x3d6122660cc824376f11ee842f83addc3525e2dd6756b9bcf0affa6aa88cf741'
]
```

**Returns**

`Object` - A block object, or null when no block was found. See `eth_getBlockByHash`

----------------

### Request

> 
> **Header**
> 
> |Key|Value|Description|
> |---|---|---|
> |Content-Type|application/json||
> 
> **Body**
> 
> ```
> {
> 	"jsonrpc":"2.0",
> 	"method":"tg_getHeaderByHash",
> 	"params":[
> 		"0x3d6122660cc824376f11ee842f83addc3525e2dd6756b9bcf0affa6aa88cf741"
> 	],
> 	"id":1
> }
> ```
> 

### Examples:

> 

----------------

## getLogsByHash

```
POST {{HOST}}
```

Returns an array of arrays of logs generated by the transactions in the block given by the block's hash.

**Note:** The returned value is an array of arrays of log entries. There is an entry for each transaction in the block.

If transaction `X` did not create any logs, the entry at `result[X]` will be `null`

If transaction `X` generated `N` logs, the entry at position `result[X]` will be an array of `N` log objects

**Parameters**

`blockHash`: `DATA`, 32 bytes, hash of block at which to retreive data

**Returns**

`Array` - An array of arrays of log objects, some of which may be `null`, found in the block. See `eth_getFilterChanges`

----------------

### Request

> 
> **Header**
> 
> |Key|Value|Description|
> |---|---|---|
> |Content-Type|application/json||
> 
> **Body**
> 
> ```
> {
> 	"jsonrpc":"2.0",
> 	"method":"eth_getLogsByHash",
> 	"params":[
> 		"0x2f244c154cbacb0305581295b80efa6dffb0224b60386a5fc6ae9585e2a140c4"
> 	],
> 	"id":1
> }
> ```
> 

### Examples:

> 

----------------

## issuance

```
POST {{HOST}}
```

Returns the total issuance (block reward plus uncle reward) for the given block.

**Parameters**

`QUANTITY|TAG` - Integer of a block number, or the string `"earliest"`, `"latest"` or `"pending"`, as in the default block parameter

**Returns**

`Object` - an `Issuance` object of type

```
{
  blockReward: "0x478eae0e571ba000", // the issuance to the miner of the block (includes nephew reward but not transaction fees)
  uncleReward: "0x340aad21b3b70000", // the issuance to miners of included uncle (if any)
  issuance: "0x7b995b300ad2a000" // the sum of blockReward and uncleReward
}
```

----------------

### Request

> 
> **Header**
> 
> |Key|Value|Description|
> |---|---|---|
> |Content-Type|application/json||
> 
> **Body**
> 
> ```
> {
> 	"jsonrpc":"2.0",
> 	"method":"tg_issuance",
> 	"params":[
> 		"0x3"
> 	],
> 	"id":1
> }
> ```
> 

### Examples:

> 

----------------

## storageRangeAt

```
POST {{HOST}}
```

Returns information about a range of storage locations (if any) for the given address.

**Parameters**

`blockHash`: `DATA`, 32 bytes, hash of block at which to retreive data

`txIndex`: `QUANTITY`, 8 bytes, transaction index in the give block

`contractAddress`: `DATA` 20 bytes, contract address from which to retreive storage data

`keyStart`: `DATA` 32 bytes, storage key to retreive

`maxResult`: QUANTITY, 8 bytes, the number of values to retreive

**Returns**

Returns `StorageRangeResult` which is defined as

```
type StorageRangeResult struct {
    `storage` StorageMap // see below
    `nextKey` *common.Hash // nil if `storage` includes the last key in the trie
}
```

`StorageMap` is a type defined as `map[common.Hash]StorageEntry` and `StorageEntry` is defined as

```
type StorageEntry struct {
	key   *common.Hash
	value common.Hash
}
```

----------------

### Request

> 
> **Header**
> 
> |Key|Value|Description|
> |---|---|---|
> |Content-Type|application/json||
> 
> **Body**
> 
> ```
> {
> 	"jsonrpc":"2.0",
> 	"method":"debug_storageRangeAt",
> 	"params":[
> 		"0xd3f1853788b02e31067f2c6e65cb0ae56729e23e3c92e2393af9396fa182701d", 
>     1,
>     "0xb734c74ff4087493373a27834074f80acbd32827",
> 		"0x00",
>     2
> 	],
> 	"id":1
> }
> ```
> 

### Examples:

> 

----------------

## accountRange

```
POST {{HOST}}
```

Returns a range of accounts involved in the given block range

**Parameters**

`block: `QUANTITY|TAG` - Integer of a block number, or the string `"earliest"`, `"latest"` or `"pending"`, as in the default block parameter

`keyStart`: DATA, N bytes, a prefix against which to match account addresses (report only on accounts addresses that begin with this prefix, default matches all accounts)

`maxResult`: QUANTITY, 8 bytes, the maximum number of accounts to retreive

`excludeCode`: `Boolean`, if `true`, do not return byte code from the address, if `false` return the byte code (if any)

`excludeStorage`: `Boolean`, if `true`, do not return storage from the address, if `false` return storage (if any)

`excludeMissingPreimages`: `Boolean`, if `true`, do not return missing preimages, if `false` do return them

**Returns**

Returns `IteratorDump` which is defined as

```
type IteratorDump struct {
	root     string
	accounts map[common.Address]DumpAccount
	next     []byte // nil if no more accounts
}
```

`DumpAccount` is a type defined as

```
type DumpAccount struct {
	balance   string
	nonce     uint64
	root      string
	codeHash  string
	code      string
	storage   map[string]string
	address   *common.Address
	secureKey hexutil.Bytes // If we don't have address, we can output the key
}
```

----------------

### Request

> 
> **Header**
> 
> |Key|Value|Description|
> |---|---|---|
> |Content-Type|application/json||
> 
> **Body**
> 
> ```
> {
> 	"jsonrpc":"2.0",
> 	"method":"debug_accountRange",
> 	"params":[
> 		"0x2", 
> 		[10],
>     1,
>     true,
>     true,
>     true
> 	],
> 	"id":1
> }
> ```
> 

### Examples:

> 

----------------

## getModifiedAccountsByNumber

```
POST {{HOST}}
```

Returns a list of accounts modified in the given block.

**Parameters**

`startNum`: `QUANTITY|TAG` - Integer of a first block number to process, or the string `"earliest"`, `"latest"` or `"pending"`

`endNum`: `QUANTITY|TAG` - Integer of a last block number (inclusive) to process, or the string `"earliest"`, `"latest"` or `"pending"`. Optional, defaults to `startNum`

**Returns**

`Array` of `DATA`, 20 Bytes - Array of addresses modifed in the given block range

----------------

### Request

> 
> **Header**
> 
> |Key|Value|Description|
> |---|---|---|
> |Content-Type|application/json||
> 
> **Body**
> 
> ```
> {
> 	"jsonrpc":"2.0",
> 	"method":"debug_getModifiedAccountsByNumber",
> 	"params":[
> 		"0xccccd",
> 		"0xcccce"
> 	],
> 	"id":1
> }
> ```
> 

### Examples:

> 

----------------

## getModifiedAccountsByHash

```
POST {{HOST}}
```

Returns a list of accounts modified in the given block.

**Parameters**

`startHash`: `DATA`, 32 bytes, the first hash of block at which to retreive data

`endHash`: `DATA`, 32 bytes, the last hash of block at which to retreive data. Optional, defaults to `startHash`

**Returns**

`Array` of `DATA`, 20 Bytes - Array of addresses modifed in the given block range

----------------

### Request

> 
> **Header**
> 
> |Key|Value|Description|
> |---|---|---|
> |Content-Type|application/json||
> 
> **Body**
> 
> ```
> {
> 	"jsonrpc":"2.0",
> 	"method":"debug_getModifiedAccountsByHash",
> 	"params":[
> 		"0x2a1af018e33bcbd5015c96a356117a5251fcccf94a9c7c8f0148e25fdee37aec",
> 		"0x4e3d3e7eee350df0ee6e94a44471ee2d22cfb174db89bbf8e6c5f6aef7b360c5"
> 	],
> 	"id":1
> }
> ```
> 

### Examples:

> 

----------------

## traceTransaction

```
POST {{HOST}}
```

Returns Geth style transaction traces.

**Parameters**

`txHash`: `DATA`, 32 bytes, hash of transaction to trace.
`config` - TODO

```
TODO
```

**Returns**

TODO

----------------

### Request

> 
> **Header**
> 
> |Key|Value|Description|
> |---|---|---|
> |Content-Type|application/json||
> 
> **Body**
> 
> ```
> {
> 	"jsonrpc":"2.0",
> 	"method":"debug_traceTransaction",
> 	"params":[
> 		"0x893c428fed019404f704cf4d9be977ed9ca01050ed93dccdd6c169422155586f"
> 	],
> 	"id":1
> }
> ```
> 

### Examples:

> 

----------------

## getCompilers (deprecated)

```
POST {{HOST}}
```

Returns a list of available compilers in the client.

**Deprecated** This function will be removed in the future.

**Parameters**

none

**Returns**

`Array` of `String` - Array of available compilers

----------------

### Request

> 
> **Header**
> 
> |Key|Value|Description|
> |---|---|---|
> |Content-Type|application/json||
> 
> **Body**
> 
> ```
> {
> 	"jsonrpc":"2.0",
> 	"method":"eth_getCompilers",
> 	"params":[],
> 	"id":1
> }
> ```
> 

### Examples:

> 

----------------

## compileLLL (deprecated)

```
POST {{HOST}}
```

Returns compiled LLL code.

**Deprecated** This function will be removed in the future.

**Parameters**

`String` - The source code

```
params: [
   "/* some serpent */",
]
```

**Returns**

`DATA` - The compiled source code

----------------

### Request

> 
> **Header**
> 
> |Key|Value|Description|
> |---|---|---|
> |Content-Type|application/json||
> 
> **Body**
> 
> ```
> {
> 	"jsonrpc":"2.0",
> 	"method":"eth_compileLLL",
> 	"params":[
> 		"(returnlll (suicide (caller)))"
> 	],
> 	"id":1
> }
> ```
> 

### Examples:

> 

----------------

## compileSolidity (deprecated)

```
POST {{HOST}}
```

Returns compiled solidity code.

**Deprecated** This function will be removed in the future.

**Parameters**

`String` - The source code

```
params: [
   "/* some serpent */",
]
```

**Returns**

`DATA` - The compiled source code

----------------

### Request

> 
> **Header**
> 
> |Key|Value|Description|
> |---|---|---|
> |Content-Type|application/json||
> 
> **Body**
> 
> ```
> {
> 	"jsonrpc":"2.0",
> 	"method":"eth_compileSolidity",
> 	"params":[
> 		"contract test { function multiply(uint a) returns(uint d) {   return a * 7;   } }"
> 	],
> 	"id":1
> }
> ```
> 

### Examples:

> 

----------------

## compileSerpent (deprecated)

```
POST {{HOST}}
```

Returns compiled serpent code.

**Deprecated** This function will be removed in the future.

**Parameters**

`String` - The source code

```
params: [
   "/* some serpent */",
]
```

**Returns**

`DATA` - The compiled source code

----------------

### Request

> 
> **Header**
> 
> |Key|Value|Description|
> |---|---|---|
> |Content-Type|application/json||
> 
> **Body**
> 
> ```
> {
> 	"jsonrpc":"2.0",
> 	"method":"eth_compileSerpent",
> 	"params":["/* some serpent */"],
> 	"id":1
> }
> ```
> 

### Examples:

> 

----------------

## sign (deprecated)

```
POST {{HOST}}
```

Calculates an Ethereum specific signature with: `sign(keccak256("\x19Ethereum Signed Message:\n" + len(message) + message)))`.

**Deprecated** This function will be removed in the future.

**Note** Adding a prefix to the message makes the calculated signature recognisable as an Ethereum specific signature. This prevents misuse where a malicious DApp can sign arbitrary data (e.g. transaction) and use the signature to impersonate the victim.

**Note** The address provided to sign the message must be unlocked.

**Parameters**

account, message

`DATA`, 20 Bytes - address

`DATA`, N Bytes - message to sign

**Returns**

`DATA` - The signature

----------------

### Request

> 
> **Header**
> 
> |Key|Value|Description|
> |---|---|---|
> |Content-Type|application/json||
> 
> **Body**
> 
> ```
> {
> 	"jsonrpc":"2.0",
> 	"method":"eth_sign",
> 	"params":[
> 		"0x9b2055d370f73ec7d8a03e965129118dc8f5bf83", 
> 		"0xdeadbeef"
> 	],
> 	"id":1
> }
> ```
> 

### Examples:

> 

----------------

## getString (deprecated)

```
POST {{HOST}}
```

Returns string from the local database.

**Deprecated** This function will be removed in the future.

**Parameters**

`String` - Database name

`String` - Key name

```
params: [
  "testDB",
  "myKey"
]
```

**Returns**

`String` - The previously stored string

----------------

### Request

> 
> **Header**
> 
> |Key|Value|Description|
> |---|---|---|
> |Content-Type|application/json||
> 
> **Body**
> 
> ```
> {
> 	"jsonrpc":"2.0",
> 	"method":"db_getString",
> 	"params":[
> 		"testDB",
> 		"myKey"
> 	],
> 	"id":73
> }
> ```
> 

### Examples:

> 

----------------

## putString (deprecated)

```
POST {{HOST}}
```

Stores a string in the local database.

**Deprecated** This function will be removed in the future.

**Parameters**

`String` - Database name

`String` - Key name

`String` - String to store

```
params: [
  "testDB",
  "myKey",
  "myString"
]
```

**Returns**

`Boolean` - `true` if the value was stored, `false` otherwise

----------------

### Request

> 
> **Header**
> 
> |Key|Value|Description|
> |---|---|---|
> |Content-Type|application/json||
> 
> **Body**
> 
> ```
> {
> 	"jsonrpc":"2.0",
> 	"method":"db_putString",
> 	"params":[
> 		"testDB",
> 		"myKey",
> 		"myString"
> 	],
> 	"id":73
> }
> ```
> 

### Examples:

> 

----------------

## getHex (deprecated)

```
POST {{HOST}}
```

Returns binary data from the local database.

**Deprecated** This function will be removed in the future.

**Parameters**

`String` - Database name

`String` - Key name

```
params: [
  "testDB",
  "myKey",
]
```

**Returns**

`DATA` - The previously stored data

----------------

### Request

> 
> **Header**
> 
> |Key|Value|Description|
> |---|---|---|
> |Content-Type|application/json||
> 
> **Body**
> 
> ```
> {
> 	"jsonrpc":"2.0",
> 	"method":"db_getHex"
> 	,"params":[
> 		"testDB",
> 		"myKey"
> 	],
> 	"id":73
> }
> ```
> 

### Examples:

> 

----------------

## putHex (deprecated)

```
POST {{HOST}}
```

Stores binary data in the local database.

**Deprecated** This function will be removed in the future.

**Parameters**

`String` - Database name

`String` - Key name

`DATA` - The data to store

```
params: [
  "testDB",
  "myKey",
  "0x68656c6c6f20776f726c64"
]
```

**Returns**

`Boolean` - `true` if the value was stored, `false` otherwise

----------------

### Request

> 
> **Header**
> 
> |Key|Value|Description|
> |---|---|---|
> |Content-Type|application/json||
> 
> **Body**
> 
> ```
> {
> 	"jsonrpc":"2.0",
> 	"method":"db_putHex",
> 	"params":[
> 		"testDB",
> 		"myKey",
> 		"0x68656c6c6f20776f726c64"
> 	],
> 	"id":73
> }
> ```
> 

### Examples:

> 

----------------

## post (deprecated)

```
POST {{HOST}}
```

Sends a whisper message.

**Deprecated** This function will be removed in the future.

**Parameters**

`Object` - The whisper post object

`from`: `DATA`, 60 Bytes - (optional) The identity of the sender

`to`: `DATA`, 60 Bytes - (optional) The identity of the receiver. When present whisper will encrypt the message so that only the receiver can decrypt it

`topics`: `Array of DATA` - Array of DATA topics, for the receiver to identify messages

`payload`: `DATA` - The payload of the message

`priority`: `QUANTITY` - The integer of the priority in a range

`ttl`: `QUANTITY` - Integer of the time to live in seconds

```
params: [{
  from: "0x04f96a5e25610293e42a73908e93ccc8c4d4dc0edcfa9fa872f50cb214e08ebf61a03e245533f97284d442460f2998cd41858798ddfd4d661997d3940272b717b1",
  to: "0x3e245533f97284d442460f2998cd41858798ddf04f96a5e25610293e42a73908e93ccc8c4d4dc0edcfa9fa872f50cb214e08ebf61a0d4d661997d3940272b717b1",
  topics: ["0x776869737065722d636861742d636c69656e74", "0x4d5a695276454c39425154466b61693532"],
  payload: "0x7b2274797065223a226d6",
  priority: "0x64",
  ttl: "0x64",
}]
```

**Returns**

`Boolean` - `true` if the message was send, `false` otherwise

----------------

### Request

> 
> **Header**
> 
> |Key|Value|Description|
> |---|---|---|
> |Content-Type|application/json||
> 
> **Body**
> 
> ```
> {
> 	"jsonrpc":"2.0",
> 	"method":"shh_post",
> 	"params":[{
> 		"from":"0xc931d93e97ab07fe42d923478ba2465f2..",
> 		"topics": [
> 			"0x68656c6c6f20776f726c64"
> 		],
> 		"payload":"0x68656c6c6f20776f726c64",
> 		"ttl":"0x64",
> 		"priority":"0x64"
> 	}],
> 	"id":73
> }
> ```
> 

### Examples:

> 

----------------

## version (deprecated)

```
POST {{HOST}}
```

Returns the current whisper protocol version.

**Deprecated** This function will be removed in the future.

**Parameters**

none

**Returns**

`String` - The current whisper protocol version

----------------

### Request

> 
> **Header**
> 
> |Key|Value|Description|
> |---|---|---|
> |Content-Type|application/json||
> 
> **Body**
> 
> ```
> {
> 	"jsonrpc":"2.0",
> 	"method":"shh_version",
> 	"params":[],
> 	"id":67
> }
> ```
> 

### Examples:

> 

----------------

## newIdentity (deprecated)

```
POST {{HOST}}
```

Creates new whisper identity in the client.

**Deprecated** This function will be removed in the future.

**Parameters**

none

**Returns**

`DATA`, 60 Bytes - The address of the new identiy

----------------

### Request

> 
> **Header**
> 
> |Key|Value|Description|
> |---|---|---|
> |Content-Type|application/json||
> 
> **Body**
> 
> ```
> {
> 	"jsonrpc":"2.0",
> 	"method":"shh_newIdentity",
> 	"params":[],
> 	"id":73
> }
> ```
> 

### Examples:

> 

----------------

## hasIdentity (deprecated)

```
POST {{HOST}}
```

Checks if the client hold the private keys for a given identity.

**Deprecated** This function will be removed in the future.

**Parameters**

`DATA`, 60 Bytes - The identity address to check

```
params: [
  "0x04f96a5e25610293e42a73908e93ccc8c4d4dc0edcfa9fa872f50cb214e08ebf61a03e245533f97284d442460f2998cd41858798ddfd4d661997d3940272b717b1"
]
```

**Returns**

`Boolean` - `true` if the client holds the privatekey for that identity, `false` otherwise

----------------

### Request

> 
> **Header**
> 
> |Key|Value|Description|
> |---|---|---|
> |Content-Type|application/json||
> 
> **Body**
> 
> ```
> {
> 	"jsonrpc":"2.0",
> 	"method":"shh_hasIdentity",
> 	"params":[
> 		"0x04f96a5e25610293e42a73908e93ccc8c4d4dc0edcfa9fa872f50cb214e08ebf61a03e245533f97284d442460f2998cd41858798ddfd4d661997d3940272b717b1"
> 	],
> 	"id":73
> }
> ```
> 

### Examples:

> 

----------------

## newGroup (deprecated)

```
POST {{HOST}}
```

Create a new group.

**Deprecated** This function will be removed in the future.

**Parameters**

none

**Returns**

`DATA`, 60 Bytes - The address of the new group

----------------

### Request

> 
> **Header**
> 
> |Key|Value|Description|
> |---|---|---|
> |Content-Type|application/json||
> 
> **Body**
> 
> ```
> {
> 	"jsonrpc":"2.0",
> 	"method":"shh_newGroup",
> 	"params":[],
> 	"id":73
> }
> ```
> 

### Examples:

> 

----------------

## addToGroup (deprecated)

```
POST {{HOST}}
```

Add to a group.

**Deprecated** This function will be removed in the future.

**Parameters**

`DATA`, 60 Bytes - The identity address to add to a group

```
params: [
  "0x04f96a5e25610293e42a73908e93ccc8c4d4dc0edcfa9fa872f50cb214e08ebf61a03e245533f97284d442460f2998cd41858798ddfd4d661997d3940272b717b1"
]
```

**Returns**

`Boolean` - `true` if the identity was successfully added to the group, `false` otherwise

----------------

### Request

> 
> **Header**
> 
> |Key|Value|Description|
> |---|---|---|
> |Content-Type|application/json||
> 
> **Body**
> 
> ```
> {
> 	"jsonrpc":"2.0",
> 	"method":"shh_addToGroup",
> 	"params":[
> 		"0x04f96a5e25610293e42a73908e93ccc8c4d4dc0edcfa9fa872f50cb214e08ebf61a03e245533f97284d442460f2998cd41858798ddfd4d661997d3940272b717b1"
> 	],
> 	"id":73
> }
> ```
> 

### Examples:

> 

----------------

## newFilter (deprecated)

```
POST {{HOST}}
```

Creates filter to notify, when client receives whisper message matching the filter options.

**Deprecated** This function will be removed in the future.

**Parameters**

`Object` - The filter options

`to`: `DATA`, 60 Bytes - (optional) Identity of the receiver. When present it will try to decrypt any incoming message if the client holds the private key to this identity

`topics`: `Array of DATA` - Array of `DATA` topics which the incoming message's topics should match

```
params: [{
   "topics": ['0x12341234bf4b564f'],
   "to": "0x04f96a5e25610293e42a73908e93ccc8c4d4dc0edcfa9fa872f50cb214e08ebf61a03e245533f97284d442460f2998cd41858798ddfd4d661997d3940272b717b1"
}]
```

**Returns**

`QUANTITY` - The newly created filter id

----------------

### Request

> 
> **Header**
> 
> |Key|Value|Description|
> |---|---|---|
> |Content-Type|application/json||
> 
> **Body**
> 
> ```
> {
> 	"jsonrpc":"2.0",
> 	"method":"shh_newFilter",
> 	"params":[{
> 		"topics": [
> 			"0x12341234bf4b564f"
> 		],
> 		"to": "0x2341234bf4b2341234bf4b564f..."
> 	}],
> 	"id":73
> }
> ```
> 

### Examples:

> 

----------------

## uninstallFilter (deprecated)

```
POST {{HOST}}
```

Uninstalls a filter with given id.

**Deprecated** This function will be removed in the future.

**Parameters**

`QUANTITY` - The filter id

```
params: [
  "0x7" // 7
]
```

**Returns**

`Boolean` - `true` if the filter was successfully uninstalled, `false` otherwise

----------------

### Request

> 
> **Header**
> 
> |Key|Value|Description|
> |---|---|---|
> |Content-Type|application/json||
> 
> **Body**
> 
> ```
> {
> 	"jsonrpc":"2.0",
> 	"method":"shh_uninstallFilter",
> 	"params":[
> 		"0x7"
> 	],
> 	"id":73
> }
> ```
> 

### Examples:

> 

----------------

## getFilterChanges (deprecated)

```
POST {{HOST}}
```

Polling method for whisper filters. Returns new messages since the last call of this method.

**Deprecated** This function will be removed in the future.

**Note** calling the `shh_getMessages` method, will reset the buffer for this method, so that you won't receive duplicate messages.

**Parameters**

`QUANTITY` - The filter id

```
params: [
  "0x7" // 7
]
```

**Returns**

`Array` - Array of messages received since last poll

`hash`: `DATA`, 32 Bytes (?) - The hash of the message

`from`: `DATA`, 60 Bytes - The sender of the message, if a sender was specified

`to`: `DATA`, 60 Bytes - The receiver of the message, if a receiver was specified

`expiry`: `QUANTITY` - Integer of the time in seconds when this message should expire

`ttl`: `QUANTITY` - Integer of the time the message should float in the system in seconds

`sent`: `QUANTITY` - Integer of the unix timestamp when the message was sent

`topics`: `Array of DATA` - Array of DATA topics the message contained

`payload`: `DATA` - The payload of the message.

`workProved`: `QUANTITY` - Integer of the work this message required before it was send

----------------

### Request

> 
> **Header**
> 
> |Key|Value|Description|
> |---|---|---|
> |Content-Type|application/json||
> 
> **Body**
> 
> ```
> {
> 	"jsonrpc":"2.0",
> 	"method":"shh_getFilterChanges",
> 	"params":[
> 		"0x7"
> 	],
> 	"id":73
> }
> ```
> 

### Examples:

> 

----------------

## getMessages (deprecated)

```
POST {{HOST}}
```

Get all messages matching a filter. Unlike `shh_getFilterChanges` this returns all messages.

**Deprecated** This function will be removed in the future.

**Parameters**

`QUANTITY` - The filter id

```
params: [
  "0x7" // 7
]
```

**Returns**

`Array` - Array of messages received since last poll

----------------

### Request

> 
> **Header**
> 
> |Key|Value|Description|
> |---|---|---|
> |Content-Type|application/json||
> 
> **Body**
> 
> ```
> {
> 	"jsonrpc":"2.0",
> 	"method":"shh_getMessages",
> 	"params":[
> 		"0x7"
> 	],
> 	"id":73
> }
> ```
> 

### Examples:

> 

----------------

----------------

Built with [Postdown][PyPI].

Author: [Titor](https://github.com/TitorX)

[PyPI]:    https://pypi.python.org/pypi/Postdown
