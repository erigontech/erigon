.. # define a hard line break for HTML
.. |br| raw:: html

   <br />

RPC Interfaces
================

A collection holding all the Ethereum JSON RPC API calls

--------------

web3_clientVersion
------------------

Returns the current client version.

**Parameters**

.. list-table::
   :widths: 25 75
   :header-rows: 1

   * - Type
     - Description
   * - ``NONE``
     - 


**Example**

::

   curl -s --data '{"jsonrpc":"2.0","method":"web3_clientVersion","params":[],"id":"1"}' -H "Content-Type: application/json" -X POST http://localhost:8545

**Returns**

.. list-table::
   :widths: 25 75
   :header-rows: 1

   * - Type
     - Description
   * - ``STRING``
     - The current client version string including node name and version

--------------

web3_sha3
---------

Returns Keccak-256 (not the standardized SHA3-256) of the given data.

**Parameters**

.. list-table::
   :widths: 25 75
   :header-rows: 1

   * - Type
     - Description
   * - ``DATA``
     - The data to convert into a SHA3 hash


**Example**

::

   curl -s --data '{"jsonrpc":"2.0","method":"web3_sha3","params":["0x68656c6c6f20776f726c64"],"id":"1"}' -H "Content-Type: application/json" -X POST http://localhost:8545

**Returns**

.. list-table::
   :widths: 25 75
   :header-rows: 1

   * - Type
     - Description
   * - ``DATA``
     - The SHA3 result of the given input string

--------------

net_listening
-------------

Returns ``true`` if client is actively listening for network connections.

.. warning::
   TODO: The code currently returns a hard coded ``true`` value. Remove hard coded value.

**Parameters**

.. list-table::
   :widths: 25 75
   :header-rows: 1

   * - Type
     - Description
   * - ``NONE``
     - 


**Example**

::

   curl -s --data '{"jsonrpc":"2.0","method":"net_listening","params":[],"id":"1"}' -H "Content-Type: application/json" -X POST http://localhost:8545

**Returns**

.. list-table::
   :widths: 25 75
   :header-rows: 1

   * - Type
     - Description
   * - ``BOOLEAN``
     - ``true`` when listening, ``false`` otherwise

--------------

net_version
-----------

Returns the current network id.

**Parameters**

.. list-table::
   :widths: 25 75
   :header-rows: 1

   * - Type
     - Description
   * - ``NONE``
     - 


**Example**

::

   curl -s --data '{"jsonrpc":"2.0","method":"net_version","params":[],"id":"1"}' -H "Content-Type: application/json" -X POST http://localhost:8545

**Returns**

.. list-table::
   :widths: 25 75
   :header-rows: 1

   * - Type
     - Description
   * - ``STRING``
     - The current network id. One of |br|  ``1``: Ethereum Mainnet |br|  ``4``: Rinkeby Testnet |br|  ``5``: Görli Testnet |br|

--------------

net_peerCount
-------------

Returns number of peers currently connected to the client.

.. warning::
   TODO: This routine currently returns a hard coded value of '25'

**Parameters**

.. list-table::
   :widths: 25 75
   :header-rows: 1

   * - Type
     - Description
   * - ``NONE``
     - 


**Example**

::

   curl -s --data '{"jsonrpc":"2.0","method":"net_peerCount","params":[],"id":"1"}' -H "Content-Type: application/json" -X POST http://localhost:8545

**Returns**

.. list-table::
   :widths: 25 75
   :header-rows: 1

   * - Type
     - Description
   * - ``QUANTITY``
     - Integer of the number of connected peers

--------------

eth_getBlockByNumber
--------------------

Returns information about a block given the block's number.

**Parameters**

.. list-table::
   :widths: 25 75
   :header-rows: 1

   * - Type
     - Description
   * - ``QUANTITY | TAG``
     - Integer block number or one of "earliest", "latest" or "pending"
   * - ``BOOLEAN``
     - If ``true`` it returns the full transaction objects, if ``false`` only the hashes of the transactions


**Example**

::

   curl -s --data '{"jsonrpc":"2.0","method":"eth_getBlockByNumber","params":["0xf4629",false],"id":"1"}' -H "Content-Type: application/json" -X POST http://localhost:8545

**Returns**

Object - An object of type Block defined as:

.. list-table::
   :widths: 15 15 70
   :header-rows: 1

   * - Type
     - Name
     - Description
   * - ``QUANTITY``
     - ``number``
     - The block number or ``null`` when pending
   * - ``DATA, 32 BYTES``
     - ``hash``
     - Hash of the block or ``null`` when pending
   * - ``DATA, 32 BYTES``
     - ``parentHash``
     - Hash of the parent block
   * - ``DATA, 8 BYTES``
     - ``nonce``
     - Hash of the proof of work or ``null`` when pending
   * - ``DATA, 32 BYTES``
     - ``sha3Uncles``
     - SHA3 of the uncles data in the block
   * - ``DATA, 256 BYTES``
     - ``logsBloom``
     - The bloom filter for the block's logs or ``null`` when pending
   * - ``DATA, 32 BYTES``
     - ``transactionsRoot``
     - The root of the transaction trie of the block
   * - ``DATA, 32 BYTES``
     - ``stateRoot``
     - The root of the final state trie of the block
   * - ``DATA, 32 BYTES``
     - ``receiptsRoot``
     - The root of the receipts trie of the block
   * - ``DATA, 20 BYTES``
     - ``miner``
     - The address of the beneficiary to whom the mining rewards were given
   * - ``QUANTITY``
     - ``difficulty``
     - Integer of the difficulty for this block
   * - ``QUANTITY``
     - ``totalDifficulty``
     - Integer of the total difficulty of the chain until this block
   * - ``DATA``
     - ``extraData``
     - The extra data field of this block
   * - ``QUANTITY``
     - ``size``
     - Integer the size of this block in bytes
   * - ``QUANTITY``
     - ``gasLimit``
     - The maximum gas allowed in this block
   * - ``QUANTITY``
     - ``gasUsed``
     - The total used gas by all transactions in this block
   * - ``QUANTITY``
     - ``timestamp``
     - The unix timestamp for when the block was collated
   * - ``ARRAY``
     - ``transactions``
     - Array of transaction objects, or 32 Bytes transaction hashes depending on the last given parameter
   * - ``ARRAY``
     - ``uncles``
     - Array of uncle hashes

--------------

eth_getBlockByHash
------------------

Returns information about a block given the block's hash.

**Parameters**

.. list-table::
   :widths: 25 75
   :header-rows: 1

   * - Type
     - Description
   * - ``DATA, 32 BYTES``
     - Hash of a block
   * - ``BOOLEAN``
     - If ``true`` it returns the full transaction objects, if ``false`` only the hashes of the transactions


**Example**

::

   curl -s --data '{"jsonrpc":"2.0","method":"eth_getBlockByHash","params":["0x0b4c6fb75ded4b90218cf0346b0885e442878f104e1b60bf75d5b6860eeacd53",false],"id":"1"}' -H "Content-Type: application/json" -X POST http://localhost:8545

**Returns**

Object - An object of type Block as described at eth_getBlockByNumber, or ``null`` when no block was found

.. list-table::
   :widths: 25 75
   :header-rows: 1

   * - Type
     - Description
   * - ``OBJECT``
     - An object of type Block as described at ``eth_getBlockByNumber``, or ``null`` when no block was found

--------------

eth_getBlockTransactionCountByNumber
------------------------------------

Returns the number of transactions in a block given the block's block number.

**Parameters**

.. list-table::
   :widths: 25 75
   :header-rows: 1

   * - Type
     - Description
   * - ``QUANTITY | TAG``
     - Integer block number or one of "earliest", "latest" or "pending"


**Example**

::

   curl -s --data '{"jsonrpc":"2.0","method":"eth_getBlockTransactionCountByNumber","params":["0xf4629"],"id":"1"}' -H "Content-Type: application/json" -X POST http://localhost:8545

**Returns**

.. list-table::
   :widths: 25 75
   :header-rows: 1

   * - Type
     - Description
   * - ``QUANTITY``
     - Integer of the number of transactions in this block

--------------

eth_getBlockTransactionCountByHash
----------------------------------

Returns the number of transactions in a block given the block's block hash.

**Parameters**

.. list-table::
   :widths: 25 75
   :header-rows: 1

   * - Type
     - Description
   * - ``DATA, 32 BYTES``
     - hash of a block


**Example**

::

   curl -s --data '{"jsonrpc":"2.0","method":"eth_getBlockTransactionCountByHash","params":["0x0b4c6fb75ded4b90218cf0346b0885e442878f104e1b60bf75d5b6860eeacd53"],"id":"1"}' -H "Content-Type: application/json" -X POST http://localhost:8545

**Returns**

.. list-table::
   :widths: 25 75
   :header-rows: 1

   * - Type
     - Description
   * - ``QUANTITY``
     - Integer of the number of transactions in this block

--------------

eth_getTransactionByHash
------------------------

Returns information about a transaction given the transaction's hash.

**Parameters**

.. list-table::
   :widths: 25 75
   :header-rows: 1

   * - Type
     - Description
   * - ``DATA, 32 BYTES``
     - hash of a transaction


**Example**

::

   curl -s --data '{"jsonrpc":"2.0","method":"eth_getTransactionByHash","params":["0xb2fea9c4b24775af6990237aa90228e5e092c56bdaee74496992a53c208da1ee"],"id":"1"}' -H "Content-Type: application/json" -X POST http://localhost:8545

**Returns**

Object - An object of type Transaction or ``null`` when no transaction was found

.. list-table::
   :widths: 15 15 70
   :header-rows: 1

   * - Type
     - Name
     - Description
   * - ``DATA, 32 BYTES``
     - ``hash``
     - hash of the transaction
   * - ``QUANTITY``
     - ``nonce``
     - The number of transactions made by the sender prior to this one
   * - ``DATA, 32 BYTES``
     - ``blockHash``
     - hash of the block where this transaction was in. null when its pending
   * - ``QUANTITY``
     - ``blockNumber``
     - block number where this transaction was in. null when its pending
   * - ``QUANTITY``
     - ``transactionIndex``
     - Integer of the transactions index position in the block. null when its pending
   * - ``DATA, 20 BYTES``
     - ``from``
     - address of the sender
   * - ``DATA, 20 BYTES``
     - ``to``
     - address of the receiver. null when its a contract creation transaction
   * - ``QUANTITY``
     - ``value``
     - value transferred in Wei
   * - ``QUANTITY``
     - ``gasPrice``
     - gas price provided by the sender in Wei
   * - ``QUANTITY``
     - ``gas``
     - gas provided by the sender
   * - ``DATA``
     - ``input``
     - The data send along with the transaction

--------------

eth_getTransactionByBlockHashAndIndex
-------------------------------------

Returns information about a transaction given the block's hash and a transaction index.

**Parameters**

.. list-table::
   :widths: 25 75
   :header-rows: 1

   * - Type
     - Description
   * - ``DATA, 32 BYTES``
     - hash of a block
   * - ``QUANTITY``
     - Integer of the transaction index position


**Example**

::

   curl -s --data '{"jsonrpc":"2.0","method":"eth_getTransactionByBlockHashAndIndex","params":["0x785b221ec95c66579d5ae14eebe16284a769e948359615d580f02e646e93f1d5","0x25"],"id":"1"}' -H "Content-Type: application/json" -X POST http://localhost:8545

**Returns**

Object - An object of type Transaction or ``null`` when no transaction was found. See eth_getTransactionByHash

.. list-table::
   :widths: 25 75
   :header-rows: 1

   * - Type
     - Description
   * - ``OBJECT``
     - An object of type Transaction or ``null`` when no transaction was found. See ``eth_getTransactionByHash``

--------------

eth_getTransactionByBlockNumberAndIndex
---------------------------------------

Returns information about a transaction given a block number and transaction index.

**Parameters**

.. list-table::
   :widths: 25 75
   :header-rows: 1

   * - Type
     - Description
   * - ``QUANTITY | TAG``
     - Integer block number or one of "earliest", "latest" or "pending"
   * - ``QUANTITY``
     - The transaction index position


**Example**

::

   curl -s --data '{"jsonrpc":"2.0","method":"eth_getTransactionByBlockNumberAndIndex","params":["0x52a90b","0x25"],"id":"1"}' -H "Content-Type: application/json" -X POST http://localhost:8545

**Returns**

Object - An object of type Transaction or ``null`` when no transaction was found. See eth_getTransactionByHash

.. list-table::
   :widths: 25 75
   :header-rows: 1

   * - Type
     - Description
   * - ``OBJECT``
     - An object of type Transaction or ``null`` when no transaction was found. See ``eth_getTransactionByHash``

--------------

eth_getTransactionReceipt
-------------------------

Returns the receipt of a transaction given the transaction's hash.

.. note::
   Receipts are not available for pending transactions.

**Parameters**

.. list-table::
   :widths: 25 75
   :header-rows: 1

   * - Type
     - Description
   * - ``DATA, 32 BYTES``
     - hash of a transaction


**Example**

::

   curl -s --data '{"jsonrpc":"2.0","method":"eth_getTransactionReceipt","params":["0xa3ece39ae137617669c6933b7578b94e705e765683f260fcfe30eaa41932610f"],"id":"1"}' -H "Content-Type: application/json" -X POST http://localhost:8545

**Returns**

Object - An object of type TransactionReceipt or ``null`` when no receipt was found

.. list-table::
   :widths: 15 15 70
   :header-rows: 1

   * - Type
     - Name
     - Description
   * - ``DATA, 32 BYTES``
     - ``transactionHash``
     - hash of the transaction
   * - ``QUANTITY``
     - ``transactionIndex``
     - Integer of the transactions index position in the block
   * - ``DATA, 32 BYTES``
     - ``blockHash``
     - hash of the block where this transaction was in
   * - ``QUANTITY``
     - ``blockNumber``
     - block number where this transaction was in
   * - ``QUANTITY``
     - ``cumulativeGasUsed``
     - The total amount of gas used when this transaction was executed in the block
   * - ``QUANTITY``
     - ``gasUsed``
     - The amount of gas used by this specific transaction alone
   * - ``DATA, 20 BYTES``
     - ``contractAddress``
     - The contract address created, if the transaction was a contract creation, null otherwise
   * - ``ARRAY``
     - ``logs``
     - Array of log objects, which this transaction generated
   * - ``DATA, 256 BYTES``
     - ``logsBloom``
     - Bloom filter for light clients to quickly retrieve related logs.
   * - ``DATA 32 BYTES``
     - ``root``
     - post-transaction stateroot (if the block is pre-Byzantium)
   * - ``QUANTITY``
     - ``status``
     - either 1 = success or 0 = failure (if block is Byzatnium or later)

--------------

eth_getUncleByBlockNumberAndIndex
---------------------------------

Returns information about an uncle given a block's number and the index of the uncle.

**Parameters**

.. list-table::
   :widths: 25 75
   :header-rows: 1

   * - Type
     - Description
   * - ``QUANTITY | TAG``
     - Integer block number or one of "earliest", "latest" or "pending"
   * - ``QUANTITY``
     - The uncle's index position


**Example**

::

   curl -s --data '{"jsonrpc":"2.0","method":"eth_getUncleByBlockNumberAndIndex","params":["0x3","0x0"],"id":"1"}' -H "Content-Type: application/json" -X POST http://localhost:8545

**Returns**

Object - An object of type Block (with zero transactions), or ``null`` when no uncle was found. See eth_getBlockByHash

.. list-table::
   :widths: 25 75
   :header-rows: 1

   * - Type
     - Description
   * - ``OBJECT``
     - An object of type Block (with zero transactions), or ``null`` when no uncle was found. See ``eth_getBlockByHash``

--------------

eth_getUncleByBlockHashAndIndex
-------------------------------

Returns information about an uncle given a block's hash and the index of the uncle.

**Parameters**

.. list-table::
   :widths: 25 75
   :header-rows: 1

   * - Type
     - Description
   * - ``DATA, 32 BYTES``
     - Hash of the block holding the uncle
   * - ``QUANTITY``
     - The uncle's index position


**Example**

::

   curl -s --data '{"jsonrpc":"2.0","method":"eth_getUncleByBlockHashAndIndex","params":["0x3d6122660cc824376f11ee842f83addc3525e2dd6756b9bcf0affa6aa88cf741","0x0"],"id":"1"}' -H "Content-Type: application/json" -X POST http://localhost:8545

**Returns**

Object - An object of type Block (with zero transactions), or ``null`` when no uncle was found. See eth_getBlockByHash

.. list-table::
   :widths: 25 75
   :header-rows: 1

   * - Type
     - Description
   * - ``OBJECT``
     - An object of type Block (with zero transactions), or ``null`` when no uncle was found. See ``eth_getBlockByHash``

--------------

eth_getUncleCountByBlockNumber
------------------------------

Returns the number of uncles in the block, if any.

**Parameters**

.. list-table::
   :widths: 25 75
   :header-rows: 1

   * - Type
     - Description
   * - ``QUANTITY | TAG``
     - Integer block number or one of "earliest", "latest" or "pending"


**Example**

::

   curl -s --data '{"jsonrpc":"2.0","method":"eth_getUncleCountByBlockNumber","params":["0x3"],"id":"1"}' -H "Content-Type: application/json" -X POST http://localhost:8545

**Returns**

.. list-table::
   :widths: 25 75
   :header-rows: 1

   * - Type
     - Description
   * - ``QUANTITY``
     - The number of uncles in the block, if any

--------------

eth_getUncleCountByBlockHash
----------------------------

Returns the number of uncles in the block, if any.

**Parameters**

.. list-table::
   :widths: 25 75
   :header-rows: 1

   * - Type
     - Description
   * - ``DATA, 32 BYTES``
     - Hash of the block containing the uncle


**Example**

::

   curl -s --data '{"jsonrpc":"2.0","method":"eth_getUncleCountByBlockHash","params":["0x3d6122660cc824376f11ee842f83addc3525e2dd6756b9bcf0affa6aa88cf741"],"id":"1"}' -H "Content-Type: application/json" -X POST http://localhost:8545

**Returns**

.. list-table::
   :widths: 25 75
   :header-rows: 1

   * - Type
     - Description
   * - ``QUANTITY``
     - The number of uncles in the block, if any

--------------

eth_newPendingTransactionFilter
-------------------------------

Creates a pending transaction filter in the node. To check if the state has changed, call ``eth_getFilterChanges``.

**Parameters**

.. list-table::
   :widths: 25 75
   :header-rows: 1

   * - Type
     - Description
   * - ``NONE``
     - 


**Example**

::

   curl -s --data '{"jsonrpc":"2.0","method":"eth_newPendingTransactionFilter","params":[],"id":"1"}' -H "Content-Type: application/json" -X POST http://localhost:8545

**Returns**

.. list-table::
   :widths: 25 75
   :header-rows: 1

   * - Type
     - Description
   * - ``QUANTITY``
     - A filter id

--------------

eth_newBlockFilter
------------------

Creates a block filter in the node, to notify when a new block arrives. To check if the state has changed, call ``eth_getFilterChanges``.

**Parameters**

.. list-table::
   :widths: 25 75
   :header-rows: 1

   * - Type
     - Description
   * - ``NONE``
     - 


**Example**

::

   curl -s --data '{"jsonrpc":"2.0","method":"eth_newBlockFilter","params":[],"id":"1"}' -H "Content-Type: application/json" -X POST http://localhost:8545

**Returns**

.. list-table::
   :widths: 25 75
   :header-rows: 1

   * - Type
     - Description
   * - ``QUANTITY``
     - A filter id

--------------

eth_newFilter
-------------

Creates an arbitrary filter object, based on filter options, to notify when the state changes (logs). To check if the state has changed, call ``eth_getFilterChanges``.

**Parameters**

.. list-table::
   :widths: 25 75
   :header-rows: 1

   * - Type
     - Description
   * - ``QUANTITY``
     - TAG|(optional, default "latest") Integer block number, or "earliest", "latest" or "pending" for not yet mined transactions
   * - ``QUANTITY``
     - TAG|(optional, default "latest") Integer block number, or "earliest", "latest" or "pending" for not yet mined transactions
   * - ``DATA``
     - 
   * - ``ARRAY OF DATA, 20 BYTES``
     - (optional) Contract address or a list of addresses from which logs should originate
   * - ``ARRAY OF DATA,``
     - (optional) Array of 32 Bytes DATA topics. Topics are order-dependent. Each topic can also be an array of DATA with "or" options


**Example**

::

   curl -s --data '{"jsonrpc":"2.0","method":"eth_newFilter","params":[{"fromBlock":"0x1","toBlock":"0x2","address":"0x8888f1f195afa192cfee860698584c030f4c9db1","topics":["0x000000000000000000000000a94f5374fce5edbc8e2a8697c15331677e6ebf0b",null,["0x000000000000000000000000a94f5374fce5edbc8e2a8697c15331677e6ebf0b","0x0000000000000000000000000aff3454fce5edbc8cca8697c15331677e6ebccc"]]}],"id":"1"}' -H "Content-Type: application/json" -X POST http://localhost:8545

**Returns**

.. list-table::
   :widths: 25 75
   :header-rows: 1

   * - Type
     - Description
   * - ``QUANTITY``
     - A filter id


**Examples**

A note on specifying topic filters
Topics are order-dependent. A transaction with a log with topics [A, B] will be matched by the following topic filters
[] "anything"
[A] "A in first position (and anything after)"
[null, B] "anything in first position AND B in second position (and anything after)"
[A, B] "A in first position AND B in second position (and anything after)"
[[A, B], [A, B]] "(A OR B) in first position AND (A OR B) in second position (and anything after)"

--------------

eth_uninstallFilter
-------------------

Uninstalls a previously-created filter given the filter's id. Always uninstall filters when no longer needed.

.. note::
   Filters timeout when they are not requested with ``eth_getFilterChanges`` for a period of time.

**Parameters**

.. list-table::
   :widths: 25 75
   :header-rows: 1

   * - Type
     - Description
   * - ``QUANTITY``
     - The filter id


**Example**

::

   curl -s --data '{"jsonrpc":"2.0","method":"eth_uninstallFilter","params":["0xdeadbeef"],"id":"1"}' -H "Content-Type: application/json" -X POST http://localhost:8545

**Returns**

.. list-table::
   :widths: 25 75
   :header-rows: 1

   * - Type
     - Description
   * - ``BOOLEAN``
     - ``true`` if the filter was successfully uninstalled, ``false`` otherwise

--------------

eth_getFilterChanges
--------------------

Returns an array of objects of type Log, an array of block hashes (for ``eth_newBlockFilter``) or an array of transaction hashes (for ``eth_newPendingTransactionFilter``) or an empty array if nothing has changed since the last poll.

.. note::
   In solidity: The first topic is the hash of the signature of the event (if you have not declared the event anonymous.

**Parameters**

.. list-table::
   :widths: 25 75
   :header-rows: 1

   * - Type
     - Description
   * - ``QUANTITY``
     - The filter id


**Example**

::

   curl -s --data '{"jsonrpc":"2.0","method":"eth_getFilterChanges","params":["0xdeadbeef"],"id":"1"}' -H "Content-Type: application/json" -X POST http://localhost:8545

**Returns**

Object - An object of type FilterLog is defined as

.. list-table::
   :widths: 15 15 70
   :header-rows: 1

   * - Type
     - Name
     - Description
   * - ``BOOLEAN``
     - ``removed``
     - ``true`` when the log was removed, due to a chain reorganization. ``false`` if its a valid log
   * - ``QUANTITY``
     - ``logIndex``
     - Integer of the log index position in the block. null when its pending log
   * - ``QUANTITY``
     - ``transactionIndex``
     - Integer of the transactions index position log was created from. null when its pending log
   * - ``DATA, 32 BYTES``
     - ``transactionHash``
     - hash of the transactions this log was created from. null when its pending log
   * - ``DATA, 32 BYTES``
     - ``blockHash``
     - hash of the block where this log was in. null when its pending. null when its pending log
   * - ``QUANTITY``
     - ``blockNumber``
     - The block number where this log was in. null when its pending. null when its pending log
   * - ``DATA, 20 BYTES``
     - ``address``
     - address from which this log originated
   * - ``DATA``
     - ``data``
     - contains one or more 32 Bytes non-indexed arguments of the log
   * - ``ARRAY OF DATA``
     - ``topics``
     - Array of 0 to 4 32 Bytes DATA of indexed log arguments.

--------------

eth_getLogs
-----------

Returns an array of logs matching a given filter object.

**Parameters**

.. list-table::
   :widths: 25 75
   :header-rows: 1

   * - Type
     - Description
   * - ``OBJECT``
     - An object of type Filter, see ``eth_newFilter`` parameters


**Example**

::

   curl -s --data '{"jsonrpc":"2.0","method":"eth_getLogs","params":[{"topics":["0x000000000000000000000000a94f5374fce5edbc8e2a8697c15331677e6ebf0b"]}],"id":"1"}' -H "Content-Type: application/json" -X POST http://localhost:8545

**Returns**

Array - An array of type Log or an empty array if nothing has changed since last poll. See eth_getFilterChanges

.. list-table::
   :widths: 25 75
   :header-rows: 1

   * - Type
     - Description
   * - ``ARRAY``
     - An array of type Log or an empty array if nothing has changed since last poll. See ``eth_getFilterChanges``

--------------

eth_getBalance
--------------

Returns the balance of an account for a given address.

**Parameters**

.. list-table::
   :widths: 25 75
   :header-rows: 1

   * - Type
     - Description
   * - ``DATA, 20 BYTES``
     - Address to check for balance
   * - ``QUANTITY | TAG``
     - Integer block number or one of "earliest", "latest" or "pending"


**Example**

::

   curl -s --data '{"jsonrpc":"2.0","method":"eth_getBalance","params":["0x5df9b87991262f6ba471f09758cde1c0fc1de734","0xb443"],"id":"1"}' -H "Content-Type: application/json" -X POST http://localhost:8545

**Returns**

.. list-table::
   :widths: 25 75
   :header-rows: 1

   * - Type
     - Description
   * - ``QUANTITY``
     - Integer of the current balance in wei

--------------

eth_getTransactionCount
-----------------------

Returns the number of transactions sent from an address (the nonce).

**Parameters**

.. list-table::
   :widths: 25 75
   :header-rows: 1

   * - Type
     - Description
   * - ``DATA, 20 BYTES``
     - Address from which to retrieve nonce
   * - ``QUANTITY | TAG``
     - Integer block number or one of "earliest", "latest" or "pending"


**Example**

::

   curl -s --data '{"jsonrpc":"2.0","method":"eth_getTransactionCount","params":["0xfd2605a2bf58fdbb90db1da55df61628b47f9e8c","0xc443"],"id":"1"}' -H "Content-Type: application/json" -X POST http://localhost:8545

**Returns**

.. list-table::
   :widths: 25 75
   :header-rows: 1

   * - Type
     - Description
   * - ``QUANTITY``
     - Integer of the number of transactions sent from this address

--------------

eth_getCode
-----------

Returns the byte code at a given address (if it's a smart contract).

**Parameters**

.. list-table::
   :widths: 25 75
   :header-rows: 1

   * - Type
     - Description
   * - ``DATA, 20 BYTES``
     - Address from which to retreive byte code
   * - ``QUANTITY | TAG``
     - Integer block number or one of "earliest", "latest" or "pending"


**Example**

::

   curl -s --data '{"jsonrpc":"2.0","method":"eth_getCode","params":["0x109c4f2ccc82c4d77bde15f306707320294aea3f","0xc443"],"id":"1"}' -H "Content-Type: application/json" -X POST http://localhost:8545

**Returns**

.. list-table::
   :widths: 25 75
   :header-rows: 1

   * - Type
     - Description
   * - ``DATA``
     - The byte code (if any) found at the given address

--------------

eth_getStorageAt
----------------

Returns the value from a storage position at a given address.

**Parameters**

.. list-table::
   :widths: 25 75
   :header-rows: 1

   * - Type
     - Description
   * - ``DATA, 20 BYTES``
     - Address of the contract whose storage to retreive
   * - ``QUANTITY``
     - Integer of the position in the storage
   * - ``QUANTITY | TAG``
     - Integer block number or one of "earliest", "latest" or "pending"


**Example**

::

   curl -s --data '{"jsonrpc":"2.0","method":"eth_getStorageAt","params":["0x109c4f2ccc82c4d77bde15f306707320294aea3f","0x0","0xc443"],"id":"1"}' -H "Content-Type: application/json" -X POST http://localhost:8545

**Returns**

.. list-table::
   :widths: 25 75
   :header-rows: 1

   * - Type
     - Description
   * - ``DATA``
     - The value at this storage position

--------------

eth_blockNumber
---------------

Returns the block number of most recent block.

**Parameters**

.. list-table::
   :widths: 25 75
   :header-rows: 1

   * - Type
     - Description
   * - ``NONE``
     - 


**Example**

::

   curl -s --data '{"jsonrpc":"2.0","method":"eth_blockNumber","params":[],"id":"1"}' -H "Content-Type: application/json" -X POST http://localhost:8545

**Returns**

.. list-table::
   :widths: 25 75
   :header-rows: 1

   * - Type
     - Description
   * - ``QUANTITY``
     - Integer of the current highest block number the client is on

--------------

eth_syncing
-----------

Returns a data object detailing the status of the sync process or ``false`` if not syncing.

**Parameters**

.. list-table::
   :widths: 25 75
   :header-rows: 1

   * - Type
     - Description
   * - ``NONE``
     - 


**Example**

::

   curl -s --data '{"jsonrpc":"2.0","method":"eth_syncing","params":[],"id":"1"}' -H "Content-Type: application/json" -X POST http://localhost:8545

**Returns**

Object - An object of type Syncing or ``false`` if not syncing.

.. list-table::
   :widths: 15 15 70
   :header-rows: 1

   * - Type
     - Name
     - Description
   * - ``QUANTITY``
     - ``startingBlock``
     - The block at which the import started (will only be reset, after the sync reached his head)
   * - ``QUANTITY``
     - ``currentBlock``
     - The current block, same as ``eth_blockNumber``
   * - ``QUANTITY``
     - ``highestBlock``
     - The estimated highest block

--------------

eth_chainId
-----------

Returns the current ethereum chainId.

**Parameters**

.. list-table::
   :widths: 25 75
   :header-rows: 1

   * - Type
     - Description
   * - ``NONE``
     - 


**Example**

::

   curl -s --data '{"jsonrpc":"2.0","method":"eth_chainId","params":[],"id":"1"}' -H "Content-Type: application/json" -X POST http://localhost:8545

**Returns**

.. list-table::
   :widths: 25 75
   :header-rows: 1

   * - Type
     - Description
   * - ``QUANTITY``
     - The current chainId

--------------

eth_protocolVersion
-------------------

Returns the current ethereum protocol version.

**Parameters**

.. list-table::
   :widths: 25 75
   :header-rows: 1

   * - Type
     - Description
   * - ``NONE``
     - 


**Example**

::

   curl -s --data '{"jsonrpc":"2.0","method":"eth_protocolVersion","params":[],"id":"1"}' -H "Content-Type: application/json" -X POST http://localhost:8545

**Returns**

.. list-table::
   :widths: 25 75
   :header-rows: 1

   * - Type
     - Description
   * - ``QUANTITY``
     - The current ethereum protocol version

--------------

eth_gasPrice
------------

Returns the current price per gas in wei.

**Parameters**

.. list-table::
   :widths: 25 75
   :header-rows: 1

   * - Type
     - Description
   * - ``NONE``
     - 


**Example**

::

   curl -s --data '{"jsonrpc":"2.0","method":"eth_gasPrice","params":[],"id":"1"}' -H "Content-Type: application/json" -X POST http://localhost:8545

**Returns**

.. list-table::
   :widths: 25 75
   :header-rows: 1

   * - Type
     - Description
   * - ``QUANTITY``
     - Integer of the current gas price in wei

--------------

eth_call
--------

Executes a new message call immediately without creating a transaction on the block chain.

**Parameters**

.. list-table::
   :widths: 25 75
   :header-rows: 1

   * - Type
     - Description
   * - ``DATA, 20 BYTES``
     - (optional) The address the transaction is sent from
   * - ``DATA, 20 BYTES``
     - The address the transaction is directed to
   * - ``QUANTITY``
     - (optional) Integer of the gas provided for the transaction execution. ``eth_call`` consumes zero gas, but this parameter may be needed by some executions
   * - ``QUANTITY``
     - (optional) Integer of the gasPrice used for each paid gas
   * - ``QUANTITY``
     - (optional) Integer of the value sent with this transaction
   * - ``DATA``
     - (optional) Hash of the method signature and encoded parameters. For details see Ethereum Contract ABI
   * - ``QUANTITY | TAG``
     - Integer block number or one of "earliest", "latest" or "pending"


**Example**

::

   curl -s --data '{"jsonrpc":"2.0","method":"eth_call","params":[{"to":"0x08a2e41fb99a7599725190b9c970ad3893fa33cf","data":"0x18160ddd"},"0xa2f2e0"],"id":"1"}' -H "Content-Type: application/json" -X POST http://localhost:8545

**Returns**

.. list-table::
   :widths: 25 75
   :header-rows: 1

   * - Type
     - Description
   * - ``DATA``
     - The return value of executed contract

--------------

eth_estimateGas
---------------

Returns an estimate of how much gas is necessary to allow the transaction to complete. The transaction will not be added to the blockchain.

.. note::
   The estimate may be significantly more than the amount of gas actually used by the transaction for a variety of reasons including EVM mechanics and node performance.

.. note::
   If no gas limit is specified geth uses the block gas limit from the pending block as an upper bound. As a result the returned estimate might not be enough to executed the call/transaction when the amount of gas is higher than the pending block gas limit.

**Parameters**

.. list-table::
   :widths: 25 75
   :header-rows: 1

   * - Type
     - Description
   * - ``OBJECT``
     - An object of type Call, see ``eth_call`` parameters, expect that all properties are optional


**Example**

::

   curl -s --data '{"jsonrpc":"2.0","method":"eth_estimateGas","params":[{"to":"0x3d597789ea16054a084ac84ce87f50df9198f415","from":"0x3d597789ea16054a084ac84ce87f50df9198f415","value":"0x1"}],"id":"1"}' -H "Content-Type: application/json" -X POST http://localhost:8545

**Returns**

.. list-table::
   :widths: 25 75
   :header-rows: 1

   * - Type
     - Description
   * - ``QUANTITY``
     - The estimated amount of gas needed for the call

--------------

eth_sendTransaction
-------------------

Creates new message call transaction or a contract creation if the data field contains code.

.. note::
   Use ``eth_getTransactionReceipt`` to get the contract address, after the transaction was mined, when you created a contract

**Parameters**

.. list-table::
   :widths: 25 75
   :header-rows: 1

   * - Type
     - Description
   * - ``DATA, 20 BYTES``
     - The address the transaction is send from
   * - ``DATA, 20 BYTES``
     - (optional when creating new contract) The address the transaction is directed to
   * - ``QUANTITY``
     - (optional, default 90000) Integer of the gas provided for the transaction execution. It will return unused gas
   * - ``QUANTITY``
     - (optional, default To-Be-Determined) Integer of the gasPrice used for each paid gas
   * - ``QUANTITY``
     - (optional) Integer of the value sent with this transaction
   * - ``DATA``
     - The compiled code of a contract OR the hash of the invoked method signature and encoded parameters. For details see Ethereum Contract ABI
   * - ``QUANTITY``
     - (optional) Integer of a nonce. This allows to overwrite your own pending transactions that use the same nonce


**Example**

::

   curl -s --data '{"jsonrpc":"2.0","method":"eth_sendTransaction","params":[{"from":"0xb60e8dd61c5d32be8058bb8eb970870f07233155","to":"0xd46e8dd67c5d32be8058bb8eb970870f07244567","gas":"0x76c0","gasPrice":"0x9184e72a000","value":"0x9184e72a","data":"0xd46e8dd67c5d32be8d46e8dd67c5d32be8058bb8eb970870f072445675058bb8eb970870f072445675"}],"id":"1"}' -H "Content-Type: application/json" -X POST http://localhost:8545

**Returns**

.. list-table::
   :widths: 25 75
   :header-rows: 1

   * - Type
     - Description
   * - ``DATA, 32 BYTES``
     - The transaction hash, or the zero hash if the transaction is not yet available

--------------

eth_sendRawTransaction
----------------------

Creates new message call transaction or a contract creation for previously-signed transactions.

.. note::
   Use ``eth_getTransactionReceipt`` to get the contract address, after the transaction was mined, when you created a contract.

**Parameters**

.. list-table::
   :widths: 25 75
   :header-rows: 1

   * - Type
     - Description
   * - ``DATA``
     - The signed transaction data


**Example**

::

   curl -s --data '{"jsonrpc":"2.0","method":"eth_sendRawTransaction","params":["0xd46e8dd67c5d32be8d46e8dd67c5d32be8058bb8eb970870f072445675058bb8eb970870f072445675"],"id":"1"}' -H "Content-Type: application/json" -X POST http://localhost:8545

**Returns**

.. list-table::
   :widths: 25 75
   :header-rows: 1

   * - Type
     - Description
   * - ``DATA, 32 BYTES``
     - The transaction hash, or the zero hash if the transaction is not yet available

--------------

eth_getProof
------------

See this EIP of more information: https://github.com/ethereum/EIPs/issues/1186

**Parameters**

.. list-table::
   :widths: 25 75
   :header-rows: 1

   * - Type
     - Description
   * - ``DATA, 20 BYTES``
     - The address of the storage locations being proved
   * - ``DATAARRAY``
     - one or more storage locations to prove
   * - ``QUANTITY | TAG``
     - Integer block number or one of "earliest", "latest" or "pending"


**Example**

::

   curl -s --data '{"id":"1","jsonrpc":"2.0","method":"eth_getProof","params":["0x7F0d15C7FAae65896648C8273B6d7E43f58Fa842",["0x56e81f171bcc55a6ff8345e692c0f86e5b48e01b996cadc001622fb5e363b421"],"latest"]}' -H "Content-Type: application/json" -X POST http://localhost:8545

**Returns**

.. list-table::
   :widths: 25 75
   :header-rows: 1

   * - Type
     - Description
   * - ``DATA``
     - The Merkel proof of the storage locations

--------------

eth_coinbase
------------

Returns the current client coinbase address.

**Parameters**

.. list-table::
   :widths: 25 75
   :header-rows: 1

   * - Type
     - Description
   * - ``NONE``
     - 


**Example**

::

   curl -s --data '{"jsonrpc":"2.0","method":"eth_coinbase","params":[],"id":"1"}' -H "Content-Type: application/json" -X POST http://localhost:8545

**Returns**

.. list-table::
   :widths: 25 75
   :header-rows: 1

   * - Type
     - Description
   * - ``DATA, 20 BYTES``
     - The current coinbase address

--------------

eth_hashrate
------------

Returns the number of hashes per second that the node is mining with.

**Parameters**

.. list-table::
   :widths: 25 75
   :header-rows: 1

   * - Type
     - Description
   * - ``NONE``
     - 


**Example**

::

   curl -s --data '{"jsonrpc":"2.0","method":"eth_hashrate","params":[],"id":"1"}' -H "Content-Type: application/json" -X POST http://localhost:8545

**Returns**

.. list-table::
   :widths: 25 75
   :header-rows: 1

   * - Type
     - Description
   * - ``QUANTITY``
     - Number of hashes per second

--------------

eth_mining
----------

Returns ``true`` if client is actively mining new blocks.

**Parameters**

.. list-table::
   :widths: 25 75
   :header-rows: 1

   * - Type
     - Description
   * - ``NONE``
     - 


**Example**

::

   curl -s --data '{"jsonrpc":"2.0","method":"eth_mining","params":[],"id":"1"}' -H "Content-Type: application/json" -X POST http://localhost:8545

**Returns**

.. list-table::
   :widths: 25 75
   :header-rows: 1

   * - Type
     - Description
   * - ``BOOLEAN``
     - ``true`` if the client is mining, ``false`` otherwise

--------------

eth_getWork
-----------

Returns the hash of the current block, the seedHash, and the boundary condition to be met ('target').

**Parameters**

.. list-table::
   :widths: 25 75
   :header-rows: 1

   * - Type
     - Description
   * - ``NONE``
     - 


**Example**

::

   curl -s --data '{"jsonrpc":"2.0","method":"eth_getWork","params":[],"id":"1"}' -H "Content-Type: application/json" -X POST http://localhost:8545

**Returns**

Object - An object of type Work (an array of three hashes representing block header pow-hash, seed hash and boundary condition

.. list-table::
   :widths: 15 15 70
   :header-rows: 1

   * - Type
     - Name
     - Description
   * - ``DATA, 32 BYTES``
     - ``current``
     - current block header pow-hash
   * - ``DATA, 32 BYTES``
     - ``seed``
     - The seed hash used for the DAG
   * - ``DATA, 32 BYTES``
     - ``boundary``
     - The boundary condition ('target'), 2^256 / difficulty

--------------

eth_submitWork
--------------

Submits a proof-of-work solution to the blockchain.

**Parameters**

.. list-table::
   :widths: 25 75
   :header-rows: 1

   * - Type
     - Description
   * - ``DATA, 8 BYTES``
     - The nonce found (64 bits)
   * - ``DATA, 32 BYTES``
     - The header's pow-hash (256 bits)
   * - ``DATA, 32 BYTES``
     - The mix digest (256 bits)


**Example**

::

   curl -s --data '{"jsonrpc":"2.0","method":"eth_submitWork","params":["0x1","0x1234567890abcdef1234567890abcdef1234567890abcdef1234567890abcdef","0xD16E5700000000000000000000000000D16E5700000000000000000000000000"],"id":"1"}' -H "Content-Type: application/json" -X POST http://localhost:8545

**Returns**

.. list-table::
   :widths: 25 75
   :header-rows: 1

   * - Type
     - Description
   * - ``BOOLEAN``
     - ``true`` if the provided solution is valid, ``false`` otherwise

--------------

eth_submitHashrate
------------------

Submit the mining hashrate to the blockchain.

**Parameters**

.. list-table::
   :widths: 25 75
   :header-rows: 1

   * - Type
     - Description
   * - ``DATA, 32 BYTES``
     - a hexadecimal string representation of the hash rate
   * - ``STRING``
     - A random hexadecimal ID identifying the client


**Example**

::

   curl -s --data '{"jsonrpc":"2.0","method":"eth_submitHashrate","params":["0x0000000000000000000000000000000000000000000000000000000000500000","0x59daa26581d0acd1fce254fb7e85952f4c09d0915afd33d3886cd914bc7d283c"],"id":"1"}' -H "Content-Type: application/json" -X POST http://localhost:8545

**Returns**

.. list-table::
   :widths: 25 75
   :header-rows: 1

   * - Type
     - Description
   * - ``BOOLEAN``
     - ``true`` if submitting went through succesfully, ``false`` otherwise

--------------

trace_call
----------

Executes the given call and returns a number of possible traces for it.

**Parameters**

.. list-table::
   :widths: 25 75
   :header-rows: 1

   * - Type
     - Description
   * - ``FROM: DATA, 20 BYTES``
     - (optional) 20 Bytes|The address the transaction is send from.
   * - ``TO: DATA, 20 BYTES``
     - (optional when creating new contract) 20 Bytes|The address the transaction is directed to.
   * - ``GAS: QUANTITY``
     - (optional) Integer formatted as a hex string of the gas provided for the transaction execution. ``eth_call`` consumes zero gas, but this parameter may be needed by some executions.
   * - ``GASPRICE: QUANTITY``
     - (optional) Integer formatted as a hex string of the gas price used for each paid gas.
   * - ``VALUE: QUANTITY``
     - (optional) Integer formatted as a hex string of the value sent with this transaction.
   * - ``DATA: DATA``
     - (optional) 4 byte hash of the method signature followed by encoded parameters. For details see Ethereum Contract ABI.
   * - ``STRINGARRAY``
     - An array of strings, one or more of: "vmTrace", "trace", "stateDiff".
   * - ``QUANTITY | TAG``
     - (optional) Integer of a block number, or the string 'earliest', 'latest' or 'pending'.


**Example**

::

   curl -s --data '{"jsonrpc":"2.0","method":"trace_call","params":[{"from":"0x407d73d8a49eeb85d32cf465507dd71d507100c1","to":"0xa94f5374fce5edbc8e2a8697c15331677e6ebf0b","value":"0x186a0"},["trace","vmTrace"],"latest"],"id":"1"}' -H "Content-Type: application/json" -X POST http://localhost:8545

**Returns**

Array - An array of type BlockTrace

.. list-table::
   :widths: 25 75
   :header-rows: 1

   * - Type
     - Description
   * - ``ARRAY``
     - An array of type BlockTrace

--------------

trace_callMany
--------------

Performs multiple call traces on top of the same block. i.e. transaction n will be executed on top of a pending block with all n-1 transactions applied (traced) first. Allows to trace dependent transactions.

**Parameters**

.. list-table::
   :widths: 25 75
   :header-rows: 1

   * - Type
     - Description
   * - ``CALLARRAY``
     - An array of Call objects plus strings, one or more of: "vmTrace", "trace", "stateDiff".
   * - ``QUANTITY | TAG``
     - (optional) integer block number, or the string 'latest', 'earliest' or 'pending', see the default block parameter.


**Example**

::

   curl -s --data '{"jsonrpc":"2.0","method":"trace_callMany","params":[[[{"from":"0x407d73d8a49eeb85d32cf465507dd71d507100c1","to":"0xa94f5374fce5edbc8e2a8697c15331677e6ebf0b","value":"0x186a0"},["trace"]],[{"from":"0x407d73d8a49eeb85d32cf465507dd71d507100c1","to":"0xa94f5374fce5edbc8e2a8697c15331677e6ebf0b","value":"0x186a0"},["trace"]]],"latest"],"id":"1"}' -H "Content-Type: application/json" -X POST http://localhost:8545

**Returns**

Array - An array of type BlockTrace

.. list-table::
   :widths: 25 75
   :header-rows: 1

   * - Type
     - Description
   * - ``ARRAY``
     - An array of type BlockTrace

--------------

trace_rawTransaction
--------------------

Traces a call to ``eth_sendRawTransaction`` without making the call, returning the traces

**Parameters**

.. list-table::
   :widths: 25 75
   :header-rows: 1

   * - Type
     - Description
   * - ``DATA``
     - Raw transaction data.
   * - ``STRINGARRAY``
     - Type of trace, one or more of: "vmTrace", "trace", "stateDiff".


**Example**

::

   curl -s --data '{"jsonrpc":"2.0","method":"trace_rawTransaction","params":["0x17104ac9d3312d8c136b7f44d4b8b47852618065ebfa534bd2d3b5ef218ca1f3",["vmTrace"]],"id":"1"}' -H "Content-Type: application/json" -X POST http://localhost:8545

**Returns**

Object - An object of type BlockTrace.

.. list-table::
   :widths: 25 75
   :header-rows: 1

   * - Type
     - Description
   * - ``OBJECT``
     - An object of type BlockTrace.

--------------

trace_replayBlockTransactions
-----------------------------

Replays all transactions in a block returning the requested traces for each transaction.

**Parameters**

.. list-table::
   :widths: 25 75
   :header-rows: 1

   * - Type
     - Description
   * - ``QUANTITY | TAG``
     - Integer of a block number, or the string 'earliest', 'latest' or 'pending'.
   * - ``STRINGARRAY``
     - Type of trace, one or more of: "vmTrace", "trace", "stateDiff".


**Example**

::

   curl -s --data '{"jsonrpc":"2.0","method":"trace_replayBlockTransactions","params":["0x2",["trace"]],"id":"1"}' -H "Content-Type: application/json" -X POST http://localhost:8545

**Returns**

Array - An array of type BlockTrace.

.. list-table::
   :widths: 25 75
   :header-rows: 1

   * - Type
     - Description
   * - ``ARRAY``
     - An array of type BlockTrace.

--------------

trace_replayTransaction
-----------------------

Replays a transaction, returning the traces.

**Parameters**

.. list-table::
   :widths: 25 75
   :header-rows: 1

   * - Type
     - Description
   * - ``DATA, 32 BYTES``
     - The transaction's hash.
   * - ``STRINGARRAY``
     - Type of trace, one or more of: "vmTrace", "trace", "stateDiff".


**Example**

::

   curl -s --data '{"jsonrpc":"2.0","method":"trace_replayTransaction","params":["0x02d4a872e096445e80d05276ee756cefef7f3b376bcec14246469c0cd97dad8f",["trace"]],"id":"1"}' -H "Content-Type: application/json" -X POST http://localhost:8545

**Returns**

Object - An object of type BlockTrace.

.. list-table::
   :widths: 25 75
   :header-rows: 1

   * - Type
     - Description
   * - ``OBJECT``
     - An object of type BlockTrace.

--------------

trace_transaction
-----------------

Returns traces for the given transaction

**Parameters**

.. list-table::
   :widths: 25 75
   :header-rows: 1

   * - Type
     - Description
   * - ``DATA, 32 BYTES``
     - The transaction's hash


**Example**

::

   curl -s --data '{"jsonrpc":"2.0","method":"trace_transaction","params":["0x17104ac9d3312d8c136b7f44d4b8b47852618065ebfa534bd2d3b5ef218ca1f3"],"id":"1"}' -H "Content-Type: application/json" -X POST http://localhost:8545

**Returns**

Array - An array of type AdhocTrace, see trace_filter.

.. list-table::
   :widths: 25 75
   :header-rows: 1

   * - Type
     - Description
   * - ``ARRAY``
     - An array of type AdhocTrace, see ``trace_filter``.

--------------

trace_get
---------

Returns trace at given position.

**Parameters**

.. list-table::
   :widths: 25 75
   :header-rows: 1

   * - Type
     - Description
   * - ``DATA, 32 BYTES``
     - The transaction's hash.
   * - ``QUANTITYARRAY``
     - The index position of the trace.


**Example**

::

   curl -s --data '{"jsonrpc":"2.0","method":"trace_get","params":["0x17104ac9d3312d8c136b7f44d4b8b47852618065ebfa534bd2d3b5ef218ca1f3",["0x0"]],"id":"1"}' -H "Content-Type: application/json" -X POST http://localhost:8545

**Returns**

Array - An array of type AdhocTrace, see trace_filter.

.. list-table::
   :widths: 25 75
   :header-rows: 1

   * - Type
     - Description
   * - ``ARRAY``
     - An array of type AdhocTrace, see ``trace_filter``.

--------------

trace_block
-----------

Returns traces created at given block.

**Parameters**

.. list-table::
   :widths: 25 75
   :header-rows: 1

   * - Type
     - Description
   * - ``QUANTITY | TAG``
     - Integer of a block number, or the string 'earliest', 'latest' or 'pending'.


**Example**

::

   curl -s --data '{"jsonrpc":"2.0","method":"trace_block","params":["0x3"],"id":"1"}' -H "Content-Type: application/json" -X POST http://localhost:8545

**Returns**

Array - An array of type AdhocTrace.

.. list-table::
   :widths: 25 75
   :header-rows: 1

   * - Type
     - Description
   * - ``ARRAY``
     - An array of type AdhocTrace.

--------------

trace_filter
------------

Returns traces matching given filter

**Parameters**

.. list-table::
   :widths: 25 75
   :header-rows: 1

   * - Type
     - Description
   * - ``FROMBLOCK: QUANTITY | TAG``
     - (optional) From this block.
   * - ``TOBLOCK: QUANTITY | TAG``
     - (optional) To this block.
   * - ``FROMADDRESS: DATA, 20 BYTES``
     - (optional) Sent from these addresses.
   * - ``TOADDRESS: DATA, 20 BYTES``
     - (optional) Sent to these addresses.
   * - ``AFTER: QUANTITY``
     - (optional) The offset trace number
   * - ``COUNT: QUANTITY``
     - (optional) Integer number of traces to display in a batch.


**Example**

::

   curl -s --data '{"jsonrpc":"2.0","method":"trace_filter","params":[{"fromBlock":"0x3","toBlock":"0x3"}],"id":"1"}' -H "Content-Type: application/json" -X POST http://localhost:8545

**Returns**

Array - An array of type AdHocTrace matching the given filter.

.. list-table::
   :widths: 25 75
   :header-rows: 1

   * - Type
     - Description
   * - ``ARRAY``
     - An array of type AdHocTrace matching the given filter.

--------------

erigon_forks
--------

Returns the genesis block hash and a sorted list of already passed fork block numbers as well as the next fork block (if applicable)

**Parameters**

.. list-table::
   :widths: 25 75
   :header-rows: 1

   * - Type
     - Description
   * - ``NONE``
     - 


**Example**

::

   curl -s --data '{"jsonrpc":"2.0","method":"erigon_forks","params":[],"id":"1"}' -H "Content-Type: application/json" -X POST http://localhost:8545

**Returns**

Object - An object of type Fork

.. list-table::
   :widths: 15 15 70
   :header-rows: 1

   * - Type
     - Name
     - Description
   * - ``DATA, 32 BYTES``
     - ``genesis``
     - The hash of the genesis block
   * - ``ARRAY OF QUANTITY``
     - ``passed``
     - Array of block numbers passed by this client
   * - ``QUANTITY``
     - ``next``
     - (optional) the next fork block

--------------

erigon_getHeaderByNumber
--------------------

Returns a block's header given a block number ignoring the block's transaction and uncle list (may be faster).

**Parameters**

.. list-table::
   :widths: 25 75
   :header-rows: 1

   * - Type
     - Description
   * - ``QUANTITY | TAG``
     - Integer block number or one of "earliest", "latest" or "pending"


**Example**

::

   curl -s --data '{"jsonrpc":"2.0","method":"erigon_getHeaderByNumber","params":["0x3"],"id":"1"}' -H "Content-Type: application/json" -X POST http://localhost:8545

**Returns**

Object - An object of type BlockHeader or ``null`` when no block was found. See eth_getBlockByHash

.. list-table::
   :widths: 25 75
   :header-rows: 1

   * - Type
     - Description
   * - ``OBJECT``
     - An object of type BlockHeader or ``null`` when no block was found. See ``eth_getBlockByHash``

--------------

erigon_getHeaderByHash
------------------

Returns a block's header given a block's hash.

**Parameters**

.. list-table::
   :widths: 25 75
   :header-rows: 1

   * - Type
     - Description
   * - ``DATA, 32 BYTES``
     - Hash of a block


**Example**

::

   curl -s --data '{"jsonrpc":"2.0","method":"erigon_getHeaderByHash","params":["0x3d6122660cc824376f11ee842f83addc3525e2dd6756b9bcf0affa6aa88cf741"],"id":"1"}' -H "Content-Type: application/json" -X POST http://localhost:8545

**Returns**

Object - An object of type BlockHeader or ``null`` when no block was found. See eth_getBlockByHash

.. list-table::
   :widths: 25 75
   :header-rows: 1

   * - Type
     - Description
   * - ``OBJECT``
     - An object of type BlockHeader or ``null`` when no block was found. See ``eth_getBlockByHash``

--------------

erigon_getLogsByHash
----------------

Returns an array of arrays of logs generated by the transactions in the block given by the block's hash.

.. note::
   The returned value is an array of arrays of log entries. There is an entry for each transaction in the block. |br|  |br| If transaction X did not create any logs, the entry at result[X] will be null |br|  |br| If transaction X generated N logs, the entry at position result[X] will be an array of N log objects

**Parameters**

.. list-table::
   :widths: 25 75
   :header-rows: 1

   * - Type
     - Description
   * - ``DATA, 32 BYTES``
     - Hash of block at which to retreive data


**Example**

::

   curl -s --data '{"jsonrpc":"2.0","method":"erigon_getLogsByHash","params":["0x2f244c154cbacb0305581295b80efa6dffb0224b60386a5fc6ae9585e2a140c4"],"id":"1"}' -H "Content-Type: application/json" -X POST http://localhost:8545

**Returns**

Array - An array of type Log some of which may be null found in the block. See eth_getFilterChanges

.. list-table::
   :widths: 25 75
   :header-rows: 1

   * - Type
     - Description
   * - ``ARRAY``
     - An array of type Log some of which may be null found in the block. See ``eth_getFilterChanges``

--------------
erigon_issuance
-----------

Returns the total issuance (block reward plus uncle reward) for the given block.

**Parameters**

.. list-table::
   :widths: 25 75
   :header-rows: 1

   * - Type
     - Description
   * - ``QUANTITY | TAG``
     - Integer block number or one of "earliest", "latest" or "pending"


**Example**

::

   curl -s --data '{"jsonrpc":"2.0","method":"erigon_issuance","params":["0x3"],"id":"1"}' -H "Content-Type: application/json" -X POST http://localhost:8545

**Returns**

Object - An object of type Issuance

.. list-table::
   :widths: 15 15 70
   :header-rows: 1

   * - Type
     - Name
     - Description
   * - ``QUANTITY``
     - ``blockReward``
     - The issuance to the miner of the block (includes nephew reward but not transaction fees)
   * - ``QUANTITY``
     - ``uncleReward``
     - The issuance to miners of included uncle (if any)
   * - ``QUANTITY``
     - ``issuance``
     - The sum of blockReward and uncleReward

--------------

debug_storageRangeAt
--------------------

Returns information about a range of storage locations (if any) for the given address.

**Parameters**

.. list-table::
   :widths: 25 75
   :header-rows: 1

   * - Type
     - Description
   * - ``DATA, 32 BYTES``
     - Hash of block at which to retreive data
   * - ``QUANTITY, 8 BYTES``
     - Transaction index in the give block
   * - ``DATA, 20 BYTES``
     - Contract address from which to retreive storage data
   * - ``DATA, 32 BYTES``
     - Storage key to retreive
   * - ``QUANTITY, 8 BYTES``
     - The number of values to retreive


**Example**

::

   curl -s --data '{"jsonrpc":"2.0","method":"debug_storageRangeAt","params":["0xd3f1853788b02e31067f2c6e65cb0ae56729e23e3c92e2393af9396fa182701d",1,"0xb734c74ff4087493373a27834074f80acbd32827","0x00",2],"id":"1"}' -H "Content-Type: application/json" -X POST http://localhost:8545

**Returns**

Object - An object of type StorageRangeResult which is defined as

.. list-table::
   :widths: 15 15 70
   :header-rows: 1

   * - Type
     - Name
     - Description
   * - ``KEY/VALUE``
     - ``pair``
     - A key value pair of the storage location
   * - ``DATA, 32 BYTES``
     - ``nextKey``
     - (optional) Hash pointing to next storage pair or empty

--------------

debug_accountRange
------------------

Returns a range of accounts involved in the given block range

**Parameters**

.. list-table::
   :widths: 25 75
   :header-rows: 1

   * - Type
     - Description
   * - ``QUANTITY | TAG``
     - Integer block number or one of "earliest", "latest" or "pending"
   * - ``DATAARRAY``
     - an array of prefixs against which to match account addresses (report only on accounts addresses that begin with this prefix, default matches all accounts)
   * - ``QUANTITY, 8 BYTES``
     - the maximum number of accounts to retreive
   * - ``BOOLEAN``
     - if true, do not return byte code from the address, if ``false`` return the byte code (if any)
   * - ``BOOLEAN``
     - if true, do not return storage from the address, if ``false`` return storage (if any)
   * - ``BOOLEAN``
     - if true, do not return missing preimages, if ``false`` do return them


**Example**

::

   curl -s --data '{"jsonrpc":"2.0","method":"debug_accountRange","params":["0xaaaaa",[1],1,true,true,true],"id":"1"}' -H "Content-Type: application/json" -X POST http://localhost:8545

**Returns**

Object - An object of type IteratorDump which is defined as

.. list-table::
   :widths: 15 15 70
   :header-rows: 1

   * - Type
     - Name
     - Description
   * - ``STRING``
     - ``root``
     - IteratorDump
   * - ``MAP[COMMON.ADDRESS]DUMPACCOUNT``
     - ``accounts``
     - IteratorDump
   * - ``[]BYTE``
     - ``next``
     - IteratorDump
   * - ``STRING``
     - ``balance``
     - DumpAccount
   * - ``UINT64``
     - ``nonce``
     - DumpAccount
   * - ``STRING``
     - ``root``
     - DumpAccount
   * - ``STRING``
     - ``codeHash``
     - DumpAccount
   * - ``STRING``
     - ``code``
     - DumpAccount
   * - ``MAP[STRING]STRING``
     - ``storage``
     - DumpAccount
   * - ``COMMON.ADDRESS``
     - ``address``
     - (optional) DumpAccount
   * - ``HEXUTIL.BYTES``
     - ``secureKey``
     - DumpAccount

--------------

debug_getModifiedAccountsByNumber
---------------------------------

Returns a list of accounts modified in the given block.

**Parameters**

.. list-table::
   :widths: 25 75
   :header-rows: 1

   * - Type
     - Description
   * - ``QUANTITY | TAG``
     - Integer block number or one of "earliest", "latest" or "pending"
   * - ``QUANTITY | TAG``
     - Integer block number or one of "earliest", "latest" or "pending". Optional, defaults to startNum


**Example**

::

   curl -s --data '{"jsonrpc":"2.0","method":"debug_getModifiedAccountsByNumber","params":["0xccccd","0xcccce"],"id":"1"}' -H "Content-Type: application/json" -X POST http://localhost:8545

**Returns**

.. list-table::
   :widths: 25 75
   :header-rows: 1

   * - Type
     - Description
   * - ``ARRAY OF DATA, 20 BYTES``
     - Array of addresses modifed in the given block range

--------------

debug_getModifiedAccountsByHash
-------------------------------

Returns a list of accounts modified in the given block.

**Parameters**

.. list-table::
   :widths: 25 75
   :header-rows: 1

   * - Type
     - Description
   * - ``DATA, 32 BYTES``
     - the first hash of block at which to retreive data
   * - ``DATA, 32 BYTES``
     - the last hash of block at which to retreive data. Optional, defaults to startHash


**Example**

::

   curl -s --data '{"jsonrpc":"2.0","method":"debug_getModifiedAccountsByHash","params":["0x2a1af018e33bcbd5015c96a356117a5251fcccf94a9c7c8f0148e25fdee37aec","0x4e3d3e7eee350df0ee6e94a44471ee2d22cfb174db89bbf8e6c5f6aef7b360c5"],"id":"1"}' -H "Content-Type: application/json" -X POST http://localhost:8545

**Returns**

.. list-table::
   :widths: 25 75
   :header-rows: 1

   * - Type
     - Description
   * - ``ARRAY OF DATA, 20 BYTES``
     - Array of addresses modifed in the given block range

--------------

debug_traceTransaction
----------------------

Returns Geth style transaction traces.

**Parameters**

.. list-table::
   :widths: 25 75
   :header-rows: 1

   * - Type
     - Description
   * - ``DATA, 32 BYTES``
     - hash of transaction to trace.


**Example**

::

   curl -s --data '{"jsonrpc":"2.0","method":"debug_traceTransaction","params":["0x893c428fed019404f704cf4d9be977ed9ca01050ed93dccdd6c169422155586f"],"id":"1"}' -H "Content-Type: application/json" -X POST http://localhost:8545

**Returns**

.. list-table::
   :widths: 25 75
   :header-rows: 1

   * - Type
     - Description
   * - ``STACK_TRACE``
     - An array of stack traces as per Geth

--------------

eth_accounts
------------

Returns a list of addresses owned by the client.

.. warning::
   This function has been deprecated.

**Parameters**

.. list-table::
   :widths: 25 75
   :header-rows: 1

   * - Type
     - Description
   * - ``NONE``
     - 


**Example**

::

   curl -s --data '{"jsonrpc":"2.0","method":"eth_accounts","params":[],"id":"1"}' -H "Content-Type: application/json" -X POST http://localhost:8545

**Returns**

.. list-table::
   :widths: 25 75
   :header-rows: 1

   * - Type
     - Description
   * - ``ARRAY OF DATA, 20 BYTES``
     - addresses owned by the client

--------------

eth_getCompilers
----------------

Returns a list of available compilers in the client.

.. warning::
   This function has been deprecated.

**Parameters**

.. list-table::
   :widths: 25 75
   :header-rows: 1

   * - Type
     - Description
   * - ``NONE``
     - 


**Example**

::

   curl -s --data '{"jsonrpc":"2.0","method":"eth_getCompilers","params":[],"id":"1"}' -H "Content-Type: application/json" -X POST http://localhost:8545

**Returns**

Array - An array of type String of available compilers

.. list-table::
   :widths: 25 75
   :header-rows: 1

   * - Type
     - Description
   * - ``ARRAY``
     - An array of type String of available compilers

--------------

eth_compileLLL
--------------

Returns compiled LLL code.

.. warning::
   This function has been deprecated.

**Parameters**

.. list-table::
   :widths: 25 75
   :header-rows: 1

   * - Type
     - Description
   * - ``STRING``
     - The source code


**Example**

::

   curl -s --data '{"jsonrpc":"2.0","method":"eth_compileLLL","params":["(returnlll(suicide(caller)))"],"id":"1"}' -H "Content-Type: application/json" -X POST http://localhost:8545

**Returns**

.. list-table::
   :widths: 25 75
   :header-rows: 1

   * - Type
     - Description
   * - ``DATA``
     - The compiled source code

--------------

eth_compileSolidity
-------------------

Returns compiled solidity code.

.. warning::
   This function has been deprecated.

**Parameters**

.. list-table::
   :widths: 25 75
   :header-rows: 1

   * - Type
     - Description
   * - ``STRING``
     - The source code


**Example**

::

   curl -s --data '{"jsonrpc":"2.0","method":"eth_compileSolidity","params":["contracttest{functionmultiply(uinta)returns(uintd){returna*7;}}"],"id":"1"}' -H "Content-Type: application/json" -X POST http://localhost:8545

**Returns**

.. list-table::
   :widths: 25 75
   :header-rows: 1

   * - Type
     - Description
   * - ``DATA``
     - The compiled source code

--------------

eth_compileSerpent
------------------

Returns compiled serpent code.

.. warning::
   This function has been deprecated.

**Parameters**

.. list-table::
   :widths: 25 75
   :header-rows: 1

   * - Type
     - Description
   * - ``STRING``
     - The source code


**Example**

::

   curl -s --data '{"jsonrpc":"2.0","method":"eth_compileSerpent","params":["/*someserpent*/"],"id":"1"}' -H "Content-Type: application/json" -X POST http://localhost:8545

**Returns**

.. list-table::
   :widths: 25 75
   :header-rows: 1

   * - Type
     - Description
   * - ``DATA``
     - The compiled source code

--------------

eth_sign
--------

Calculates an Ethereum specific signature with: sign(keccak256("\x19Ethereum Signed Message:\n" + len(message) + message))).

.. warning::
   This function has been deprecated.

**Parameters**

.. list-table::
   :widths: 25 75
   :header-rows: 1

   * - Type
     - Description
   * - ``DATA, 20 BYTES``
     - address
   * - ``DATA``
     - message to sign


**Example**

::

   curl -s --data '{"jsonrpc":"2.0","method":"eth_sign","params":["0x9b2055d370f73ec7d8a03e965129118dc8f5bf83","0xdeadbeef"],"id":"1"}' -H "Content-Type: application/json" -X POST http://localhost:8545

**Returns**

.. list-table::
   :widths: 25 75
   :header-rows: 1

   * - Type
     - Description
   * - ``DATA``
     - The signature

--------------

db_getString
------------

Returns string from the local database.

.. warning::
   This function has been deprecated.

**Parameters**

.. list-table::
   :widths: 25 75
   :header-rows: 1

   * - Type
     - Description
   * - ``STRING``
     - Database name
   * - ``STRING``
     - Key name


**Example**

::

   curl -s --data '{"jsonrpc":"2.0","method":"db_getString","params":["testDB","myKey"],"id":"1"}' -H "Content-Type: application/json" -X POST http://localhost:8545

**Returns**

.. list-table::
   :widths: 25 75
   :header-rows: 1

   * - Type
     - Description
   * - ``STRING``
     - The previously stored string

--------------

db_putString
------------

Stores a string in the local database.

.. warning::
   This function has been deprecated.

**Parameters**

.. list-table::
   :widths: 25 75
   :header-rows: 1

   * - Type
     - Description
   * - ``STRING``
     - Database name
   * - ``STRING``
     - Key name
   * - ``STRING``
     - String to store


**Example**

::

   curl -s --data '{"jsonrpc":"2.0","method":"db_putString","params":["testDB","myKey","myString"],"id":"1"}' -H "Content-Type: application/json" -X POST http://localhost:8545

**Returns**

.. list-table::
   :widths: 25 75
   :header-rows: 1

   * - Type
     - Description
   * - ``BOOLEAN``
     - ``true`` if the value was stored, ``false`` otherwise

--------------

db_getHex
---------

Returns binary data from the local database.

.. warning::
   This function has been deprecated.

**Parameters**

.. list-table::
   :widths: 25 75
   :header-rows: 1

   * - Type
     - Description
   * - ``STRING``
     - Database name
   * - ``STRING``
     - Key name


**Example**

::

   curl -s --data '{"jsonrpc":"2.0","method":"db_getHex","params":["testDB","myKey"],"id":"1"}' -H "Content-Type: application/json" -X POST http://localhost:8545

**Returns**

.. list-table::
   :widths: 25 75
   :header-rows: 1

   * - Type
     - Description
   * - ``DATA``
     - The previously stored data

--------------

db_putHex
---------

Stores binary data in the local database.

.. warning::
   This function has been deprecated.

**Parameters**

.. list-table::
   :widths: 25 75
   :header-rows: 1

   * - Type
     - Description
   * - ``STRING``
     - Database name
   * - ``STRING``
     - Key name
   * - ``DATA``
     - The data to store


**Example**

::

   curl -s --data '{"jsonrpc":"2.0","method":"db_putHex","params":["testDB","myKey","0x68656c6c6f20776f726c64"],"id":"1"}' -H "Content-Type: application/json" -X POST http://localhost:8545

**Returns**

.. list-table::
   :widths: 25 75
   :header-rows: 1

   * - Type
     - Description
   * - ``BOOLEAN``
     - ``true`` if the value was stored, ``false`` otherwise

--------------
