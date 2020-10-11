==============
RPCDaemon Code
==============

`web3_`
=======

.. code-block:: go

    func (api *Web3APIImpl) ClientVersion(_ context.Context) (string, error)

ClientVersion returns the node name

.. code-block:: go

    func (api *Web3APIImpl) Sha3(_ context.Context, input hexutil.Bytes) hexutil.Bytes

Sha3 applies the ethereum sha3 implementation on the input.

`eth_`
======

.. code-block:: go

    func NewEthAPI(db ethdb.KV, dbReader ethdb.Database, eth ethdb.Backend, gascap uint64) *APIImpl

EthAPI is the **eth_** container and contains every json rpc that starts with **eth_**. ApiImpl its just the implementation of EthAPI interface.

.. code-block:: go

    func (api *APIImpl) BlockNumber(ctx context.Context) (hexutil.Uint64, error)

BlockNumber returns the latest block number of the chain. (**eth_blockNumber**)

.. code-block:: go

    func (api *APIImpl) Call(ctx context.Context, args ethapi.CallArgs, blockNrOrHash rpc.BlockNumberOrHash, overrides *map[common.Address]ethapi.Account) (hexutil.Bytes, error)

Call implents **eth_call**

.. code-block:: go

    func (api *APIImpl) ChainId(_ context.Context) (hexutil.Uint64, error)

ChainId returns the chain id from the config. **eth_chainId**

.. code-block:: go

    func (api *APIImpl) Coinbase(_ context.Context) (common.Address, error)

Coinbase is the address that mining rewards will be sent to. **eth_coinbase**

.. code-block:: go

    func (api *APIImpl) EstimateGas(ctx context.Context, args ethapi.CallArgs) (hexutil.Uint64, error)

EstimateGas returns an estimate of the amount of gas needed to execute the given transaction against the current pending block. **eth_estimateGas**

.. code-block:: go

    func (api *APIImpl) GetBalance(_ context.Context, address common.Address, blockNrOrHash rpc.BlockNumberOrHash) (*hexutil.Big, error)

GetBalance returns balance of a specific account. **eth_getBalance**

.. code-block:: go

    func (api *APIImpl) GetBlockByHash(ctx context.Context, hash common.Hash, fullTx bool) (map[string]interface{}, error)

GetBlockByHash returns the block assigned to a given hash. **eth_getBlockByHash**

.. code-block:: go

    func (api *APIImpl) GetBlockByNumber(ctx context.Context, number rpc.BlockNumber, fullTx bool) (map[string]interface{}, error)

GetBlockByNumber returns the block of a certainNumber. **eth_getBlockByNumber**

.. code-block:: go

    func (api *APIImpl) GetBlockTransactionCountByHash(ctx context.Context, blockHash common.Hash) (*hexutil.Uint, error)

GetBlockTransactionCountByHash returns the number of transactions in the block. **eth_getBlockTransactionCountByHash**

.. code-block:: go

    func (api *APIImpl) GetBlockTransactionCountByNumber(ctx context.Context, blockNr rpc.BlockNumber) (*hexutil.Uint, error)

GetBlockTransactionCountByNumber returns the number of transactions in the block.

**eth_getBlockTransactionCountByNumber**

.. code-block:: go

    func (api *APIImpl) GetCode(ctx context.Context, address common.Address, blockNrOrHash rpc.BlockNumberOrHash) (hexutil.Bytes, error)

GetCode returns the code stored at the given address in the state for the given block number. **eth_getCode**

.. code-block:: go

    func (api *APIImpl) GetHeaderByHash(_ context.Context, hash common.Hash) (*types.Header, error)

GetHeaderByHash returns a block's header by hash. **eth_getHeaderByHash**

.. code-block:: go

    func (api *APIImpl) GetHeaderByNumber(_ context.Context, number rpc.BlockNumber) (*types.Header, error)

GetHeaderByNumber returns a block's header by number. **eth_getHeaderByNumber**

.. code-block:: go

    func (api *APIImpl) GetLogs(ctx context.Context, crit filters.FilterCriteria) ([]*types.Log, error)

GetLogs returns logs matching the given argument that are stored within the state. **eth_getLogs**

.. code-block:: go

    func (api *APIImpl) GetLogsByHash(ctx context.Context, hash common.Hash) ([][]*types.Log, error)

GetLogsByHash non-standard RPC that returns all logs in a block. **eth_getLogsByHash**

.. code-block:: go

    func (api *APIImpl) GetStorageAt(ctx context.Context, address common.Address, index string, blockNrOrHash rpc.BlockNumberOrHash) (string, error)

GetStorageAt returns a 32-byte long, zero-left-padded value at storage location 'index' of address 'address'. Returns '0x' if no value. **eth_getStorageAt**

.. code-block:: go

    func (api *APIImpl) GetTransactionByBlockHashAndIndex(ctx context.Context, blockHash common.Hash, txIndex hexutil.Uint64) (*RPCTransaction, error)

GetTransactionByBlockHashAndIndex returns the transaction for the given block hash and index. **eth_getTransactionByBlockHashAndIndex**

.. code-block:: go

    func (api *APIImpl) GetTransactionByBlockNumberAndIndex(ctx context.Context, blockNr rpc.BlockNumber, txIndex hexutil.Uint) (*RPCTransaction, error)

GetTransactionByBlockNumberAndIndex returns the transaction for the given block number and index. **eth_getTransactionByBlockNumberAndIndex**

.. code-block:: go

    func (api *APIImpl) GetTransactionByHash(ctx context.Context, hash common.Hash) (*RPCTransaction, error)

GetTransactionByHash returns the transaction for the given hash. **eth_getTransactionHash**

.. code-block:: go

    func (api *APIImpl) GetTransactionCount(ctx context.Context, address common.Address, blockNrOrHash rpc.BlockNumberOrHash) (*hexutil.Uint64, error)

GetTransactionCount returns the number of transactions the given address has sent for the given block number. **eth_getTransactionCount**

.. code-block:: go

    func (api *APIImpl) GetTransactionReceipt(ctx context.Context, hash common.Hash) (map[string]interface{}, error)

GetTransactionReceipt returns the transaction receipt of a transaction. **eth_getTransactionReceipt**

.. code-block:: go

    func (api *APIImpl) GetUncleByBlockHashAndIndex(ctx context.Context, hash common.Hash, index hexutil.Uint) (map[string]interface{}, error)

GetUncleByBlockHashAndIndex returns the uncle block for the given block hash and index. When fullTx is true all transactions in the block are returned in full detail, otherwise only the transaction hash is returned. **eth_getUncleByBlockHashAndIndex**

.. code-block:: go

    func (api *APIImpl) GetUncleByBlockNumberAndIndex(ctx context.Context, number rpc.BlockNumber, index hexutil.Uint) (map[string]interface{}, error)

GetUncleByBlockNumberAndIndex returns the uncle block for the given block hash and index. When fullTx is true all transactions in the block are returned in full detail, otherwise only the transaction hash is returned. **eth_getUncleByBlockHashAndIndex**

.. code-block:: go

    func (api *APIImpl) GetUncleCountByBlockHash(ctx context.Context, hash common.Hash) *hexutil.Uint

GetUncleCountByBlockHash returns number of uncles in the block for the given block hash. **eth_getUncleCountByBlockHash**

.. code-block:: go

    func (api *APIImpl) GetUncleCountByBlockNumber(ctx context.Context, number rpc.BlockNumber) *hexutil.Uint

GetUncleCountByBlockNumber returns number of uncles in the block for the given block number
**eth_getUncleCountByBlockNumber**


.. code-block:: go

    func (api *APIImpl) SendRawTransaction(_ context.Context, encodedTx hexutil.Bytes) (common.Hash, error)

SendRawTransaction send a raw transaction.**eth_sendRawTransaction**


.. code-block:: go

    func (api *APIImpl) Syncing(ctx context.Context) (interface{}, error)

Syncing - we can return the progress of the very first stage as the highest block, and then the progress of the very last stage as the current block. **eth_syncing**

`net_`
======

.. code-block:: go

    func NewNetAPIImpl(eth ethdb.Backend) *NetAPIImpl

NewNetAPIImpl returns NetAPIImplImpl instance

.. code-block:: go

    func (api *NetAPIImpl) Listening(_ context.Context) (bool, error)

Listening implements RPC call for **net_listening**.

.. code-block:: go

    func (api *NetAPIImpl) PeerCount(_ context.Context) (hexutil.Uint, error)

PeerCount implements RPC call for **net_peerCount**

.. code-block:: go

    func (api *NetAPIImpl) Version(_ context.Context) (string, error)

Version implements RPC call for **net_version**

`trace_`
========

.. code-block:: go

    func NewTraceAPI(db ethdb.KV, dbReader ethdb.Getter, cfg *cli.Flags) *TraceAPIImpl

implementation of the parity traces based.

.. code-block:: go

    func (api *TraceAPIImpl) Block(ctx context.Context, blockNr rpc.BlockNumber) (ParityTraces, error)

Implements parity **trace_block**

.. code-block:: go

    func (api *TraceAPIImpl) Call(ctx context.Context, call CallParam, blockNr rpc.BlockNumber) ([]interface{}, error)

Call Implements **trace_call**

.. code-block:: go

    func (api *TraceAPIImpl) CallMany(ctx context.Context, calls CallParams) ([]interface{}, error)

CallMany Implements **trace_call**

.. code-block:: go

    func (api *TraceAPIImpl) Filter(ctx context.Context, req TraceFilterRequest) (ParityTraces, error)

Filter Implements **trace_filter**

Tutorial: Build add personalized methods to daemon
=====================================================

`TODO`
