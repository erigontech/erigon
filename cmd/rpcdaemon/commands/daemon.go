package commands

import (
	"context"
	"fmt"
	"math/big"
	"strings"

	"github.com/ledgerwatch/turbo-geth/common"
	"github.com/ledgerwatch/turbo-geth/common/hexutil"
	"github.com/ledgerwatch/turbo-geth/consensus"
	"github.com/ledgerwatch/turbo-geth/core"
	"github.com/ledgerwatch/turbo-geth/core/rawdb"
	"github.com/ledgerwatch/turbo-geth/core/state"
	"github.com/ledgerwatch/turbo-geth/core/types"
	"github.com/ledgerwatch/turbo-geth/core/vm"
	"github.com/ledgerwatch/turbo-geth/eth"
	"github.com/ledgerwatch/turbo-geth/ethdb"
	"github.com/ledgerwatch/turbo-geth/internal/ethapi"
	"github.com/ledgerwatch/turbo-geth/log"
	"github.com/ledgerwatch/turbo-geth/node"
	"github.com/ledgerwatch/turbo-geth/params"
	"github.com/ledgerwatch/turbo-geth/rpc"
	"github.com/spf13/cobra"
)

// splitAndTrim splits input separated by a comma
// and trims excessive white space from the substrings.
func splitAndTrim(input string) []string {
	result := strings.Split(input, ",")
	for i, r := range result {
		result[i] = strings.TrimSpace(r)
	}
	return result
}

// EthAPI is a collection of functions that are exposed in the
type EthAPI interface {
	BlockNumber(ctx context.Context) (hexutil.Uint64, error)
	GetBlockByNumber(ctx context.Context, number rpc.BlockNumber, fullTx bool) (map[string]interface{}, error)
	GetBalance(_ context.Context, address common.Address, blockNrOrHash rpc.BlockNumberOrHash) (*hexutil.Big, error)
	GetTransactionReceipt(ctx context.Context, hash common.Hash) (map[string]interface{}, error)
}

// APIImpl is implementation of the EthAPI interface based on remote Db access
type APIImpl struct {
	db           ethdb.KV
	dbReader     ethdb.Getter
	chainContext core.ChainContext
}

// PrivateDebugAPI
type PrivateDebugAPI interface {
	StorageRangeAt(ctx context.Context, blockHash common.Hash, txIndex uint64, contractAddress common.Address, keyStart hexutil.Bytes, maxResult int) (StorageRangeResult, error)
	TraceTransaction(ctx context.Context, hash common.Hash, config *eth.TraceConfig) (interface{}, error)
	GetModifiedAccountsByNumber(ctx context.Context, startNum rpc.BlockNumber, endNum *rpc.BlockNumber) ([]common.Address, error)
	GetModifiedAccountsByHash(_ context.Context, startHash common.Hash, endHash *common.Hash) ([]common.Address, error)
}

// APIImpl is implementation of the EthAPI interface based on remote Db access
type PrivateDebugAPIImpl struct {
	db           ethdb.KV
	dbReader     ethdb.Getter
	chainContext core.ChainContext
}

// NewAPI returns APIImpl instance
func NewAPI(db ethdb.KV, dbReader ethdb.Getter, chainContext core.ChainContext) *APIImpl {
	return &APIImpl{
		db:           db,
		dbReader:     dbReader,
		chainContext: chainContext,
	}
}

// NewPrivateDebugAPI returns PrivateDebugAPIImpl instance
func NewPrivateDebugAPI(db ethdb.KV, dbReader ethdb.Getter, chainContext core.ChainContext) *PrivateDebugAPIImpl {
	return &PrivateDebugAPIImpl{
		db:           db,
		dbReader:     dbReader,
		chainContext: chainContext,
	}
}

func (api *APIImpl) BlockNumber(ctx context.Context) (hexutil.Uint64, error) {
	blockNumber, err := rawdb.ReadLastBlockNumber(api.dbReader)
	if err != nil {
		return 0, err
	}

	return hexutil.Uint64(blockNumber), nil
}

func getReceiptsByReApplyingTransactions(db ethdb.Database, chainConfig *params.ChainConfig, block *types.Block, number uint64) (types.Receipts, error) {
	var receipts types.Receipts
	batch := db.NewBatch()
	defer batch.Rollback()
	core.UsePlainStateExecution = true
	stateReader := state.NewPlainStateReader(batch)
	stateWriter := state.NewPlainStateWriter(batch, block.NumberU64())

	bc := &chainContext{db: db}
	// where the magic happens
	receipts, err := core.ExecuteBlockEphemerally(chainConfig, &vm.Config{}, bc, bc.Engine(), block, stateReader, stateWriter, nil)
	if err != nil {
		return nil, err
	}
	return receipts, nil
}

func (api *APIImpl) GetTransactionReceipt(ctx context.Context, hash common.Hash) (map[string]interface{}, error) {
	// Retrieve the transaction and assemble its EVM context
	tx, blockHash, blockNumber, txIndex := rawdb.ReadTransaction(api.dbReader, hash)
	if tx == nil {
		return nil, fmt.Errorf("transaction %#x not found", hash)
	}

	bc := &blockGetter{api.dbReader}
	msg, _, ibs, dbstate, err := ComputeTxEnv(ctx, bc, params.MainnetChainConfig, &chainContext{db: api.dbReader}, api.db, blockHash, txIndex, nil)
	if err != nil {
		return nil, err
	}

	//vmenv := vm.NewEVM(vmctx, ibs, params.MainnetChainConfig, vm.Config{}, nil)
	gp := new(core.GasPool).AddGas(msg.Gas())
	//result, err := core.ApplyMessage(vmenv, msg, gp)
	//if err == nil {
	//	return nil, err
	//}
	//if result == nil {
	//	return nil, nil
	//}
	//// Update the state with pending changes
	//if err = ibs.FinalizeTx(ctx, dbstate); err != nil {
	//	return nil, err
	//}
	//
	var usedGas = new(uint64)
	//*usedGas += result.UsedGas
	//
	//// Create a new receipt for the transaction, storing the intermediate root and gas used by the tx
	//// based on the eip phase, we're passing whether the root touch-delete accounts.
	//receipt := types.NewReceipt(result.Failed(), *usedGas)
	//receipt.TxHash = tx.Hash()
	//receipt.GasUsed = result.UsedGas
	//// if the transaction created a contract, store the creation address in the receipt.
	//if msg.To() == nil {
	//	receipt.ContractAddress = crypto.CreateAddress(vmenv.Context.Origin, tx.Nonce())
	//}
	//// Set the receipt logs and create a bloom for filtering
	//receipt.Logs = ibs.GetLogs(tx.Hash())
	//receipt.Bloom = types.CreateBloom(types.Receipts{receipt})

	cc := &chainContext{api.dbReader}
	header := rawdb.ReadHeader(api.dbReader, blockHash, blockNumber)
	receipt, err := core.ApplyTransaction(params.MainnetChainConfig, cc, nil, gp, ibs, dbstate, header, tx, usedGas, vm.Config{}, nil)
	if err != nil {
		return nil, err
	}

	var signer types.Signer = types.FrontierSigner{}
	if tx.Protected() {
		signer = types.NewEIP155Signer(tx.ChainID().ToBig())
	}
	from, _ := types.Sender(signer, tx)

	// Fill in the derived information in the logs
	if receipt.Logs != nil {
		for i, log := range receipt.Logs {
			log.BlockNumber = blockNumber
			log.TxHash = hash
			log.TxIndex = uint(txIndex)
			log.BlockHash = blockHash
			log.Index = uint(i)
		}
	}
	// Now reconstruct the bloom filter
	receipt.Bloom = types.CreateBloom(types.Receipts{receipt})
	fields := map[string]interface{}{
		"blockHash":         blockHash,
		"blockNumber":       hexutil.Uint64(blockNumber),
		"transactionHash":   hash,
		"transactionIndex":  hexutil.Uint64(txIndex),
		"from":              from,
		"to":                tx.To(),
		"gasUsed":           hexutil.Uint64(receipt.GasUsed),
		"cumulativeGasUsed": hexutil.Uint64(receipt.CumulativeGasUsed),
		"contractAddress":   nil,
		"logs":              receipt.Logs,
		"logsBloom":         receipt.Bloom,
	}

	// Assign receipt status or post state.
	if len(receipt.PostState) > 0 {
		fields["root"] = hexutil.Bytes(receipt.PostState)
	} else {
		fields["status"] = hexutil.Uint(receipt.Status)
	}
	if receipt.Logs == nil {
		fields["logs"] = [][]*types.Log{}
	}
	// If the ContractAddress is 20 0x0 bytes, assume it is not a contract creation
	if receipt.ContractAddress != (common.Address{}) {
		fields["contractAddress"] = receipt.ContractAddress
	}
	return fields, nil
}

type blockGetter struct {
	dbReader rawdb.DatabaseReader
}

func (g *blockGetter) GetBlockByHash(hash common.Hash) *types.Block {
	return rawdb.ReadBlockByHash(g.dbReader, hash)

}

func (g *blockGetter) GetBlock(hash common.Hash, number uint64) *types.Block {
	return rawdb.ReadBlock(g.dbReader, hash, number)
}

type chainContext struct {
	db rawdb.DatabaseReader
}

func NewChainContext(db rawdb.DatabaseReader) *chainContext {
	return &chainContext{
		db: db,
	}

}

type powEngine struct {
}

func (c *powEngine) VerifyHeader(chain consensus.ChainReader, header *types.Header, seal bool) error {

	panic("must not be called")
}
func (c *powEngine) VerifyHeaders(chain consensus.ChainReader, headers []*types.Header, seals []bool) (chan<- struct{}, <-chan error) {
	panic("must not be called")
}
func (c *powEngine) VerifyUncles(chain consensus.ChainReader, block *types.Block) error {
	panic("must not be called")
}
func (c *powEngine) VerifySeal(chain consensus.ChainReader, header *types.Header) error {
	panic("must not be called")
}
func (c *powEngine) Prepare(chain consensus.ChainReader, header *types.Header) error {
	panic("must not be called")
}
func (c *powEngine) Finalize(chainConfig *params.ChainConfig, header *types.Header, state *state.IntraBlockState, txs []*types.Transaction, uncles []*types.Header) {
	panic("must not be called")
}
func (c *powEngine) FinalizeAndAssemble(chainConfig *params.ChainConfig, header *types.Header, state *state.IntraBlockState, txs []*types.Transaction,
	uncles []*types.Header, receipts []*types.Receipt) (*types.Block, error) {
	panic("must not be called")
}
func (c *powEngine) Seal(_ consensus.Cancel, chain consensus.ChainReader, block *types.Block, results chan<- consensus.ResultWithContext, stop <-chan struct{}) error {
	panic("must not be called")
}
func (c *powEngine) SealHash(header *types.Header) common.Hash {
	panic("must not be called")
}
func (c *powEngine) CalcDifficulty(chain consensus.ChainReader, time uint64, parent *types.Header) *big.Int {
	panic("must not be called")
}
func (c *powEngine) APIs(chain consensus.ChainReader) []rpc.API {
	panic("must not be called")
}

func (c *powEngine) Close() error {
	panic("must not be called")
}

func (c *powEngine) Author(header *types.Header) (common.Address, error) {
	return header.Coinbase, nil
}

func (c *chainContext) GetHeader(hash common.Hash, number uint64) *types.Header {
	return rawdb.ReadHeader(c.db, hash, number)
}

func (c *chainContext) Engine() consensus.Engine {
	return &powEngine{}
}

// GetBlockByNumber see https://github.com/ethereum/wiki/wiki/JSON-RPC#eth_getblockbynumber
// see internal/ethapi.PublicBlockChainAPI.GetBlockByNumber
func (api *APIImpl) GetBlockByNumber(ctx context.Context, number rpc.BlockNumber, fullTx bool) (map[string]interface{}, error) {
	additionalFields := make(map[string]interface{})

	block := rawdb.ReadBlockByNumber(api.dbReader, uint64(number.Int64()))
	if block == nil {
		return nil, nil
	}

	additionalFields["totalDifficulty"] = rawdb.ReadTd(api.dbReader, block.Hash(), uint64(number.Int64()))
	response, err := api.rpcMarshalBlock(block, true, fullTx, additionalFields)

	if err == nil && number == rpc.PendingBlockNumber {
		// Pending blocks need to nil out a few fields
		for _, field := range []string{"hash", "nonce", "miner"} {
			response[field] = nil
		}
	}
	return response, err
}

// StorageRangeAt re-implementation of eth/api.go:StorageRangeAt
func (api *PrivateDebugAPIImpl) StorageRangeAt(ctx context.Context, blockHash common.Hash, txIndex uint64, contractAddress common.Address, keyStart hexutil.Bytes, maxResult int) (StorageRangeResult, error) {
	_, _, _, stateReader, err := ComputeTxEnv(ctx, &blockGetter{api.dbReader}, params.MainnetChainConfig, &chainContext{db: api.dbReader}, api.db, blockHash, txIndex, nil)
	if err != nil {
		return StorageRangeResult{}, err
	}
	return StorageRangeAt(stateReader, contractAddress, keyStart, maxResult)
}

// computeIntraBlockState retrieves the state database associated with a certain block.
// If no state is locally available for the given block, a number of blocks are
// attempted to be reexecuted to generate the desired state.
func (api *PrivateDebugAPIImpl) computeIntraBlockState(block *types.Block) (*state.IntraBlockState, *state.DbState) {
	// If we have the state fully available, use that
	dbstate := state.NewDbState(api.db, block.NumberU64())
	statedb := state.New(dbstate)
	return statedb, dbstate
}

// rpcMarshalBlock reimplementation of ethapi.rpcMarshalBlock
func (api *APIImpl) rpcMarshalBlock(b *types.Block, inclTx bool, fullTx bool, additional map[string]interface{}) (map[string]interface{}, error) {
	fields, err := ethapi.RPCMarshalBlock(b, inclTx, fullTx)
	if err != nil {
		return nil, err
	}

	for k, v := range additional {
		fields[k] = v
	}

	return fields, err
}

func daemon(cmd *cobra.Command, cfg Config) {
	vhosts := splitAndTrim(cfg.rpcVirtualHost)
	cors := splitAndTrim(cfg.rpcCORSDomain)
	enabledApis := splitAndTrim(cfg.rpcAPI)

	var db ethdb.KV
	var err error
	if cfg.remoteDbAddress != "" {
		db, err = ethdb.NewRemote().Path(cfg.remoteDbAddress).Open()
	} else if cfg.chaindata != "" {
		if database, errOpen := ethdb.Open(cfg.chaindata); errOpen == nil {
			db = database.KV()
		} else {
			err = errOpen
		}
	} else {
		err = fmt.Errorf("either remote db or bolt db must be specified")
	}
	if err != nil {
		log.Error("Could not connect to remoteDb", "error", err)
		return
	}

	var rpcAPI = []rpc.API{}

	dbReader := ethdb.NewObjectDatabase(db)
	chainContext := NewChainContext(dbReader)
	apiImpl := NewAPI(db, dbReader, chainContext)
	dbgAPIImpl := NewPrivateDebugAPI(db, dbReader, chainContext)

	for _, enabledAPI := range enabledApis {
		switch enabledAPI {
		case "eth":
			rpcAPI = append(rpcAPI, rpc.API{
				Namespace: "eth",
				Public:    true,
				Service:   EthAPI(apiImpl),
				Version:   "1.0",
			})
		case "debug":
			rpcAPI = append(rpcAPI, rpc.API{
				Namespace: "debug",
				Public:    true,
				Service:   PrivateDebugAPI(dbgAPIImpl),
				Version:   "1.0",
			})

		default:
			log.Error("Unrecognised", "api", enabledAPI)
		}
	}
	httpEndpoint := fmt.Sprintf("%s:%d", cfg.rpcListenAddress, cfg.rpcPort)

	// register apis and create handler stack
	srv := rpc.NewServer()
	err = node.RegisterApisFromWhitelist(rpcAPI, enabledApis, srv, false)
	if err != nil {
		log.Error("Could not start register RPC apis", "error", err)
		return
	}
	handler := node.NewHTTPHandlerStack(srv, cors, vhosts)

	listener, _, err := node.StartHTTPEndpoint(httpEndpoint, rpc.DefaultHTTPTimeouts, handler)
	if err != nil {
		log.Error("Could not start RPC api", "error", err)
		return
	}
	extapiURL := fmt.Sprintf("http://%s", httpEndpoint)
	log.Info("HTTP endpoint opened", "url", extapiURL)

	defer func() {
		listener.Close()
		log.Info("HTTP endpoint closed", "url", httpEndpoint)
	}()

	sig := <-cmd.Context().Done()
	log.Info("Exiting...", "signal", sig)
}
