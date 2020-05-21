package commands

import (
	"context"
	"fmt"
	"math/big"
	"strings"

	lru "github.com/hashicorp/golang-lru"
	"github.com/ledgerwatch/turbo-geth/common"
	"github.com/ledgerwatch/turbo-geth/common/hexutil"
	"github.com/ledgerwatch/turbo-geth/consensus"
	"github.com/ledgerwatch/turbo-geth/core"
	"github.com/ledgerwatch/turbo-geth/core/rawdb"
	"github.com/ledgerwatch/turbo-geth/core/state"
	"github.com/ledgerwatch/turbo-geth/core/types"
	"github.com/ledgerwatch/turbo-geth/eth"
	"github.com/ledgerwatch/turbo-geth/ethdb"
	"github.com/ledgerwatch/turbo-geth/ethdb/remote/remotechain"
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
}

// APIImpl is implementation of the EthAPI interface based on remote Db access
type APIImpl struct {
	db           ethdb.KV
	dbReader     ethdb.Getter
	chainContext core.ChainContext
}

// PrivateDebugAPI
type PrivateDebugAPI interface {
	StorageRangeAt(ctx context.Context, blockHash common.Hash, txIndex uint64, contractAddress common.Address, keyStart hexutil.Bytes, maxResult int) (eth.StorageRangeResult, error)
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
	var blockNumber uint64

	if err := api.db.View(ctx, func(tx ethdb.Tx) error {
		var err error
		blockNumber, err = remotechain.ReadLastBlockNumber(tx)
		if err != nil {
			return err
		}
		return nil
	}); err != nil {
		return 0, err
	}

	return hexutil.Uint64(blockNumber), nil
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
	headerCache *lru.Cache // Cache for the most recent block headers

	db rawdb.DatabaseReader
}

func NewChainContext(db rawdb.DatabaseReader) *chainContext {
	headerCache, _ := lru.New(512)
	return &chainContext{
		headerCache: headerCache,
		db:          db,
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
	// Short circuit if the header's already in the cache, retrieve otherwise
	if header, ok := c.headerCache.Get(hash); ok {
		return header.(*types.Header)
	}
	header := rawdb.ReadHeader(c.db, hash, number)
	if header == nil {
		return nil
	}
	// Cache the found header for next time and return
	c.headerCache.Add(hash, header)
	return header
}

func (c *chainContext) Engine() consensus.Engine {
	return &powEngine{}
}

// GetBlockByNumber see https://github.com/ethereum/wiki/wiki/JSON-RPC#eth_getblockbynumber
// see internal/ethapi.PublicBlockChainAPI.GetBlockByNumber
func (api *APIImpl) GetBlockByNumber(ctx context.Context, number rpc.BlockNumber, fullTx bool) (map[string]interface{}, error) {
	var err error
	var block *types.Block
	additionalFields := make(map[string]interface{})

	err = api.db.View(ctx, func(tx ethdb.Tx) error {
		block, err = remotechain.GetBlockByNumber(tx, uint64(number.Int64()))
		if err != nil {
			return err
		}
		additionalFields["totalDifficulty"], err = remotechain.ReadTd(tx, block.Hash(), uint64(number.Int64()))
		if err != nil {
			return err
		}
		return nil
	})
	if err != nil {
		return nil, err
	}

	if block != nil {
		response, err := api.rpcMarshalBlock(block, true, fullTx, additionalFields)

		if err == nil && number == rpc.PendingBlockNumber {
			// Pending blocks need to nil out a few fields
			for _, field := range []string{"hash", "nonce", "miner"} {
				response[field] = nil
			}
		}
		return response, err
	}
	return nil, nil
}

// StorageRangeAt re-implementation of eth/api.go:StorageRangeAt
func (api *PrivateDebugAPIImpl) StorageRangeAt(ctx context.Context, blockHash common.Hash, txIndex uint64, contractAddress common.Address, keyStart hexutil.Bytes, maxResult int) (eth.StorageRangeResult, error) {
	_, _, _, dbstate, err := eth.ComputeTxEnv(ctx, &blockGetter{api.dbReader}, params.MainnetChainConfig, &chainContext{db: api.dbReader}, api.dbReader, blockHash, txIndex)
	if err != nil {
		return eth.StorageRangeResult{}, err
	}

	//dbstate.SetBlockNr(block.NumberU64())
	//statedb.CommitBlock(api.eth.chainConfig.IsEIP158(block.Number()), dbstate)
	return eth.StorageRangeAt(dbstate, contractAddress, keyStart, maxResult)
}

// computeIntraBlockState retrieves the state database associated with a certain block.
// If no state is locally available for the given block, a number of blocks are
// attempted to be reexecuted to generate the desired state.
func (api *PrivateDebugAPIImpl) computeIntraBlockState(block *types.Block) (*state.IntraBlockState, *state.DbState) {
	// If we have the state fully available, use that
	dbstate := state.NewDbState(api.dbReader, block.NumberU64())
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

	db, err := ethdb.NewRemote().Path(cfg.remoteDbAddress).Open(cmd.Context())
	if err != nil {
		log.Error("Could not connect to remoteDb", "error", err)
		return
	}

	var rpcAPI = []rpc.API{}

	dbReader := ethdb.NewRemoteBoltDatabase(db)
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
