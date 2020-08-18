package commands

import (
	"context"
	"fmt"
	"github.com/ledgerwatch/turbo-geth/cmd/rpcdaemon/cli"
	"github.com/spf13/cobra"
	"math/big"

	"github.com/ledgerwatch/turbo-geth/common"
	"github.com/ledgerwatch/turbo-geth/consensus"
	"github.com/ledgerwatch/turbo-geth/core/rawdb"
	"github.com/ledgerwatch/turbo-geth/core/state"
	"github.com/ledgerwatch/turbo-geth/core/types"
	"github.com/ledgerwatch/turbo-geth/ethdb"
	"github.com/ledgerwatch/turbo-geth/internal/ethapi"
	"github.com/ledgerwatch/turbo-geth/log"
	"github.com/ledgerwatch/turbo-geth/params"
	"github.com/ledgerwatch/turbo-geth/rpc"
)

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

func (c *powEngine) VerifyHeader(chain consensus.ChainHeaderReader, header *types.Header, seal bool) error {

	panic("must not be called")
}
func (c *powEngine) VerifyHeaders(chain consensus.ChainHeaderReader, headers []*types.Header, seals []bool) (func(), <-chan error) {
	panic("must not be called")
}
func (c *powEngine) VerifyUncles(chain consensus.ChainReader, block *types.Block) error {
	panic("must not be called")
}
func (c *powEngine) VerifySeal(chain consensus.ChainHeaderReader, header *types.Header) error {
	panic("must not be called")
}
func (c *powEngine) Prepare(chain consensus.ChainHeaderReader, header *types.Header) error {
	panic("must not be called")
}
func (c *powEngine) Finalize(chainConfig *params.ChainConfig, header *types.Header, state *state.IntraBlockState, txs []*types.Transaction, uncles []*types.Header) {
	panic("must not be called")
}
func (c *powEngine) FinalizeAndAssemble(chainConfig *params.ChainConfig, header *types.Header, state *state.IntraBlockState, txs []*types.Transaction,
	uncles []*types.Header, receipts []*types.Receipt) (*types.Block, error) {
	panic("must not be called")
}
func (c *powEngine) Seal(_ consensus.Cancel, chain consensus.ChainHeaderReader, block *types.Block, results chan<- consensus.ResultWithContext, stop <-chan struct{}) error {
	panic("must not be called")
}
func (c *powEngine) SealHash(header *types.Header) common.Hash {
	panic("must not be called")
}
func (c *powEngine) CalcDifficulty(chain consensus.ChainHeaderReader, time uint64, parent *types.Header) *big.Int {
	panic("must not be called")
}
func (c *powEngine) APIs(chain consensus.ChainHeaderReader) []rpc.API {
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
		return nil, fmt.Errorf("block not found: %d", number.Int64())
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

func GetAPI(db ethdb.KV, eth ethdb.Backend, enabledApis []string) []rpc.API {
	var rpcAPI []rpc.API

	dbReader := ethdb.NewObjectDatabase(db)
	chainContext := NewChainContext(dbReader)
	apiImpl := NewAPI(db, dbReader, chainContext, eth)
	netImpl := NewNetAPIImpl(eth)
	dbgAPIImpl := NewPrivateDebugAPI(db, dbReader)

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
		case "net":
			rpcAPI = append(rpcAPI, rpc.API{
				Namespace: "net",
				Public:    true,
				Service:   NetAPI(netImpl),
				Version:   "1.0",
			})

		default:
			log.Error("Unrecognised", "api", enabledAPI)
		}
	}
	return rpcAPI
}
