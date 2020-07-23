package commands

import (
	"context"
	"fmt"
	"github.com/ledgerwatch/turbo-geth/common"
	"github.com/ledgerwatch/turbo-geth/common/hexutil"
	"github.com/ledgerwatch/turbo-geth/core"
	"github.com/ledgerwatch/turbo-geth/core/rawdb"
	"github.com/ledgerwatch/turbo-geth/core/state"
	"github.com/ledgerwatch/turbo-geth/core/types"
	"github.com/ledgerwatch/turbo-geth/eth"
	"github.com/ledgerwatch/turbo-geth/ethdb"
	"github.com/ledgerwatch/turbo-geth/params"
	"github.com/ledgerwatch/turbo-geth/rpc"
)

// PrivateDebugAPI
type PrivateDebugAPI interface {
	StorageRangeAt(ctx context.Context, blockHash common.Hash, txIndex uint64, contractAddress common.Address, keyStart hexutil.Bytes, maxResult int) (StorageRangeResult, error)
	TraceTransaction(ctx context.Context, hash common.Hash, config *eth.TraceConfig) (interface{}, error)
	AccountRange(ctx context.Context, blockNrOrHash rpc.BlockNumberOrHash, start []byte, maxResults int, nocode, nostorage, incompletes bool) (state.IteratorDump, error)
}

// APIImpl is implementation of the EthAPI interface based on remote Db access
type PrivateDebugAPIImpl struct {
	db           ethdb.KV
	dbReader     ethdb.Getter
	chainContext core.ChainContext
}

// NewPrivateDebugAPI returns PrivateDebugAPIImpl instance
func NewPrivateDebugAPI(db ethdb.KV, dbReader ethdb.Getter, chainContext core.ChainContext) *PrivateDebugAPIImpl {
	return &PrivateDebugAPIImpl{
		db:           db,
		dbReader:     dbReader,
		chainContext: chainContext,
	}
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



// AccountRange re-implementation of eth/api.go:AccountRange
func (api *PrivateDebugAPIImpl) AccountRange(ctx context.Context, blockNrOrHash rpc.BlockNumberOrHash, start []byte, maxResults int, nocode, nostorage, incompletes bool) (state.IteratorDump, error) {
	var blockNumber uint64
	fmt.Println(blockNrOrHash)
	if number, ok := blockNrOrHash.Number(); ok {

		if number == rpc.PendingBlockNumber {
			return state.IteratorDump{}, fmt.Errorf("accountRange for pending block not supported")
		} else {

			if number == rpc.LatestBlockNumber {

			} else {
				blockNumber = uint64(number)
			}
		}
	} else if hash, ok := blockNrOrHash.Hash(); ok {
		block := rawdb.ReadBlockByHash(api.dbReader,hash)
		if block == nil {
			return state.IteratorDump{}, fmt.Errorf("block %s not found", hash.Hex())
		}
		blockNumber = block.NumberU64()
	}

	//if maxResults > state.AccountRangeMaxResults || maxResults <= 0 {
	//	maxResults = AccountRangeMaxResults
	//}

	fmt.Println("bn", blockNumber, maxResults)
	fmt.Println("last block", )
	dumper := state.NewDumper(api.db, blockNumber)
	res,err:=dumper.IteratorDump(nocode, nostorage, incompletes, start, maxResults)
	if err!=nil {
		return state.IteratorDump{}, err
	}
	//hash:=rawdb.ReadCanonicalHash(api.dbReader, blockNumber)
	//if hash!=(common.Hash{}) {
	//	header:=rawdb.ReadHeader(api.dbReader,hash, blockNumber)
	//	if header!=nil {
	//		res.Root=header.Root.String()
	//	}
	//}
	return res, err
}
