package arbitrum

import (
	"context"
	"fmt"

	"github.com/erigontech/erigon-lib/common"
	"github.com/erigontech/erigon-lib/kv"
	"github.com/erigontech/erigon/core"
	"github.com/erigontech/erigon/core/state"
	"github.com/erigontech/erigon/core/types"
	"github.com/erigontech/erigon/core/vm"
	"github.com/erigontech/erigon/eth/tracers"
	"github.com/pkg/errors"
)

var (
	ErrDepthLimitExceeded = errors.New("state recreation l2 gas depth limit exceeded")
)

type StateReleaseFunc tracers.StateReleaseFunc

var NoopStateRelease StateReleaseFunc = func() {}

type StateBuildingLogFunction func(targetHeader, header *types.Header, hasState bool)
type StateForHeaderFunction func(header *types.Header) (*state.IntraBlockState, StateReleaseFunc, error)

// finds last available state and header checking it first for targetHeader then looking backwards
// if maxDepthInL2Gas is positive, it constitutes a limit for cumulative l2 gas used of the traversed blocks
// else if maxDepthInL2Gas is -1, the traversal depth is not limited
// otherwise only targetHeader state is checked and no search is performed
func FindLastAvailableState(ctx context.Context, bc core.BlockChain, stateFor StateForHeaderFunction, targetHeader *types.Header, logFunc StateBuildingLogFunction, maxDepthInL2Gas int64) (*state.IntraBlockState, *types.Header, StateReleaseFunc, error) {
	genesis := bc.Config().ArbitrumChainParams.GenesisBlockNum
	currentHeader := targetHeader
	var statedb *state.IntraBlockState
	var err error
	var l2GasUsed uint64
	release := NoopStateRelease
	for ctx.Err() == nil {
		lastHeader := currentHeader
		statedb, release, err = stateFor(currentHeader)
		if err == nil {
			break
		}
		if maxDepthInL2Gas > 0 {
			receipts := bc.GetReceiptsByHash(currentHeader.Hash())
			if receipts == nil {
				return nil, lastHeader, nil, fmt.Errorf("failed to get receipts for hash %v", currentHeader.Hash())
			}
			for _, receipt := range receipts {
				l2GasUsed += receipt.GasUsed - receipt.GasUsedForL1
			}
			if l2GasUsed > uint64(maxDepthInL2Gas) {
				return nil, lastHeader, nil, ErrDepthLimitExceeded
			}
		} else if maxDepthInL2Gas != InfiniteMaxRecreateStateDepth {
			return nil, lastHeader, nil, err
		}
		if logFunc != nil {
			logFunc(targetHeader, currentHeader, false)
		}
		if currentHeader.Number.Uint64() <= genesis {
			return nil, lastHeader, nil, errors.Wrap(err, fmt.Sprintf("moved beyond genesis looking for state %d, genesis %d", targetHeader.Number.Uint64(), genesis))
		}
		var tx kv.Tx // TODO
		currentHeader, err = bc.Header(ctx, tx, currentHeader.ParentHash, currentHeader.Number.Uint64()-1)
		if currentHeader == nil || err != nil {
			return nil, lastHeader, nil, fmt.Errorf("chain doesn't contain parent of block %d hash %v", lastHeader.Number, lastHeader.Hash())
		}
	}
	return statedb, currentHeader, release, ctx.Err()
}

func AdvanceStateByBlock(ctx context.Context, bc core.BlockChain, state *state.IntraBlockState, targetHeader *types.Header, blockToRecreate uint64, prevBlockHash common.Hash, logFunc StateBuildingLogFunction) (*state.IntraBlockState, *types.Block, error) {
	var tx kv.Tx // TODO
	block, err := bc.BlockByNumber(ctx, tx, blockToRecreate)
	if block == nil || err != nil {
		return nil, nil, fmt.Errorf("block not found while recreating: %d", blockToRecreate)
	}
	if block.ParentHash() != prevBlockHash {
		return nil, nil, fmt.Errorf("reorg detected: number %d expectedPrev: %v foundPrev: %v", blockToRecreate, prevBlockHash, block.ParentHash())
	}
	if logFunc != nil {
		logFunc(targetHeader, block.Header(), true)
	}
	_, _, _, err = bc.Processor().Process(block, state, vm.Config{})
	if err != nil {
		return nil, nil, fmt.Errorf("failed recreating state for block %d : %w", blockToRecreate, err)
	}
	return state, block, nil
}

func AdvanceStateUpToBlock(ctx context.Context, bc core.BlockChain, state *state.IntraBlockState, targetHeader *types.Header, lastAvailableHeader *types.Header, logFunc StateBuildingLogFunction) (*state.IntraBlockState, error) {
	returnedBlockNumber := targetHeader.Number.Uint64()
	blockToRecreate := lastAvailableHeader.Number.Uint64() + 1
	prevHash := lastAvailableHeader.Hash()
	for ctx.Err() == nil {
		ibs, block, err := AdvanceStateByBlock(ctx, bc, state, targetHeader, blockToRecreate, prevHash, logFunc)
		if err != nil {
			return nil, err
		}
		prevHash = block.Hash()
		if blockToRecreate >= returnedBlockNumber {
			if block.Hash() != targetHeader.Hash() {
				return nil, fmt.Errorf("blockHash doesn't match when recreating number: %d expected: %v got: %v", blockToRecreate, targetHeader.Hash(), block.Hash())
			}
			return ibs, nil
		}
		blockToRecreate++
	}
	return nil, ctx.Err()
}
