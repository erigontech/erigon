package commands

import (
	"context"
	"fmt"
	"time"

	"github.com/ledgerwatch/turbo-geth/common"
	"github.com/ledgerwatch/turbo-geth/core"
	"github.com/ledgerwatch/turbo-geth/core/rawdb"
	"github.com/ledgerwatch/turbo-geth/core/state"
	"github.com/ledgerwatch/turbo-geth/core/types"
	"github.com/ledgerwatch/turbo-geth/core/vm"
	"github.com/ledgerwatch/turbo-geth/eth"
	"github.com/ledgerwatch/turbo-geth/ethdb"
	"github.com/ledgerwatch/turbo-geth/params"
)

const (
	// defaultTraceTimeout is the amount of time a single transaction can execute
	// by default before being forcefully aborted.
	defaultTraceTimeout = 5 * time.Second
)

// TraceTransaction returns the structured logs created during the execution of EVM
// and returns them as a JSON object.
func (api *PrivateDebugAPIImpl) TraceTransaction(ctx context.Context, hash common.Hash, config *eth.TraceConfig) (interface{}, error) {
	// Retrieve the transaction and assemble its EVM context
	tx, blockHash, _, txIndex := rawdb.ReadTransaction(api.dbReader, hash)
	if tx == nil {
		return nil, fmt.Errorf("transaction %#x not found", hash)
	}
	msg, vmctx, ibs, _, err := eth.ComputeTxEnv(ctx, &blockGetter{api.dbReader}, params.MainnetChainConfig, &chainContext{db: api.dbReader}, api.db, blockHash, txIndex)
	if err != nil {
		return nil, err
	}
	// Trace the transaction and return
	return eth.TraceTx(ctx, msg, vmctx, ibs, config)
}

// computeTxEnv returns the execution environment of a certain transaction.
func ComputeTxEnv(ctx context.Context, blockGetter BlockGetter, cfg *params.ChainConfig, chain core.ChainContext, chainKV ethdb.KV, blockHash common.Hash, txIndex uint64) (core.Message, vm.Context, *state.IntraBlockState, *StateReader, error) {
	// Create the parent state database
	block := blockGetter.GetBlockByHash(blockHash)
	if block == nil {
		return nil, vm.Context{}, nil, nil, fmt.Errorf("block %x not found", blockHash)
	}
	parent := blockGetter.GetBlock(block.ParentHash(), block.NumberU64()-1)
	if parent == nil {
		return nil, vm.Context{}, nil, nil, fmt.Errorf("parent %x not found", block.ParentHash())
	}

	statedb, reader := ComputeIntraBlockState(chainKV, parent)

	if txIndex == 0 && len(block.Transactions()) == 0 {
		return nil, vm.Context{}, statedb, reader, nil
	}
	// Recompute transactions up to the target index.
	signer := types.MakeSigner(cfg, block.Number())

	for idx, tx := range block.Transactions() {
		select {
		default:
		case <-ctx.Done():
			return nil, vm.Context{}, nil, nil, ctx.Err()
		}
		statedb.Prepare(tx.Hash(), blockHash, idx)

		// Assemble the transaction call message and return if the requested offset
		msg, _ := tx.AsMessage(signer)
		EVMcontext := core.NewEVMContext(msg, block.Header(), chain, nil)
		if idx == int(txIndex) {
			return msg, EVMcontext, statedb, reader, nil
		}
		// Not yet the searched for transaction, execute on top of the current state
		vmenv := vm.NewEVM(EVMcontext, statedb, cfg, vm.Config{})
		if _, err := core.ApplyMessage(vmenv, msg, new(core.GasPool).AddGas(tx.Gas())); err != nil {
			return nil, vm.Context{}, nil, nil, fmt.Errorf("transaction %x failed: %v", tx.Hash(), err)
		}
		// Ensure any modifications are committed to the state
		// Only delete empty objects if EIP158/161 (a.k.a Spurious Dragon) is in effect
		_ = statedb.FinalizeTx(vmenv.ChainConfig().WithEIPsFlags(context.Background(), block.Number()), reader)
	}
	return nil, vm.Context{}, nil, nil, fmt.Errorf("transaction index %d out of range for block %x", txIndex, blockHash)
}

// computeIntraBlockState retrieves the state database associated with a certain block.
// If no state is locally available for the given block, a number of blocks are
// attempted to be reexecuted to generate the desired state.
func ComputeIntraBlockState(chainKV ethdb.KV, block *types.Block) (*state.IntraBlockState, *StateReader) {
	// If we have the state fully available, use that
	reader := NewStateReader(chainKV, block.NumberU64())
	statedb := state.New(reader)
	return statedb, reader
}

type BlockGetter interface {
	// GetBlockByHash retrieves a block from the database by hash, caching it if found.
	GetBlockByHash(hash common.Hash) *types.Block
	// GetBlock retrieves a block from the database by hash and number,
	// caching it if found.
	GetBlock(hash common.Hash, number uint64) *types.Block
}
