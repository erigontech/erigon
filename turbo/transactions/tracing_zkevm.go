package transactions

import (
	"context"
	"fmt"
	"math/big"

	libcommon "github.com/ledgerwatch/erigon-lib/common"
	"github.com/ledgerwatch/erigon-lib/kv"
	"github.com/ledgerwatch/log/v3"

	"github.com/ledgerwatch/erigon/chain"
	"github.com/ledgerwatch/erigon/consensus"
	"github.com/ledgerwatch/erigon/core"
	"github.com/ledgerwatch/erigon/core/state"
	"github.com/ledgerwatch/erigon/core/types"
	"github.com/ledgerwatch/erigon/core/vm"
	"github.com/ledgerwatch/erigon/core/vm/evmtypes"
	"github.com/ledgerwatch/erigon/eth/stagedsync"
	"github.com/ledgerwatch/erigon/turbo/rpchelper"
	"github.com/ledgerwatch/erigon/turbo/services"
	"github.com/ledgerwatch/erigon/zk/hermez_db"
)

// ComputeTxEnv returns the execution environment of a certain transaction.
func ComputeTxEnv_ZkEvm(ctx context.Context, engine consensus.EngineReader, block *types.Block, cfg *chain.Config, headerReader services.HeaderReader, dbtx kv.Tx, txIndex int, historyV3 bool) (core.Message, evmtypes.BlockContext, evmtypes.TxContext, *state.IntraBlockState, state.StateReader, error) {
	reader, err := rpchelper.CreateHistoryStateReader(dbtx, block.NumberU64(), txIndex, historyV3, cfg.ChainName)
	if err != nil {
		return nil, evmtypes.BlockContext{}, evmtypes.TxContext{}, nil, nil, err
	}

	// Create the parent state database
	statedb := state.New(reader)

	if txIndex == 0 && len(block.Transactions()) == 0 {
		return nil, evmtypes.BlockContext{}, evmtypes.TxContext{}, statedb, reader, nil
	}
	getHeader := func(hash libcommon.Hash, n uint64) *types.Header {
		h, _ := headerReader.HeaderByNumber(ctx, dbtx, n)
		return h
	}
	header := block.HeaderNoCopy()
	parentHeader, err := headerReader.HeaderByHash(ctx, dbtx, header.ParentHash)
	if err != nil {
		// TODO(eip-4844): Do we need to propagate this error?
		log.Error("Can't get parent block's header:", err)
	}
	var excessDataGas *big.Int
	if parentHeader != nil {
		excessDataGas = parentHeader.ExcessDataGas
	}
	BlockContext := core.NewEVMBlockContext(header, core.GetHashFn(header, getHeader), engine, nil, excessDataGas)

	// Recompute transactions up to the target index.
	signer := types.MakeSigner(cfg, block.NumberU64())
	if historyV3 {
		rules := cfg.Rules(BlockContext.BlockNumber, BlockContext.Time)
		txn := block.Transactions()[txIndex]
		statedb.Prepare(txn.Hash(), block.Hash(), txIndex)
		msg, _ := txn.AsMessage(*signer, block.BaseFee(), rules)
		if msg.FeeCap().IsZero() && engine != nil {
			syscall := func(contract libcommon.Address, data []byte) ([]byte, error) {
				return core.SysCallContract(contract, data, *cfg, statedb, header, engine, true /* constCall */, excessDataGas)
			}
			msg.SetIsFree(engine.IsServiceTransaction(msg.From(), syscall))
		}

		TxContext := core.NewEVMTxContext(msg)
		return msg, BlockContext, TxContext, statedb, reader, nil
	}
	vmenv := vm.NewEVM(BlockContext, evmtypes.TxContext{}, statedb, cfg, vm.Config{})
	rules := vmenv.ChainRules()

	consensusHeaderReader := stagedsync.NewChainReaderImpl(cfg, dbtx, nil)

	core.InitializeBlockExecution(engine.(consensus.Engine), consensusHeaderReader, header, block.Transactions(), block.Uncles(), cfg, statedb, excessDataGas)

	hermezReader := hermez_db.NewHermezDbReader(dbtx)

	///////////////////////////////////////////
	// [zkevm] finish set preexecution state //
	///////////////////////////////////////////
	//[zkevm] get batches between last block and this one
	// plus this blocks ger
	lastBatchInserted, err := hermezReader.GetBatchNoByL2Block(BlockContext.BlockNumber - 1)
	if err != nil {
		return nil, evmtypes.BlockContext{}, evmtypes.TxContext{}, nil, nil, fmt.Errorf("failed to get last batch inserted: %v", err)
	}

	// write batches between last block and this if they exist
	currentBatch, err := hermezReader.GetBatchNoByL2Block(BlockContext.BlockNumber)
	if err != nil {
		return nil, evmtypes.BlockContext{}, evmtypes.TxContext{}, nil, nil, err
	}

	gersInBetween, err := hermezReader.GetBatchGlobalExitRoots(lastBatchInserted, currentBatch)
	if err != nil {
		return nil, evmtypes.BlockContext{}, evmtypes.TxContext{}, nil, nil, err
	}

	blockGer, err := hermezReader.GetBlockGlobalExitRoot(BlockContext.BlockNumber)
	if err != nil {
		return nil, evmtypes.BlockContext{}, evmtypes.TxContext{}, nil, nil, err
	}

	l1BlockHash, err := hermezReader.GetBlockL1BlockHash(BlockContext.BlockNumber)
	if err != nil {
		return nil, evmtypes.BlockContext{}, evmtypes.TxContext{}, nil, nil, err
	}

	parentHash := block.ParentHash()
	statedb.SyncerPreExecuteStateSet(cfg, BlockContext.BlockNumber, BlockContext.Time, &parentHash, &blockGer, &l1BlockHash, &gersInBetween)
	///////////////////////////////////////////
	// [zkevm] finish set preexecution state //
	///////////////////////////////////////////
	for idx, txn := range block.Transactions() {
		select {
		default:
		case <-ctx.Done():
			return nil, evmtypes.BlockContext{}, evmtypes.TxContext{}, nil, nil, ctx.Err()
		}
		statedb.Prepare(txn.Hash(), block.Hash(), idx)

		// Assemble the transaction call message and return if the requested offset
		msg, _ := txn.AsMessage(*signer, block.BaseFee(), rules)

		effectiveGasPricePercentage, _ := hermezReader.GetEffectiveGasPricePercentage(txn.Hash())
		msg.SetEffectiveGasPricePercentage(effectiveGasPricePercentage)

		if msg.FeeCap().IsZero() && engine != nil {
			syscall := func(contract libcommon.Address, data []byte) ([]byte, error) {
				return core.SysCallContract(contract, data, *cfg, statedb, header, engine, true /* constCall */, excessDataGas)
			}
			msg.SetIsFree(engine.IsServiceTransaction(msg.From(), syscall))
		}

		TxContext := core.NewEVMTxContext(msg)
		if idx == txIndex {
			return msg, BlockContext, TxContext, statedb, reader, nil
		}
		vmenv.Reset(TxContext, statedb)
		// Not yet the searched for transaction, execute on top of the current state
		if _, err := core.ApplyMessage(vmenv, msg, new(core.GasPool).AddGas(txn.GetGas()), true /* refunds */, false /* gasBailout */); err != nil {
			return nil, evmtypes.BlockContext{}, evmtypes.TxContext{}, nil, nil, fmt.Errorf("transaction %x failed: %w", txn.Hash(), err)
		}
		// Ensure any modifications are committed to the state
		// Only delete empty objects if EIP161 (part of Spurious Dragon) is in effect
		_ = statedb.FinalizeTx(rules, reader.(*state.PlainState))

		if idx+1 == len(block.Transactions()) {
			// Return the state from evaluating all txs in the block, note no msg or TxContext in this case
			return nil, BlockContext, evmtypes.TxContext{}, statedb, reader, nil
		}
	}
	return nil, evmtypes.BlockContext{}, evmtypes.TxContext{}, nil, nil, fmt.Errorf("transaction index %d out of range for block %x", txIndex, block.Hash())
}
