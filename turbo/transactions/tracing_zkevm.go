package transactions

import (
	"context"
	"fmt"
	"math/big"

	libcommon "github.com/gateway-fm/cdk-erigon-lib/common"
	"github.com/gateway-fm/cdk-erigon-lib/kv"
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
	// BlockContext := core.NewEVMBlockContext(header, core.GetHashFn(header, getHeader), engine, nil, excessDataGas)
	hermezReader := hermez_db.NewHermezDbReader(dbtx)

	vmConfig := vm.NewTraceVmConfig()

	blockContext, excessDataGas, _, _, err := core.PrepareBlockTxExecution(cfg, &vmConfig, core.GetHashFn(header, getHeader), nil, engine.(consensus.Engine), stagedsync.NewChainReaderImpl(cfg, dbtx, nil), block, statedb, hermezReader, block.GasLimit())
	if err != nil {
		return nil, evmtypes.BlockContext{}, evmtypes.TxContext{}, nil, nil, err
	}

	// Recompute transactions up to the target index.
	signer := types.MakeSigner(cfg, block.NumberU64())
	if historyV3 {
		rules := cfg.Rules(blockContext.BlockNumber, blockContext.Time)
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
		return msg, *blockContext, TxContext, statedb, reader, nil
	}

	gp := new(core.GasPool).AddGas(block.GasLimit())
	for idx, txn := range block.Transactions() {
		select {
		default:
		case <-ctx.Done():
			return nil, evmtypes.BlockContext{}, evmtypes.TxContext{}, nil, nil, ctx.Err()
		}

		txHash := txn.Hash()
		vmenv, effectiveGasPricePercentage, err := core.PrepareForTxExecution(cfg, &vmConfig, blockContext, hermezReader, statedb, block, &txHash, txIndex)
		if err != nil {
			return nil, evmtypes.BlockContext{}, evmtypes.TxContext{}, nil, nil, err
		}

		msg, txContext, err := core.GetTxContext(cfg, engine, statedb, header, txn, vmenv, effectiveGasPricePercentage)
		if err != nil {
			return nil, evmtypes.BlockContext{}, evmtypes.TxContext{}, nil, nil, err
		}

		if idx == txIndex {
			return msg, vmenv.Context(), txContext, statedb, reader, nil
		}

		if _, _, err := core.ApplyMessageWithTxContext(msg, txContext, gp, statedb, reader.(*state.PlainState), header.Number, txn, nil, vmenv); err != nil {
			return nil, evmtypes.BlockContext{}, evmtypes.TxContext{}, nil, nil, err
		}

		if idx+1 == len(block.Transactions()) {
			// Return the state from evaluating all txs in the block, note no msg or TxContext in this case
			return nil, vmenv.Context(), evmtypes.TxContext{}, statedb, reader, nil
		}
	}
	return nil, evmtypes.BlockContext{}, evmtypes.TxContext{}, nil, nil, fmt.Errorf("transaction index %d out of range for block %x", txIndex, block.Hash())
}
