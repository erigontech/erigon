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

type TxEnv struct {
	Msg            core.Message
	BlockContext   evmtypes.BlockContext
	TxContext      evmtypes.TxContext
	Ibs            *state.IntraBlockState
	StateReader    state.StateReader
	GlobalExitRoot *libcommon.Hash
	L1BlockHash    *libcommon.Hash
}

// ComputeTxEnv returns the execution environment of a certain transaction.
func ComputeTxEnv_ZkEvm(ctx context.Context, engine consensus.EngineReader, block *types.Block, cfg *chain.Config, headerReader services.HeaderReader, dbtx kv.Tx, txIndex int, historyV3 bool) (TxEnv, error) {
	reader, err := rpchelper.CreateHistoryStateReader(dbtx, block.NumberU64(), txIndex, historyV3, cfg.ChainName)
	if err != nil {
		return TxEnv{}, err
	}

	// Create the parent state database
	statedb := state.New(reader)

	if txIndex == 0 && len(block.Transactions()) == 0 {
		return TxEnv{}, nil
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
	vmConfig.Debug = false
	blockContext, excessDataGas, ger, l1BlockHash, err := core.PrepareBlockTxExecution(cfg, &vmConfig, core.GetHashFn(header, getHeader), nil, engine.(consensus.Engine), stagedsync.NewChainReaderImpl(cfg, dbtx, nil), block, statedb, hermezReader, block.GasLimit())
	if err != nil {
		return TxEnv{}, err
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
		txEnv := TxEnv{
			Msg:            msg,
			BlockContext:   *blockContext,
			TxContext:      TxContext,
			Ibs:            statedb,
			StateReader:    reader,
			GlobalExitRoot: ger,
			L1BlockHash:    l1BlockHash,
		}
		return txEnv, nil
	}

	gp := new(core.GasPool).AddGas(block.GasLimit())
	for idx, txn := range block.Transactions() {
		select {
		default:
		case <-ctx.Done():
			return TxEnv{}, ctx.Err()
		}

		txHash := txn.Hash()
		vmenv, effectiveGasPricePercentage, err := core.PrepareForTxExecution(cfg, &vmConfig, blockContext, hermezReader, statedb, block, &txHash, txIndex)
		if err != nil {
			return TxEnv{}, err
		}

		msg, txContext, err := core.GetTxContext(cfg, engine, statedb, header, txn, vmenv, effectiveGasPricePercentage)
		if err != nil {
			return TxEnv{}, err
		}

		if idx == txIndex {
			txEnv := TxEnv{
				Msg:            msg,
				BlockContext:   vmenv.Context(),
				TxContext:      txContext,
				Ibs:            statedb,
				StateReader:    reader,
				GlobalExitRoot: ger,
				L1BlockHash:    l1BlockHash,
			}
			return txEnv, nil
		}

		if _, _, err := core.ApplyMessageWithTxContext(msg, txContext, gp, statedb, reader.(*state.PlainState), header.Number, txn, nil, vmenv, true); err != nil {
			return TxEnv{}, err
		}

		if idx+1 == len(block.Transactions()) {
			// Return the state from evaluating all txs in the block, note no msg or TxContext in this case
			txEnv := TxEnv{
				Msg:            msg,
				BlockContext:   vmenv.Context(),
				TxContext:      evmtypes.TxContext{},
				Ibs:            statedb,
				StateReader:    reader,
				GlobalExitRoot: ger,
				L1BlockHash:    l1BlockHash,
			}
			return txEnv, nil
		}
	}
	return TxEnv{}, fmt.Errorf("transaction index %d out of range for block %x", txIndex, block.Hash())
}
