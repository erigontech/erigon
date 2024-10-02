// Copyright 2014 The go-ethereum Authors
// This file is part of the go-ethereum library.
//
// The go-ethereum library is free software: you can redistribute it and/or modify
// it under the terms of the GNU Lesser General Public License as published by
// the Free Software Foundation, either version 3 of the License, or
// (at your option) any later version.
//
// The go-ethereum library is distributed in the hope that it will be useful,
// but WITHOUT ANY WARRANTY; without even the implied warranty of
// MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE. See the
// GNU Lesser General Public License for more details.
//
// You should have received a copy of the GNU Lesser General Public License
// along with the go-ethereum library. If not, see <http://www.gnu.org/licenses/>.

// Package core implements the Ethereum consensus protocol.
package core

import (
	"errors"
	"fmt"
	"time"

	"github.com/ledgerwatch/erigon-lib/common"
	libcommon "github.com/ledgerwatch/erigon-lib/common"
	"github.com/ledgerwatch/erigon/smt/pkg/blockinfo"
	"github.com/ledgerwatch/log/v3"

	"github.com/ledgerwatch/erigon-lib/chain"

	"github.com/ledgerwatch/erigon/common/math"
	"github.com/ledgerwatch/erigon/consensus"
	"github.com/ledgerwatch/erigon/consensus/misc"
	"github.com/ledgerwatch/erigon/core/state"
	"github.com/ledgerwatch/erigon/core/types"
	"github.com/ledgerwatch/erigon/core/vm"
	"github.com/ledgerwatch/erigon/core/vm/evmtypes"
	"github.com/ledgerwatch/erigon/zk/hermez_db"
	"github.com/ledgerwatch/erigon/zk/utils"
)

type EphemeralExecResultZk struct {
	*EphemeralExecResult
	BlockInfoTree *common.Hash `json:"blockInfoTree,omitempty"`
}

// ExecuteBlockEphemerally runs a block from provided stateReader and
// writes the result to the provided stateWriter
func ExecuteBlockEphemerallyZk(
	chainConfig *chain.Config,
	vmConfig *vm.Config,
	blockHashFunc func(n uint64) libcommon.Hash,
	engine consensus.Engine,
	block *types.Block,
	stateReader state.StateReader,
	stateWriter state.WriterWithChangeSets,
	chainReader consensus.ChainReader,
	getTracer func(txIndex int, txHash libcommon.Hash) (vm.EVMLogger, error),
	roHermezDb state.ReadOnlyHermezDb,
	prevBlockRoot *common.Hash,
) (*EphemeralExecResultZk, error) {

	defer blockExecutionTimer.ObserveDuration(time.Now())
	block.Uncles()
	ibs := state.New(stateReader)
	header := block.Header()
	blockTransactions := block.Transactions()
	blockGasLimit := block.GasLimit()

	if !chainConfig.IsForkID8Elderberry(block.NumberU64()) {
		blockGasLimit = utils.ForkId7BlockGasLimit
	}

	gp := new(GasPool).AddGas(blockGasLimit)

	var (
		rejectedTxs []*RejectedTx
		includedTxs types.Transactions
		receipts    types.Receipts
	)

	blockContext, _, ger, l1Blockhash, err := PrepareBlockTxExecution(chainConfig, vmConfig, blockHashFunc, nil, engine, chainReader, block, ibs, roHermezDb, blockGasLimit)
	if err != nil {
		return nil, err
	}

	blockNum := block.NumberU64()
	usedGas := new(uint64)
	txInfos := []blockinfo.ExecutedTxInfo{}

	for txIndex, tx := range blockTransactions {
		ibs.SetTxContext(tx.Hash(), block.Hash(), txIndex)
		writeTrace := false
		if vmConfig.Debug && vmConfig.Tracer == nil {
			tracer, err := getTracer(txIndex, tx.Hash())
			if err != nil {
				return nil, fmt.Errorf("could not obtain tracer: %w", err)
			}
			vmConfig.Tracer = tracer
			writeTrace = true
		}
		txHash := tx.Hash()
		evm, effectiveGasPricePercentage, err := PrepareForTxExecution(chainConfig, vmConfig, blockContext, roHermezDb, ibs, block, &txHash, txIndex)
		if err != nil {
			return nil, err
		}

		receipt, execResult, err := ApplyTransaction_zkevm(chainConfig, engine, evm, gp, ibs, state.NewNoopWriter(), header, tx, usedGas, effectiveGasPricePercentage, true)
		if err != nil {
			return nil, err
		}
		if writeTrace {
			if ftracer, ok := vmConfig.Tracer.(vm.FlushableTracer); ok {
				ftracer.Flush(tx)
			}

			vmConfig.Tracer = nil
		}

		localReceipt := CreateReceiptForBlockInfoTree(receipt, chainConfig, blockNum, execResult)
		ProcessReceiptForBlockExecution(receipt, roHermezDb, chainConfig, blockNum, header, tx)

		if err != nil {
			if !vmConfig.StatelessExec {
				return nil, fmt.Errorf("could not apply tx %d from block %d [%v]: %w", txIndex, block.NumberU64(), tx.Hash().Hex(), err)
			}
			rejectedTxs = append(rejectedTxs, &RejectedTx{txIndex, err.Error()})
		} else {
			includedTxs = append(includedTxs, tx)
			if !vmConfig.NoReceipts {
				receipts = append(receipts, receipt)
			}
		}
		if !chainConfig.IsForkID7Etrog(block.NumberU64()) {
			if err := ibs.ScalableSetSmtRootHash(roHermezDb); err != nil {
				return nil, err
			}
		}

		txSender, ok := tx.GetSender()
		if !ok {
			signer := types.MakeSigner(chainConfig, blockNum, block.Time())
			txSender, err = tx.Sender(*signer)
			if err != nil {
				return nil, err
			}
		}

		txInfos = append(txInfos, blockinfo.ExecutedTxInfo{
			Tx:                tx,
			Receipt:           localReceipt,
			EffectiveGasPrice: effectiveGasPricePercentage,
			Signer:            &txSender,
		})
	}

	var l2InfoRoot *libcommon.Hash
	if chainConfig.IsForkID7Etrog(blockNum) {
		l2InfoRoot, err = blockinfo.BuildBlockInfoTree(
			&header.Coinbase,
			header.Number.Uint64(),
			header.Time,
			blockGasLimit,
			*usedGas,
			*ger,
			*l1Blockhash,
			*prevBlockRoot,
			&txInfos,
		)
		if err != nil {
			return nil, err
		}
	}

	ibs.PostExecuteStateSet(chainConfig, block.NumberU64(), l2InfoRoot)

	receiptSha := types.DeriveSha(receipts)
	// [zkevm] todo
	//if !vmConfig.StatelessExec && chainConfig.IsByzantium(header.Number.Uint64()) && !vmConfig.NoReceipts && receiptSha != block.ReceiptHash() {
	//	return nil, fmt.Errorf("mismatched receipt headers for block %d (%s != %s)", block.NumberU64(), receiptSha.Hex(), block.ReceiptHash().Hex())
	//}

	// in zkEVM we don't have headers to check GasUsed against
	//if !vmConfig.StatelessExec && *usedGas != header.GasUsed && header.GasUsed > 0 {
	//	return nil, fmt.Errorf("gas used by execution: %d, in header: %d", *usedGas, header.GasUsed)
	//}

	var bloom types.Bloom
	if !vmConfig.NoReceipts {
		bloom = types.CreateBloom(receipts)
		// [zkevm] todo
		//if !vmConfig.StatelessExec && bloom != header.Bloom {
		//	return nil, fmt.Errorf("bloom computed by execution: %x, in header: %x", bloom, header.Bloom)
		//}
	}
	if !vmConfig.ReadOnly {
		txs := blockTransactions
		if _, _, _, err := FinalizeBlockExecution(engine, stateReader, block.Header(), txs, block.Uncles(), stateWriter, chainConfig, ibs, receipts, block.Withdrawals(), chainReader, false, log.New()); err != nil {
			return nil, err
		}
	}
	blockLogs := ibs.Logs()
	execRs := &EphemeralExecResultZk{
		EphemeralExecResult: &EphemeralExecResult{
			TxRoot:      types.DeriveSha(includedTxs),
			ReceiptRoot: receiptSha,
			Bloom:       bloom,
			LogsHash:    rlpHash(blockLogs),
			Receipts:    receipts,
			Difficulty:  (*math.HexOrDecimal256)(header.Difficulty),
			GasUsed:     math.HexOrDecimal64(*usedGas),
			Rejected:    rejectedTxs,
		},
		BlockInfoTree: l2InfoRoot,
	}

	return execRs, nil
}

func PrepareBlockTxExecution(
	chainConfig *chain.Config,
	vmConfig *vm.Config,
	blockHashFunc func(n uint64) common.Hash,
	author *common.Address,
	engine consensus.Engine,
	chainReader consensus.ChainHeaderReader,
	block *types.Block,
	ibs *state.IntraBlockState,
	roHermezDb state.ReadOnlyHermezDb,
	blockGasLimit uint64,
) (blockContext *evmtypes.BlockContext, excessDataGas *uint64, ger, l1BlockHash *common.Hash, err error) {
	var blockNum uint64
	if block != nil {
		blockNum = block.NumberU64()
	}

	prevBlockheader := chainReader.GetHeaderByNumber(blockNum - 1)
	// TODO(eip-4844): understand why chainReader is sometimes nil (e.g. certain test cases)
	if prevBlockheader != nil {
		excessDataGas = prevBlockheader.ExcessBlobGas
	}

	if !vmConfig.ReadOnly {
		if err := InitializeBlockExecution(engine, chainReader, block.Header(), chainConfig, ibs, log.Root()); err != nil {
			return nil, nil, nil, nil, err
		}
	}

	if chainConfig.DAOForkBlock != nil && chainConfig.DAOForkBlock.Cmp(block.Number()) == 0 {
		misc.ApplyDAOHardFork(ibs)
	}

	///////////////////////////////////////////
	// [zkevm] set preexecution state 		 //
	///////////////////////////////////////////
	//[zkevm] - get the last batch number so we can check for empty batches in between it and the new one
	lastBatchInserted, err := roHermezDb.GetBatchNoByL2Block(blockNum - 1)
	if err != nil && !errors.Is(err, hermez_db.ErrorNotStored) {
		return nil, nil, nil, nil, fmt.Errorf("failed to get last batch inserted: %v", err)
	}

	// write batches between last block and this if they exist
	currentBatch, err := roHermezDb.GetBatchNoByL2Block(blockNum)
	if err != nil && !errors.Is(err, hermez_db.ErrorNotStored) {
		return nil, nil, nil, nil, err
	}

	//[zkevm] get batches between last block and this one
	// plus this blocks ger
	gersInBetween, err := roHermezDb.GetBatchGlobalExitRoots(lastBatchInserted, currentBatch)
	if err != nil {
		return nil, nil, nil, nil, err
	}

	blockGer, err := roHermezDb.GetBlockGlobalExitRoot(blockNum)
	if err != nil {
		return nil, nil, nil, nil, err
	}
	blockL1BlockHash, err := roHermezDb.GetBlockL1BlockHash(blockNum)
	if err != nil {
		return nil, nil, nil, nil, err
	}

	blockTime := block.Time()
	prevBlockRoot := prevBlockheader.Root
	l1InfoTreeIndexReused, err := roHermezDb.GetReusedL1InfoTreeIndex(blockNum)
	if err != nil {
		return nil, nil, nil, nil, err
	}
	ibs.SyncerPreExecuteStateSet(chainConfig, blockNum, blockTime, &prevBlockRoot, &blockGer, &blockL1BlockHash, gersInBetween, l1InfoTreeIndexReused)
	///////////////////////////////////////////
	// [zkevm] finish set preexecution state //
	///////////////////////////////////////////

	blockContextImpl := NewEVMBlockContext(block.Header(), blockHashFunc, engine, author)

	return &blockContextImpl, excessDataGas, &blockGer, &blockL1BlockHash, nil
}

func CreateReceiptForBlockInfoTree(receipt *types.Receipt, chainConfig *chain.Config, blockNum uint64, execResult *ExecutionResult) *types.Receipt {
	// [hack]TODO: remove this after bug is fixed
	localReceipt := receipt.Clone()
	if !chainConfig.IsForkID8Elderberry(blockNum) && errors.Is(execResult.Err, vm.ErrUnsupportedPrecompile) {
		localReceipt.Status = 1
	}

	return localReceipt
}

func ProcessReceiptForBlockExecution(receipt *types.Receipt, roHermezDb state.ReadOnlyHermezDb, chainConfig *chain.Config, blockNum uint64, header *types.Header, tx types.Transaction) error {
	// forkid8 the poststate is empty
	// forkid8 also fixed the bugs with logs and cumulative gas used
	if !chainConfig.IsForkID8Elderberry(blockNum) {
		// the stateroot in the transactions that comes from the datastream
		// is the one after smart contract writes so it can't be used
		// but since pre forkid7 blocks have 1 tx only, we can use the block root
		if chainConfig.IsForkID7Etrog(blockNum) {
			// receipt root holds the intermediate stateroot after the tx
			intermediateState, err := roHermezDb.GetIntermediateTxStateRoot(blockNum, tx.Hash())
			if err != nil {
				return err
			}
			receipt.PostState = intermediateState.Bytes()
		} else {
			receipt.PostState = header.Root.Bytes()
		}

		//[hack] log0 pre forkid8 are not included in the rpc logs
		// also pre forkid8 comulative gas used is same as gas used
		var fixedLogs types.Logs
		for _, l := range receipt.Logs {
			if len(l.Topics) == 0 && len(l.Data) == 0 {
				continue
			}
			fixedLogs = append(fixedLogs, l)
		}
		receipt.Logs = fixedLogs
		receipt.CumulativeGasUsed = receipt.GasUsed
	}

	if !chainConfig.IsNormalcy(blockNum) {
		for _, l := range receipt.Logs {
			l.ApplyPaddingToLogsData(chainConfig.IsForkID8Elderberry(blockNum), chainConfig.IsForkID12Banana(blockNum))
		}
	}

	return nil
}
