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
	"fmt"
	"math/big"
	"time"

	libcommon "github.com/ledgerwatch/erigon-lib/common"

	"github.com/ledgerwatch/erigon/chain"

	"github.com/ledgerwatch/erigon-lib/kv"

	"github.com/ledgerwatch/erigon/common/math"
	"github.com/ledgerwatch/erigon/consensus"
	"github.com/ledgerwatch/erigon/consensus/misc"
	"github.com/ledgerwatch/erigon/core/state"
	"github.com/ledgerwatch/erigon/core/types"
	"github.com/ledgerwatch/erigon/core/vm"
	"github.com/ledgerwatch/erigon/smt/pkg/blockinfo"
	txTypes "github.com/ledgerwatch/erigon/zk/tx"
)

// ExecuteBlockEphemerally runs a block from provided stateReader and
// writes the result to the provided stateWriter
func ExecuteBlockEphemerallyZk(
	chainConfig *chain.Config,
	vmConfig *vm.Config,
	blockHashFunc func(n uint64) libcommon.Hash,
	engine consensus.Engine,
	prevBlockHash *libcommon.Hash,
	block *types.Block,
	stateReader state.StateReader,
	stateWriter state.WriterWithChangeSets,
	chainReader consensus.ChainHeaderReader,
	getTracer func(txIndex int, txHash libcommon.Hash) (vm.EVMLogger, error),
	dbTx kv.RwTx,
	roHermezDb state.ReadOnlyHermezDb,
) (*EphemeralExecResult, error) {

	defer BlockExecutionTimer.UpdateDuration(time.Now())
	block.Uncles()
	ibs := state.New(stateReader)
	header := block.Header()
	blockTransactions := block.Transactions()
	blockGasLimit := block.GasLimit()
	gp := new(GasPool)
	gp.AddGas(blockGasLimit)

	var (
		rejectedTxs []*RejectedTx
		includedTxs types.Transactions
		receipts    types.Receipts
	)

	var excessDataGas *big.Int
	if chainReader != nil {
		// TODO(eip-4844): understand why chainReader is sometimes nil (e.g. certain test cases)
		ph := chainReader.GetHeaderByHash(block.ParentHash())
		if ph != nil {
			excessDataGas = ph.ExcessDataGas
		}
	}

	if !vmConfig.ReadOnly {
		if err := InitializeBlockExecution(engine, chainReader, header, blockTransactions, block.Uncles(), chainConfig, ibs, excessDataGas); err != nil {
			return nil, err
		}
	}

	if chainConfig.DAOForkBlock != nil && chainConfig.DAOForkBlock.Cmp(block.Number()) == 0 {
		misc.ApplyDAOHardFork(ibs)
	}

	blockNum := block.NumberU64()

	//[zkevm] - get the last batch number so we can check for empty batches in between it and the new one
	lastBatchInserted, err := roHermezDb.GetBatchNoByL2Block(blockNum - 1)
	if err != nil {
		return nil, fmt.Errorf("failed to get last batch inserted: %v", err)
	}

	// write batches between last block and this if they exist
	currentBatch, err := roHermezDb.GetBatchNoByL2Block(blockNum)
	if err != nil {
		return nil, err
	}

	//[zkevm] get batches between last block and this one
	// plus this blocks ger
	gersInBetween, err := roHermezDb.GetBatchGlobalExitRoots(lastBatchInserted, currentBatch)
	if err != nil {
		return nil, err
	}

	blockGer, l1BlockHash, err := roHermezDb.GetBlockGlobalExitRoot(blockNum)
	if err != nil {
		return nil, err
	}
	blockTime := block.Time()
	ibs.SyncerPreExecuteStateSet(chainConfig, blockNum, blockTime, prevBlockHash, &blockGer, &l1BlockHash, &gersInBetween)
	blockInfoTree := blockinfo.NewBlockInfoTree()
	if chainConfig.IsForkID7Etrog(blockNum) {
		coinbase := block.Coinbase()

		if err := blockInfoTree.InitBlockHeader(
			prevBlockHash,
			&coinbase,
			blockNum,
			blockGasLimit,
			blockTime,
			&blockGer,
			&l1BlockHash,
		); err != nil {
			return nil, err
		}
	}

	noop := state.NewNoopWriter()
	logIndex := int64(0)
	usedGas := new(uint64)

	for txIndex, tx := range blockTransactions {
		ibs.Prepare(tx.Hash(), block.Hash(), txIndex)
		writeTrace := false
		if vmConfig.Debug && vmConfig.Tracer == nil {
			tracer, err := getTracer(txIndex, tx.Hash())
			if err != nil {
				return nil, fmt.Errorf("could not obtain tracer: %w", err)
			}
			vmConfig.Tracer = tracer
			writeTrace = true
		}

		gp.Reset(blockGasLimit)

		effectiveGasPricePercentage, err := roHermezDb.GetEffectiveGasPricePercentage(tx.Hash())
		if err != nil {
			return nil, err
		}

		receipt, execResult, err := ApplyTransaction_zkevm(chainConfig, blockHashFunc, engine, nil, gp, ibs, noop, header, tx, usedGas, *vmConfig, excessDataGas, effectiveGasPricePercentage)
		if writeTrace {
			if ftracer, ok := vmConfig.Tracer.(vm.FlushableTracer); ok {
				ftracer.Flush(tx)
			}

			vmConfig.Tracer = nil
		}

		//TODO: remove this after bug is fixed
		localReceipt := *receipt
		if execResult.Err == vm.ErrUnsupportedPrecompile {
			localReceipt.Status = 1
		}

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
			ibs.ScalableSetSmtRootHash(roHermezDb)
		}

		txSender, _ := tx.GetSender()
		if chainConfig.IsForkID7Etrog(blockNum) {
			l2TxHash, err := txTypes.ComputeL2TxHash(
				chainConfig.ChainID,
				tx.GetValue(),
				tx.GetPrice(),
				tx.GetNonce(),
				tx.GetGas(),
				tx.GetTo(),
				&txSender,
				tx.GetData(),
			)
			if err != nil {
				return nil, err
			}

			//block info tree
			_, err = blockInfoTree.SetBlockTx(
				&l2TxHash,
				txIndex,
				&localReceipt,
				logIndex,
				*usedGas,
				effectiveGasPricePercentage,
			)
			if err != nil {
				return nil, err
			}
		}

		// increment logIndex for next turn
		// log idex counts all the logs in all txs in the block
		logIndex += int64(len(receipt.Logs))
	}

	var l2InfoRoot libcommon.Hash
	if chainConfig.IsForkID7Etrog(blockNum) {
		// [zkevm] - set the block info tree root
		root, err := blockInfoTree.SetBlockGasUsed(*usedGas)
		if err != nil {
			return nil, err
		}
		l2InfoRoot = libcommon.BigToHash(root)
	}

	ibs.PostExecuteStateSet(chainConfig, block.NumberU64(), &l2InfoRoot)

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
		if _, _, _, err := FinalizeBlockExecution(engine, stateReader, block.Header(), txs, block.Uncles(), stateWriter, chainConfig, ibs, receipts, block.Withdrawals(), chainReader, false, excessDataGas); err != nil {
			return nil, err
		}
	}
	blockLogs := ibs.Logs()
	execRs := &EphemeralExecResult{
		TxRoot:      types.DeriveSha(includedTxs),
		ReceiptRoot: receiptSha,
		Bloom:       bloom,
		LogsHash:    rlpHash(blockLogs),
		Receipts:    receipts,
		Difficulty:  (*math.HexOrDecimal256)(header.Difficulty),
		GasUsed:     math.HexOrDecimal64(*usedGas),
		Rejected:    rejectedTxs,
	}

	return execRs, nil
}
