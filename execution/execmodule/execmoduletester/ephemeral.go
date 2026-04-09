// Copyright 2026 The Erigon Authors
// This file is part of Erigon.
//
// Erigon is free software: you can redistribute it and/or modify
// it under the terms of the GNU Lesser General Public License as published by
// the Free Software Foundation, either version 3 of the License, or
// (at your option) any later version.
//
// Erigon is distributed in the hope that it will be useful,
// but WITHOUT ANY WARRANTY; without even the implied warranty of
// MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE. See the
// GNU Lesser General Public License for more details.
//
// You should have received a copy of the GNU Lesser General Public License
// along with Erigon. If not, see <http://www.gnu.org/licenses/>.

package execmoduletester

import (
	"fmt"
	"math/big"

	"github.com/erigontech/erigon/common"
	"github.com/erigontech/erigon/db/kv/membatchwithdb"
	"github.com/erigontech/erigon/db/kv/rawdbv3"
	"github.com/erigontech/erigon/db/rawdb"
	"github.com/erigontech/erigon/execution/protocol"
	"github.com/erigontech/erigon/execution/state"
	"github.com/erigontech/erigon/execution/tests/blockgen"
	"github.com/erigontech/erigon/execution/types"
	"github.com/erigontech/erigon/execution/vm"
)

// initEphemeral sets up the MemoryMutation overlay on top of the MDBX database.
// Called after genesis has been committed via the full pipeline.
func (emt *ExecModuleTester) initEphemeral() error {
	roTx, err := emt.DB.BeginTemporalRo(emt.Ctx) //nolint:gocritic
	if err != nil {
		return fmt.Errorf("ephemeral: BeginTemporalRo: %w", err)
	}
	defer func() {
		if emt.ephemeralOverlay == nil { // setup failed — clean up
			roTx.Rollback()
		}
	}()

	overlay, err := membatchwithdb.NewMemoryBatch(roTx, "", emt.Log)
	if err != nil {
		return fmt.Errorf("ephemeral: NewMemoryBatch: %w", err)
	}

	txNum, err := rawdbv3.TxNums.Max(emt.Ctx, roTx, 0)
	if err != nil {
		overlay.Close()
		return fmt.Errorf("ephemeral: TxNums.Max: %w", err)
	}

	emt.ephemeralRoTx = roTx
	emt.ephemeralOverlay = overlay
	emt.ephemeralTxNum = txNum + 1 // next available
	emt.ephemeralHeaders = make(map[common.Hash]*types.Header)
	emt.ephemeralTDs = make(map[common.Hash]*big.Int)

	// Seed with genesis.
	emt.ephemeralHeaders[emt.Genesis.Hash()] = emt.Genesis.Header()
	genTd, _ := rawdb.ReadTd(roTx, emt.Genesis.Hash(), 0)
	if genTd != nil {
		emt.ephemeralTDs[emt.Genesis.Hash()] = genTd
	}
	emt.ephemeralLastHash = emt.Genesis.Hash()

	return nil
}

// closeEphemeral releases ephemeral resources.
func (emt *ExecModuleTester) closeEphemeral() {
	if emt.ephemeralOverlay != nil {
		emt.ephemeralOverlay.Close()
		emt.ephemeralOverlay = nil
	}
	if emt.ephemeralRoTx != nil {
		emt.ephemeralRoTx.Rollback()
		emt.ephemeralRoTx = nil
	}
}

func (emt *ExecModuleTester) ephemeralChainReader() *LightChainReader {
	return &LightChainReader{
		Config_: emt.ChainConfig,
		Headers: emt.ephemeralHeaders,
		TDs:     emt.ephemeralTDs,
		Tx:      emt.ephemeralOverlay,
	}
}

// verifyBlock performs pre-execution validation: header, uncles, withdrawals,
// transactions root, gas limit, and blob gas. Shared by insertChainEphemeral
// and DryRunBlock.
func (emt *ExecModuleTester) verifyBlock(cr *LightChainReader, block *types.Block) error {
	header := block.Header()

	if err := emt.Engine.VerifyHeader(cr, header, false); err != nil {
		return err
	}
	if err := emt.Engine.VerifyUncles(cr, header, block.Uncles()); err != nil {
		return err
	}
	if emt.ChainConfig.IsShanghai(header.Time) && block.Withdrawals() == nil {
		return fmt.Errorf("block #%v missing withdrawals field (post-Shanghai)", block.Number())
	}
	if header.WithdrawalsHash != nil {
		if computedRoot := types.DeriveSha(types.Withdrawals(block.Withdrawals())); computedRoot != *header.WithdrawalsHash {
			return fmt.Errorf("block #%v withdrawals root mismatch: computed %x, header %x", block.Number(), computedRoot, *header.WithdrawalsHash)
		}
	}
	if computedTxRoot := types.DeriveSha(block.Transactions()); computedTxRoot != header.TxHash {
		return fmt.Errorf("block #%v transactions root mismatch: computed %x, header %x", block.Number(), computedTxRoot, header.TxHash)
	}
	if header.GasUsed > header.GasLimit {
		return fmt.Errorf("block #%v gas used %d exceeds gas limit %d", block.Number(), header.GasUsed, header.GasLimit)
	}
	if header.BlobGasUsed != nil {
		var expectedBlobGas uint64
		for _, txn := range block.Transactions() {
			expectedBlobGas += txn.GetBlobGas()
		}
		if expectedBlobGas != *header.BlobGasUsed {
			return fmt.Errorf("block #%v blob gas mismatch: txns=%d, header=%d", block.Number(), expectedBlobGas, *header.BlobGasUsed)
		}
	}
	return nil
}

// executeBlock runs ExecuteBlockEphemerally on the given overlay with panic recovery.
func (emt *ExecModuleTester) executeBlock(cr *LightChainReader, block *types.Block, overlay *membatchwithdb.MemoryMutation, txNum uint64) error {
	header := block.Header()
	stateReader := state.NewReaderV3(overlay)
	stateWriter := state.NewWriter(overlay, nil, txNum)
	stateWriter.SetForceWrites(true) // required: overlay needs explicit writes for multi-block system contract state

	blockHashFunc := protocol.GetHashFn(header, func(hash common.Hash, number uint64) (*types.Header, error) {
		h := cr.GetHeader(hash, number)
		if h == nil {
			return nil, fmt.Errorf("header not found: %x at %d", hash, number)
		}
		return h, nil
	})

	var execErr error
	func() {
		defer func() {
			if r := recover(); r != nil {
				execErr = fmt.Errorf("panic during block execution: %v", r)
			}
		}()
		_, execErr = protocol.ExecuteBlockEphemerally(
			emt.ChainConfig, &vm.Config{},
			blockHashFunc, emt.Engine, block,
			stateReader, stateWriter,
			cr, nil, emt.Log,
		)
	}()
	return execErr
}

// insertChainEphemeral executes valid blocks on the main MemoryMutation overlay.
func (emt *ExecModuleTester) insertChainEphemeral(cp *blockgen.ChainPack) error {
	cr := emt.ephemeralChainReader()

	for i := 0; i < cp.Length(); i++ {
		block := cp.Blocks[i]

		if err := emt.verifyBlock(cr, block); err != nil {
			return err
		}

		emt.ephemeralTxNum++
		if err := emt.executeBlock(cr, block, emt.ephemeralOverlay, emt.ephemeralTxNum); err != nil {
			return err
		}

		// Update txNum for the transactions in this block + end-of-block system tx.
		emt.ephemeralTxNum += uint64(block.Transactions().Len())
		emt.ephemeralTxNum++

		// Record header and total difficulty.
		header := block.Header()
		emt.ephemeralHeaders[block.Hash()] = header
		parentTd := emt.ephemeralTDs[header.ParentHash]
		if parentTd == nil {
			parentTd, _ = rawdb.ReadTd(emt.ephemeralRoTx, header.ParentHash, header.Number.Uint64()-1)
		}
		blockTd := new(big.Int)
		if parentTd != nil {
			blockTd.Set(parentTd)
		}
		blockTd.Add(blockTd, header.Difficulty.ToBig())
		emt.ephemeralTDs[block.Hash()] = blockTd
		emt.ephemeralLastHash = block.Hash()
	}
	return nil
}

// DryRunBlock executes a block on a throwaway overlay without merging state.
// Returns nil if the block executes successfully, or the execution error.
// Used for expected-invalid blocks to avoid leaking state into the main overlay.
func (emt *ExecModuleTester) DryRunBlock(block *types.Block) error {
	cr := emt.ephemeralChainReader()

	if err := emt.verifyBlock(cr, block); err != nil {
		return err
	}

	throwaway, err := membatchwithdb.NewMemoryBatch(emt.ephemeralOverlay, "", emt.Log)
	if err != nil {
		return err
	}
	defer throwaway.Close()

	return emt.executeBlock(cr, block, throwaway, emt.ephemeralTxNum+1)
}

// RecordEphemeralHeader records a block header in the ephemeral chain reader's
// in-memory maps without executing the block. Used for side-chain blocks whose
// headers are needed for uncle verification.
func (emt *ExecModuleTester) RecordEphemeralHeader(header *types.Header) {
	emt.ephemeralHeaders[header.Hash()] = header
}

// EphemeralOverlay returns the MemoryMutation overlay for state reads.
func (emt *ExecModuleTester) EphemeralOverlay() *membatchwithdb.MemoryMutation {
	return emt.ephemeralOverlay
}

// EphemeralLastBlockHash returns the hash of the last successfully executed block.
func (emt *ExecModuleTester) EphemeralLastBlockHash() common.Hash {
	return emt.ephemeralLastHash
}
