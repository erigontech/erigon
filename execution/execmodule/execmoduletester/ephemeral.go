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
	emt.ephemeralRoTx = roTx // owned by closeEphemeral; not deferred here
	defer func() {
		if emt.ephemeralOverlay == nil { // setup failed — clean up
			emt.ephemeralRoTx = nil
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
		roTx.Rollback()
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

// insertChainEphemeral executes blocks on the MemoryMutation overlay using
// ExecuteBlockEphemerally instead of the full staged-sync pipeline.
// Each block is executed on a nested overlay so that failed executions
// (expected-invalid blocks) don't leave partial state on the main overlay.
func (emt *ExecModuleTester) insertChainEphemeral(cp *blockgen.ChainPack) error {
	cr := &LightChainReader{
		Config_: emt.ChainConfig,
		Headers: emt.ephemeralHeaders,
		TDs:     emt.ephemeralTDs,
		Tx:      emt.ephemeralOverlay,
	}

	for i := 0; i < cp.Length(); i++ {
		block := cp.Blocks[i]
		header := block.Header()

		// Verify header (gas limit, timestamp, difficulty, etc.).
		if err := emt.Engine.VerifyHeader(cr, header, false); err != nil {
			return err
		}

		// Verify uncles.
		if err := emt.Engine.VerifyUncles(cr, header, block.Uncles()); err != nil {
			return err
		}

		// Post-Shanghai blocks must have a withdrawals field in the RLP.
		// Go's RLP decoder is lenient and sets it to nil if absent.
		if emt.ChainConfig.IsShanghai(header.Time) && block.Withdrawals() == nil {
			return fmt.Errorf("block #%v missing withdrawals field (post-Shanghai)", block.Number())
		}

		// Validate withdrawals root matches actual withdrawals.
		if header.WithdrawalsHash != nil {
			computedRoot := types.DeriveSha(types.Withdrawals(block.Withdrawals()))
			if computedRoot != *header.WithdrawalsHash {
				return fmt.Errorf("block #%v withdrawals root mismatch: computed %x, header %x", block.Number(), computedRoot, *header.WithdrawalsHash)
			}
		}

		// Validate transactions root matches actual transactions.
		computedTxRoot := types.DeriveSha(block.Transactions())
		if computedTxRoot != header.TxHash {
			return fmt.Errorf("block #%v transactions root mismatch: computed %x, header %x", block.Number(), computedTxRoot, header.TxHash)
		}

		// Check gas used doesn't exceed gas limit.
		if header.GasUsed > header.GasLimit {
			return fmt.Errorf("block #%v gas used %d exceeds gas limit %d", block.Number(), header.GasUsed, header.GasLimit)
		}

		// Validate blob gas used matches actual blob count.
		if header.BlobGasUsed != nil {
			var expectedBlobGas uint64
			for _, txn := range block.Transactions() {
				expectedBlobGas += txn.GetBlobGas()
			}
			if expectedBlobGas != *header.BlobGasUsed {
				return fmt.Errorf("block #%v blob gas mismatch: txns=%d, header=%d", block.Number(), expectedBlobGas, *header.BlobGasUsed)
			}
		}

		// Execute directly on the main overlay. Valid blocks (the only ones
		// routed here by insertBlocks) are expected to succeed. DomainDelPrefix
		// must see all keys from previous blocks, which requires operating on
		// the main overlay rather than a nested one.
		emt.ephemeralTxNum++

		stateReader := state.NewReaderV3(emt.ephemeralOverlay)
		stateWriter := state.NewWriter(emt.ephemeralOverlay, nil, emt.ephemeralTxNum)
		stateWriter.ForceWrites = true // overlay execution: must write even if original==value

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
		if execErr != nil {
			return execErr
		}

		// Update txNum for the transactions in this block + end-of-block system tx.
		emt.ephemeralTxNum += uint64(block.Transactions().Len())
		emt.ephemeralTxNum++

		// Record header and total difficulty.
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
	cr := &LightChainReader{
		Config_: emt.ChainConfig,
		Headers: emt.ephemeralHeaders,
		TDs:     emt.ephemeralTDs,
		Tx:      emt.ephemeralOverlay,
	}

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
		computedRoot := types.DeriveSha(types.Withdrawals(block.Withdrawals()))
		if computedRoot != *header.WithdrawalsHash {
			return fmt.Errorf("block #%v withdrawals root mismatch", block.Number())
		}
	}
	computedTxRoot := types.DeriveSha(block.Transactions())
	if computedTxRoot != header.TxHash {
		return fmt.Errorf("block #%v transactions root mismatch", block.Number())
	}
	if header.GasUsed > header.GasLimit {
		return fmt.Errorf("block #%v gas used exceeds gas limit", block.Number())
	}
	if header.BlobGasUsed != nil {
		var expectedBlobGas uint64
		for _, txn := range block.Transactions() {
			expectedBlobGas += txn.GetBlobGas()
		}
		if expectedBlobGas != *header.BlobGasUsed {
			return fmt.Errorf("block #%v blob gas mismatch", block.Number())
		}
	}

	throwaway, err := membatchwithdb.NewMemoryBatch(emt.ephemeralOverlay, "", emt.Log)
	if err != nil {
		return err
	}
	defer throwaway.Close()

	txNum := emt.ephemeralTxNum + 1
	stateReader := state.NewReaderV3(throwaway)
	stateWriter := state.NewWriter(throwaway, nil, txNum)
	stateWriter.ForceWrites = true

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
	return execErr // nil means "block executed successfully" — throwaway overlay is discarded
}

// RecordEphemeralHeader records a block header in the ephemeral chain reader's
// in-memory maps without executing the block. Used for side-chain blocks whose
// headers are needed for uncle verification.
func (emt *ExecModuleTester) RecordEphemeralHeader(header *types.Header) {
	emt.ephemeralHeaders[header.Hash()] = header
}

// IsEphemeral returns true if the tester is in ephemeral (overlay) mode.
func (emt *ExecModuleTester) IsEphemeral() bool { return emt.ephemeral }

// EphemeralOverlay returns the MemoryMutation overlay for state reads.
func (emt *ExecModuleTester) EphemeralOverlay() *membatchwithdb.MemoryMutation {
	return emt.ephemeralOverlay
}

// EphemeralLastBlockHash returns the hash of the last successfully executed block.
func (emt *ExecModuleTester) EphemeralLastBlockHash() common.Hash {
	return emt.ephemeralLastHash
}
