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
	"github.com/erigontech/erigon/execution/rlp"
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
	emt.ephemeralBlocks = make(map[common.Hash]*types.Block)
	emt.ephemeralTDs = make(map[common.Hash]*big.Int)

	// Seed with genesis.
	emt.ephemeralBlocks[emt.Genesis.Hash()] = emt.Genesis
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
		Blocks:  emt.ephemeralBlocks,
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
	// EIP-7934: block RLP size limit (Osaka+).
	blockRlpSize := block.EncodingSize()
	blockRlpSize += rlp.ListPrefixLen(blockRlpSize)
	if blockRlpSize > emt.ChainConfig.GetMaxRlpBlockSize(header.Time) {
		return fmt.Errorf("block #%v exceeds max RLP size: %d > %d", block.Number(), blockRlpSize, emt.ChainConfig.GetMaxRlpBlockSize(header.Time))
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

		// Record block and total difficulty.
		header := block.Header()
		emt.ephemeralBlocks[block.Hash()] = block
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
// Returns nil if the block passes all verifiable checks, or an error describing
// the invalidity. Used for expected-invalid blocks.
//
// Note: ephemeral mode cannot compute state root commitments or BAL from
// execution results. After execution passes, if the header requires BAL
// validation (BlockAccessListHash is set), an error is returned because the
// BAL content cannot be verified — this is the only remaining source of
// invalidity when all other checks pass.
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

	if err := emt.executeBlock(cr, block, throwaway, emt.ephemeralTxNum+1); err != nil {
		return err
	}

	// Execution passed — check for validations that ephemeral mode cannot perform.
	// BAL content validation requires VersionedIO (per-tx I/O tracking) which is
	// only available in the full staged-sync pipeline.
	if block.Header().BlockAccessListHash != nil {
		return fmt.Errorf("block #%v: BAL content validation requires full pipeline (ephemeral mode cannot compute BAL from execution)", block.Number())
	}

	return nil
}

// RecordEphemeralBlock records a block in the ephemeral chain reader's
// in-memory map without executing it. Used for side-chain blocks whose
// headers are needed for uncle verification.
func (emt *ExecModuleTester) RecordEphemeralBlock(block *types.Block) {
	emt.ephemeralBlocks[block.Hash()] = block
}

// EphemeralOverlay returns the MemoryMutation overlay for state reads.
func (emt *ExecModuleTester) EphemeralOverlay() *membatchwithdb.MemoryMutation {
	return emt.ephemeralOverlay
}

// EphemeralLastBlockHash returns the hash of the last successfully executed block.
func (emt *ExecModuleTester) EphemeralLastBlockHash() common.Hash {
	return emt.ephemeralLastHash
}

// EphemeralBlock returns a block from the ephemeral in-memory map, or nil.
func (emt *ExecModuleTester) EphemeralBlock(hash common.Hash) *types.Block {
	return emt.ephemeralBlocks[hash]
}
