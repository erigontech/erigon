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

package execmodule_test

import (
	"context"
	"sync/atomic"
	"testing"
	"time"

	"github.com/holiman/uint256"
	"github.com/stretchr/testify/require"

	"github.com/erigontech/erigon/common"
	"github.com/erigontech/erigon/db/dbservices"
	"github.com/erigontech/erigon/db/kv"
	"github.com/erigontech/erigon/db/kv/dbutils"
	"github.com/erigontech/erigon/db/rawdb"
	"github.com/erigontech/erigon/execution/chain"
	"github.com/erigontech/erigon/execution/execmodule"
	"github.com/erigontech/erigon/execution/execmodule/execmoduletester"
	"github.com/erigontech/erigon/execution/protocol/params"
	"github.com/erigontech/erigon/execution/tests/blockgen"
	"github.com/erigontech/erigon/execution/types"
)

func TestExecModule_GivenReorgPastFinalised_WhenFinality_ThenInvalidFCU(t *testing.T) {
	// in normal circumstances our MAX_REORG_DEPTH aligns with the depth of the finalised hash
	// (i.e. on ethereum we have T-96 finalised block in 99.999999% of the time and our MAX_REORG_DEPTH=96)
	// this test check that when in that we're in that scenario, and we get a FCU for a fork that goes beyond
	// the finalised number we return invalid fcu
	ctx, cancel := context.WithTimeout(t.Context(), time.Minute)
	defer cancel()
	const (
		e2RetireStepSize  = 10
		e3RetireStepSize  = 3 // 3 txns per block (1 + 2 system txns)
		maxReorgDepth     = 2
		chainLen          = e2RetireStepSize + maxReorgDepth + 1
		finalisedBlockNum = chainLen - maxReorgDepth // when normal finality matches our maxReorgDepth
		reorgBlock        = finalisedBlockNum - maxReorgDepth
	)
	emt := execmoduletester.New(
		t,
		execmoduletester.WithChainConfig(chain.AllProtocolChanges),
		execmoduletester.WithMaxReorgDepth(maxReorgDepth),
		execmoduletester.WithE2RetireStep(e2RetireStepSize),
		execmoduletester.WithStepSize(e3RetireStepSize),
	)
	require.NoError(t, emt.WaitForBlockRetirement(ctx))
	require.NoError(t, emt.WaitForStateRetirement(ctx))
	cp1, err := emt.GenerateChain(chainLen, func(i int, gen *blockgen.BlockGen) {
		tx, err := types.SignTx(
			types.NewTransaction(
				gen.TxNonce(emt.Address),
				common.Address{1},
				uint256.NewInt(10_000),
				params.TxGas,
				uint256.NewInt(emt.Genesis.BaseFee().Uint64()),
				nil,
			),
			*types.LatestSignerForChainID(emt.ChainConfig.ChainID),
			emt.Key,
		)
		require.NoError(t, err)
		gen.AddTx(tx)
	})
	require.NoError(t, err)
	cp2, err := emt.GenerateChain(chainLen, func(i int, gen *blockgen.BlockGen) {
		var to common.Address
		if i < reorgBlock {
			to = common.Address{1}
		} else {
			to = common.Address{2}
		}
		tx, err := types.SignTx(
			types.NewTransaction(
				gen.TxNonce(emt.Address),
				to,
				uint256.NewInt(10_000),
				params.TxGas,
				uint256.NewInt(emt.Genesis.BaseFee().Uint64()),
				nil,
			),
			*types.LatestSignerForChainID(emt.ChainConfig.ChainID),
			emt.Key,
		)
		require.NoError(t, err)
		gen.AddTx(tx)
	})
	require.NoError(t, err)
	safeHash := cp1.Blocks[finalisedBlockNum-1].Hash()
	finalisedHash := safeHash
	fcuOptSeq := make([][]execmoduletester.UFCOpt, chainLen)
	for i, h := range cp1.Headers {
		if h.Number.Uint64() <= uint64(maxReorgDepth) {
			fcuOptSeq[i] = []execmoduletester.UFCOpt{}
		} else {
			idx := min(i-maxReorgDepth, finalisedBlockNum-1)
			fcuOptSeq[i] = []execmoduletester.UFCOpt{
				execmoduletester.WithSafeHash(cp1.Headers[idx].Hash()),
				execmoduletester.WithFinalisedHash(cp1.Headers[idx].Hash()),
			}
		}
	}
	// chain 1 block insert + fcu with head=T', safe=T'-2, finalised=T'-2
	err = emt.InsertValidateAndUfc1By1(
		ctx,
		cp1.Blocks,
		execmoduletester.WithFcuOptSeq(fcuOptSeq),
		execmoduletester.WithWaitForBlockRetirement(),
		execmoduletester.WithWaitForStateFiles(),
	)
	require.NoError(t, err)
	// chain 2 block insert + fcu with head=T'', safe=T'-2, finalised=T'-2, reorgPoint=T'-4, maxReorgDepth=2
	status, err := emt.InsertBlocks(ctx, cp2.Blocks[reorgBlock-1:])
	require.NoError(t, err)
	require.Equal(t, execmodule.ExecutionStatusSuccess, status)
	result, err := emt.UpdateForkChoice(
		ctx,
		cp2.TopBlock.Header(),
		execmoduletester.WithSafeHash(safeHash),
		execmoduletester.WithFinalisedHash(finalisedHash),
	)
	require.NoError(t, err)
	require.Equal(t, execmodule.ExecutionStatusInvalidForkchoice, result.Status)
	// also check that we didnt create any snapshot files for non-finalised blocks
	err = emt.BlockSnapshots.OpenFolder()
	require.NoError(t, err)
	require.Equal(t, uint64(9), emt.BlockSnapshots.BlocksAvailable())
	tx, err := emt.DB.BeginTemporalRo(ctx)
	require.NoError(t, err)
	defer tx.Rollback()
	require.Equal(t, uint64(33), tx.Debug().TxNumsInFiles(kv.CommitmentDomain))
}

// A reorg that unwinds the stored finalized block is refused. It is a too deep reorg when the requested
// safe and finalized blocks are on the new head's chain, and an invalid fork choice when one is not.
func TestExecModule_GivenReorgBelowFinalisedBlock_WhenFinality_ThenReorgTooDeep(t *testing.T) {
	for _, tc := range []struct {
		name            string
		blocks          int    // canonical and fork blocks above the shared prefix; the canonical first one is finalized
		safe, finalised string // requested block: "fork" tip, finalized "canonical" block or "prefix" tip
		want            execmodule.ExecutionStatus
	}{
		{name: "finalized at the fork tip height", blocks: 1, safe: "fork", finalised: "fork", want: execmodule.ExecutionStatusReorgTooDeep},
		{name: "finalized above the fork point", blocks: 2, safe: "fork", finalised: "prefix", want: execmodule.ExecutionStatusReorgTooDeep},
		{name: "safe off the new chain", blocks: 1, safe: "canonical", finalised: "fork", want: execmodule.ExecutionStatusInvalidForkchoice},
		{name: "finalized off the new chain", blocks: 1, safe: "fork", finalised: "canonical", want: execmodule.ExecutionStatusInvalidForkchoice},
	} {
		t.Run(tc.name, func(t *testing.T) {
			ctx, cancel := context.WithTimeout(t.Context(), time.Minute)
			defer cancel()
			emt := execmoduletester.New(t, execmoduletester.WithChainConfig(chain.AllProtocolChanges))
			prefix, err := emt.GenerateChain(2, nil)
			require.NoError(t, err)
			require.NoError(t, emt.InsertValidateAndUfc1By1(ctx, prefix.Blocks))
			canonical, err := emt.GenerateChainFrom(prefix.TopBlock, tc.blocks, func(_ int, gen *blockgen.BlockGen) {
				gen.SetCoinbase(common.Address{1})
			})
			require.NoError(t, err)
			fork, err := emt.GenerateChainFrom(prefix.TopBlock, tc.blocks, func(_ int, gen *blockgen.BlockGen) {
				gen.SetCoinbase(common.Address{2})
			})
			require.NoError(t, err)
			require.NotEqual(t, canonical.TopBlock.Hash(), fork.TopBlock.Hash())
			canonicalHash, finalisedHash := canonical.TopBlock.Hash(), canonical.Blocks[0].Hash()
			fcuOpts := make([][]execmoduletester.UFCOpt, tc.blocks)
			fcuOpts[tc.blocks-1] = []execmoduletester.UFCOpt{
				execmoduletester.WithSafeHash(finalisedHash),
				execmoduletester.WithFinalisedHash(finalisedHash),
			}
			require.NoError(t, emt.InsertValidateAndUfc1By1(ctx, canonical.Blocks, execmoduletester.WithFcuOptSeq(fcuOpts)))
			status, err := emt.InsertBlocks(ctx, fork.Blocks)
			require.NoError(t, err)
			require.Equal(t, execmodule.ExecutionStatusSuccess, status)
			validation, err := emt.ValidateChain(ctx, fork.TopBlock)
			require.NoError(t, err)
			require.Equal(t, execmodule.ExecutionStatusSuccess, validation.ValidationStatus)
			requested := map[string]common.Hash{"fork": fork.TopBlock.Hash(), "canonical": finalisedHash, "prefix": prefix.TopBlock.Hash()}
			result, err := emt.UpdateForkChoice(ctx, fork.TopBlock.Header(),
				execmoduletester.WithSafeHash(namedHash(t, requested, tc.safe)),
				execmoduletester.WithFinalisedHash(namedHash(t, requested, tc.finalised)))
			require.NoError(t, err)
			require.Equal(t, tc.want, result.Status)
			forkchoice, err := emt.ExecModule.GetForkChoice(ctx)
			require.NoError(t, err)
			require.Equal(t, canonicalHash, forkchoice.HeadHash)
			require.Equal(t, finalisedHash, forkchoice.SafeHash)
			require.Equal(t, finalisedHash, forkchoice.FinalizedHash)
			require.NoError(t, emt.DB.View(ctx, func(tx kv.Tx) error {
				canonicalHashAfterReorg, err := rawdb.ReadCanonicalHash(tx, canonical.TopBlock.NumberU64())
				require.NoError(t, err)
				require.Equal(t, canonicalHash, canonicalHashAfterReorg)
				return nil
			}))
		})
	}
}

// Retirement can delete part of a side chain. A missing side block at or below the stored finalized height is not
// downloaded: the reorg would cross finality anyway. Only one above it is still reported as missing.
func TestExecModule_GivenMissingSideBlock_WhenReorgBelowFinalisedBlock_ThenRefusedWithoutDownload(t *testing.T) {
	// The fork point (height 2) lies under the missing block and is unknown to the fork choice, so a requested
	// block under the missing one, canonical or unknown, may be on the new chain. Only a known block at or above
	// the missing height that is not on it makes the request inconsistent.
	for _, tc := range []struct {
		name            string
		missing         int    // index of the deleted fork block; the canonical second block (height 4) is finalized
		safe, finalized string // "fork tip", "zero", "stored finalized", "fork point" or "unknown"
		want            execmodule.ExecutionStatus
	}{
		{name: "below the finalized block", missing: 0, safe: "fork tip", finalized: "fork tip", want: execmodule.ExecutionStatusReorgTooDeep},
		{name: "below the finalized block, zero safe and finalized", missing: 0, safe: "zero", finalized: "zero", want: execmodule.ExecutionStatusReorgTooDeep},
		{name: "below the finalized block, finalized off the new chain", missing: 0, safe: "fork tip", finalized: "stored finalized", want: execmodule.ExecutionStatusInvalidForkchoice},
		{name: "below the finalized block, finalized at the fork point", missing: 0, safe: "fork tip", finalized: "fork point", want: execmodule.ExecutionStatusReorgTooDeep},
		{name: "below the finalized block, unknown finalized", missing: 0, safe: "fork tip", finalized: "unknown", want: execmodule.ExecutionStatusReorgTooDeep},
		{name: "at the finalized height", missing: 1, safe: "fork tip", finalized: "fork tip", want: execmodule.ExecutionStatusReorgTooDeep},
		{name: "at the finalized height, finalized at the fork point", missing: 1, safe: "fork tip", finalized: "fork point", want: execmodule.ExecutionStatusReorgTooDeep},
		{name: "at the finalized height, finalized off the new chain", missing: 1, safe: "fork tip", finalized: "stored finalized", want: execmodule.ExecutionStatusInvalidForkchoice},
		{name: "at the finalized height, safe off the new chain", missing: 1, safe: "stored finalized", finalized: "fork point", want: execmodule.ExecutionStatusInvalidForkchoice},
		{name: "above the finalized block", missing: 2, safe: "fork tip", finalized: "fork tip", want: execmodule.ExecutionStatusMissingSegment},
	} {
		t.Run(tc.name, func(t *testing.T) {
			ctx, cancel := context.WithTimeout(t.Context(), time.Minute)
			defer cancel()
			emt := execmoduletester.New(t, execmoduletester.WithChainConfig(chain.AllProtocolChanges))
			prefix, err := emt.GenerateChain(2, nil)
			require.NoError(t, err)
			require.NoError(t, emt.InsertValidateAndUfc1By1(ctx, prefix.Blocks))
			canonical, err := emt.GenerateChainFrom(prefix.TopBlock, 3, func(_ int, gen *blockgen.BlockGen) {
				gen.SetCoinbase(common.Address{1})
			})
			require.NoError(t, err)
			fork, err := emt.GenerateChainFrom(prefix.TopBlock, 4, func(_ int, gen *blockgen.BlockGen) {
				gen.SetCoinbase(common.Address{2})
			})
			require.NoError(t, err)
			finalisedHash := canonical.Blocks[1].Hash()
			require.NoError(t, emt.InsertValidateAndUfc1By1(ctx, canonical.Blocks[:2]))
			// The next fork choice commits the inserted fork blocks, so the deleted header is really gone.
			status, err := emt.InsertBlocks(ctx, fork.Blocks)
			require.NoError(t, err)
			require.Equal(t, execmodule.ExecutionStatusSuccess, status)
			require.NoError(t, emt.InsertValidateAndUfc1By1(ctx, canonical.Blocks[2:], execmoduletester.WithFcuOptSeq([][]execmoduletester.UFCOpt{
				{execmoduletester.WithSafeHash(finalisedHash), execmoduletester.WithFinalisedHash(finalisedHash)},
			})))
			missing := fork.Blocks[tc.missing]
			require.NoError(t, emt.DB.Update(ctx, func(tx kv.RwTx) error {
				rawdb.DeleteHeader(tx, missing.Hash(), missing.NumberU64())
				return nil
			}))
			require.NoError(t, emt.DB.View(ctx, func(tx kv.Tx) error {
				header, err := emt.BlockReader.Header(ctx, tx, missing.Hash(), missing.NumberU64())
				require.NoError(t, err)
				require.Nil(t, header)
				return nil
			}))
			hashes := map[string]common.Hash{
				"fork tip": fork.TopBlock.Hash(), "zero": {}, "stored finalized": finalisedHash,
				"fork point": prefix.TopBlock.Hash(), "unknown": common.HexToHash("0x01"),
			}
			result, err := emt.UpdateForkChoice(ctx, fork.TopBlock.Header(),
				execmoduletester.WithSafeHash(namedHash(t, hashes, tc.safe)),
				execmoduletester.WithFinalisedHash(namedHash(t, hashes, tc.finalized)))
			require.NoError(t, err)
			require.Equal(t, tc.want, result.Status)
			forkchoice, err := emt.ExecModule.GetForkChoice(ctx)
			require.NoError(t, err)
			require.Equal(t, canonical.TopBlock.Hash(), forkchoice.HeadHash)
			require.Equal(t, finalisedHash, forkchoice.FinalizedHash)
		})
	}
}

// A head whose chain goes down to a block at height 0 other than this chain's genesis is refused as an invalid fork
// choice, whether or not a block is finalized.
func TestExecModule_GivenHeadOnOtherGenesis_WhenUpdateForkChoice_ThenInvalidForkchoice(t *testing.T) {
	for _, tc := range []struct {
		name      string
		finalised bool // finalize the second of the three canonical blocks
	}{
		{name: "finalized block", finalised: true},
		{name: "nothing finalized", finalised: false},
	} {
		t.Run(tc.name, func(t *testing.T) {
			ctx, cancel := context.WithTimeout(t.Context(), time.Minute)
			defer cancel()
			emt := execmoduletester.New(t, execmoduletester.WithChainConfig(chain.AllProtocolChanges))
			canonical, err := emt.GenerateChain(3, nil)
			require.NoError(t, err)
			var fcuOpts [][]execmoduletester.UFCOpt
			if tc.finalised {
				finalisedHash := canonical.Blocks[1].Hash()
				fcuOpts = [][]execmoduletester.UFCOpt{nil, nil, {execmoduletester.WithSafeHash(finalisedHash), execmoduletester.WithFinalisedHash(finalisedHash)}}
			}
			require.NoError(t, emt.InsertValidateAndUfc1By1(ctx, canonical.Blocks, execmoduletester.WithFcuOptSeq(fcuOpts)))
			otherGenesisHeader := types.CopyHeader(emt.Genesis.Header())
			otherGenesisHeader.Extra = []byte("other genesis")
			otherGenesis := types.NewBlockWithHeader(otherGenesisHeader, nil)
			childHeader := types.CopyHeader(canonical.Blocks[0].Header())
			childHeader.ParentHash = otherGenesis.Hash()
			child := types.NewBlockWithHeader(childHeader, nil)
			status, err := emt.InsertBlocks(ctx, []*types.Block{otherGenesis, child})
			require.NoError(t, err)
			require.Equal(t, execmodule.ExecutionStatusSuccess, status)
			result, err := emt.UpdateForkChoice(ctx, child.Header())
			require.NoError(t, err)
			require.Equal(t, execmodule.ExecutionStatusInvalidForkchoice, result.Status)
			forkchoice, err := emt.ExecModule.GetForkChoice(ctx)
			require.NoError(t, err)
			require.Equal(t, canonical.TopBlock.Hash(), forkchoice.HeadHash)
		})
	}
}

// The total difficulty of old canonical blocks is pruned (PRUNE_TOTAL_DIFFICULTY). The walk still stops at such a
// fork point instead of going down to genesis, and canonical blocks below it still count as on the new chain.
func TestExecModule_GivenForkPointWithPrunedTD_WhenReorgBelowFinalisedBlock_ThenWalkStopsAtForkPoint(t *testing.T) {
	ctx, cancel := context.WithTimeout(t.Context(), time.Minute)
	defer cancel()
	counter := &headerReadCounter{}
	emt := execmoduletester.New(t, execmoduletester.WithChainConfig(chain.AllProtocolChanges),
		execmoduletester.WithBlockReaderWrap(func(br dbservices.FullBlockReader) dbservices.FullBlockReader {
			counter.FullBlockReader = br
			return counter
		}))
	prefix, err := emt.GenerateChain(10, nil)
	require.NoError(t, err)
	require.NoError(t, emt.InsertValidateAndUfc1By1(ctx, prefix.Blocks))
	canonical, err := emt.GenerateChainFrom(prefix.TopBlock, 4, func(_ int, gen *blockgen.BlockGen) {
		gen.SetCoinbase(common.Address{1})
	})
	require.NoError(t, err)
	fork, err := emt.GenerateChainFrom(prefix.TopBlock, 2, func(_ int, gen *blockgen.BlockGen) {
		gen.SetCoinbase(common.Address{2})
	})
	require.NoError(t, err)
	finalisedHash := canonical.Blocks[2].Hash()
	require.NoError(t, emt.InsertValidateAndUfc1By1(ctx, canonical.Blocks, execmoduletester.WithFcuOptSeq([][]execmoduletester.UFCOpt{
		nil, nil, nil, {execmoduletester.WithSafeHash(finalisedHash), execmoduletester.WithFinalisedHash(finalisedHash)},
	})))
	status, err := emt.InsertBlocks(ctx, fork.Blocks)
	require.NoError(t, err)
	require.Equal(t, execmodule.ExecutionStatusSuccess, status)
	require.NoError(t, emt.DB.Update(ctx, func(tx kv.RwTx) error {
		for _, block := range prefix.Blocks {
			if err := tx.Delete(kv.HeaderTD, dbutils.HeaderKey(block.NumberU64(), block.Hash())); err != nil {
				return err
			}
		}
		return nil
	}))
	forkPoint := prefix.TopBlock.NumberU64()
	counter.maxHeight.Store(&forkPoint)
	result, err := emt.UpdateForkChoice(ctx, fork.TopBlock.Header(),
		execmoduletester.WithSafeHash(prefix.Blocks[4].Hash()),
		execmoduletester.WithFinalisedHash(prefix.TopBlock.Hash()))
	counter.maxHeight.Store(nil)
	require.NoError(t, err)
	require.Equal(t, execmodule.ExecutionStatusReorgTooDeep, result.Status)
	require.Zero(t, counter.reads.Load(), "the walk read headers at or below the fork point")
	forkchoice, err := emt.ExecModule.GetForkChoice(ctx)
	require.NoError(t, err)
	require.Equal(t, canonical.TopBlock.Hash(), forkchoice.HeadHash)
}

// headerReadCounter counts the header reads at or below maxHeight while it is set.
type headerReadCounter struct {
	dbservices.FullBlockReader
	maxHeight atomic.Pointer[uint64]
	reads     atomic.Int32
}

func (r *headerReadCounter) Header(ctx context.Context, tx kv.Getter, hash common.Hash, blockNum uint64) (*types.Header, error) {
	if h := r.maxHeight.Load(); h != nil && blockNum <= *h {
		r.reads.Add(1)
	}
	return r.FullBlockReader.Header(ctx, tx, hash, blockNum)
}

// namedHash returns the hash a test case names, failing the test for a name the table does not define.
func namedHash(t *testing.T, hashes map[string]common.Hash, name string) common.Hash {
	t.Helper()
	hash, ok := hashes[name]
	require.True(t, ok, "unknown block name %q", name)
	return hash
}

func TestExecModule_GivenSameHeadFCULowersFinalised_WhenReorgAboveIt_ThenReorg(t *testing.T) {
	// A finalized import can put the stored checkpoint ahead of the CL's.
	// Reorg checks must use the CL's later update, even if it did not move the head.
	ctx := t.Context()
	const (
		chainLen          = 20
		finalisedBlockNum = 2
		forkBlockNum      = 10
	)
	emt := execmoduletester.New(t, execmoduletester.WithChainConfig(chain.AllProtocolChanges))
	canonical, err := emt.GenerateChain(chainLen, nil)
	require.NoError(t, err)
	status, err := emt.InsertBlocks(ctx, canonical.Blocks)
	require.NoError(t, err)
	require.Equal(t, execmodule.ExecutionStatusSuccess, status)
	tipHash := canonical.TopBlock.Hash()
	result, err := emt.UpdateForkChoice(ctx, canonical.TopBlock.Header(),
		execmoduletester.WithSafeHash(tipHash),
		execmoduletester.WithFinalisedHash(tipHash))
	require.NoError(t, err)
	require.Equal(t, execmodule.ExecutionStatusSuccess, result.Status)
	emt.ExecModule.Drain()
	assertPersistedForkchoice(t, emt.DB, tipHash, tipHash, tipHash)

	finalisedHash := canonical.Blocks[finalisedBlockNum-1].Hash()
	result, err = emt.UpdateForkChoice(ctx, canonical.TopBlock.Header(),
		execmoduletester.WithSafeHash(finalisedHash),
		execmoduletester.WithFinalisedHash(finalisedHash))
	require.NoError(t, err)
	require.Equal(t, execmodule.ExecutionStatusSuccess, result.Status)
	emt.ExecModule.Drain()
	assertPersistedForkchoice(t, emt.DB, tipHash, finalisedHash, finalisedHash)

	fork, err := emt.GenerateChainFrom(canonical.Blocks[forkBlockNum-1], chainLen-forkBlockNum, func(_ int, gen *blockgen.BlockGen) {
		gen.SetCoinbase(common.Address{1})
	})
	require.NoError(t, err)
	require.NotEqual(t, tipHash, fork.TopBlock.Hash())
	status, err = emt.InsertBlocks(ctx, fork.Blocks)
	require.NoError(t, err)
	require.Equal(t, execmodule.ExecutionStatusSuccess, status)
	validation, err := emt.ValidateChain(ctx, fork.TopBlock)
	require.NoError(t, err)
	require.Equal(t, execmodule.ExecutionStatusSuccess, validation.ValidationStatus)
	result, err = emt.UpdateForkChoice(ctx, fork.TopBlock.Header(),
		execmoduletester.WithSafeHash(finalisedHash),
		execmoduletester.WithFinalisedHash(finalisedHash))
	require.NoError(t, err)
	require.Equal(t, execmodule.ExecutionStatusSuccess, result.Status, "reorg above the updated finalized block must succeed")
	emt.ExecModule.Drain()
	assertPersistedForkchoice(t, emt.DB, fork.TopBlock.Hash(), finalisedHash, finalisedHash)
	require.NoError(t, emt.DB.View(ctx, func(tx kv.Tx) error {
		canonicalHash, err := rawdb.ReadCanonicalHash(tx, fork.TopBlock.NumberU64())
		require.NoError(t, err)
		require.Equal(t, fork.TopBlock.Hash(), canonicalHash)
		return nil
	}))
}

func TestExecModule_GivenReorgPastMaxReorgDepth_WhenNonFinality_ThenReorg(t *testing.T) {
	// in normal circumstances our MAX_REORG_DEPTH aligns with the depth of the finalised hash
	// (i.e. on ethereum we have T-96 finalised block in 99.999999% of the time and our MAX_REORG_DEPTH=96)
	// this test check that when in the highly unlikely scenario of non-finality we support long reorgs
	// longer than MAX_REORG_DEPTH but not beyond the last finalised hash.
	ctx, cancel := context.WithTimeout(t.Context(), time.Minute)
	defer cancel()
	const (
		e2RetireStepSize  = 10
		e3RetireStepSize  = 3 // 3 txns per block (1 + 2 system txns)
		maxReorgDepth     = 2
		chainLen          = e2RetireStepSize + maxReorgDepth + 1
		finalisedBlockNum = chainLen - 3*maxReorgDepth // when non-finality exceeds our MAX_REORG_DEPTH
		reorgBlock        = finalisedBlockNum + 2      // and reorg block is still a descendant of the finalised block
	)
	emt := execmoduletester.New(
		t,
		execmoduletester.WithChainConfig(chain.AllProtocolChanges),
		execmoduletester.WithMaxReorgDepth(maxReorgDepth),
		execmoduletester.WithE2RetireStep(e2RetireStepSize),
		execmoduletester.WithStepSize(e3RetireStepSize),
	)
	require.NoError(t, emt.WaitForBlockRetirement(ctx))
	require.NoError(t, emt.WaitForStateRetirement(ctx))
	cp1, err := emt.GenerateChain(chainLen, func(i int, gen *blockgen.BlockGen) {
		tx, err := types.SignTx(
			types.NewTransaction(
				gen.TxNonce(emt.Address),
				common.Address{1},
				uint256.NewInt(10_000),
				params.TxGas,
				uint256.NewInt(emt.Genesis.BaseFee().Uint64()),
				nil,
			),
			*types.LatestSignerForChainID(emt.ChainConfig.ChainID),
			emt.Key,
		)
		require.NoError(t, err)
		gen.AddTx(tx)
	})
	require.NoError(t, err)
	cp2, err := emt.GenerateChain(chainLen, func(i int, gen *blockgen.BlockGen) {
		var to common.Address
		if i+1 < reorgBlock {
			to = common.Address{1}
		} else {
			to = common.Address{2}
		}
		nonce := gen.TxNonce(emt.Address)
		tx, err := types.SignTx(
			types.NewTransaction(
				nonce,
				to,
				uint256.NewInt(10_000),
				params.TxGas,
				uint256.NewInt(emt.Genesis.BaseFee().Uint64()),
				nil,
			),
			*types.LatestSignerForChainID(emt.ChainConfig.ChainID),
			emt.Key,
		)
		require.NoError(t, err)
		gen.AddTx(tx)
	})
	require.NoError(t, err)
	safeHash := cp1.Blocks[finalisedBlockNum-1].Hash()
	finalisedHash := safeHash
	fcuOptSeq := make([][]execmoduletester.UFCOpt, len(cp1.Blocks))
	for i, h := range cp1.Headers {
		if h.Number.Uint64() <= uint64(maxReorgDepth) {
			fcuOptSeq[i] = []execmoduletester.UFCOpt{}
		} else {
			idx := min(i-maxReorgDepth, finalisedBlockNum-1)
			fcuOptSeq[i] = []execmoduletester.UFCOpt{
				execmoduletester.WithSafeHash(cp1.Headers[idx].Hash()),
				execmoduletester.WithFinalisedHash(cp1.Headers[idx].Hash()),
			}
		}
	}
	// chain 1 block insert + fcu with head=T', safe=T'-6, finalised=T'-6
	err = emt.InsertValidateAndUfc1By1(
		ctx,
		cp1.Blocks,
		execmoduletester.WithFcuOptSeq(fcuOptSeq),
		execmoduletester.WithWaitForBlockRetirement(),
		execmoduletester.WithWaitForStateFiles(),
	)
	require.NoError(t, err)
	// chain 2 block insert + fcu with head=T'', safe=T'-6, finalised=T'-6, reorgPoint=T'-4, maxReorgDepth=2
	status, err := emt.InsertBlocks(ctx, cp2.Blocks[reorgBlock-1:])
	require.NoError(t, err)
	require.Equal(t, execmodule.ExecutionStatusSuccess, status)
	result, err := emt.UpdateForkChoice(
		ctx,
		cp2.TopBlock.Header(),
		execmoduletester.WithSafeHash(safeHash),
		execmoduletester.WithFinalisedHash(finalisedHash),
	)
	require.NoError(t, err)
	require.Equal(t, execmodule.ExecutionStatusSuccess, result.Status)
	// also check that we didnt create any snapshot files for non-finalised blocks
	require.NoError(t, emt.WaitForBlockRetirement(ctx))
	require.NoError(t, emt.WaitForStateRetirement(ctx))
	err = emt.BlockSnapshots.OpenFolder()
	require.NoError(t, err)
	require.Equal(t, uint64(0), emt.BlockSnapshots.BlocksAvailable())
	tx, err := emt.DB.BeginTemporalRo(ctx)
	require.NoError(t, err)
	defer tx.Rollback()
	require.Equal(t, uint64(21), tx.Debug().TxNumsInFiles(kv.CommitmentDomain))
}
