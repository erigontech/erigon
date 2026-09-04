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
	"testing"
	"time"

	"github.com/holiman/uint256"
	"github.com/stretchr/testify/require"

	"github.com/erigontech/erigon/common"
	"github.com/erigontech/erigon/db/kv"
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

func TestExecModule_GivenReorgAtFinalisedBlock_WhenFinality_ThenInvalidFCU(t *testing.T) {
	ctx, cancel := context.WithTimeout(t.Context(), time.Minute)
	defer cancel()
	emt := execmoduletester.New(t, execmoduletester.WithChainConfig(chain.AllProtocolChanges))
	prefix, err := emt.GenerateChain(2, nil)
	require.NoError(t, err)
	require.NoError(t, emt.InsertValidateAndUfc1By1(ctx, prefix.Blocks))
	canonical, err := emt.GenerateChainFrom(prefix.TopBlock, 1, func(_ int, gen *blockgen.BlockGen) {
		gen.SetCoinbase(common.Address{1})
	})
	require.NoError(t, err)
	fork, err := emt.GenerateChainFrom(prefix.TopBlock, 1, func(_ int, gen *blockgen.BlockGen) {
		gen.SetCoinbase(common.Address{2})
	})
	require.NoError(t, err)
	require.NotEqual(t, canonical.TopBlock.Hash(), fork.TopBlock.Hash())
	canonicalHash := canonical.TopBlock.Hash()
	require.NoError(t, emt.InsertValidateAndUfc1By1(ctx, canonical.Blocks, execmoduletester.WithFcuOptSeq([][]execmoduletester.UFCOpt{{
		execmoduletester.WithSafeHash(canonicalHash),
		execmoduletester.WithFinalisedHash(canonicalHash),
	}})))
	status, err := emt.InsertBlocks(ctx, fork.Blocks)
	require.NoError(t, err)
	require.Equal(t, execmodule.ExecutionStatusSuccess, status)
	validation, err := emt.ValidateChain(ctx, fork.TopBlock.Header())
	require.NoError(t, err)
	require.Equal(t, execmodule.ExecutionStatusSuccess, validation.ValidationStatus)
	forkHash := fork.TopBlock.Hash()
	result, err := emt.UpdateForkChoice(ctx, fork.TopBlock.Header(), execmoduletester.WithSafeHash(forkHash), execmoduletester.WithFinalisedHash(forkHash))
	require.NoError(t, err)
	require.Equal(t, execmodule.ExecutionStatusInvalidForkchoice, result.Status)
	forkchoice, err := emt.ExecModule.GetForkChoice(ctx)
	require.NoError(t, err)
	require.Equal(t, canonicalHash, forkchoice.HeadHash)
	require.Equal(t, canonicalHash, forkchoice.SafeHash)
	require.Equal(t, canonicalHash, forkchoice.FinalizedHash)
	require.NoError(t, emt.DB.View(ctx, func(tx kv.Tx) error {
		canonicalHashAfterReorg, err := rawdb.ReadCanonicalHash(tx, canonical.TopBlock.NumberU64())
		require.NoError(t, err)
		require.Equal(t, canonicalHash, canonicalHashAfterReorg)
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
