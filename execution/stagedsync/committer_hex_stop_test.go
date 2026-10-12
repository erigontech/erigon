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

package stagedsync

import (
	"context"
	"errors"
	"testing"

	"github.com/holiman/uint256"
	"github.com/stretchr/testify/require"

	"github.com/erigontech/erigon/common"
	"github.com/erigontech/erigon/common/log/v3"
	"github.com/erigontech/erigon/db/dbservices"
	"github.com/erigontech/erigon/db/kv"
	"github.com/erigontech/erigon/db/rawdb"
	dbstate "github.com/erigontech/erigon/db/state"
	"github.com/erigontech/erigon/execution/chain"
	"github.com/erigontech/erigon/execution/commitment"
	"github.com/erigontech/erigon/execution/stagedsync/stages"
	"github.com/erigontech/erigon/execution/types"
	"github.com/erigontech/erigon/execution/types/accounts"
)

func TestHexShadowStopWindowKeepsActivationWindow(t *testing.T) {
	const activationBlock = 3
	const maxReorgDepth = 2
	require.False(t, shouldStopHexShadow(activationBlock, maxReorgDepth, activationBlock+maxReorgDepth), "hex shadow must fold through the reorg window")
	require.True(t, shouldStopHexShadow(activationBlock, maxReorgDepth, activationBlock+maxReorgDepth+1), "hex shadow must stop after the reorg window")
}

func TestCommitmentCalculatorStopsHexShadowAfterWindow(t *testing.T) {
	activationTime := uint64(10)
	cc := &commitmentCalculator{
		chainConfig:   &chain.Config{BinaryTrieTime: &activationTime},
		maxReorgDepth: 2,
	}
	cc.recordBlockTarget(commitTarget{blockNum: 9, blockTime: activationTime - 1})
	for blockNum := uint64(10); blockNum <= 12; blockNum++ {
		cc.recordBlockTarget(commitTarget{blockNum: blockNum, blockTime: activationTime})
		cc.stopHexShadowAtWindow(t.Context(), commitTarget{blockNum: blockNum, blockTime: activationTime})
		require.False(t, cc.ShadowDomainStopped(kv.CommitmentDomain), "hex shadow must fold through block %d", blockNum)
	}
	cc.stopHexShadowAtWindow(t.Context(), commitTarget{blockNum: 13, blockTime: activationTime})
	require.True(t, cc.ShadowDomainStopped(kv.CommitmentDomain), "hex shadow must stop on block 13")
}

func TestCommitmentCalculatorUsesCurrentBranchHeaderForActivation(t *testing.T) {
	activationTime := uint64(10)
	cc := &commitmentCalculator{
		chainConfig:   &chain.Config{BinaryTrieTime: &activationTime},
		blockReader:   uncommittedActivationHeaderReader{},
		maxReorgDepth: 1,
	}
	cc.stopHexShadowAtWindow(t.Context(), commitTarget{blockNum: 7, blockTime: activationTime})
	require.True(t, cc.ShadowDomainStopped(kv.CommitmentDomain), "hex shadow must stop at the first block after the activation window")
}

func TestCommitmentCalculatorKeepsHexWhenActivationIsUnresolved(t *testing.T) {
	activationTime := uint64(10)
	cc := &commitmentCalculator{
		chainConfig:   &chain.Config{BinaryTrieTime: &activationTime},
		blockReader:   missingActivationHeaderReader{},
		maxReorgDepth: 1,
	}
	cc.stopHexShadowAtWindow(t.Context(), commitTarget{blockNum: 7, blockTime: activationTime})
	require.False(t, cc.ShadowDomainStopped(kv.CommitmentDomain), "an unresolved activation must not stop the hex shadow")
}

func TestCommitmentCalculatorResolvesActivationFromBatchTargets(t *testing.T) {
	activationTime := uint64(10)
	cc := &commitmentCalculator{
		chainConfig:   &chain.Config{BinaryTrieTime: &activationTime},
		blockReader:   missingActivationHeaderReader{},
		maxReorgDepth: 2,
	}
	for blockNum := uint64(2); blockNum <= 4; blockNum++ {
		cc.recordBlockTarget(commitTarget{blockNum: blockNum, blockTime: 0})
	}
	for blockNum := uint64(5); blockNum <= 8; blockNum++ {
		cc.recordBlockTarget(commitTarget{blockNum: blockNum, blockTime: activationTime})
		cc.stopHexShadowAtWindow(t.Context(), commitTarget{blockNum: blockNum, blockTime: activationTime})
	}
	require.Equal(t, uint64(5), cc.activationBlock)
	require.True(t, cc.ShadowDomainStopped(kv.CommitmentDomain), "the crossing target must resolve the activation block")
}

func TestCommitmentCalculatorSearchesBelowRestartBlock(t *testing.T) {
	activationTime := uint64(10)
	cc := &commitmentCalculator{
		chainConfig:   &chain.Config{BinaryTrieTime: &activationTime},
		blockReader:   hexStopHeaderReader{},
		firstBlockNum: 7,
		hasFirstBlock: true,
		maxReorgDepth: 2,
	}
	cc.stopHexShadowAtWindow(t.Context(), commitTarget{blockNum: 7, blockTime: activationTime})
	cc.stopHexShadowAtWindow(t.Context(), commitTarget{blockNum: 8, blockTime: activationTime})
	require.Equal(t, uint64(5), cc.activationBlock)
	require.True(t, cc.ShadowDomainStopped(kv.CommitmentDomain), "a restart inside the window must retain the fork activation")
}

func TestCommitmentCalculatorDiscardsLocalStopAfterRejectedBlock(t *testing.T) {
	cc := &commitmentCalculator{}
	cc.stopShadowDomain(kv.CommitmentDomain)
	require.True(t, cc.ShadowDomainStopped(kv.CommitmentDomain))
	cc.handleMessage(t.Context(), &blockResult{Err: errors.New("rejected")})
	require.False(t, cc.ShadowDomainStopped(kv.CommitmentDomain), "a rejected block must discard its local shadow stop")
}

func TestCommitmentCalculatorStopsFoldingHexAtWindowEnd(t *testing.T) {
	db, tx, doms := dualCalculatorTest(t)
	roTx, err := db.BeginTemporalRo(t.Context())
	require.NoError(t, err)
	t.Cleanup(roTx.Rollback)
	doms.SetInMemHistoryReads(true)
	doms.SetDisableInlineTouchKey(true)
	activationTime := uint64(10)
	cc := &commitmentCalculator{
		doms:          doms,
		db:            db,
		roTx:          roTx,
		chainConfig:   &chain.Config{BinaryTrieTime: &activationTime},
		maxReorgDepth: 2,
	}
	address := common.Address{0x42}
	reader := &asOfStateReader{sd: doms, roTx: roTx, commitmentDomain: kv.CommitmentDomain}
	var roots [4][]byte
	cc.recordBlockTarget(commitTarget{blockNum: 9, blockTime: activationTime - 1})
	for blockNum := uint64(10); blockNum <= 13; blockNum++ {
		cc.recordBlockTarget(commitTarget{blockNum: blockNum, blockTime: activationTime})
		account := accounts.Account{Nonce: blockNum, Balance: *uint256.NewInt(blockNum), CodeHash: accounts.EmptyCodeHash}
		accountBytes := accounts.SerialiseV3(&account)
		previous, _, err := doms.GetLatest(kv.AccountsDomain, tx, address[:])
		require.NoError(t, err)
		require.NoError(t, doms.DomainPut(kv.AccountsDomain, tx, address[:], accountBytes, blockNum, previous))
		updates := commitment.NewUpdates(commitment.ModeUpdate, t.TempDir(), commitment.KeyToHexNibbleHash)
		updates.TouchPlainKey(string(address[:]), accountBytes, updates.TouchAccount)
		feed := dualHexFeed(address[:], commitment.Update{Flags: commitment.BalanceUpdate | commitment.NonceUpdate | commitment.CodeUpdate, Nonce: blockNum})
		result, err := cc.computeDualFromUpdatesWithRole(t.Context(), commitTarget{blockNum: blockNum, lastTxNum: blockNum, blockTime: activationTime}, updates, reader,
			doms.GetCommitmentCtxForDomain(kv.CommitmentDomain), doms.GetCommitmentCtxForDomain(kv.CommitmentBinDomain), feed, nil)
		updates.Close()
		require.NoError(t, err)
		require.NotEmpty(t, result.canonicalRoot)
		roots[blockNum-10], err = doms.GetCommitmentCtxForDomain(kv.CommitmentDomain).Trie().RootHash()
		require.NoError(t, err)
	}
	require.NotEqual(t, roots[0], roots[1], "the hex shadow must fold block 11")
	require.NotEqual(t, roots[1], roots[2], "the hex shadow must fold through block 12")
	require.Equal(t, roots[2], roots[3], "the hex shadow must not fold block 13")
	require.True(t, cc.ShadowDomainStopped(kv.CommitmentDomain), "the hex shadow must be stopped at the window boundary")
}

func TestAutomaticHexStopRecordsMarkerInCommitmentTransaction(t *testing.T) {
	_, tx, doms := dualCalculatorTest(t)
	activationTime := uint64(10)
	cc := &commitmentCalculator{
		doms:          doms,
		roTx:          tx,
		chainConfig:   &chain.Config{BinaryTrieTime: &activationTime},
		maxReorgDepth: 0,
	}
	cc.recordBlockTarget(commitTarget{blockNum: 3, blockTime: activationTime - 1})
	cc.recordBlockTarget(commitTarget{blockNum: 4, blockTime: activationTime})
	cc.stopHexShadowAtWindow(t.Context(), commitTarget{blockNum: 4, blockTime: activationTime})
	cc.recordBlockTarget(commitTarget{blockNum: 5, blockTime: activationTime})
	cc.stopHexShadowAtWindow(t.Context(), commitTarget{blockNum: 5, blockTime: activationTime})
	require.True(t, cc.ShadowDomainStopped(kv.CommitmentDomain))
	require.NoError(t, recordStoppedCommitmentDomains(tx, cc.shadowStopped))
	stopped, err := rawdb.ReadCommitmentDomainStopped(tx, kv.CommitmentDomain)
	require.NoError(t, err)
	require.True(t, stopped, "the automatic stop marker must share the commitment transaction")
}

func TestCommittedHexStopUpdatesLiveAggregator(t *testing.T) {
	_, tx, doms := dualCalculatorTest(t)
	agg, ok := tx.AggTx().(interface{ Agg() *dbstate.Aggregator })
	require.True(t, ok)
	live := agg.Agg()
	require.NoError(t, rawdb.WriteCommitmentDomainStopped(tx, kv.CommitmentDomain))
	require.False(t, live.CommitmentDomainStopped(kv.CommitmentDomain))
	require.NoError(t, doms.Commit(t.Context(), tx))
	require.True(t, live.CommitmentDomainStopped(kv.CommitmentDomain), "a committed stop must update the live aggregator")
}

func TestStoppedHexShadowUnwindRefusesAcrossActivation(t *testing.T) {
	_, tx, _ := dualCalculatorTest(t)
	require.NoError(t, rawdb.WriteCommitmentDomainStopped(tx, kv.CommitmentDomain))
	activationTime := uint64(10)
	err := checkStoppedHexShadowUnwind(t.Context(), tx, hexStopHeaderReader{}, &chain.Config{BinaryTrieTime: &activationTime}, 13, 4)
	require.Error(t, err, "an unwind below activation must be refused after the hex shadow stops")
	require.True(t, errors.Is(err, ErrTooDeepUnwind), "the stopped-shadow refusal must be a too-deep unwind")
	require.NoError(t, checkStoppedHexShadowUnwind(t.Context(), tx, hexStopHeaderReader{}, &chain.Config{BinaryTrieTime: &activationTime}, 13, 5))
}

func TestUnwindExecutionStageRejectsStoppedHexShadowAcrossActivation(t *testing.T) {
	_, tx, doms := dualCalculatorTest(t)
	defer doms.Close()
	activationTime := uint64(10)
	require.NoError(t, rawdb.WriteCommitmentDomainStopped(tx, kv.CommitmentDomain))
	err := UnwindExecutionStage(
		&UnwindState{ID: stages.Execution, UnwindPoint: 4, CurrentBlockNumber: 13},
		&StageState{ID: stages.Execution, BlockNumber: 13},
		doms,
		tx,
		context.Background(),
		ExecuteBlockCfg{blockReader: hexStopHeaderReader{}, chainConfig: &chain.Config{BinaryTrieTime: &activationTime}},
		log.New(),
	)
	require.ErrorIs(t, err, ErrTooDeepUnwind)
}

type hexStopHeaderReader struct {
	dbservices.FullBlockReader
}

func (hexStopHeaderReader) CanonicalHash(context.Context, kv.Getter, uint64) (common.Hash, bool, error) {
	return common.Hash{}, false, nil
}

func (hexStopHeaderReader) HeaderByNumber(_ context.Context, _ kv.Getter, number uint64) (*types.Header, error) {
	timestamp := uint64(0)
	if number >= 5 {
		timestamp = 10
	}
	return &types.Header{Time: timestamp}, nil
}

type uncommittedActivationHeaderReader struct {
	dbservices.FullBlockReader
}

func (uncommittedActivationHeaderReader) HeaderByNumber(_ context.Context, _ kv.Getter, number uint64) (*types.Header, error) {
	if number >= 7 {
		return nil, nil
	}
	if number >= 5 {
		return &types.Header{Time: 10}, nil
	}
	return &types.Header{}, nil
}

type missingActivationHeaderReader struct {
	dbservices.FullBlockReader
}

func (missingActivationHeaderReader) HeaderByNumber(context.Context, kv.Getter, uint64) (*types.Header, error) {
	return nil, nil
}
