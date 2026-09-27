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
	"bytes"
	"errors"
	"math"
	"sort"
	"sync/atomic"
	"testing"
	"time"

	"github.com/erigontech/erigon/common/crypto"
	"github.com/erigontech/erigon/common/dbg"
	"github.com/erigontech/erigon/common/log/v3"
	"github.com/erigontech/erigon/db/kv"
	"github.com/erigontech/erigon/db/rawdb"
	"github.com/erigontech/erigon/db/state/execctx"
	"github.com/erigontech/erigon/db/state/statecfg"
	"github.com/erigontech/erigon/execution/chain"
	"github.com/erigontech/erigon/execution/commitment"
	"github.com/erigontech/erigon/execution/commitment/commitmentdb"
	"github.com/erigontech/erigon/execution/commitment/eip8297"
	pbt "github.com/erigontech/erigon/execution/commitment/v3/pbt"
	"github.com/holiman/uint256"
	"github.com/stretchr/testify/require"
)

func dualCalculatorTest(t *testing.T) (kv.TemporalRwDB, kv.TemporalRwTx, *execctx.SharedDomains) {
	t.Helper()
	bin, dual, parallel := statecfg.ExperimentalBinCommitment, statecfg.ExperimentalHexBinCommitment, statecfg.ExperimentalParallelCommitment
	v3, schema := statecfg.ExperimentalCommitmentV3, statecfg.Schema
	hash, suite := statecfg.BinCommitmentHash, commitment.PBinHashSuiteName()
	t.Cleanup(func() {
		statecfg.ExperimentalBinCommitment, statecfg.ExperimentalHexBinCommitment, statecfg.ExperimentalParallelCommitment = bin, dual, parallel
		statecfg.ExperimentalCommitmentV3, statecfg.Schema = v3, schema
		statecfg.BinCommitmentHash = hash
		require.NoError(t, commitment.SetPBinHashSuite(suite))
	})
	statecfg.ExperimentalBinCommitment, statecfg.ExperimentalHexBinCommitment, statecfg.ExperimentalParallelCommitment = true, true, false
	statecfg.ExperimentalCommitmentV3 = true
	statecfg.EnableCommitmentV3Records(&statecfg.Schema.CommitmentDomain)
	return setupStepTest(t)
}

func assertPBinEngineIdentity(t *testing.T, tx kv.TemporalTx) {
	t.Helper()
	require.NoError(t, pbt.ValidateEngineIdentityFromTx(tx, kv.CommitmentBinDomain))
}

func TestDualCalculatorUsesHexCollectorWithBinarySelected(t *testing.T) {
	db, tx, _ := dualCalculatorTest(t)
	doms, err := execctx.NewSharedDomains(t.Context(), tx, log.New(), execctx.WithCommitmentDomain(kv.CommitmentBinDomain))
	require.NoError(t, err)
	t.Cleanup(doms.Close)
	doms.SetInMemHistoryReads(true)
	doms.SetDisableInlineTouchKey(true)
	cc, err := newCommitmentCalculator(t.Context(), t.Context(), doms, db, &chain.Config{}, "test", log.New(), false, math.MaxUint64, nil, nil, nil)
	require.NoError(t, err)
	t.Cleanup(cc.Stop)
	require.Equal(t, commitment.VariantCommitmentV3, doms.GetCommitmentCtxForDomain(kv.CommitmentDomain).Trie().Variant())
	require.Equal(t, commitment.ModeCollect, cc.updates.Mode())
	require.True(t, cc.forcePerBlockCompute)
}

func TestBinComputeCommitmentUsesProductionWorkerFactory(t *testing.T) {
	workers := dbg.TipTrieWarmupers
	dbg.TipTrieWarmupers = 8
	t.Cleanup(func() { dbg.TipTrieWarmupers = workers })
	db, tx, doms := dualCalculatorTest(t)
	cc, err := newCommitmentCalculator(t.Context(), t.Context(), doms, db, &chain.Config{}, "prefetch", log.New(), false, math.MaxUint64, nil, nil, nil)
	require.NoError(t, err)
	t.Cleanup(cc.Stop)
	require.NotNil(t, cc.state.prefetch)
	require.True(t, cc.state.prefetch.bin[kv.CommitmentBinDomain])
	binCtx := doms.GetCommitmentCtxForDomain(kv.CommitmentBinDomain)
	trie, ok := binCtx.Trie().(interface{ SetCoreActivityHook(func(bool)) })
	require.True(t, ok)
	var active, peak atomic.Int32
	started := make(chan struct{})
	var startedOnce atomic.Bool
	trie.SetCoreActivityHook(func(start bool) {
		if start {
			current := active.Add(1)
			for {
				old := peak.Load()
				if old >= current || peak.CompareAndSwap(old, current) {
					break
				}
			}
			if current > 1 && startedOnce.CompareAndSwap(false, true) {
				close(started)
			}
			select {
			case <-started:
			case <-time.After(time.Second):
			}
			return
		}
		active.Add(-1)
	})
	ops := make([]pbt.Op, 0, 16)
	for i := range 16 {
		value, err := eip8297.EncodeBasicData(uint64(i+1), uint256.NewInt(uint64(i+1)), 0)
		require.NoError(t, err)
		key := make([]byte, eip8297.AccountKeyLength)
		key[1] = byte(i << 4)
		ops = append(ops, pbt.Op{Key: key, Value: value})
	}
	sort.Slice(ops, func(i, j int) bool { return bytes.Compare(ops[i].Key, ops[j].Key) < 0 })
	binCtx.SetPBinOps(ops)
	_, err = binCtx.ComputeCommitment(t.Context(), tx, false, 1, 1, "factory", nil)
	require.NoError(t, err)
	require.Greater(t, peak.Load(), int32(1))
	_ = db
}

func dualHexFeed(key []byte, update commitment.Update) *commitment.Feed {
	hash := crypto.Keccak256(key)
	var feedHash [32]byte
	copy(feedHash[:], hash)
	return &commitment.Feed{Keys: 1, Accounts: []commitment.FeedAccount{{Hash: feedHash, Update: &update}}}
}

func TestStoppedShadowSurvivesCalculatorReplacement(t *testing.T) {
	_, tx, doms := dualCalculatorTest(t)
	first := &commitmentCalculator{roTx: tx, doms: doms}
	first.stopShadowDomain(kv.CommitmentBinDomain)
	second := &commitmentCalculator{roTx: tx, doms: doms}
	require.True(t, second.ShadowDomainStopped(kv.CommitmentBinDomain))
}

type dualReplayContext struct {
	commitment.PatriciaContext
	writes int
	err    error
}

func (c *dualReplayContext) PutBranch(_, _, _ []byte) error {
	c.writes++
	return c.err
}

func TestDualCompletionDiscardsFailedBinaryFold(t *testing.T) {
	backend := &dualReplayContext{}
	buffered := commitmentdb.NewBufferedPatriciaContext(backend)
	require.NoError(t, buffered.PutBranch([]byte{1}, []byte{2}, []byte{3}))
	cc := &commitmentCalculator{}
	result, err := cc.finishDualFolds(kv.CommitmentDomain, kv.CommitmentBinDomain, []dualFoldResult{
		{domain: kv.CommitmentDomain, root: []byte{4}},
		{domain: kv.CommitmentBinDomain, err: errors.New("fold failed")},
	}, &commitmentFoldArm{buffered: buffered})
	require.NoError(t, err)
	require.Equal(t, []byte{4}, result.canonicalRoot)
	require.Nil(t, result.shadowRoot)
	require.Zero(t, backend.writes)
	require.True(t, cc.ShadowDomainStopped(kv.CommitmentBinDomain))
}

func TestDualCompletionDoesNotReplayShadowAfterCanonicalFailure(t *testing.T) {
	backend := &dualReplayContext{}
	buffered := commitmentdb.NewBufferedPatriciaContext(backend)
	require.NoError(t, buffered.PutBranch([]byte{1}, []byte{2}, nil))
	cc := &commitmentCalculator{}
	result, err := cc.finishDualFolds(kv.CommitmentDomain, kv.CommitmentBinDomain, []dualFoldResult{
		{domain: kv.CommitmentDomain, err: errors.New("fold failed")},
		{domain: kv.CommitmentBinDomain, root: []byte{4}},
	}, &commitmentFoldArm{buffered: buffered})
	require.Error(t, err)
	require.Nil(t, result.canonicalRoot)
	require.Zero(t, backend.writes)
}

func TestDualCompletionStopsShadowOnReplayError(t *testing.T) {
	for _, canonical := range []kv.Domain{kv.CommitmentDomain, kv.CommitmentBinDomain} {
		t.Run(canonical.String(), func(t *testing.T) {
			shadow := otherCommitmentDomain(canonical)
			backend := &dualReplayContext{err: errors.New("write failed")}
			buffered := commitmentdb.NewBufferedPatriciaContext(backend)
			require.NoError(t, buffered.PutBranch([]byte{1}, []byte{2}, nil))
			cc := &commitmentCalculator{}
			result, err := cc.finishDualFolds(canonical, shadow, []dualFoldResult{
				{domain: kv.CommitmentDomain, root: []byte{3}},
				{domain: kv.CommitmentBinDomain, root: []byte{4}},
			}, &commitmentFoldArm{buffered: buffered})
			require.NoError(t, err)
			require.Equal(t, 1, backend.writes)
			require.NotNil(t, result.canonicalRoot)
			require.Nil(t, result.shadowRoot)
			require.True(t, cc.ShadowDomainStopped(shadow))
		})
	}
}

func TestRecordStoppedCommitmentDomainsPersistsStop(t *testing.T) {
	_, tx, _ := dualCalculatorTest(t)
	stopper, ok := tx.AggTx().(interface{ StopCommitmentDomain(kv.Domain) })
	require.True(t, ok)
	stopper.StopCommitmentDomain(kv.CommitmentBinDomain)
	require.NoError(t, recordStoppedCommitmentDomains(tx))
	stopped, err := rawdb.ReadCommitmentDomainStopped(tx, kv.CommitmentBinDomain)
	require.NoError(t, err)
	require.True(t, stopped)
	stopped, err = rawdb.ReadCommitmentDomainStopped(tx, kv.CommitmentDomain)
	require.NoError(t, err)
	require.False(t, stopped)
}
