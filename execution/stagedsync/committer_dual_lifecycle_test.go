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
	"errors"
	"math"
	"testing"

	"github.com/erigontech/erigon/common/log/v3"
	"github.com/erigontech/erigon/db/kv"
	"github.com/erigontech/erigon/db/rawdb"
	"github.com/erigontech/erigon/db/state/execctx"
	"github.com/erigontech/erigon/db/state/statecfg"
	"github.com/erigontech/erigon/execution/chain"
	"github.com/erigontech/erigon/execution/commitment"
	"github.com/erigontech/erigon/execution/commitment/commitmentdb"
	"github.com/stretchr/testify/require"
)

func dualCalculatorTest(t *testing.T) (kv.TemporalRwDB, kv.TemporalRwTx, *execctx.SharedDomains) {
	t.Helper()
	bin, dual, parallel := statecfg.ExperimentalBinCommitment, statecfg.ExperimentalHexBinCommitment, statecfg.ExperimentalParallelCommitment
	hash, suite := statecfg.BinCommitmentHash, commitment.PBinHashSuiteName()
	t.Cleanup(func() {
		statecfg.ExperimentalBinCommitment, statecfg.ExperimentalHexBinCommitment, statecfg.ExperimentalParallelCommitment = bin, dual, parallel
		statecfg.BinCommitmentHash = hash
		require.NoError(t, commitment.SetPBinHashSuite(suite))
	})
	statecfg.ExperimentalBinCommitment, statecfg.ExperimentalHexBinCommitment, statecfg.ExperimentalParallelCommitment = true, true, false
	return setupStepTest(t)
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
	key := make([]byte, 20)
	key[0] = 1
	cc.updates.TouchPlainKeyDirect(string(key), &commitment.Update{Flags: commitment.NonceUpdate, Nonce: 1})
	var hashed []byte
	require.NoError(t, cc.updates.HashSort(t.Context(), nil, func(hk, pk []byte, update *commitment.Update) error {
		hashed = append([]byte(nil), hk...)
		return nil
	}))
	require.Equal(t, commitment.KeyToHexNibbleHash(key), hashed)
	require.True(t, cc.forcePerBlockCompute)
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
