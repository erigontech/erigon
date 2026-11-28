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

package fork_graph

import (
	"encoding/binary"
	"math"
	"sync"
	"testing"
	"time"

	dto "github.com/prometheus/client_model/go"
	"github.com/spf13/afero"
	"github.com/stretchr/testify/require"

	"github.com/erigontech/erigon/cl/beacon/beacon_router_configuration"
	"github.com/erigontech/erigon/cl/beacon/synced_data"
	"github.com/erigontech/erigon/cl/clparams"
	"github.com/erigontech/erigon/cl/cltypes"
	"github.com/erigontech/erigon/cl/cltypes/solid"
	"github.com/erigontech/erigon/cl/phase1/core/state"
	"github.com/erigontech/erigon/common"
	"github.com/erigontech/erigon/diagnostics/metrics"
)

const shareTestValidators = 1_200

func shareTestValidator(index uint64) solid.Validator {
	validator := solid.NewValidator()
	var publicKey [48]byte
	binary.LittleEndian.PutUint64(publicKey[:8], index+1)
	validator.SetPublicKey(publicKey)
	validator.SetEffectiveBalance(32_000_000_000)
	validator.SetExitEpoch(math.MaxUint64)
	validator.SetWithdrawableEpoch(math.MaxUint64)
	return validator
}

func shareTestState(t testing.TB) *state.CachingBeaconState {
	t.Helper()
	cfg := &clparams.MainnetBeaconConfig
	st := state.New(cfg)
	st.SetVersion(clparams.FuluVersion)
	st.SetLatestExecutionPayloadHeader(cltypes.NewEth1Header(clparams.FuluVersion))
	limit := int(cfg.ValidatorRegistryLimit)
	validators := solid.NewValidatorSet(limit)
	balances := make([]uint64, shareTestValidators)
	for i := range shareTestValidators {
		validators.Append(shareTestValidator(uint64(i)))
		balances[i] = 32_000_000_000
	}
	st.SetValidators(validators)
	st.SetBalances(solid.NewUint64ListSSZFromSlice(limit, balances))
	st.SetInactivityScores(make([]uint64, shareTestValidators))
	st.SetPreviousEpochParticipation(solid.NewParticipationBitList(shareTestValidators, limit))
	st.SetCurrentEpochParticipation(solid.NewParticipationBitList(shareTestValidators, limit))
	require.NoError(t, st.InitBeaconState())
	return st
}

func aheadOf(t testing.TB, st *state.CachingBeaconState) *state.CachingBeaconState {
	t.Helper()
	ahead, err := st.Copy()
	require.NoError(t, err)
	require.NoError(t, ahead.SetEffectiveBalanceForValidatorAtIndex(7, 31_000_000_000))
	require.NoError(t, ahead.SetEffectiveBalanceForValidatorAtIndex(700, 31_000_000_000))
	for i := range 4 {
		require.NoError(t, ahead.AddValidator(shareTestValidator(uint64(10_000+i)), 32_000_000_000))
	}
	return ahead
}

func shareTestGraph(t testing.TB, anchor *state.CachingBeaconState, syncedData synced_data.SyncedData) *forkGraphDisk {
	t.Helper()
	graph, err := NewForkGraphDisk(anchor, syncedData, afero.NewMemMapFs(), beacon_router_configuration.RouterConfiguration{})
	require.NoError(t, err)
	return graph.(*forkGraphDisk)
}

func TestReplayStateSharesRegistryWithSyncedHead(t *testing.T) {
	cfg := &clparams.MainnetBeaconConfig
	notSynced := synced_data.NewSyncedDataManager(cfg, true)
	synced := synced_data.NewSyncedDataManager(cfg, true)
	anchor := shareTestState(t)
	require.NoError(t, synced.OnHeadStateWithBlockRoot(aheadOf(t, anchor), common.Hash{0x22}))

	for _, testCase := range []struct {
		name       string
		syncedData synced_data.SyncedData
		wantShared bool
	}{
		{name: "synced head", syncedData: synced, wantShared: true},
		{name: "not synced", syncedData: notSynced},
		{name: "no synced data"},
	} {
		t.Run(testCase.name, func(t *testing.T) {
			graph := shareTestGraph(t, anchor, testCase.syncedData)
			wantRoot, err := anchor.HashSSZ()
			require.NoError(t, err)
			before := sharedValidatorChunksCounter.GetValueUint64()

			got, err := graph.ReplayState(graph.anchorRoot)

			require.NoError(t, err)
			require.NotNil(t, got)
			gotRoot, err := got.HashSSZ()
			require.NoError(t, err)
			require.Equal(t, wantRoot, gotRoot)
			for index := range shareTestValidators {
				gotIndex, ok := got.ValidatorIndexByPubkey(shareTestValidator(uint64(index)).PublicKey())
				require.True(t, ok)
				require.Equal(t, uint64(index), gotIndex)
			}
			_, ok := got.ValidatorIndexByPubkey(shareTestValidator(10_000).PublicKey())
			require.False(t, ok, "a validator the replayed state does not hold")
			shared := sharedValidatorChunksCounter.GetValueUint64() - before
			if testCase.wantShared {
				require.Positive(t, shared)
			} else {
				require.Zero(t, shared)
			}
		})
	}
}

func TestReplayStateSharingConcurrentWithHeadUse(t *testing.T) {
	cfg := &clparams.MainnetBeaconConfig
	manager := synced_data.NewSyncedDataManager(cfg, true)
	anchor := shareTestState(t)
	graph := shareTestGraph(t, anchor, manager)
	require.NoError(t, manager.OnHeadStateWithBlockRoot(aheadOf(t, anchor), common.Hash{0x22}))
	wantRoot, err := anchor.HashSSZ()
	require.NoError(t, err)

	const rounds = 60
	heads := make([]*state.CachingBeaconState, rounds)
	for round := range heads {
		heads[round] = aheadOf(t, anchor)
		require.NoError(t, heads[round].SetEffectiveBalanceForValidatorAtIndex(round, 5))
	}
	var wg sync.WaitGroup
	fail := func(err error) { t.Error(err) }
	wg.Go(func() {
		for round := range rounds {
			graph.replayedStates.clear()
			before := sharedValidatorChunksCounter.GetValueUint64()
			got, err := graph.ReplayState(graph.anchorRoot)
			if err != nil || got == nil {
				t.Errorf("replay returned state %v, err %v", got != nil, err)
				return
			}
			if shared := sharedValidatorChunksCounter.GetValueUint64() - before; shared == 0 {
				t.Errorf("replay %d shared no validator chunks", round)
				return
			}
			root, err := got.HashSSZ()
			if err != nil || root != wantRoot {
				t.Errorf("replayed root %x, err %v", root, err)
				return
			}
			if err := got.SetEffectiveBalanceForValidatorAtIndex(300, 1); err != nil {
				fail(err)
				return
			}
			if _, err := got.HashSSZ(); err != nil {
				fail(err)
				return
			}
		}
	})
	wg.Go(func() {
		for round, head := range heads {
			if err := manager.OnHeadStateWithBlockRoot(head, common.Hash{0x22, byte(round)}); err != nil {
				fail(err)
				return
			}
		}
	})
	wg.Go(func() {
		for range rounds {
			err := manager.ViewHeadState(func(head *state.CachingBeaconState) error {
				_, err := head.Copy()
				return err
			})
			if err != nil {
				fail(err)
				return
			}
		}
	})
	wg.Go(func() {
		for range rounds {
			err := manager.ViewHeadState(func(head *state.CachingBeaconState) error {
				_, err := head.HashSSZ()
				return err
			})
			if err != nil {
				fail(err)
				return
			}
		}
	})
	wg.Go(func() {
		for range rounds {
			// The forkchoice stage dumps states from inside ViewHeadState.
			err := manager.ViewHeadState(func(head *state.CachingBeaconState) error {
				return graph.DumpBeaconStateOnDisk(common.Hash{0x33}, head, true)
			})
			if err != nil {
				fail(err)
				return
			}
		}
	})
	done := make(chan struct{})
	go func() {
		wg.Wait()
		close(done)
	}()
	select {
	case <-done:
	case <-time.After(60 * time.Second):
		t.Fatal("deadlock between ViewHeadState and the state dump lock")
	}
}

type stateDumpLockProbe struct {
	synced_data.SyncedData
	graph *forkGraphDisk
	views int
	held  bool
}

func (p *stateDumpLockProbe) ViewHeadState(fn synced_data.ViewHeadStateFn) error {
	p.views++
	if p.graph.stateDumpLock.TryLock() {
		p.graph.stateDumpLock.Unlock()
	} else {
		p.held = true
	}
	return p.SyncedData.ViewHeadState(fn)
}

func TestReplayStateViewsTheHeadOutsideTheStateDumpLock(t *testing.T) {
	manager := synced_data.NewSyncedDataManager(&clparams.MainnetBeaconConfig, true)
	anchor := shareTestState(t)
	require.NoError(t, manager.OnHeadStateWithBlockRoot(aheadOf(t, anchor), common.Hash{0x22}))
	probe := &stateDumpLockProbe{SyncedData: manager}
	probe.graph = shareTestGraph(t, anchor, probe)

	got, err := probe.graph.ReplayState(probe.graph.anchorRoot)

	require.NoError(t, err)
	require.NotNil(t, got)
	require.Positive(t, probe.views)
	require.False(t, probe.held, "the head was viewed while stateDumpLock was held")
}

func TestReadBeaconStateFromDiskObservesTheDecodedSize(t *testing.T) {
	anchor := shareTestState(t)
	graph := shareTestGraph(t, anchor, nil)
	size := metrics.GetOrCreateHistogram("decode_ssz_beacon_state_size")
	read := func() (count uint64, sum float64) {
		metric := &dto.Metric{}
		require.NoError(t, size.Write(metric))
		return metric.GetHistogram().GetSampleCount(), metric.GetHistogram().GetSampleSum()
	}
	countBefore, sumBefore := read()

	for range 2 {
		_, err := graph.readBeaconStateFromDisk(graph.anchorRoot)
		require.NoError(t, err)
	}

	countAfter, sumAfter := read()
	require.Equal(t, uint64(2), countAfter-countBefore)
	require.Equal(t, float64(2*anchor.EncodingSizeSSZ()), sumAfter-sumBefore)
}
