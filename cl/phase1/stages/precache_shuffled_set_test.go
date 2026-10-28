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

package stages

import (
	"encoding/binary"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/erigontech/erigon/cl/clparams"
	"github.com/erigontech/erigon/cl/cltypes/solid"
	"github.com/erigontech/erigon/cl/phase1/core/caches"
	"github.com/erigontech/erigon/cl/phase1/core/state"
	"github.com/erigontech/erigon/cl/phase1/core/state/shuffling"
	"github.com/erigontech/erigon/common"
	"github.com/erigontech/erigon/common/log/v3"
)

func preCacheTestState(t *testing.T, cfg *clparams.BeaconChainConfig, validators int, seed byte) *state.CachingBeaconState {
	t.Helper()
	s := state.New(cfg)
	for i := range validators {
		var pk [48]byte
		binary.BigEndian.PutUint64(pk[:], uint64(i))
		pk[47] = seed
		v := solid.NewValidator()
		v.SetActivationEpoch(0)
		v.SetExitEpoch(^uint64(0))
		v.SetPublicKey(pk)
		v.SetEffectiveBalance(32_000_000_000)
		require.NoError(t, s.AddValidator(v, 32_000_000_000))
	}
	require.NoError(t, s.SetSlot(3*cfg.SlotsPerEpoch+1))
	for i := range int(cfg.SlotsPerHistoricalRoot) {
		require.NoError(t, s.SetBlockRootAt(i, common.Hash{seed, byte(i), byte(i >> 8)}))
	}
	for i := range int(cfg.EpochsPerHistoricalVector) {
		require.NoError(t, s.SetRandaoMixAt(i, common.Hash{seed, byte(i), byte(i >> 8)}))
	}
	return s
}

func TestPreCacheNextShuffledValidatorSetReadsTheStateBeforeReturning(t *testing.T) {
	cfg := &clparams.MainnetBeaconConfig
	head := preCacheTestState(t, cfg, 50_000, 0xa1)
	other := preCacheTestState(t, cfg, 40_000, 0xb2)

	ref, err := head.Copy()
	require.NoError(t, err)
	epoch := state.Epoch(ref) + 1
	root, err := ref.GetBlockRootAtSlot((epoch-1)*cfg.SlotsPerEpoch - 1)
	require.NoError(t, err)
	mix := ref.GetRandaoMix(int((epoch + cfg.EpochsPerHistoricalVector - cfg.MinSeedLookahead - 1) % cfg.EpochsPerHistoricalVector))
	indicies := ref.GetActiveValidatorsIndices(epoch)
	want := shuffling.ComputeShuffledIndicies(cfg, mix, make([]uint64, len(indicies)), indicies, epoch*cfg.SlotsPerEpoch)

	require.Eventually(t, func() bool { return !workingPreCacheNextShuffledValidatorSet.Load() }, 10*time.Second, time.Millisecond)
	preCacheNextShuffledValidatorSet(log.Root(), &Cfg{beaconCfg: cfg}, head)
	require.NoError(t, other.CopyInto(head))
	require.Eventually(t, func() bool { return !workingPreCacheNextShuffledValidatorSet.Load() }, 10*time.Second, time.Millisecond)

	got, ok := caches.ShuffledIndiciesCacheGlobal.Get(epoch, root)
	require.True(t, ok)
	require.Equal(t, want, got)
}
