// Copyright 2024 The Erigon Authors
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

package attestation_producer_test

import (
	"encoding/json"
	"testing"

	"github.com/erigontech/erigon/cl/antiquary/tests"
	"github.com/erigontech/erigon/cl/clparams"
	"github.com/erigontech/erigon/cl/cltypes/solid"
	"github.com/erigontech/erigon/cl/phase1/core/state"
	"github.com/erigontech/erigon/cl/validator/attestation_producer"
	"github.com/erigontech/erigon/common"
	"github.com/stretchr/testify/require"
)

func TestAttestationProducer(t *testing.T) {
	attProducer := attestation_producer.New(&clparams.MainnetBeaconConfig)

	_, _, headState := tests.GetPhase0Random()
	root, err := headState.BlockRoot()
	require.NoError(t, err)

	att, err := attProducer.ProduceAndCacheAttestationData(headState, root, headState.Slot())
	require.NoError(t, err)

	attJson, err := json.Marshal(att)
	require.NoError(t, err)

	// check if the json match with the expected value
	require.JSONEq(t, string(attJson), `{"slot":"8322","index":"0","beacon_block_root":"0xeffdd8ef40c3c901f0724d48e04ce257967cf1da31929f3b6db614f89ef8d660","source":{"epoch":"258","root":"0x31885d5a2405876b7203f9cc1a7e115b9977412107c51c81ab4fd49bde93905e"},"target":{"epoch":"260","root":"0x86979f6f6dc7626064ef0d38d4dffb89e91d1d4c18492e3fb7d7ee93cedca3ed"}}`)
}

func TestAttestationProducerHeadChange(t *testing.T) {
	for _, lateBlock := range []bool{false, true} {
		name := "same slot reorg"
		if lateBlock {
			name = "late block"
		}
		t.Run(name, func(t *testing.T) {
			cfg := clparams.MainnetBeaconConfig
			producer := attestation_producer.New(&cfg)
			headState := state.New(&cfg)
			slot := cfg.SlotsPerEpoch
			require.NoError(t, headState.SetSlot(slot))
			if lateBlock {
				slot++
			}
			oldRoot := common.Hash{1}
			newRoot := common.Hash{2}
			oldData, err := producer.ProduceAndCacheAttestationData(headState, oldRoot, slot)
			require.NoError(t, err)
			_, ok := producer.CachedAttestationData(newRoot, slot)
			require.False(t, ok)
			require.NoError(t, headState.SetSlot(slot))
			require.NoError(t, headState.SetBlockRootAt(int(cfg.SlotsPerEpoch), oldRoot))
			source := solid.Checkpoint{Root: common.Hash{3}}
			headState.SetCurrentJustifiedCheckpoint(source)

			data, err := producer.ProduceAndCacheAttestationData(headState, newRoot, slot)
			require.NoError(t, err)
			require.Equal(t, newRoot, data.BeaconBlockRoot)
			require.Equal(t, source, data.Source)
			if lateBlock {
				require.Equal(t, oldRoot, data.Target.Root)
			} else {
				require.Equal(t, newRoot, data.Target.Root)
			}
			cached, ok := producer.CachedAttestationData(newRoot, slot)
			require.True(t, ok)
			require.Equal(t, data, cached)
			cached, ok = producer.CachedAttestationData(oldRoot, slot)
			require.True(t, ok)
			require.Equal(t, oldData, cached)
		})
	}
}

func TestAttestationProducerPreviousSlotAfterHeadAdvance(t *testing.T) {
	cfg := clparams.MainnetBeaconConfig
	producer := attestation_producer.New(&cfg)
	headState := state.New(&cfg)
	slot := cfg.SlotsPerEpoch
	require.NoError(t, headState.SetSlot(slot))
	oldRoot, newRoot := common.Hash{1}, common.Hash{2}
	want, err := producer.ProduceAndCacheAttestationData(headState, oldRoot, slot)
	require.NoError(t, err)
	require.NoError(t, headState.SetBlockRootAt(int(slot), oldRoot))
	require.NoError(t, headState.SetSlot(slot+1))

	got, err := producer.ProduceAndCacheAttestationData(headState, newRoot, slot)
	require.NoError(t, err)
	require.Equal(t, want, got)
	cached, ok := producer.CachedAttestationData(newRoot, slot)
	require.True(t, ok)
	require.Equal(t, got, cached)
}

func TestAttestationProducerPreviousEpochAfterHeadAdvance(t *testing.T) {
	cfg := clparams.MainnetBeaconConfig
	producer := attestation_producer.New(&cfg)
	headState := state.New(&cfg)
	slot := 2*cfg.SlotsPerEpoch - 1
	oldRoot, newRoot := common.Hash{1}, common.Hash{2}
	oldSource := solid.Checkpoint{Root: common.Hash{3}}
	headState.SetCurrentJustifiedCheckpoint(oldSource)
	require.NoError(t, headState.SetSlot(slot))
	require.NoError(t, headState.SetBlockRootAt(int(cfg.SlotsPerEpoch), common.Hash{4}))
	want, err := producer.ProduceAndCacheAttestationData(headState, oldRoot, slot)
	require.NoError(t, err)
	require.NoError(t, headState.SetBlockRootAt(int(slot), oldRoot))
	require.NoError(t, headState.SetSlot(slot+1))
	headState.SetPreviousJustifiedCheckpoint(oldSource)
	headState.SetCurrentJustifiedCheckpoint(solid.Checkpoint{Epoch: 1, Root: common.Hash{5}})

	got, err := producer.ProduceAndCacheAttestationData(headState, newRoot, slot)
	require.NoError(t, err)
	require.Equal(t, want, got)
}

func TestAttestationProducerRejectsSlotTwoEpochsBeforeHead(t *testing.T) {
	cfg := clparams.MainnetBeaconConfig
	producer := attestation_producer.New(&cfg)
	headState := state.New(&cfg)
	require.NoError(t, headState.SetSlot(3*cfg.SlotsPerEpoch))
	slot := 2*cfg.SlotsPerEpoch - 1
	require.NoError(t, headState.SetBlockRootAt(int(cfg.SlotsPerEpoch), common.Hash{1}))
	require.NoError(t, headState.SetBlockRootAt(int(slot), common.Hash{2}))

	_, err := producer.ProduceAndCacheAttestationData(headState, common.Hash{3}, slot)
	require.ErrorContains(t, err, "head state is more than one epoch ahead")
}
