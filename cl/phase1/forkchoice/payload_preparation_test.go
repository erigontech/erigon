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

package forkchoice

import (
	"context"
	"errors"
	"fmt"
	"testing"
	"time"

	"github.com/erigontech/erigon/cl/clparams"
	"github.com/erigontech/erigon/cl/cltypes"
	"github.com/erigontech/erigon/common"
	lru "github.com/hashicorp/golang-lru/v2"
	"github.com/stretchr/testify/require"
)

func TestPayloadPreparationYieldsToRelevantImports(t *testing.T) {
	for _, slot := range []uint64{8, 9, 10, 11, 12} {
		t.Run(fmt.Sprint(slot), func(t *testing.T) {
			var imports PayloadPreparationImports
			finishImport := imports.BeginImport(slot)
			_, finish, ok := imports.TryBegin(t.Context(), 9, 11, nil, nil)
			require.Equal(t, slot < 9 || slot > 11, ok)
			if ok {
				finish()
			}
			finishImport()
			_, finish, ok = imports.TryBegin(t.Context(), 9, 11, nil, nil)
			require.True(t, ok)
			finish()
		})
	}
}

func TestPayloadImportInProgress(t *testing.T) {
	var imports PayloadPreparationImports
	finishFirst := imports.BeginImport(10)
	finishSecond := imports.BeginImport(10)
	finishOther := imports.BeginImport(11)

	require.True(t, imports.ImportInProgress(10))
	require.True(t, imports.ImportInProgress(11))
	require.False(t, imports.ImportInProgress(12))

	finishFirst()
	require.True(t, imports.ImportInProgress(10))
	finishSecond()
	require.False(t, imports.ImportInProgress(10))
	finishOther()
	require.False(t, imports.ImportInProgress(11))
}

func TestOnBlockCancelsPayloadPreparationBeforeStoreLock(t *testing.T) {
	cfg := clparams.MainnetBeaconConfig
	f := &ForkChoiceStore{beaconCfg: &cfg}
	ctx, finish, ok := f.TryBeginPayloadPreparation(t.Context(), 9, 11)
	require.True(t, ok)
	defer finish()
	block := cltypes.NewSignedBeaconBlock(&cfg, clparams.Phase0Version)
	block.Block.Slot = 10
	f.mu.Lock()
	done := make(chan error, 1)
	go func() { done <- f.OnBlock(t.Context(), block, false, false, false) }()
	defer func() {
		f.mu.Unlock()
		require.ErrorIs(t, <-done, ErrBlockTooEarly)
	}()
	require.Eventually(t, func() bool { return ctx.Err() != nil }, time.Second, time.Millisecond)
}

func TestAuthenticatedEnvelopeImportCancelsPayloadPreparation(t *testing.T) {
	cfg, blockState, block, envelope := validAdmissionCancellationFixture(t)
	pending, err := lru.New[common.Hash, *cltypes.SignedExecutionPayloadEnvelope](queueCacheSize)
	require.NoError(t, err)
	f := &ForkChoiceStore{
		beaconCfg: cfg,
		forkGraph: dumpFailingForkGraph{
			dataAvailabilityForkGraph: dataAvailabilityForkGraph{state: blockState, block: block},
			err:                       errors.New("disk unavailable"),
		},
		pendingEnvelopes: pending,
	}
	ctx, finish, ok := f.TryBeginPayloadPreparation(t.Context(), block.Block.Slot, block.Block.Slot+1)
	require.True(t, ok)
	defer finish()

	_, err = f.applyEnvelope(t.Context(), envelope, false, true, true, queueMissingEnvelope, time.Now())
	require.ErrorIs(t, err, ErrExecutionPayloadEnvelopePersistenceFailed)
	require.ErrorIs(t, context.Cause(ctx), ErrPayloadImportInProgress)
}

func TestInvalidEnvelopeDoesNotCancelPayloadPreparation(t *testing.T) {
	cfg, blockState, block, envelope := validAdmissionCancellationFixture(t)
	envelope.Signature[0] ^= 0xff
	pending, err := lru.New[common.Hash, *cltypes.SignedExecutionPayloadEnvelope](queueCacheSize)
	require.NoError(t, err)
	pendingLocal, err := lru.New[common.Hash, *cltypes.SignedExecutionPayloadEnvelope](queueCacheSize)
	require.NoError(t, err)
	f := &ForkChoiceStore{
		beaconCfg:                      cfg,
		forkGraph:                      dataAvailabilityForkGraph{state: blockState, block: block},
		pendingEnvelopes:               pending,
		pendingLocalSelfBuildEnvelopes: pendingLocal,
	}
	ctx, finish, ok := f.TryBeginPayloadPreparation(t.Context(), block.Block.Slot, block.Block.Slot+1)
	require.True(t, ok)
	defer finish()

	require.ErrorIs(t, f.OnExecutionPayload(t.Context(), envelope, false, true), ErrInvalidExecutionPayloadEnvelope)
	require.NoError(t, ctx.Err())
	require.NoError(t, context.Cause(ctx))
}

func TestPayloadPreparationNewImportCancelsAttempt(t *testing.T) {
	var imports PayloadPreparationImports
	ctx, finish, ok := imports.TryBegin(t.Context(), 9, 11, nil, nil)
	require.True(t, ok)
	defer finish()
	finishImport := imports.BeginImport(10)
	defer finishImport()
	require.ErrorIs(t, ctx.Err(), context.Canceled)
}

func TestPayloadPreparationYieldsToParkedImports(t *testing.T) {
	var imports PayloadPreparationImports
	ctx, _, ok := imports.TryBegin(t.Context(), 9, 11, func() bool { return true }, nil)
	require.False(t, ok)
	require.ErrorIs(t, context.Cause(ctx), ErrPayloadImportInProgress)
	_, finish, ok := imports.TryBegin(t.Context(), 9, 11, func() bool { return false }, nil)
	require.True(t, ok)
	finish()
}

func TestPayloadPreparationImportArrivesDuringAdmission(t *testing.T) {
	var imports PayloadPreparationImports
	_, _, ok := imports.TryBegin(t.Context(), 9, 11, func() bool {
		finish := imports.BeginImport(10)
		finish()
		return false
	}, nil)
	require.False(t, ok)
}

func TestPayloadPreparationUsesKnownBlockSlotForParkedEnvelopes(t *testing.T) {
	queue, err := lru.New[common.Hash, *cltypes.SignedExecutionPayloadEnvelope](1)
	require.NoError(t, err)
	root := common.Hash{1}
	graph := &dataAvailabilityForkGraph{}
	f := &ForkChoiceStore{pendingEnvelopes: queue, forkGraph: graph}
	queue.Add(root, &cltypes.SignedExecutionPayloadEnvelope{Message: &cltypes.ExecutionPayloadEnvelope{BeaconBlockRoot: root, Payload: &cltypes.Eth1Block{SlotNumber: 10}}})
	_, finish, ok := f.TryBeginPayloadPreparation(t.Context(), 10, 11)
	require.True(t, ok)
	finish()

	graph.block = &cltypes.SignedBeaconBlock{Block: &cltypes.BeaconBlock{Slot: 10}}
	_, finish, ok = f.TryBeginPayloadPreparation(t.Context(), 10, 11)
	require.True(t, ok)
	finish()

	graph.block.Block.Slot = 11
	_, _, ok = f.TryBeginPayloadPreparation(t.Context(), 10, 11)
	require.False(t, ok)

	graph.block.Block.Slot = 10
	finishPending := f.BeginPendingPayloadEnvelope(root)
	defer finishPending()
	_, _, ok = f.TryBeginPayloadPreparation(t.Context(), 10, 11)
	require.False(t, ok)
}

func TestPersistedPendingEnvelopeDoesNotHoldPayloadPreparation(t *testing.T) {
	queue, err := lru.New[common.Hash, *cltypes.SignedExecutionPayloadEnvelope](1)
	require.NoError(t, err)
	root := common.Hash{1}
	graph := &persistedEnvelopeForkGraph{dataAvailabilityForkGraph: dataAvailabilityForkGraph{
		block: &cltypes.SignedBeaconBlock{Block: &cltypes.BeaconBlock{Slot: 11}},
	}}
	f := &ForkChoiceStore{pendingEnvelopes: queue, forkGraph: graph}
	queue.Add(root, &cltypes.SignedExecutionPayloadEnvelope{Message: &cltypes.ExecutionPayloadEnvelope{BeaconBlockRoot: root}})

	_, _, ok := f.TryBeginPayloadPreparation(t.Context(), 10, 11)
	require.False(t, ok)
	graph.hasEnvelope = true
	_, finish, ok := f.TryBeginPayloadPreparation(t.Context(), 10, 11)
	require.True(t, ok)
	finish()
}

func TestPersistedRegisteredEnvelopeDoesNotHoldPayloadPreparation(t *testing.T) {
	root := common.Hash{1}
	graph := &persistedEnvelopeForkGraph{
		dataAvailabilityForkGraph: dataAvailabilityForkGraph{
			block: &cltypes.SignedBeaconBlock{Block: &cltypes.BeaconBlock{Slot: 10}},
		},
		hasEnvelope: true,
	}
	f := &ForkChoiceStore{forkGraph: graph}
	finishPending := f.BeginPendingPayloadEnvelope(root)
	defer finishPending()

	_, finish, ok := f.TryBeginPayloadPreparation(t.Context(), 10, 11)
	require.True(t, ok)
	finish()
}

func TestPendingEnvelopeRegistrationUsesKnownBlockSlot(t *testing.T) {
	var imports PayloadPreparationImports
	root := common.Hash{1}
	known := map[common.Hash]uint64{}
	blockSlot := func(root common.Hash) (uint64, bool) {
		slot, ok := known[root]
		return slot, ok
	}
	finishPending := imports.BeginPendingEnvelope(root)
	tryBegin := func(firstSlot, lastSlot uint64) bool {
		_, finish, ok := imports.TryBegin(t.Context(), firstSlot, lastSlot, nil, blockSlot)
		if ok {
			finish()
		}
		return ok
	}

	require.True(t, tryBegin(10, 11))
	known[root] = 10
	require.False(t, tryBegin(10, 11))
	require.True(t, tryBegin(11, 12))
	finishPending()
	require.True(t, tryBegin(10, 11))
}

func TestKnownPendingEnvelopeDoesNotCancelPayloadPreparation(t *testing.T) {
	root := common.Hash{1}
	graph := &dataAvailabilityForkGraph{block: &cltypes.SignedBeaconBlock{Block: &cltypes.BeaconBlock{Slot: 10}}}
	f := &ForkChoiceStore{forkGraph: graph}
	ctx, finish, ok := f.TryBeginPayloadPreparation(t.Context(), 10, 11)
	require.True(t, ok)

	finishPending := f.BeginPendingPayloadEnvelope(root)
	defer finishPending()
	require.NoError(t, context.Cause(ctx))
	finish()
	_, _, ok = f.TryBeginPayloadPreparation(t.Context(), 10, 11)
	require.False(t, ok)
}

func TestPersistenceFailureParksSelectedHeadWithoutHoldingPreparation(t *testing.T) {
	for _, local := range []bool{false, true} {
		t.Run(fmt.Sprint("local=", local), func(t *testing.T) {
			cfg, blockState, block, envelope := validAdmissionCancellationFixture(t)
			pending, err := lru.New[common.Hash, *cltypes.SignedExecutionPayloadEnvelope](queueCacheSize)
			require.NoError(t, err)
			pendingLocal, err := lru.New[common.Hash, *cltypes.SignedExecutionPayloadEnvelope](queueCacheSize)
			require.NoError(t, err)
			f := &ForkChoiceStore{
				beaconCfg: cfg,
				forkGraph: dumpFailingForkGraph{
					dataAvailabilityForkGraph: dataAvailabilityForkGraph{state: blockState, block: block},
					err:                       errors.New("disk unavailable"),
				},
				pendingEnvelopes:               pending,
				pendingLocalSelfBuildEnvelopes: pendingLocal,
			}
			parked := pending

			if local {
				parked = pendingLocal
				_, err = f.applyLocalSelfBuildEnvelope(t.Context(), envelope, queueMissingEnvelope)
			} else {
				_, err = f.applyEnvelope(t.Context(), envelope, false, true, true, queueMissingEnvelope, time.Now())
			}
			require.ErrorIs(t, err, ErrExecutionPayloadEnvelopePersistenceFailed)
			require.True(t, parked.Contains(envelope.Message.BeaconBlockRoot))
			_, finish, ok := f.TryBeginPayloadPreparation(t.Context(), block.Block.Slot, block.Block.Slot)
			require.True(t, ok)
			finish()
		})
	}
}
