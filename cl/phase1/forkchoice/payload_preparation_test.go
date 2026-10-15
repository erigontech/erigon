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
	"fmt"
	"testing"
	"time"

	"github.com/erigontech/erigon/cl/clparams"
	"github.com/erigontech/erigon/cl/cltypes"
	"github.com/erigontech/erigon/cl/phase1/forkchoice/fork_graph"
	"github.com/erigontech/erigon/common"
	lru "github.com/hashicorp/golang-lru/v2"
	"github.com/stretchr/testify/require"
)

func TestPayloadPreparationYieldsToRelevantImports(t *testing.T) {
	for _, slot := range []uint64{8, 9, 10, 11, 12} {
		t.Run(fmt.Sprint(slot), func(t *testing.T) {
			var imports PayloadPreparationImports
			finishImport := imports.BeginImport(slot)
			_, finish, ok := imports.TryBegin(t.Context(), 9, 11, nil)
			require.Equal(t, slot < 9 || slot > 11, ok)
			if ok {
				finish()
			}
			finishImport()
			_, finish, ok = imports.TryBegin(t.Context(), 9, 11, nil)
			require.True(t, ok)
			finish()
		})
	}
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

type preparedEnvelopeGraph struct{ fork_graph.ForkGraph }

func (preparedEnvelopeGraph) HasEnvelope(common.Hash) bool { return true }

func TestEnvelopeImportCancelsPayloadPreparationBeforeStoreLock(t *testing.T) {
	f := &ForkChoiceStore{forkGraph: preparedEnvelopeGraph{}}
	ctx, finish, ok := f.TryBeginPayloadPreparation(t.Context(), 9, 11)
	require.True(t, ok)
	defer finish()
	envelope := &cltypes.SignedExecutionPayloadEnvelope{Message: &cltypes.ExecutionPayloadEnvelope{Payload: &cltypes.Eth1Block{SlotNumber: 10}}}
	f.mu.Lock()
	done := make(chan error, 1)
	go func() {
		_, err := f.applyEnvelope(t.Context(), envelope, false, false, false, queueMissingEnvelope, time.Now())
		done <- err
	}()
	defer func() {
		f.mu.Unlock()
		require.NoError(t, <-done)
	}()
	require.Eventually(t, func() bool { return ctx.Err() != nil }, time.Second, time.Millisecond)
}

func TestPayloadPreparationNewImportCancelsAttempt(t *testing.T) {
	var imports PayloadPreparationImports
	ctx, finish, ok := imports.TryBegin(t.Context(), 9, 11, nil)
	require.True(t, ok)
	defer finish()
	finishImport := imports.BeginImport(10)
	defer finishImport()
	require.ErrorIs(t, ctx.Err(), context.Canceled)
}

func TestPayloadPreparationYieldsToParkedImports(t *testing.T) {
	var imports PayloadPreparationImports
	_, _, ok := imports.TryBegin(t.Context(), 9, 11, func() bool { return true })
	require.False(t, ok)
	_, finish, ok := imports.TryBegin(t.Context(), 9, 11, func() bool { return false })
	require.True(t, ok)
	finish()
}

func TestPayloadPreparationImportArrivesDuringAdmission(t *testing.T) {
	var imports PayloadPreparationImports
	_, _, ok := imports.TryBegin(t.Context(), 9, 11, func() bool {
		finish := imports.BeginImport(10)
		finish()
		return false
	})
	require.False(t, ok)
}

func TestPayloadPreparationSeesParkedEnvelopesUntilEviction(t *testing.T) {
	queue, err := lru.New[common.Hash, *cltypes.SignedExecutionPayloadEnvelope](1)
	require.NoError(t, err)
	f := &ForkChoiceStore{pendingEnvelopes: queue}
	queue.Add(common.Hash{1}, &cltypes.SignedExecutionPayloadEnvelope{Message: &cltypes.ExecutionPayloadEnvelope{Payload: &cltypes.Eth1Block{SlotNumber: 10}}})
	_, _, ok := f.TryBeginPayloadPreparation(t.Context(), 10, 11)
	require.False(t, ok)
	queue.Add(common.Hash{2}, &cltypes.SignedExecutionPayloadEnvelope{Message: &cltypes.ExecutionPayloadEnvelope{Payload: &cltypes.Eth1Block{SlotNumber: 8}}})
	_, finish, ok := f.TryBeginPayloadPreparation(t.Context(), 10, 11)
	require.True(t, ok)
	finish()
}
