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

package services

import (
	"context"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/erigontech/erigon/cl/cltypes"
	"github.com/erigontech/erigon/cl/persistence/beacon_indicies"
	"github.com/erigontech/erigon/common"
	"github.com/erigontech/erigon/db/kv"
)

const acceptTestTimeout = 10 * time.Second

type blockingOnBlockStore struct {
	*onBlockErrorStore
	entered   chan struct{}
	release   chan struct{}
	panicWith any
	ctxErr    chan error
}

func (s *blockingOnBlockStore) OnBlock(ctx context.Context, block *cltypes.SignedBeaconBlock, newPayload, fullValidation, checkDataAvaiability bool) error {
	s.entered <- struct{}{}
	<-s.release
	s.ctxErr <- ctx.Err()
	if s.panicWith != nil {
		panic(s.panicWith)
	}
	return s.onBlockErrorStore.OnBlock(ctx, block, newPayload, fullValidation, checkDataAvaiability)
}

type skippingOnBlockStore struct {
	*onBlockErrorStore
}

func (s *skippingOnBlockStore) OnBlock(context.Context, *cltypes.SignedBeaconBlock, bool, bool, bool) error {
	s.calls.Add(1)
	return nil
}

func newAcceptFixture(t *testing.T) (*blockService, *cltypes.SignedBeaconBlock, *blockingOnBlockStore, common.Hash) {
	t.Helper()
	service, block, processing, _ := newFuluColumnGossipFixture(t)
	processing.err = nil
	root, err := block.Block.HashSSZ()
	require.NoError(t, err)
	store := &blockingOnBlockStore{
		onBlockErrorStore: processing,
		entered:           make(chan struct{}, 1),
		release:           make(chan struct{}),
		ctxErr:            make(chan error, 1),
	}
	service.forkchoiceStore = store
	t.Cleanup(func() {
		select {
		case <-store.release:
		default:
			close(store.release)
		}
	})
	return service, block, store, common.Hash(root)
}

func (s *blockingOnBlockStore) unblock() {
	close(s.release)
}

func processMessageWithin(t *testing.T, ctx context.Context, service *blockService, block *cltypes.SignedBeaconBlock) error {
	t.Helper()
	done := make(chan error, 1)
	go func() { done <- service.ProcessMessage(ctx, nil, block) }()
	select {
	case err := <-done:
		return err
	case <-time.After(acceptTestTimeout):
		t.Fatal("ProcessMessage did not return while the block import was still running")
		return nil
	}
}

func waitForGossipImports(t *testing.T, service *blockService) {
	t.Helper()
	require.Eventually(t, func() bool { return len(service.importSlots) == 0 }, acceptTestTimeout, time.Millisecond)
}

func blockPersisted(t *testing.T, service *blockService, root common.Hash) bool {
	t.Helper()
	var persisted bool
	require.NoError(t, service.db.View(t.Context(), func(tx kv.Tx) error {
		slot, err := beacon_indicies.ReadBlockSlotByBlockRoot(tx, root)
		persisted = slot != nil
		return err
	}))
	return persisted
}

func TestProcessMessageAcceptsFuluBlockBeforeImportFinishes(t *testing.T) {
	service, block, store, root := newAcceptFixture(t)

	require.NoError(t, processMessageWithin(t, t.Context(), service, block))
	select {
	case <-store.entered:
	case <-time.After(acceptTestTimeout):
		t.Fatal("the background import did not reach OnBlock")
	}
	require.False(t, blockPersisted(t, service, root))

	store.unblock()
	require.Eventually(t, func() bool { return blockPersisted(t, service, root) }, acceptTestTimeout, time.Millisecond)
	waitForGossipImports(t, service)
}

func TestProcessMessageHoldsPayloadPreparationFromAcceptUntilImportEnds(t *testing.T) {
	service, block, store, _ := newAcceptFixture(t)
	var started func()
	service.startGossipImport = func(f func()) { started = f }
	fcu := service.forkchoiceStore

	require.NoError(t, processMessageWithin(t, t.Context(), service, block))

	require.NotNil(t, started)
	require.True(t, fcu.PayloadImportInProgress(block.Block.Slot), "the hold must cover the gap between accept and import start")
	store.unblock()
	started()
	require.False(t, fcu.PayloadImportInProgress(block.Block.Slot))
	require.Empty(t, service.importSlots)
}

func TestProcessMessageImportSurvivesCancelingTheGossipContext(t *testing.T) {
	service, block, store, root := newAcceptFixture(t)
	ctx, cancel := context.WithCancel(t.Context())
	defer cancel()

	require.NoError(t, processMessageWithin(t, ctx, service, block))
	select {
	case <-store.entered:
	case <-time.After(acceptTestTimeout):
		t.Fatal("the background import did not reach OnBlock")
	}
	cancel()
	store.unblock()

	select {
	case err := <-store.ctxErr:
		require.NoError(t, err, "canceling the gossip context must not cancel the import")
	case <-time.After(acceptTestTimeout):
		t.Fatal("OnBlock did not finish")
	}
	require.Eventually(t, func() bool { return blockPersisted(t, service, root) }, acceptTestTimeout, time.Millisecond)
	waitForGossipImports(t, service)
	_, queued := service.blocksScheduledForLaterExecution.Load(root)
	require.False(t, queued)
}

func TestProcessMessageImportsInlineWhenAllImportSlotsAreBusy(t *testing.T) {
	service, block, store, root := newAcceptFixture(t)
	for range cap(service.importSlots) {
		service.importSlots <- struct{}{}
	}
	service.startGossipImport = func(func()) { t.Error("a busy service must not start another background import") }
	inline := gossipBlockImportInlineCounter
	imported := gossipBlockImportAfterAcceptCounter.WithLabelValues("imported")
	beforeInline, beforeImported := inline.GetValue(), imported.GetValue()
	store.unblock()

	require.NoError(t, service.ProcessMessage(t.Context(), nil, block))

	require.True(t, blockPersisted(t, service, root), "the inline import must finish before ProcessMessage returns")
	require.Equal(t, beforeInline+1, inline.GetValue())
	require.Equal(t, beforeImported+1, imported.GetValue(), "an inline import is counted once with its outcome")
	require.Len(t, service.importSlots, cap(service.importSlots), "the inline import must not release a slot it did not take")
	require.False(t, service.forkchoiceStore.PayloadImportInProgress(block.Block.Slot))
}

func TestProcessMessageRecoversFromPanicInBackgroundImport(t *testing.T) {
	service, block, store, _ := newAcceptFixture(t)
	store.panicWith = "boom"
	panics := gossipBlockImportAfterAcceptCounter.WithLabelValues("panic")
	before := panics.GetValue()
	store.unblock()

	require.NoError(t, service.ProcessMessage(t.Context(), nil, block))

	require.Eventually(t, func() bool { return panics.GetValue() == before+1 }, acceptTestTimeout, time.Millisecond)
	waitForGossipImports(t, service)
	require.Eventually(t, func() bool {
		return !service.forkchoiceStore.PayloadImportInProgress(block.Block.Slot)
	}, acceptTestTimeout, time.Millisecond)
}

func TestProcessMessageCountsImportSkippedByForkChoice(t *testing.T) {
	service, block, store, root := newAcceptFixture(t)
	skipping := &skippingOnBlockStore{onBlockErrorStore: store.onBlockErrorStore}
	service.forkchoiceStore = skipping
	skipped := gossipBlockImportAfterAcceptCounter.WithLabelValues("skipped")
	imported := gossipBlockImportAfterAcceptCounter.WithLabelValues("imported")
	beforeSkipped, beforeImported := skipped.GetValue(), imported.GetValue()

	require.NoError(t, service.ProcessMessage(t.Context(), nil, block))

	waitForGossipImports(t, service)
	require.Equal(t, int32(1), skipping.calls.Load())
	require.Equal(t, beforeSkipped+1, skipped.GetValue())
	require.Equal(t, beforeImported, imported.GetValue())
	require.False(t, blockPersisted(t, service, root))
}
