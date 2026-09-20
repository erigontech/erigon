// Copyright 2026 The Erigon Authors
// This file is part of Erigon.
//
// Erigon is free software: you can redistribute it and/or modify
// it under the terms of the GNU Lesser General Public License as published by
// the Free Software Foundation, either version 3 of the License, or
// (at your option) any later version.

package epbs

import (
	"errors"
	"math/big"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/erigontech/erigon/common"
	"github.com/erigontech/erigon/common/dir"
)

func TestPendingPayloadStoreDoesNotOverwritePublishedIdentity(t *testing.T) {
	config := gloasCoordinatorConfig()
	input := validCoordinatorSlotInput(config)
	store, err := OpenPendingPayloadStore(filepath.Join(t.TempDir(), "pending"), &config, 2)
	require.NoError(t, err)
	assembled := validCoordinatorPayload(&config, input, big.NewInt(1_000_000_000))
	requests, _, err := decodeExecutionRequests(&config, assembled.RequestsBundle)
	require.NoError(t, err)
	first := &RetainedPayload{
		Assembled: assembled, ExecutionRequests: requests,
		BidValue: 1, BuilderIndex: input.BuilderIndex, BuilderPubkey: input.BuilderPubkey,
		SignedBidRoot: common.Hash{1}, GenesisRoot: input.GenesisValidatorsRoot,
	}
	identity := payloadIdentity(input, first.Assembled)
	require.NoError(t, store.Save(identity, first))
	second := *first
	second.SignedBidRoot = common.Hash{2}
	require.Error(t, store.Save(identity, &second))
	loaded, err := store.Load(input.Slot)
	require.NoError(t, err)
	require.Equal(t, first.SignedBidRoot, loaded[identity].SignedBidRoot)
}

func TestPendingPayloadStoreRemovesUnpublishedRecordAfterDirectorySyncFailure(t *testing.T) {
	config := gloasCoordinatorConfig()
	input := validCoordinatorSlotInput(config)
	store, err := OpenPendingPayloadStore(filepath.Join(t.TempDir(), "pending"), &config, 2)
	require.NoError(t, err)
	assembled := validCoordinatorPayload(&config, input, big.NewInt(1_000_000_000))
	requests, _, err := decodeExecutionRequests(&config, assembled.RequestsBundle)
	require.NoError(t, err)
	payload := &RetainedPayload{
		Assembled: assembled, ExecutionRequests: requests,
		BidValue: 1, BuilderIndex: input.BuilderIndex, BuilderPubkey: input.BuilderPubkey,
		SignedBidRoot: common.Hash{1}, GenesisRoot: input.GenesisValidatorsRoot,
	}
	calls := 0
	store.syncDirectory = func(path string) error {
		calls++
		if calls == 1 {
			return errors.New("injected directory sync failure")
		}
		return dir.FsyncDir(path)
	}
	identity := payloadIdentity(input, assembled)
	require.ErrorContains(t, store.Save(identity, payload), "injected directory sync failure")
	require.Equal(t, 2, calls)
	loaded, err := store.Load(input.Slot)
	require.NoError(t, err)
	require.Empty(t, loaded)
}

func TestPendingPayloadStorePruneWaitsForStoreLock(t *testing.T) {
	config := gloasCoordinatorConfig()
	store, err := OpenPendingPayloadStore(filepath.Join(t.TempDir(), "pending"), &config, 2)
	require.NoError(t, err)
	store.mu.Lock()
	started := make(chan struct{})
	done := make(chan error, 1)
	go func() {
		close(started)
		done <- store.PruneBeforeSlot(65)
	}()
	<-started
	select {
	case err := <-done:
		store.mu.Unlock()
		t.Fatalf("prune did not wait for pending write: %v", err)
	case <-time.After(20 * time.Millisecond):
	}
	store.mu.Unlock()
	require.NoError(t, <-done)
}

func TestPendingPayloadStoreRejectsCorruptionAndPrunesExpiredRecords(t *testing.T) {
	config := gloasCoordinatorConfig()
	input := validCoordinatorSlotInput(config)
	store, err := OpenPendingPayloadStore(filepath.Join(t.TempDir(), "pending"), &config, 2)
	require.NoError(t, err)
	assembled := validCoordinatorPayload(&config, input, big.NewInt(1_000_000_000))
	requests, _, err := decodeExecutionRequests(&config, assembled.RequestsBundle)
	require.NoError(t, err)
	payload := &RetainedPayload{
		Assembled: assembled, ExecutionRequests: requests,
		BidValue: 1, BuilderIndex: input.BuilderIndex, BuilderPubkey: input.BuilderPubkey,
		SignedBidRoot: common.Hash{1}, GenesisRoot: input.GenesisValidatorsRoot,
	}
	identity := payloadIdentity(input, assembled)
	require.NoError(t, store.Save(identity, payload))
	require.NoError(t, os.WriteFile(store.recordPath(identity), []byte("{incomplete"), 0o600))
	_, err = store.Load(input.Slot)
	require.Error(t, err)
	require.NoError(t, store.PruneBeforeSlot(input.Slot))
	_, err = store.Load(input.Slot)
	require.Error(t, err)
	require.NoError(t, store.PruneBeforeSlot(input.Slot+1))
	loaded, err := store.Load(input.Slot + 1)
	require.NoError(t, err)
	require.Empty(t, loaded)
}
