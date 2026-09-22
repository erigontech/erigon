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

package mock_services

import (
	"sync"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/erigontech/erigon/cl/clparams"
	"github.com/erigontech/erigon/cl/cltypes"
	"github.com/erigontech/erigon/common"
)

func TestGetHeadNodeReturnsConfiguredSnapshot(t *testing.T) {
	root := common.Hash{0x41}
	store := &ForkChoiceStorageMock{
		HeadVal:              root,
		HeadSlotVal:          42,
		HeadPayloadStatusVal: cltypes.PayloadStatusEmpty,
	}

	head, slot, err := store.GetHeadNode()

	require.NoError(t, err)
	require.Equal(t, root, head.Root)
	require.Equal(t, uint64(42), slot)
	require.Equal(t, cltypes.PayloadStatusEmpty, head.PayloadStatus)
}

func TestForkChoiceStorageMockStoresEnvelopeWithNilMap(t *testing.T) {
	mock := &ForkChoiceStorageMock{}
	root := common.HexToHash("0x1234")
	envelope := &cltypes.SignedExecutionPayloadEnvelope{Message: cltypes.NewExecutionPayloadEnvelope(&clparams.MainnetBeaconConfig)}
	envelope.Message.BeaconBlockRoot = root

	require.NotPanics(t, func() {
		require.NoError(t, mock.OnExecutionPayload(t.Context(), envelope, false, false))
	})
	require.True(t, mock.HasEnvelope(root))
	persisted, err := mock.ReadEnvelopeFromDisk(root)
	require.NoError(t, err)
	require.Same(t, envelope, persisted)
}

func TestForkChoiceStorageMockSetEnvelopeWithNilMap(t *testing.T) {
	mock := &ForkChoiceStorageMock{}
	root := common.HexToHash("0x1234")
	envelope := &cltypes.SignedExecutionPayloadEnvelope{}

	require.NotPanics(t, func() { mock.SetEnvelope(root, envelope) })
	require.True(t, mock.HasEnvelope(root))
	persisted, err := mock.ReadEnvelopeFromDisk(root)
	require.NoError(t, err)
	require.Same(t, envelope, persisted)
}

func TestForkChoiceStorageMockEnvelopeAccessIsConcurrentSafe(t *testing.T) {
	mock := &ForkChoiceStorageMock{}
	root := common.HexToHash("0x1234")
	envelope := &cltypes.SignedExecutionPayloadEnvelope{Message: cltypes.NewExecutionPayloadEnvelope(&clparams.MainnetBeaconConfig)}
	envelope.Message.BeaconBlockRoot = root

	var workers sync.WaitGroup
	for range 32 {
		workers.Add(4)
		go func() {
			defer workers.Done()
			require.NoError(t, mock.OnExecutionPayload(t.Context(), envelope, false, false))
		}()
		go func() {
			defer workers.Done()
			mock.SetEnvelope(root, envelope)
		}()
		go func() {
			defer workers.Done()
			mock.HasEnvelope(root)
		}()
		go func() {
			defer workers.Done()
			_, err := mock.ReadEnvelopeFromDisk(root)
			require.NoError(t, err)
		}()
	}
	workers.Wait()
	require.True(t, mock.HasEnvelope(root))
}
