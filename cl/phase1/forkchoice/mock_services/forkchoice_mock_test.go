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

	"github.com/erigontech/erigon/cl/cltypes"
	"github.com/erigontech/erigon/common"
)

func TestGetHeadPayloadStatusRequiresMatchingHead(t *testing.T) {
	root := common.Hash{0x41}
	store := &ForkChoiceStorageMock{HeadPayloadStatusVal: cltypes.PayloadStatusEmpty}

	status, matches := store.GetHeadPayloadStatus(root)

	require.False(t, matches)
	require.Equal(t, cltypes.PayloadStatusPending, status)

	store.HeadVal = root
	status, matches = store.GetHeadPayloadStatus(root)
	require.True(t, matches)
	require.Equal(t, cltypes.PayloadStatusEmpty, status)
}

func TestGetHeadPayloadStatusRefreshIsConcurrentSafe(t *testing.T) {
	root := common.Hash{0x41}
	store := &ForkChoiceStorageMock{
		HeadVal:              root,
		HeadPayloadStatusVal: cltypes.PayloadStatusFull,
	}
	store.HeadPayloadStatusInvalidated.Store(true)

	var calls sync.WaitGroup
	results := make(chan struct {
		status  cltypes.PayloadStatus
		matches bool
	}, 32)
	for range 32 {
		calls.Go(func() {
			status, matches := store.GetHeadPayloadStatus(root)
			results <- struct {
				status  cltypes.PayloadStatus
				matches bool
			}{status: status, matches: matches}
		})
	}
	calls.Wait()
	close(results)
	for result := range results {
		require.True(t, result.matches)
		require.Equal(t, cltypes.PayloadStatusFull, result.status)
	}
}
