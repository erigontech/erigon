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

package handler

import (
	"net/http"
	"testing"

	"github.com/stretchr/testify/require"
	"go.uber.org/mock/gomock"

	sync_mock_services "github.com/erigontech/erigon/cl/beacon/synced_data/mock_services"
	mock_services2 "github.com/erigontech/erigon/cl/phase1/forkchoice/mock_services"
	"github.com/erigontech/erigon/common"
)

// The memoized head state trails the fork choice head by however long it takes to copy
// the head state, so resolving head from it hands out a stale root for the whole window.
func TestGetHeadDoesNotTrailTheMemoizedHeadState(t *testing.T) {
	ctrl := gomock.NewController(t)
	syncedData := sync_mock_services.NewMockSyncedData(ctrl)
	syncedData.EXPECT().Syncing().Return(false).AnyTimes()
	syncedData.EXPECT().HeadRoot().Return(common.Hash{0xbb}).AnyTimes()
	syncedData.EXPECT().HeadSlot().Return(uint64(99)).AnyTimes()

	fcu := mock_services2.NewForkChoiceStorageMock(t)
	fcu.HeadVal = common.Hash{0xaa}
	fcu.HeadSlotVal = 100

	a := &ApiHandler{
		enableMemoizedHeadState: true,
		syncedData:              syncedData,
		forkchoiceStore:         fcu,
	}

	root, slot, statusCode, err := a.getHead()
	require.NoError(t, err)
	require.Equal(t, 0, statusCode)
	require.Equal(t, common.Hash{0xaa}, root)
	require.Equal(t, uint64(100), slot)
}

func TestGetHeadReportsSyncing(t *testing.T) {
	ctrl := gomock.NewController(t)
	syncedData := sync_mock_services.NewMockSyncedData(ctrl)
	syncedData.EXPECT().Syncing().Return(true).AnyTimes()

	a := &ApiHandler{
		enableMemoizedHeadState: true,
		syncedData:              syncedData,
		forkchoiceStore:         mock_services2.NewForkChoiceStorageMock(t),
	}

	_, _, statusCode, err := a.getHead()
	require.Error(t, err)
	require.Equal(t, http.StatusServiceUnavailable, statusCode)
}
