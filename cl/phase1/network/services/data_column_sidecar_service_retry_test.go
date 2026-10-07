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
	"time"

	"go.uber.org/mock/gomock"

	"github.com/erigontech/erigon/common"
)

// A stored Gloas sidecar may complete the block's data; the envelope queued for that block
// must be retried right away instead of at the next slot boundary.
func (t *dataColumnSidecarTestSuite) TestGloasProcessMessage_RetriesPendingEnvelopeAfterStoringSidecar() {
	verifyDataColumnSidecarWithCommitments = t.mockFuncs.VerifyDataColumnSidecarWithCommitments
	verifyDataColumnSidecarKZGProofsWithCommitments = t.mockFuncs.VerifyDataColumnSidecarKZGProofsWithCommitments

	t.mockSyncedData.EXPECT().Syncing().Return(false)
	t.mockEthClock.EXPECT().GetCurrentSlot().Return(testSlot).AnyTimes()
	t.mockFuncs.ctrl.RecordCall(t.mockFuncs, "VerifyDataColumnSidecarWithCommitments", gomock.Any(), gomock.Any()).Return(true).AnyTimes()
	t.mockFuncs.ctrl.RecordCall(t.mockFuncs, "VerifyDataColumnSidecarKZGProofsWithCommitments", gomock.Any(), gomock.Any()).Return(true).AnyTimes()
	t.mockForkChoice.Blocks[testBlockRoot] = createMockGloasBlock(testSlot, testBlockRoot)
	t.mockColumnSidecarStorage.EXPECT().WriteColumnSidecars(gomock.Any(), gomock.Any(), gomock.Any(), gomock.Any()).Return(nil).Times(1)
	t.mockPeerDas.EXPECT().TryScheduleRecover(gomock.Any(), gomock.Any()).Return(nil).Times(1)

	retried := make(chan common.Hash, 1)
	t.mockForkChoice.RetryPendingEnvelopeFunc = func(_ context.Context, root common.Hash) {
		retried <- root
	}

	err := t.dataColumnSidecarService.ProcessMessage(context.Background(), nil, createMockGloasDataColumnSidecar(testSlot, 0, testBlockRoot))
	t.NoError(err)
	select {
	case root := <-retried:
		t.Equal(testBlockRoot, root)
	case <-time.After(5 * time.Second):
		t.Fail("pending envelope retry was not triggered")
	}
}
