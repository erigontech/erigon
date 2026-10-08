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

	"github.com/erigontech/erigon/cl/beacon/beaconevents"
	"github.com/erigontech/erigon/common"
)

// A stored Gloas sidecar may complete the block's data; the envelope queued for that block
// must be retried right away instead of at the next slot boundary.
func (t *dataColumnSidecarTestSuite) TestGloasProcessMessage_RetriesPendingEnvelopeAfterStoringSidecar() {
	verifyDataColumnSidecarWithCommitments = t.mockFuncs.VerifyDataColumnSidecarWithCommitments
	verifyDataColumnSidecarKZGProofsWithCommitments = t.mockFuncs.VerifyDataColumnSidecarKZGProofsWithCommitments

	// Gloas must be active at testSlot: the sidecar is a Gloas sidecar.
	t.beaconConfig.ElectraForkEpoch = 0
	t.beaconConfig.FuluForkEpoch = 0
	t.beaconConfig.GloasForkEpoch = testSlot / t.beaconConfig.SlotsPerEpoch
	t.mockSyncedData.EXPECT().Syncing().Return(false)
	t.mockEthClock.EXPECT().GetCurrentSlot().Return(testSlot).AnyTimes()
	t.mockFuncs.ctrl.RecordCall(t.mockFuncs, "VerifyDataColumnSidecarWithCommitments", gomock.Any(), gomock.Any()).Return(true).AnyTimes()
	t.mockFuncs.ctrl.RecordCall(t.mockFuncs, "VerifyDataColumnSidecarKZGProofsWithCommitments", gomock.Any(), gomock.Any()).Return(true).AnyTimes()
	t.mockForkChoice.Blocks[testBlockRoot] = createMockGloasBlock(testSlot)
	t.mockColumnSidecarStorage.EXPECT().WriteColumnSidecars(gomock.Any(), gomock.Any(), gomock.Any(), gomock.Any()).Return(nil).Times(1)
	t.mockPeerDas.EXPECT().TryScheduleRecover(gomock.Any(), gomock.Any()).Return(nil).Times(1)
	// The suite's service runs under a cancelled context and a config without slot timing;
	// the hook must get a live context and a real retry budget.
	t.beaconConfig.SecondsPerSlot = 12
	service := NewDataColumnSidecarService(
		t.T().Context(), t.beaconConfig, t.mockEthClock, t.mockForkChoice, t.mockSyncedData, t.mockColumnSidecarStorage, beaconevents.NewEventEmitter(),
	)

	type retry struct {
		root   common.Hash
		ctxErr error
	}
	retried := make(chan retry, 1)
	t.mockForkChoice.RetryPendingEnvelopeFunc = func(ctx context.Context, root common.Hash) {
		retried <- retry{root: root, ctxErr: ctx.Err()}
	}

	err := service.ProcessMessage(context.Background(), nil, createMockGloasDataColumnSidecar(testSlot, 0, testBlockRoot))
	t.NoError(err)
	select {
	case got := <-retried:
		t.Equal(testBlockRoot, got.root)
		t.NoError(got.ctxErr, "the retry must run with a live context")
	case <-time.After(5 * time.Second):
		t.Fail("pending envelope retry was not triggered")
	}
}
