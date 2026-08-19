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

package chainreader

import (
	"context"
	"errors"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/erigontech/erigon/common"
	"github.com/erigontech/erigon/execution/execmodule"
)

// assembledBlockStub answers GetAssembledBlock with a fixed result and panics on anything else, so
// a test can drive the one boundary it cares about.
type assembledBlockStub struct {
	execmodule.ExecutionModule
	result execmodule.AssembledBlockResult
}

type blockingForkChoiceStub struct {
	execmodule.ExecutionModule
	started chan context.Context
	stopped chan error
}

func (s blockingForkChoiceStub) UpdateForkChoice(ctx context.Context, _, _, _ common.Hash) (execmodule.ForkChoiceResult, error) {
	s.started <- ctx
	<-ctx.Done()
	s.stopped <- context.Cause(ctx)
	return execmodule.ForkChoiceResult{Status: execmodule.ExecutionStatusBusy}, nil
}

func (s assembledBlockStub) GetAssembledBlock(context.Context, uint64) (execmodule.AssembledBlockResult, error) {
	return s.result, nil
}

func TestGetAssembledBlockDistinguishesAnUnknownIdFromAnEmptyOne(t *testing.T) {
	unknown := ChainReaderWriterEth1{executionModule: assembledBlockStub{result: execmodule.AssembledBlockResult{Unknown: true}}}
	_, _, _, _, err := unknown.GetAssembledBlock(t.Context(), 1)

	// Nothing will ever arrive for an id with no builder behind it. Reporting that as an ordinary
	// empty result leaves a caller polling it for the rest of the slot.
	require.ErrorIs(t, err, ErrUnknownPayload)

	busy := ChainReaderWriterEth1{executionModule: assembledBlockStub{result: execmodule.AssembledBlockResult{Busy: true}}}
	_, _, _, _, err = busy.GetAssembledBlock(t.Context(), 1)
	require.ErrorIs(t, err, ErrExecutionBusy)

	// A builder that simply has nothing yet is neither: the caller should keep waiting.
	building := ChainReaderWriterEth1{executionModule: assembledBlockStub{}}
	block, _, _, _, err := building.GetAssembledBlock(t.Context(), 1)
	require.NoError(t, err)
	require.Nil(t, block)
}

// The response timer reports Busy without becoming the work's cancellation cause. A later explicit
// caller cause must still reach asynchronous forkchoice work.
func TestUpdateForkChoiceTimeoutKeepsListeningForCallerCancellation(t *testing.T) {
	started := make(chan context.Context, 1)
	stopped := make(chan error, 1)
	reader := ChainReaderWriterEth1{executionModule: blockingForkChoiceStub{started: started, stopped: stopped}}
	requestCtx, cancelRequest := context.WithCancelCause(t.Context())

	result := make(chan struct {
		status execmodule.ExecutionStatus
		err    error
	}, 1)
	go func() {
		status, _, _, err := reader.UpdateForkChoice(requestCtx, common.Hash{}, common.Hash{}, common.Hash{}, 5)
		result <- struct {
			status execmodule.ExecutionStatus
			err    error
		}{status: status, err: err}
	}()
	callCtx := <-started
	callResult := <-result
	require.NoError(t, callResult.err)
	require.Equal(t, execmodule.ExecutionStatusBusy, callResult.status)

	expectedErr := errors.New("selected head changed")
	cancelRequest(expectedErr)
	require.ErrorIs(t, <-stopped, expectedErr)
	require.ErrorIs(t, context.Cause(callCtx), expectedErr)
}
