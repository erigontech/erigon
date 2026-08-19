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

package execution_client

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/erigontech/erigon/cl/clparams"
	"github.com/erigontech/erigon/common"
	"github.com/erigontech/erigon/execution/chain"
	"github.com/erigontech/erigon/execution/engineapi/engine_types"
	"github.com/erigontech/erigon/execution/execmodule"
	"github.com/erigontech/erigon/execution/execmodule/chainreader"
)

type forkChoiceModuleStub struct {
	execmodule.ExecutionModule
	result execmodule.ForkChoiceResult
}

func (s *forkChoiceModuleStub) UpdateForkChoice(context.Context, common.Hash, common.Hash, common.Hash) (execmodule.ForkChoiceResult, error) {
	return s.result, nil
}

func TestDirectHeadForkChoiceUpdateReportsBusy(t *testing.T) {
	module := &forkChoiceModuleStub{result: execmodule.ForkChoiceResult{Status: execmodule.ExecutionStatusBusy}}
	client, err := NewExecutionClientDirect(chainreader.NewChainReaderEth1(chain.AllProtocolChanges, module, time.Second), nil)
	require.NoError(t, err)

	_, err = client.ForkChoiceUpdate(t.Context(), common.Hash{}, common.Hash{}, common.Hash{0x41}, nil, clparams.ElectraVersion)

	require.ErrorIs(t, err, ErrForkChoiceBusy)
}

func TestDirectForkChoiceUpdateReportsTransientStatusesAsBusy(t *testing.T) {
	for _, status := range []execmodule.ExecutionStatus{
		execmodule.ExecutionStatusBusy,
		execmodule.ExecutionStatusMissingSegment,
		execmodule.ExecutionStatusTooFarAway,
	} {
		for _, attributes := range []*engine_types.PayloadAttributes{nil, {}} {
			module := &forkChoiceModuleStub{result: execmodule.ForkChoiceResult{Status: status}}
			client, err := NewExecutionClientDirect(chainreader.NewChainReaderEth1(chain.AllProtocolChanges, module, time.Second), nil)
			require.NoError(t, err)

			_, err = client.ForkChoiceUpdate(
				t.Context(), common.Hash{}, common.Hash{}, common.Hash{0x41}, attributes, clparams.ElectraVersion,
			)

			require.ErrorIs(t, err, ErrForkChoiceBusy, "status %d, payload build %t", status, attributes != nil)
		}
	}
}

type forkChoiceSequenceEngine struct {
	ExecutionEngine
	errors            []error
	calls             int
	supportsInsertion bool
}

func (e *forkChoiceSequenceEngine) ForkChoiceUpdate(context.Context, common.Hash, common.Hash, common.Hash, *engine_types.PayloadAttributes, clparams.StateVersion) ([]byte, error) {
	err := e.errors[e.calls]
	e.calls++
	return nil, err
}

func (e *forkChoiceSequenceEngine) SupportInsertion() bool {
	return e.supportsInsertion
}

func TestRetryForkChoiceUpdateWaitsOutContention(t *testing.T) {
	engine := &forkChoiceSequenceEngine{
		errors:            []error{ErrForkChoiceBusy, ErrForkChoiceUpdateTimeout, nil},
		supportsInsertion: true,
	}

	_, err := RetryForkChoiceUpdate(t.Context(), engine, common.Hash{}, common.Hash{}, common.Hash{0x41}, clparams.ElectraVersion)

	require.NoError(t, err)
	require.Equal(t, 3, engine.calls)
}

func TestRetryForkChoiceUpdateDoesNotRetryRemoteEngine(t *testing.T) {
	engine := &forkChoiceSequenceEngine{errors: []error{ErrForkChoiceBusy, nil}}

	_, err := RetryForkChoiceUpdate(t.Context(), engine, common.Hash{}, common.Hash{}, common.Hash{0x41}, clparams.ElectraVersion)

	require.ErrorIs(t, err, ErrForkChoiceBusy)
	require.Equal(t, 1, engine.calls)
}

type forkChoiceErrorEngine struct {
	ExecutionEngine
	err   error
	calls int
}

func (e *forkChoiceErrorEngine) ForkChoiceUpdate(context.Context, common.Hash, common.Hash, common.Hash, *engine_types.PayloadAttributes, clparams.StateVersion) ([]byte, error) {
	e.calls++
	return nil, e.err
}

func (e *forkChoiceErrorEngine) SupportInsertion() bool {
	return true
}

func TestRetryForkChoiceUpdateStopsAfterRetryWindow(t *testing.T) {
	engine := &forkChoiceErrorEngine{err: ErrForkChoiceBusy}
	ctx, cancel := context.WithTimeout(t.Context(), time.Second)
	defer cancel()

	_, err := retryForkChoiceUpdate(
		ctx, engine, common.Hash{}, common.Hash{}, common.Hash{0x41}, clparams.ElectraVersion,
		10*time.Millisecond, time.Millisecond,
	)

	require.ErrorIs(t, err, ErrForkChoiceBusy)
}

type blockingForkChoiceEngine struct {
	ExecutionEngine
}

func (e *blockingForkChoiceEngine) ForkChoiceUpdate(ctx context.Context, _ common.Hash, _ common.Hash, _ common.Hash, _ *engine_types.PayloadAttributes, _ clparams.StateVersion) ([]byte, error) {
	<-ctx.Done()
	return nil, ctx.Err()
}

func TestRetryForkChoiceUpdateBoundsEachAttemptByRetryWindow(t *testing.T) {
	_, err := retryForkChoiceUpdate(
		t.Context(), &blockingForkChoiceEngine{}, common.Hash{}, common.Hash{}, common.Hash{0x41}, clparams.ElectraVersion,
		10*time.Millisecond, time.Millisecond,
	)

	require.ErrorIs(t, err, context.DeadlineExceeded)
}

func TestRetryForkChoiceUpdateDoesNotStartWithCanceledContext(t *testing.T) {
	ctx, cancel := context.WithCancel(t.Context())
	cancel()
	engine := &forkChoiceErrorEngine{err: ErrForkChoiceBusy}

	_, err := RetryForkChoiceUpdate(ctx, engine, common.Hash{}, common.Hash{}, common.Hash{0x41}, clparams.ElectraVersion)

	require.ErrorIs(t, err, context.Canceled)
	require.Zero(t, engine.calls)
}

func TestRetryAssembleBlockReturnsFirstSuccess(t *testing.T) {
	calls := 0
	id, err := retryAssembleBlock(t.Context(), 3, time.Millisecond, func(context.Context) (uint64, error) {
		calls++
		if calls < 3 {
			return 0, chainreader.ErrExecutionBusy
		}
		return 7, nil
	})

	require.NoError(t, err)
	require.Equal(t, uint64(7), id)
	require.Equal(t, 3, calls)
}

func TestRetryAssembleBlockStopsOnRejection(t *testing.T) {
	rejected := errors.New("withdrawals before shanghai")
	calls := 0
	_, err := retryAssembleBlock(t.Context(), 30, time.Hour, func(context.Context) (uint64, error) {
		calls++
		return 0, rejected
	})

	// Only contention settles by waiting; a rejection answers the same way however often it is
	// asked, so retrying it just burns the slot.
	require.ErrorIs(t, err, rejected)
	require.Equal(t, 1, calls)
}

func TestRetryAssembleBlockGivesUpAfterAttempts(t *testing.T) {
	calls := 0
	_, err := retryAssembleBlock(t.Context(), 2, time.Millisecond, func(context.Context) (uint64, error) {
		calls++
		return 0, chainreader.ErrExecutionBusy
	})

	require.ErrorIs(t, err, chainreader.ErrExecutionBusy)
	require.Equal(t, 2, calls)
}

func TestRetryAssembleBlockStopsWhenContextIsCanceled(t *testing.T) {
	ctx, cancel := context.WithCancel(t.Context())
	calls := 0
	_, err := retryAssembleBlock(ctx, 30, time.Hour, func(context.Context) (uint64, error) {
		calls++
		cancel()
		return 0, chainreader.ErrExecutionBusy
	})

	require.ErrorIs(t, err, context.Canceled)
	require.Equal(t, 1, calls)
}

func TestRetryAssembleBlockDoesNotStartWithCanceledContext(t *testing.T) {
	ctx, cancel := context.WithCancel(t.Context())
	cancel()
	calls := 0
	_, err := retryAssembleBlock(ctx, 30, time.Hour, func(context.Context) (uint64, error) {
		calls++
		return 0, chainreader.ErrExecutionBusy
	})

	require.ErrorIs(t, err, context.Canceled)
	require.Zero(t, calls)
}

func TestRetryAssembleBlockRejectsNoAttempts(t *testing.T) {
	_, err := retryAssembleBlock(t.Context(), 0, time.Millisecond, func(context.Context) (uint64, error) {
		return 1, nil
	})
	require.EqualError(t, err, "assemble block requires at least one attempt")
}
