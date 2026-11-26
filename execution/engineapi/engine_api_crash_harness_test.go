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

package engineapi_test

import (
	"context"
	"errors"
	"os"
	"os/exec"
	"testing"
	"testing/synctest"
	"time"

	"github.com/holiman/uint256"
	"github.com/stretchr/testify/require"

	"github.com/erigontech/erigon/common"
	"github.com/erigontech/erigon/db/kv"
	"github.com/erigontech/erigon/execution/execmodule"
	"github.com/erigontech/erigon/execution/rlp"
	"github.com/erigontech/erigon/execution/types"
)

func TestCrashRecoveryRequestMode(t *testing.T) {
	t.Run("tip", func(t *testing.T) {
		var request crashRecoveryRequest
		for _, point := range []execmodule.StateTransitionPoint{
			execmodule.StateTransitionUnwindComplete,
			execmodule.StateTransitionOverlayPublished,
			execmodule.StateTransitionCommitReady,
			execmodule.StateTransitionCommitComplete,
			execmodule.StateTransitionOverlayCleared,
		} {
			request.Point = point
			require.NoError(t, request.validateMode(), "point %d", point)
		}
		for _, point := range []execmodule.StateTransitionPoint{
			execmodule.StateTransitionRPCViewBound,
			execmodule.StateTransitionFCUCatchupCommitReady,
			execmodule.StateTransitionFCUCatchupCommitComplete,
			255,
		} {
			request.Point = point
			require.ErrorContains(t, request.validateMode(), "tip mode requires a tip FCU boundary", "point %d", point)
		}
		request.Point = execmodule.StateTransitionCommitReady
		request.Downloaded = []crashRecoveryBlock{{}}
		require.ErrorContains(t, request.validateMode(), "tip-mode crash requests must not include downloaded blocks")
	})
	t.Run("catchup", func(t *testing.T) {
		request := crashRecoveryRequest{CatchupCycle: 1}
		for _, point := range []execmodule.StateTransitionPoint{
			execmodule.StateTransitionFCUCatchupCommitReady,
			execmodule.StateTransitionFCUCatchupCommitComplete,
		} {
			request.Point = point
			require.NoError(t, request.validateMode())
		}
		request.Point = execmodule.StateTransitionCommitReady
		require.ErrorContains(t, request.validateMode(), "catch-up mode requires a catch-up commit boundary")
	})
	t.Run("negative_cycle", func(t *testing.T) {
		request := crashRecoveryRequest{CatchupCycle: -1, Point: execmodule.StateTransitionCommitReady}
		require.ErrorContains(t, request.validateMode(), "catch-up cycle must not be negative")
	})
}

func TestCrashRecoveryBlocksRequireBAL(t *testing.T) {
	bal := types.NewBlockAccessListSidecar(types.BlockAccessList{})
	hash, err := bal.Hash()
	require.NoError(t, err)
	// Header extension fields are positional in RLP; include all fields before
	// the BAL hash so the round trip preserves its meaning.
	header := &types.Header{
		Number:                uint256.Int{1},
		BaseFee:               new(uint256.Int),
		WithdrawalsHash:       new(common.Hash),
		BlobGasUsed:           new(uint64),
		ExcessBlobGas:         new(uint64),
		ParentBeaconBlockRoot: new(common.Hash),
		RequestsHash:          new(common.Hash),
		BlockAccessListHash:   &hash,
		SlotNumber:            new(uint64),
	}
	block := types.NewBlockWithHeader(header, bal)
	encoded, err := rlp.EncodeToBytes(block)
	require.NoError(t, err)
	_, err = decodeCrashRecoveryBlocks([]crashRecoveryBlock{{RLP: encoded}})
	require.ErrorContains(t, err, "missing block access list")
	balBytes, err := bal.Bytes()
	require.NoError(t, err)
	decoded, err := decodeCrashRecoveryBlocks([]crashRecoveryBlock{{RLP: encoded, BAL: balBytes}})
	require.NoError(t, err)
	require.Equal(t, header.BlockAccessListHash, decoded[0].BlockAccessListHash())
	require.NotNil(t, decoded[0].BlockAccessList())
}

type crashRecoveryReadFailure struct {
	kv.TemporalRoDB
}

func (crashRecoveryReadFailure) View(context.Context, func(kv.Tx) error) error {
	return errors.New("injected database read failure")
}

func TestCrashRecoveryExecutionReadFailure(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		started := time.Now()
		err := waitCrashRecoveryExecution(t.Context(), crashRecoveryReadFailure{}, 62)
		require.ErrorContains(t, err, "injected database read failure")
		require.Equal(t, started, time.Now(), "database errors must not wait for the polling deadline")
	})
}

func TestCrashRecoveryAttemptDeadline(t *testing.T) {
	now := time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC)
	for _, tc := range []struct {
		name         string
		testDeadline time.Time
		want         time.Time
	}{
		{"no_test_deadline", time.Time{}, now.Add(rpcClientTimeout)},
		{"long_test_deadline", now.Add(2 * rpcClientTimeout), now.Add(rpcClientTimeout)},
		{"cleanup_reserve", now.Add(5 * time.Minute), now.Add(4*time.Minute + 30*time.Second)},
		{"near_deadline", now.Add(time.Second), now.Add(900 * time.Millisecond)},
		{"at_deadline", now, now},
		{"expired", now.Add(-time.Minute), now.Add(-time.Minute)},
	} {
		t.Run(tc.name, func(t *testing.T) {
			require.Equal(t, tc.want, crashRecoveryAttemptDeadline(now, tc.testDeadline))
		})
	}
}

func TestCrashRecoveryRejectsChildFailure(t *testing.T) {
	const failureChild = "ERIGON_CRASH_FAILURE_CHILD"
	if os.Getenv(failureChild) == "1" {
		t.Cleanup(func() { os.Exit(crashRecoveryFailureExitCode) })
		t.Fatal("intentional child failure")
	}
	executable, err := os.Executable()
	require.NoError(t, err)
	ctx, cancel := context.WithTimeout(t.Context(), time.Minute)
	defer cancel()
	cmd := exec.CommandContext(ctx, executable, "-test.run=^TestCrashRecoveryRejectsChildFailure$", "-test.v")
	cmd.Env = append(os.Environ(), failureChild+"=1")
	output, err := cmd.CombinedOutput()
	require.NoError(t, ctx.Err())
	var exited *exec.ExitError
	require.ErrorAs(t, err, &exited)
	require.Equal(t, crashRecoveryFailureExitCode, exited.ExitCode())
	require.Contains(t, string(output), "intentional child failure")
	require.NotContains(t, string(output), "WARNING: DATA RACE")
	require.Error(t, crashRecoveryExitError(err), "a failed child is not a boundary-triggered kill")
}

func TestCrashRecoveryWaitsForTransition(t *testing.T) {
	t.Run("boundary_and_response_ready", func(t *testing.T) {
		failed := errors.New("forkchoice failed")
		for _, tc := range []struct {
			name              string
			allowEarlySuccess bool
			response          error
		}{
			{name: "unexpected_success"},
			{name: "allowed_success", allowEarlySuccess: true},
			{name: "failure", response: failed},
			{name: "failure_with_early_success_allowed", allowEarlySuccess: true, response: failed},
		} {
			t.Run(tc.name, func(t *testing.T) {
				reached := make(chan struct{})
				close(reached)
				hold := &stateTransitionHold{point: execmodule.StateTransitionCommitReady, reached: reached}
				for range 100 {
					response := make(chan error, 1)
					response <- tc.response
					err := waitCrashRecoveryTransition(t.Context(), hold, response, tc.allowEarlySuccess)
					switch {
					case tc.response != nil:
						require.ErrorIs(t, err, tc.response)
					case !tc.allowEarlySuccess:
						require.ErrorContains(t, err, "returned before transition")
					default:
						require.NoError(t, err)
					}
				}
			})
		}
	})
	t.Run("unexpected_early_valid", func(t *testing.T) {
		synctest.Test(t, func(t *testing.T) {
			transitions := newStateTransitionController()
			ready := transitions.hold(t, execmodule.StateTransitionFCUCatchupCommitReady, 1)
			response := make(chan error, 1)
			response <- nil
			ctx, cancel := context.WithTimeout(t.Context(), time.Minute)
			defer cancel()
			require.ErrorContains(t, waitCrashRecoveryTransition(ctx, ready, response, false), "returned before transition")
		})
	})
	t.Run("early_valid", func(t *testing.T) {
		synctest.Test(t, func(t *testing.T) {
			transitions := newStateTransitionController()
			cleared := transitions.hold(t, execmodule.StateTransitionOverlayCleared, 1)
			response := make(chan error)
			done := make(chan error, 1)
			go func() { done <- waitCrashRecoveryTransition(t.Context(), cleared, response, true) }()
			response <- nil
			synctest.Wait()
			select {
			case <-done:
				t.Fatal("an early VALID response must not finish the teardown wait")
			default:
			}
			go transitions.observe(t.Context(), execmodule.StateTransitionOverlayCleared)
			require.NoError(t, <-done)
			cleared.release()
		})
	})
	t.Run("boundary_before_response", func(t *testing.T) {
		synctest.Test(t, func(t *testing.T) {
			transitions := newStateTransitionController()
			ready := transitions.hold(t, execmodule.StateTransitionCommitReady, 1)
			go transitions.observe(t.Context(), execmodule.StateTransitionCommitReady)
			require.NoError(t, waitCrashRecoveryTransition(t.Context(), ready, make(chan error), true))
			ready.release()
		})
	})
	t.Run("fcu_error", func(t *testing.T) {
		transitions := newStateTransitionController()
		ready := transitions.hold(t, execmodule.StateTransitionCommitReady, 1)
		response := make(chan error, 1)
		failed := errors.New("forkchoice failed")
		response <- failed
		require.ErrorIs(t, waitCrashRecoveryTransition(t.Context(), ready, response, true), failed)
	})
	t.Run("deadline", func(t *testing.T) {
		synctest.Test(t, func(t *testing.T) {
			transitions := newStateTransitionController()
			ready := transitions.hold(t, execmodule.StateTransitionCommitReady, 1)
			ctx, cancel := context.WithTimeout(t.Context(), time.Minute)
			defer cancel()
			require.ErrorIs(t, waitCrashRecoveryTransition(ctx, ready, nil, true), context.DeadlineExceeded)
		})
	})
}
