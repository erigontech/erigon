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
	"testing"
	"testing/synctest"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/erigontech/erigon/execution/execmodule"
)

func TestCrashRecoveryWaitsForTransition(t *testing.T) {
	t.Run("early_valid", func(t *testing.T) {
		synctest.Test(t, func(t *testing.T) {
			transitions := newStateTransitionController()
			cleared := transitions.hold(t, execmodule.StateTransitionOverlayCleared, 1)
			response := make(chan error)
			done := make(chan error, 1)
			go func() { done <- waitCrashRecoveryTransition(t.Context(), cleared, response) }()
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
			require.NoError(t, waitCrashRecoveryTransition(t.Context(), ready, make(chan error)))
			ready.release()
		})
	})
	t.Run("fcu_error", func(t *testing.T) {
		transitions := newStateTransitionController()
		ready := transitions.hold(t, execmodule.StateTransitionCommitReady, 1)
		response := make(chan error, 1)
		failed := errors.New("forkchoice failed")
		response <- failed
		require.ErrorIs(t, waitCrashRecoveryTransition(t.Context(), ready, response), failed)
	})
	t.Run("deadline", func(t *testing.T) {
		synctest.Test(t, func(t *testing.T) {
			transitions := newStateTransitionController()
			ready := transitions.hold(t, execmodule.StateTransitionCommitReady, 1)
			ctx, cancel := context.WithTimeout(t.Context(), time.Minute)
			defer cancel()
			require.ErrorIs(t, waitCrashRecoveryTransition(ctx, ready, nil), context.DeadlineExceeded)
		})
	})
}
