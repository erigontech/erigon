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

package execmodule

import (
	"context"
	"errors"
	"fmt"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/erigontech/erigon/common"
	"github.com/erigontech/erigon/execution/protocol/rules"
)

// TestHaltOnInitialSyncFailure pins the decision for a failed one-shot initial
// sync: a real pipeline failure under the parallel executor halts the process,
// while routine shutdown, post-sync errors, and serial execution keep it up.
func TestHaltOnInitialSyncFailure(t *testing.T) {
	t.Parallel()

	operational := fmt.Errorf("[Execution] %w",
		errors.New("parallel exec finished with 1 scheduled block(s) that never reached apply-loop validation"))
	verdict := fmt.Errorf("[Execution] %w", fmt.Errorf("%w: gas mismatch, block=12", rules.ErrInvalidBlock))

	require.True(t, haltOnInitialSyncFailure(operational, true, false),
		"a real failure must halt regardless of its class — the node would otherwise stay up but never sync")
	require.True(t, haltOnInitialSyncFailure(verdict, true, false))
	require.True(t, haltOnInitialSyncFailure(operational, false, true),
		"experimental BAL also selects the parallel executor")
	require.False(t, haltOnInitialSyncFailure(nil, true, false))
	require.False(t, haltOnInitialSyncFailure(fmt.Errorf("[Execution] %w", context.Canceled), true, false),
		"shutdown must not halt the node")
	stopped := fmt.Errorf("[Senders] %w", common.ErrStopped)
	require.False(t, haltOnInitialSyncFailure(stopped, true, false),
		"ETL shutdown must not halt the node")
	require.False(t, haltOnInitialSyncFailure(errors.Join(context.Canceled, stopped), true, false),
		"joined shutdown signals must not halt the node")
	require.True(t, haltOnInitialSyncFailure(errors.Join(stopped, operational), true, false),
		"a real failure must not be hidden by a shutdown signal")
	publicationErr := &initialSyncPublicationError{err: errors.New("notification dispatch failed")}
	require.False(t, haltOnInitialSyncFailure(publicationErr, true, false),
		"an error after initial sync completed must not halt the node")
	require.False(t, haltOnInitialSyncFailure(operational, false, false),
		"serial execution keeps the stay-up behavior")
}
