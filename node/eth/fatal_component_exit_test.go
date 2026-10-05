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

package eth

import (
	"context"
	"errors"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/erigontech/erigon/common/log/v3"
)

// A component that dies for good must take the process with it. Cancelling the
// backend context stops the backend's own goroutines, but the process blocks in
// Node.Wait until the stack is closed — so cancelling alone leaves a node with
// no consensus layer, serving nothing, alive until something external kills it.
func TestShutdownOnFatalStopsTheNode(t *testing.T) {
	t.Parallel()
	ctx, cancel := context.WithCancel(context.Background())
	stopped := false

	shutdownOnFatal(cancel, func() error { stopped = true; return nil }, log.New())()

	require.Error(t, ctx.Err(), "backend context must be cancelled")
	require.True(t, stopped, "the node must be stopped, or the process never leaves Node.Wait")
}

// A stack that fails to close still cancelled the context, and the failure is
// the last thing worth logging — it must not panic the dying node.
func TestShutdownOnFatalSurvivesAFailedStop(t *testing.T) {
	t.Parallel()
	ctx, cancel := context.WithCancel(context.Background())

	require.NotPanics(t, shutdownOnFatal(cancel, func() error { return errors.New("close failed") }, log.New()))
	require.Error(t, ctx.Err())
}
