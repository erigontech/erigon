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

package node

import (
	"context"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/erigontech/erigon/common/log/v3"
	"github.com/erigontech/erigon/db/datadir"
	"github.com/erigontech/erigon/node"
	"github.com/erigontech/erigon/node/nodecfg"
)

func serveUntilReturn(t *testing.T, ctx context.Context) {
	t.Helper()
	stack, err := node.New(t.Context(), &nodecfg.Config{Dirs: datadir.New(t.TempDir())}, log.New())
	require.NoError(t, err)
	served := make(chan error, 1)
	go func() { served <- (&ErigonNode{ctx: ctx, stack: stack}).Serve() }()
	select {
	case err := <-served:
		require.NoError(t, err)
	case <-time.After(10 * time.Second):
		t.Fatal("Serve did not return after the context ended")
	}
}

func TestServeClosesTheNodeWhenTheContextEnds(t *testing.T) {
	ctx, cancel := context.WithCancel(t.Context())
	time.AfterFunc(100*time.Millisecond, cancel)
	serveUntilReturn(t, ctx)
}

// A shutdown signal can end the context while the node is still being created or started.
func TestServeClosesANodeWhoseContextEndedBeforeItStarted(t *testing.T) {
	ctx, cancel := context.WithCancel(t.Context())
	cancel()
	serveUntilReturn(t, ctx)
}
