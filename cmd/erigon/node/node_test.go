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
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/erigontech/erigon/common/log/v3"
	"github.com/erigontech/erigon/db/datadir"
	"github.com/erigontech/erigon/node"
	"github.com/erigontech/erigon/node/nodecfg"
)

// recordingLifecycle records whether the node started and stopped it.
type recordingLifecycle struct{ started, stopped atomic.Bool }

func (l *recordingLifecycle) Start() error { l.started.Store(true); return nil }
func (l *recordingLifecycle) Stop() error  { l.stopped.Store(true); return nil }

// serve runs Serve on a stack with a recording lifecycle and returns the channel Serve's result arrives on.
func serve(t *testing.T, ctx context.Context) (<-chan error, *recordingLifecycle) {
	t.Helper()
	stack, err := node.New(t.Context(), &nodecfg.Config{Dirs: datadir.New(t.TempDir())}, log.New())
	require.NoError(t, err)
	lifecycle := &recordingLifecycle{}
	stack.RegisterLifecycle(lifecycle)
	served := make(chan error, 1)
	go func() { served <- (&ErigonNode{ctx: ctx, stack: stack}).Serve() }()
	return served, lifecycle
}

func requireServed(t *testing.T, served <-chan error) {
	t.Helper()
	select {
	case err := <-served:
		require.NoError(t, err)
	case <-time.After(10 * time.Second):
		t.Fatal("Serve did not return after the context ended")
	}
}

func TestServeClosesTheNodeWhenTheContextEnds(t *testing.T) {
	ctx, cancel := context.WithCancel(t.Context())
	served, lifecycle := serve(t, ctx)
	select {
	case <-served:
		t.Fatal("Serve returned before the context ended")
	case <-time.After(200 * time.Millisecond):
	}
	cancel()
	requireServed(t, served)
	require.True(t, lifecycle.stopped.Load())
}

// A shutdown signal can end the context while the node is still being created or started.
func TestServeStopsANodeWhoseContextEndedBeforeItStarted(t *testing.T) {
	ctx, cancel := context.WithCancel(t.Context())
	cancel()
	served, lifecycle := serve(t, ctx)
	requireServed(t, served)
	require.True(t, lifecycle.started.Load())
	require.True(t, lifecycle.stopped.Load(), "a lifecycle that was registered is stopped")
}

// recorder logs its starts and stops into a shared journal.
type recorder struct {
	name    string
	journal *[]string
}

func (r *recorder) Start() error { *r.journal = append(*r.journal, "start "+r.name); return nil }
func (r *recorder) Stop() error  { *r.journal = append(*r.journal, "stop "+r.name); return nil }

// TestNodeAsLifecycle: a node registered on another starts after the
// services registered before it, stops before them, and is closed with it.
func TestNodeAsLifecycle(t *testing.T) {
	newStack := func() *node.Node {
		stack, err := node.New(t.Context(), &nodecfg.Config{Name: "test"}, log.New())
		require.NoError(t, err)
		return stack
	}
	var journal []string
	parent, child := newStack(), &ErigonNode{stack: newStack()}
	parent.RegisterLifecycle(&recorder{"parent", &journal})
	child.RegisterLifecycle(&recorder{"child", &journal})
	parent.RegisterLifecycle(child)

	require.NoError(t, parent.Start())
	require.NoError(t, parent.Close())
	require.Equal(t, []string{"start parent", "start child", "stop child", "stop parent"}, journal)
	child.stack.Wait() // returns once closed
}
