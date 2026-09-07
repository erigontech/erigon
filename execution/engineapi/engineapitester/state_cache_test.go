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

package engineapitester

import (
	"context"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/erigontech/erigon/common"
	"github.com/erigontech/erigon/db/kv"
	"github.com/erigontech/erigon/db/kv/kvcache"
	"github.com/erigontech/erigon/execution/execmodule"
)

type viewOnlyCache struct {
	kvcache.Cache
	view kvcache.CacheView
}

func (c viewOnlyCache) View(context.Context, kv.TemporalTx) (kvcache.CacheView, error) {
	return c.view, nil
}

type stubCacheView struct{}

func (*stubCacheView) Get([]byte) ([]byte, error)              { return nil, nil }
func (*stubCacheView) GetCode([]byte) ([]byte, error)          { return nil, nil }
func (*stubCacheView) HasStorage(common.Address) (bool, error) { return false, nil }

func TestRPCViewObserverRunsSynchronouslyAfterBinding(t *testing.T) {
	type observation struct {
		ctx   context.Context
		point execmodule.StateTransitionPoint
	}
	type viewResult struct {
		view kvcache.CacheView
		err  error
	}

	observed := make(chan observation, 1)
	release := make(chan struct{})
	releaseObserver := sync.OnceFunc(func() { close(release) })
	t.Cleanup(releaseObserver)
	ctx := t.Context()
	expectedView := &stubCacheView{}
	cache := withRPCViewObserver(viewOnlyCache{view: expectedView}, func(ctx context.Context, point execmodule.StateTransitionPoint) {
		observed <- observation{ctx: ctx, point: point}
		<-release
	})
	result := make(chan viewResult, 1)
	go func() {
		view, err := cache.View(ctx, nil)
		result <- viewResult{view: view, err: err}
	}()

	select {
	case got := <-observed:
		require.Equal(t, ctx, got.ctx)
		require.Equal(t, execmodule.StateTransitionRPCViewBound, got.point)
	case <-time.After(time.Second):
		t.Fatal("RPC view observer was not called")
	}
	select {
	case <-result:
		t.Fatal("cache view returned before the observer was released")
	default:
	}
	releaseObserver()
	select {
	case got := <-result:
		require.NoError(t, got.err)
		require.Same(t, expectedView, got.view)
	case <-time.After(time.Second):
		t.Fatal("cache view did not return after the observer was released")
	}
}
