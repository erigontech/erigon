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

	"github.com/erigontech/erigon/db/kv"
	"github.com/erigontech/erigon/db/kv/kvcache"
	"github.com/erigontech/erigon/execution/execmodule"
)

type rpcViewObserverCache struct {
	kvcache.Cache
	observer execmodule.StateTransitionObserver
}

func withRPCViewObserver(cache kvcache.Cache, observer execmodule.StateTransitionObserver) kvcache.Cache {
	if observer == nil {
		return cache
	}
	return &rpcViewObserverCache{Cache: cache, observer: observer}
}

func (c *rpcViewObserverCache) View(ctx context.Context, tx kv.TemporalTx) (kvcache.CacheView, error) {
	view, err := c.Cache.View(ctx, tx)
	if err != nil {
		return nil, err
	}
	c.observer(ctx, execmodule.StateTransitionRPCViewBound)
	return view, nil
}
