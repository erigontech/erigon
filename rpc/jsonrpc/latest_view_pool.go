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

package jsonrpc

import (
	"context"
	"sync"
	"sync/atomic"
	"time"

	"github.com/erigontech/erigon/common/dbg"
	"github.com/erigontech/erigon/db/kv"
	"github.com/erigontech/erigon/execution/state"
	"github.com/erigontech/erigon/rpc"
)

var latestViewPoolEnabled = dbg.EnvBool("LATEST_VIEW_POOL", true)

const (
	latestViewPoolMaxIdle = 16
	latestViewPoolMaxAge  = time.Second
)

type viewGenerator interface {
	ViewGeneration() (commit uint64, stateFiles, blockFiles any, ok bool)
}

// latestViewKey identifies the database snapshot, file generations and
// overlay publication a read transaction observes.
type latestViewKey struct {
	commit     uint64
	stateFiles any
	blockFiles any
	overlay    uint64
}

// latestViewEntry is a read transaction opened on one view together with the
// state reader for "latest" in that view. It is used by one request at a time.
type latestViewEntry struct {
	key    latestViewKey
	born   time.Time
	tx     kv.TemporalTx
	reader state.StateReader
}

// latestViewPool keeps read transactions on the current view so point reads
// at "latest" skip opening a transaction and resolving the head per request.
type latestViewPool struct {
	mu       sync.Mutex
	idle     []*latestViewEntry
	sweeping bool

	hits, misses atomic.Uint64
}

func (api *APIImpl) currentLatestViewKey() (latestViewKey, bool) {
	g, ok := api.db.(viewGenerator)
	if !ok {
		return latestViewKey{}, false
	}
	commit, stateFiles, blockFiles, ok := g.ViewGeneration()
	if !ok {
		return latestViewKey{}, false
	}
	_, seq := api.filters.OverlaySnapshot()
	return latestViewKey{commit: commit, stateFiles: stateFiles, blockFiles: blockFiles, overlay: seq}, true
}

func (p *latestViewPool) take(key latestViewKey, now time.Time) *latestViewEntry {
	var stale []*latestViewEntry
	var found *latestViewEntry
	p.mu.Lock()
	for len(p.idle) > 0 {
		e := p.idle[len(p.idle)-1]
		p.idle = p.idle[:len(p.idle)-1]
		if e.key == key && now.Sub(e.born) < latestViewPoolMaxAge {
			found = e
			break
		}
		stale = append(stale, e)
	}
	p.mu.Unlock()
	for _, e := range stale {
		e.tx.Rollback()
	}
	return found
}

func (p *latestViewPool) put(e *latestViewEntry, key latestViewKey, ok bool, now time.Time) {
	if ok && e.key == key && now.Sub(e.born) < latestViewPoolMaxAge {
		p.mu.Lock()
		if len(p.idle) < latestViewPoolMaxIdle {
			p.idle = append(p.idle, e)
			if !p.sweeping {
				p.sweeping = true
				go p.sweep()
			}
			p.mu.Unlock()
			return
		}
		p.mu.Unlock()
	}
	e.tx.Rollback()
}

// pooledLatestTx hands an entry to one request; Rollback returns it to the pool.
type pooledLatestTx struct {
	kv.TemporalTx
	api      *APIImpl
	e        *latestViewEntry
	returned bool
}

func (t *pooledLatestTx) Rollback() {
	if t.returned {
		return
	}
	t.returned = true
	key, ok := t.api.currentLatestViewKey()
	t.api._latestViewPool.put(t.e, key, ok, time.Now())
}

// leaseLatest serves a point read at "latest" from a pooled transaction, or
// opens one and keeps it for the next request when its view did not move
// while it was being opened.
func (api *APIImpl) leaseLatest(ctx context.Context, blockNrOrHash rpc.BlockNumberOrHash) (kv.TemporalTx, state.StateReader, error) {
	pool := api._latestViewPool
	key, keyed := api.currentLatestViewKey()
	if keyed {
		if e := pool.take(key, time.Now()); e != nil {
			pool.hits.Add(1)
			return &pooledLatestTx{TemporalTx: e.tx, api: api, e: e}, e.reader, nil
		}
	}
	pool.misses.Add(1)

	detached := context.WithoutCancel(ctx)
	tx, reader, err := api.openStateReaderAt(detached, blockNrOrHash)
	if err != nil {
		return nil, nil, err
	}
	after, stillKeyed := api.currentLatestViewKey()
	if !keyed || !stillKeyed || after != key {
		return tx, reader, nil
	}
	e := &latestViewEntry{key: key, born: time.Now(), tx: tx, reader: reader}
	return &pooledLatestTx{TemporalTx: tx, api: api, e: e}, reader, nil
}

// sweep closes idle entries older than the maximum age, so an idle pool never
// holds a read transaction open for long, and stops once the pool is empty.
func (p *latestViewPool) sweep() {
	ticker := time.NewTicker(latestViewPoolMaxAge / 4)
	defer ticker.Stop()
	for now := range ticker.C {
		var old []*latestViewEntry
		p.mu.Lock()
		kept := p.idle[:0]
		for _, e := range p.idle {
			if now.Sub(e.born) < latestViewPoolMaxAge {
				kept = append(kept, e)
			} else {
				old = append(old, e)
			}
		}
		clear(p.idle[len(kept):])
		p.idle = kept
		empty := len(p.idle) == 0
		if empty {
			p.sweeping = false
		}
		p.mu.Unlock()
		for _, e := range old {
			e.tx.Rollback()
		}
		if empty {
			return
		}
	}
}
