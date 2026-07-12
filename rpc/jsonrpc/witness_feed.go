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
	"encoding/json"
	"sync"
	"time"

	"github.com/erigontech/erigon/common"
	"github.com/erigontech/erigon/common/log/v3"
)

const (
	witnessFeedBuffer          = 4
	witnessFeedDropLogInterval = 10 * time.Second
)

// witnessPush is one completed witness ready to fan out to subscribers.
type witnessPush struct {
	num  uint64
	hash common.Hash
	json json.RawMessage
}

// witnessFeed fans a single producer's witness pushes out to any number of
// subscribers over per-subscriber buffered channels. publish never blocks: when a
// subscriber's channel is full the oldest queued push is dropped in favor of the
// newest, since tip proving wants the freshest witness and a dropped one is still
// servable by an on-demand debug_executionWitness request.
type witnessFeed struct {
	mu     sync.Mutex
	subs   map[uint64]chan witnessPush
	nextID uint64

	dropped     uint64
	lastDropLog time.Time
}

func newWitnessFeed() *witnessFeed {
	return &witnessFeed{subs: make(map[uint64]chan witnessPush)}
}

func (f *witnessFeed) subscribe() (uint64, <-chan witnessPush) {
	f.mu.Lock()
	defer f.mu.Unlock()
	id := f.nextID
	f.nextID++
	ch := make(chan witnessPush, witnessFeedBuffer)
	f.subs[id] = ch
	return id, ch
}

func (f *witnessFeed) unsubscribe(id uint64) {
	f.mu.Lock()
	defer f.mu.Unlock()
	delete(f.subs, id)
}

func (f *witnessFeed) publish(p witnessPush) {
	f.mu.Lock()
	defer f.mu.Unlock()
	for _, ch := range f.subs {
		f.send(ch, p)
	}
}

func (f *witnessFeed) send(ch chan witnessPush, p witnessPush) {
	select {
	case ch <- p:
		return
	default:
	}
	select {
	case <-ch:
		f.recordDrop()
	default:
	}
	select {
	case ch <- p:
	default:
		f.recordDrop()
	}
}

func (f *witnessFeed) recordDrop() {
	f.dropped++
	now := time.Now()
	if now.Sub(f.lastDropLog) < witnessFeedDropLogInterval {
		return
	}
	f.lastDropLog = now
	log.Debug("[witness-feed] dropped witness push to slow subscriber", "dropped", f.dropped)
}
