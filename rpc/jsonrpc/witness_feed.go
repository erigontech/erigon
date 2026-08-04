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

	"github.com/erigontech/erigon/common"
	"github.com/erigontech/erigon/common/log/v3"
)

const witnessFeedBuffer = 4

// PrioritizedSend ends in a blocking send after freeing cap/2 slots, so a buffer under
// 2 frees nothing and would stall the builder under the feed lock.
const _ = uint(witnessFeedBuffer - 2)

// witnessPush is one completed witness ready to fan out to subscribers.
type witnessPush struct {
	num  uint64
	hash common.Hash
	json json.RawMessage
}

// witnessFeed never blocks on publish: a full subscriber channel drops its oldest
// queued pushes for the newest — tip proving wants the freshest witness. Recovering a
// dropped block means re-requesting it, which a node without commitment history can
// only serve while that block's cache entry is still resident.
type witnessFeed struct {
	mu   sync.Mutex
	subs map[chan witnessPush]struct{}
}

func newWitnessFeed() *witnessFeed {
	return &witnessFeed{subs: make(map[chan witnessPush]struct{})}
}

func (f *witnessFeed) subscribe() chan witnessPush {
	ch := make(chan witnessPush, witnessFeedBuffer)
	f.mu.Lock()
	f.subs[ch] = struct{}{}
	f.mu.Unlock()
	witnessFeedSubscribersGauge.SetInt(f.subCount())
	return ch
}

func (f *witnessFeed) unsubscribe(ch chan witnessPush) {
	f.mu.Lock()
	delete(f.subs, ch)
	f.mu.Unlock()
	witnessFeedSubscribersGauge.SetInt(f.subCount())
}

func (f *witnessFeed) subCount() int {
	f.mu.Lock()
	defer f.mu.Unlock()
	return len(f.subs)
}

func (f *witnessFeed) publish(p witnessPush) {
	f.mu.Lock()
	defer f.mu.Unlock()
	for ch := range f.subs {
		select {
		case ch <- p:
		default:
			witnessFeedDropCounter.Inc()
			log.Debug("[witness-feed] dropping oldest queued pushes for a slow subscriber")
			common.PrioritizedSend(ch, p)
		}
	}
}
