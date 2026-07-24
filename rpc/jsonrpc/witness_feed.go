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

// witnessPush is one completed witness ready to fan out to subscribers.
type witnessPush struct {
	num  uint64
	hash common.Hash
	json json.RawMessage
}

// witnessFeed never blocks on publish: a full subscriber channel drops its
// oldest queued pushes for the newest — tip proving wants the freshest witness,
// and a dropped one stays servable by an on-demand debug_executionWitness request.
type witnessFeed struct {
	mu   sync.Mutex
	subs map[chan witnessPush]struct{}
}

func newWitnessFeed() *witnessFeed {
	return &witnessFeed{subs: make(map[chan witnessPush]struct{})}
}

func (f *witnessFeed) subscribe() chan witnessPush {
	f.mu.Lock()
	defer f.mu.Unlock()
	ch := make(chan witnessPush, witnessFeedBuffer)
	f.subs[ch] = struct{}{}
	return ch
}

func (f *witnessFeed) unsubscribe(ch chan witnessPush) {
	f.mu.Lock()
	defer f.mu.Unlock()
	delete(f.subs, ch)
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
			log.Debug("[witness-feed] dropping oldest queued pushes for a slow subscriber")
			common.PrioritizedSend(ch, p)
		}
	}
}
