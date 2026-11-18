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

package stagedsync

import "github.com/erigontech/erigon/execution/state"

type balFeed struct {
	io     *state.VersionedIO
	sealed int
}

// balAdder builds one block's access list on its own goroutine, beside the apply loop.
type balAdder struct {
	b    *state.BlockAccessListBuilder
	feed chan balFeed
	done chan struct{}
}

func newBalAdder() *balAdder {
	a := &balAdder{b: state.NewBlockAccessListBuilder(), feed: make(chan balFeed, 4096), done: make(chan struct{})}
	go func() {
		defer close(a.done)
		for f := range a.feed {
			for a.b.Next()+1 < f.sealed {
				i := a.b.Next()
				a.b.Add(i, f.io.ReadSet(i), f.io.WriteSet(i))
			}
		}
	}()
	return a
}

// add hands over the txs of io sealed below sealed.
func (a *balAdder) add(io *state.VersionedIO, sealed int) { a.feed <- balFeed{io, sealed} }

// finish waits for every handed-over tx and returns the builder; nil-safe.
func (a *balAdder) finish() *state.BlockAccessListBuilder {
	if a == nil {
		return nil
	}
	close(a.feed)
	<-a.done
	return a.b
}
