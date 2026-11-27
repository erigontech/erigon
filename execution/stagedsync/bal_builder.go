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

import (
	"sync/atomic"

	"github.com/erigontech/erigon/execution/state"
)

// asyncBALBuilder adds a block's sealed txs to its access list on its own
// goroutine, so the apply loop does not pay for BlockAccessListBuilder.Add.
type asyncBALBuilder struct {
	b      *state.BlockAccessListBuilder
	sealed atomic.Int64
	woken  int
	wake   chan struct{}
	done   chan struct{}
}

const balWakeBatch = 64

func startBALBuilder(io *state.VersionedIO) *asyncBALBuilder {
	a := &asyncBALBuilder{b: state.NewBlockAccessListBuilder(), wake: make(chan struct{}, 1), done: make(chan struct{})}
	go func() {
		defer close(a.done)
		for range a.wake {
			for sealed := int(a.sealed.Load()); a.b.Next()+1 < sealed; {
				i := a.b.Next()
				a.b.Add(i, io.ReadSet(i), io.WriteSet(i))
			}
		}
	}()
	return a
}

// seal makes tx indexes up to sealed-2 available to Add, as txResult.balSealed does.
func (a *asyncBALBuilder) seal(sealed int) {
	a.sealed.Store(int64(sealed))
	if sealed-a.woken < balWakeBatch {
		return
	}
	a.woken = sealed
	select {
	case a.wake <- struct{}{}:
	default:
	}
}

// close ends sealing; the goroutine then stops without waiting for a reader.
func (a *asyncBALBuilder) close() {
	if a != nil {
		close(a.wake)
	}
}

// builder waits for the goroutine after close and returns the builder; it may
// stop before the last sealed tx, so the caller adds the remaining ones.
func (a *asyncBALBuilder) builder() *state.BlockAccessListBuilder {
	if a == nil {
		return nil
	}
	<-a.done
	return a.b
}
