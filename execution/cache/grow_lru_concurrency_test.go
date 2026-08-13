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

package cache

import (
	"encoding/binary"
	"sync/atomic"
	"testing"
	"time"

	"github.com/c2h5oh/datasize"
	"github.com/stretchr/testify/require"

	"github.com/erigontech/erigon/common/maphash"
)

func TestGrowLRU_AddDoesNotScanUnrelatedShards(t *testing.T) {
	shardHeld := make(chan struct{})
	releaseShard := make(chan struct{})
	var held atomic.Bool
	g := newGrowLRU[int](64*datasize.MB, avgBytesPerEntry, func(uint64, int) {
		if held.CompareAndSwap(false, true) {
			close(shardHeld)
			<-releaseShard
		}
	})
	evictDone := make(chan struct{})
	go func() {
		defer close(evictDone)
		for i := uint64(0); ; i++ {
			g.Add(i, int(i))
			if held.Load() {
				return
			}
		}
	}()
	<-shardHeld
	const writers = 128
	done := make(chan struct{}, writers)
	for i := 1; i <= writers; i++ {
		var key [8]byte
		binary.BigEndian.PutUint64(key[:], uint64(i))
		h := maphash.Hash(key[:])
		go func(h uint64, value int) {
			g.Add(h, value)
			done <- struct{}{}
		}(h, i)
	}
	completed := 0
	timedOut := false
	select {
	case <-done:
		completed++
	case <-time.After(time.Second):
		timedOut = true
	}
	close(releaseShard)
	<-evictDone
	for completed < writers {
		<-done
		completed++
	}
	g.Close()
	if timedOut {
		t.Fatal("add scanned every LRU shard")
	}
}

func TestGrowLRU_GrowsFromAtomicEntryCount(t *testing.T) {
	g := newGrowLRU[int](4*datasize.MB, avgBytesPerEntry, nil)
	defer g.Close()
	var key [8]byte
	for i := uint64(0); g.Len() < genericCacheStartCapacity; i++ {
		binary.BigEndian.PutUint64(key[:], i)
		g.Add(maphash.Hash(key[:]), int(i))
	}
	require.Equal(t, int64(g.Len()), g.entryCount.Load())
	before := g.cur.Load()
	binary.BigEndian.PutUint64(key[:], uint64(1)<<63)
	g.Add(maphash.Hash(key[:]), 1)
	require.NotSame(t, before, g.cur.Load())
}
