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

package pbt

import (
	"bytes"
	"encoding/binary"
	"math/rand"
	"runtime"
	"runtime/debug"
	"runtime/metrics"
	"sort"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/erigontech/erigon/db/kv"
	"github.com/erigontech/erigon/execution/commitment"
	"github.com/erigontech/erigon/execution/commitment/eip8297"
)

type pbinVerifyMemoryContext struct {
	records map[string][]byte
	reads   atomic.Int64
}

func (c *pbinVerifyMemoryContext) Branch(key []byte) ([]byte, kv.Step, error) {
	c.reads.Add(1)
	return bytes.Clone(c.records[string(key)]), 0, nil
}

func (c *pbinVerifyMemoryContext) PutBranch(key, data, _ []byte) error {
	if len(data) == 0 {
		delete(c.records, string(key))
		return nil
	}
	c.records[string(key)] = bytes.Clone(data)
	return nil
}

func (c *pbinVerifyMemoryContext) Account([]byte) (*commitment.Update, error) {
	return nil, nil
}

func (c *pbinVerifyMemoryContext) Storage([]byte) (*commitment.Update, error) {
	return nil, nil
}

func pbinBuildVerifyMemoryContext(t *testing.T, count int) *pbinVerifyMemoryContext {
	t.Helper()
	ctx := &pbinVerifyMemoryContext{records: make(map[string][]byte)}
	rng := rand.New(rand.NewSource(7))
	ops := make([]Op, 0, count)
	for len(ops) < count {
		address := make([]byte, 20)
		_, err := rng.Read(address)
		require.NoError(t, err)
		var value [eip8297.ValueLength]byte
		binary.BigEndian.PutUint64(value[24:], uint64(len(ops)+1))
		ops = append(ops, Op{Key: eip8297.TreeKeyAccount(address, eip8297.BasicDataLeafKey), Value: value})
		if rng.Intn(4) == 0 {
			for slot := 0; slot < 4 && len(ops) < count; slot++ {
				storageSlot := make([]byte, 32)
				_, err = rng.Read(storageSlot)
				require.NoError(t, err)
				binary.BigEndian.PutUint64(value[24:], uint64(len(ops)+1))
				ops = append(ops, Op{Key: eip8297.TreeKeyStorage(address, storageSlot), Value: value})
			}
		}
	}
	sort.Slice(ops, func(i, j int) bool { return bytes.Compare(ops[i].Key, ops[j].Key) < 0 })
	for start := 0; start < len(ops); start += 50_000 {
		end := min(start+50_000, len(ops))
		_, err := NewTrie(ctx).Process(ops[start:end])
		require.NoError(t, err)
	}
	return ctx
}

func pbinHeapObjects() uint64 {
	sample := []metrics.Sample{{Name: "/memory/classes/heap/objects:bytes"}}
	metrics.Read(sample)
	return sample[0].Value.Uint64()
}

func pbinLiveHeap() uint64 {
	runtime.GC()
	runtime.GC()
	return pbinHeapObjects()
}

func pbinVerifyPeak(run func() error) (uint64, error) {
	var peak atomic.Uint64
	stop := make(chan struct{})
	done := make(chan struct{})
	go func() {
		defer close(done)
		ticker := time.NewTicker(time.Millisecond)
		defer ticker.Stop()
		for {
			current := pbinHeapObjects()
			for {
				previous := peak.Load()
				if current <= previous || peak.CompareAndSwap(previous, current) {
					break
				}
			}
			select {
			case <-stop:
				return
			case <-ticker.C:
			}
		}
	}()
	err := run()
	close(stop)
	<-done
	return peak.Load(), err
}

func TestPBinVerifyHeapDoesNotScaleWithLeaves(t *testing.T) {
	oldGC := debug.SetGCPercent(5)
	t.Cleanup(func() { debug.SetGCPercent(oldGC) })
	var smallDelta, largeDelta uint64
	for _, test := range []struct {
		count int
		delta *uint64
	}{
		{count: 100_000, delta: &smallDelta},
		{count: 1_000_000, delta: &largeDelta},
	} {
		ctx := pbinBuildVerifyMemoryContext(t, test.count)
		base := pbinLiveHeap()
		peak, err := pbinVerifyPeak(func() error { return NewTrie(ctx).Verify() })
		require.NoError(t, err)
		*test.delta = peak - base
	}
	require.Less(t, smallDelta, uint64(16<<20))
	require.Less(t, largeDelta, uint64(16<<20))
}
