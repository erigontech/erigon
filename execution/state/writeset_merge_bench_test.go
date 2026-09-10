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

package state

import (
	"fmt"
	"testing"
)

func BenchmarkWriteSetMerge(b *testing.B) {
	for _, size := range []struct{ addrs, slots int }{{4, 2}, {16, 8}} {
		b.Run(fmt.Sprintf("addrs=%d/slots=%d", size.addrs, size.slots), func(b *testing.B) {
			benchWriteSetMerge(b, size.addrs, size.slots, (*WriteSet).Merge)
		})
	}
}

func BenchmarkWriteSetMergeInto(b *testing.B) {
	for _, size := range []struct{ addrs, slots int }{{4, 2}, {16, 8}} {
		b.Run(fmt.Sprintf("addrs=%d/slots=%d", size.addrs, size.slots), func(b *testing.B) {
			benchWriteSetMerge(b, size.addrs, size.slots, (*WriteSet).MergeInto)
		})
	}
}

func BenchmarkVersionedFeeMergeClone(b *testing.B) {
	for _, size := range []struct{ addrs, slots int }{{4, 2}, {16, 8}} {
		b.Run(fmt.Sprintf("addrs=%d/slots=%d", size.addrs, size.slots), func(b *testing.B) {
			benchVersionedFeeMerge(b, size.addrs, size.slots, (*WriteSet).Merge)
		})
	}
}

func BenchmarkVersionedFeeMergeInto(b *testing.B) {
	for _, size := range []struct{ addrs, slots int }{{4, 2}, {16, 8}} {
		b.Run(fmt.Sprintf("addrs=%d/slots=%d", size.addrs, size.slots), func(b *testing.B) {
			benchVersionedFeeMerge(b, size.addrs, size.slots, (*WriteSet).MergeInto)
		})
	}
}

// Both variants build their inputs inside the timed loop, since MergeInto
// consumes next and cannot reuse one pair across iterations. That dilutes the
// delta with the build cost, but it is the same cost on both sides - timing one
// variant with a warm pair and the other with a fresh one measures the harness.
func benchWriteSetMerge(b *testing.B, addrs, slots int, merge func(prev, next *WriteSet) *WriteSet) {
	b.ReportAllocs()
	for b.Loop() {
		prev, next := buildMergeBenchSets(addrs, slots)
		sinkWS = merge(prev, next)
	}
}

// The apply-loop fee-merge pipeline around the merge itself: record the tx
// write set into VersionedIO, merge the calcFees output, re-record, flush the
// merged set into the VersionMap. Build cost of the inputs is identical in
// both variants and included in the measured loop, since MergeInto consumes
// next.
func benchVersionedFeeMerge(b *testing.B, addrs, slots int, merge func(prev, next *WriteSet) *WriteSet) {
	io := NewVersionedIO(1)
	vm := NewVersionMap(nil)
	version := Version{TxIndex: 0, Incarnation: 1}
	b.ReportAllocs()
	for b.Loop() {
		txOut, tip := buildMergeBenchSets(addrs, slots)
		io.RecordWrites(version, txOut)
		merged := merge(txOut, tip)
		io.RecordWrites(version, merged)
		vm.FlushVersionedWrites(merged, true, "")
		sinkWS = merged
	}
}

// Fee-merge shape from the parallel apply loop: prev is a full tx write set,
// next is the small calcFees output.
func buildMergeBenchSets(addrs, slots int) (*WriteSet, *WriteSet) {
	prev := &WriteSet{}
	for i := range addrs {
		addr := mergeAddr(byte(i + 1))
		prev.SetBalance(addr, balanceWrite(addr, uint64(i+1), 0))
		prev.SetNonce(addr, nonceWrite(addr, uint64(i), 0))
		for s := range slots {
			key := mergeKey(byte(s + 1))
			prev.SetStorage(addr, key, storageWrite(addr, key, uint64(s)))
		}
	}
	coinbase := mergeAddr(0xfe)
	next := newWriteSet(balanceWrite(coinbase, 1000, 1))
	return prev, next
}
