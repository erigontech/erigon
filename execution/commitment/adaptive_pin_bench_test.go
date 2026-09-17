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

package commitment

import (
	"encoding/binary"
	"testing"
)

func benchContractHash(n byte) (h [32]byte) {
	for i := range h {
		h[i] = n ^ byte(i+1)
	}
	return h
}

func benchStoragePrefix(hash [32]byte, slot uint16) []byte {
	p := make([]byte, 36)
	copy(p[1:], hash[:])
	binary.BigEndian.PutUint16(p[33:], slot)
	return p
}

func BenchmarkAdaptivePinDemote(b *testing.B) {
	const pinned = 60_000
	hash := benchContractHash(0x42)
	val := make([]byte, 100)

	b.ReportAllocs()
	for i := 0; i < b.N; i++ {
		b.StopTimer()
		c := NewBranchCache(64)
		ctrl := NewAdaptivePinController(c, DefaultAdaptivePinControllerConfig(), nil)
		p := &ContractTrunkPreloadParallel{contractHash: hash[:]}
		for slot := range pinned {
			pk := benchStoragePrefix(hash, uint16(slot))
			c.PinEntry(pk, val, 0, 100)
			p.pinnedPrefixes = append(p.pinnedPrefixes, pk)
			p.pinned++
		}
		if c.PinnedCount() != pinned {
			b.Fatalf("setup pinned %d, want %d", c.PinnedCount(), pinned)
		}
		state := &adaptiveContractState{contractHash: hash, parallel: p}
		b.StartTimer()

		ctrl.demoteLocked(hash, state)

		b.StopTimer()
		if c.PinnedCount() != 0 {
			b.Fatalf("after demote pinned %d, want 0", c.PinnedCount())
		}
		c.Close()
		b.StartTimer()
	}
}

func BenchmarkAdaptivePinSnapshotMissesWhenCold(b *testing.B) {
	const coldContracts = 50_000
	c := NewBranchCache(64)
	defer c.Close()
	ctrl := NewAdaptivePinController(c, DefaultAdaptivePinControllerConfig(), nil)

	prefix := make([]byte, 33)
	for i := range coldContracts {
		binary.BigEndian.PutUint32(prefix[1:], uint32(i))
		ctrl.onCacheMiss(prefix)
	}
	if got := len(ctrl.snapshotMisses()); got != coldContracts {
		b.Fatalf("setup drained %d contracts, want %d", got, coldContracts)
	}
	ctrl.snapshotMisses()

	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		if got := len(ctrl.snapshotMisses()); got != 0 {
			b.Fatalf("cold snapshot returned %d contracts, want 0", got)
		}
	}
}
