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
	"bytes"
	"context"
	"testing"
)

func pbinBenchStorageCorpus(accounts, slotsPer int) (keys [][]byte, updates []Update) {
	for a := range accounts {
		addr := pbinOracleAddr(uint64(a + 1))
		for s := range slotsPer {
			slot := pbinOracleSlot(uint64(s + 1))
			u := Update{Flags: StorageUpdate, StorageLen: 2}
			u.Storage[0], u.Storage[1] = byte(a+1), byte(s+1)
			keys = append(keys, append(bytes.Clone(addr), slot...))
			updates = append(updates, u)
		}
	}
	return keys, updates
}

func benchmarkPBinProcess(b *testing.B, accounts, slotsPer int) {
	b.Helper()
	keys, updates := pbinBenchStorageCorpus(accounts, slotsPer)
	ms := NewMockState(b)
	if err := ms.applyPlainUpdates(keys, updates); err != nil {
		b.Fatal(err)
	}
	ctx := context.Background()

	b.ReportAllocs()
	b.ResetTimer()
	for range b.N {
		b.StopTimer()
		upd := WrapKeyUpdates(b, ModeDirect, pbinKeyHasher(), keys, updates)
		pph := NewPBinPatriciaHashed(ms)
		b.StartTimer()

		if _, err := pph.Process(ctx, upd, "", nil, WarmupConfig{}); err != nil {
			b.Fatal(err)
		}

		b.StopTimer()
		pph.Release()
		b.StartTimer()
	}
}

func BenchmarkPBinProcessStorage(b *testing.B) {
	benchmarkPBinProcess(b, 16, 64)
}
