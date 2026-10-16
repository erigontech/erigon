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
	"bytes"
	"testing"

	"github.com/c2h5oh/datasize"
	lru "github.com/hashicorp/golang-lru/v2"

	"github.com/erigontech/erigon/common"
)

func BenchmarkCodeCache_GetByCodeHash_Hit(b *testing.B) {
	c := closeOnCleanup(b, NewCodeCache(64*datasize.MB, 16*datasize.MB))
	code := bytes.Repeat([]byte{0x5b}, 2048) // 2 KiB typical contract size
	codeHash := makeCodeHash(0x11)
	c.PutWithCodeHash(makeAddr(1), code, codeHash, 0)

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		v, ok := c.GetByCodeHash(codeHash)
		if !ok || len(v) == 0 {
			b.Fatal("expected hit")
		}
	}
}

func BenchmarkCodeCache_GetByCodeHash_Miss(b *testing.B) {
	c := closeOnCleanup(b, NewCodeCache(64*datasize.MB, 16*datasize.MB))
	missHash := makeCodeHash(0x22)

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_, _ = c.GetByCodeHash(missHash)
	}
}

// BenchmarkCodeCache_Get_AddrLevel_Hit baseline: the existing addr-keyed
// path. Compare against GetByCodeHash to verify the codeHashToCode lookup is at least
// as fast (one map probe vs two: addr→hash then hash→code).
func BenchmarkCodeCache_Get_AddrLevel_Hit(b *testing.B) {
	c := closeOnCleanup(b, NewCodeCache(64*datasize.MB, 16*datasize.MB))
	code := bytes.Repeat([]byte{0x5b}, 2048)
	addr := makeAddr(1)
	c.PutWithCodeHash(addr, code, makeCodeHash(0x33), 0)

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		v, ok := c.Get(addr)
		if !ok || len(v) == 0 {
			b.Fatal("expected hit")
		}
	}
}

// BenchmarkCodeCache_GetByCodeHash_ManyAddrs_OneCode measures the workload
// shape this layer is designed for: many addresses sharing one codeHash.
// Without codeHashToCode every fresh addr would pay a file read. With codeHashToCode every
// caller that already knows the hash hits one shared entry.
func BenchmarkCodeCache_GetByCodeHash_ManyAddrs_OneCode(b *testing.B) {
	c := closeOnCleanup(b, NewCodeCache(64*datasize.MB, 16*datasize.MB))
	code := bytes.Repeat([]byte{0x5b}, 2048)
	codeHash := makeCodeHash(0x44)
	c.PutWithCodeHash(makeAddr(1), code, codeHash, 0) // populate once

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		// Caller knows the hash from a prior account read; probes codeHashToCode.
		v, ok := c.GetByCodeHash(codeHash)
		if !ok || len(v) == 0 {
			b.Fatal("expected hit")
		}
	}
}

// One popular address read by every goroutine: a contract every transaction
// touches. Sharding a mutex cannot help here, because one key is one shard, so
// this is what separates a lock-free layer from a locked one. Run with
// -cpu 1,2,4,8,16: the single-core column shows the per-op work is the same and
// everything above it is contention.
func BenchmarkAddrCodeHashHotKey(b *testing.B) {
	const txNum = 1
	addr := common.BytesToAddress(makeAddr(0xDE))
	var hash [32]byte
	copy(hash[:], makeCodeHash(0xAA))
	entry := addrCodeHashEntry{addr: addr, hash: hash, txNum: txNum}

	b.Run("locked/hashicorp", func(b *testing.B) {
		c, err := lru.New[common.Address, addrCodeHashEntry](1024)
		if err != nil {
			b.Fatal(err)
		}
		c.Add(addr, entry)
		b.ResetTimer()
		b.RunParallel(func(pb *testing.PB) {
			for pb.Next() {
				if _, ok := c.Get(addr); !ok {
					b.Error("miss on the hot key")
					return
				}
			}
		})
	})

	b.Run("sharded/generic", func(b *testing.B) {
		c := NewGenericCacheWithAvg(8*datasize.MB, 52, func([32]byte) int { return 32 }, ModeEvictLRU)
		c.Put(addr[:], hash, txNum)
		b.ResetTimer()
		b.RunParallel(func(pb *testing.PB) {
			for pb.Next() {
				if _, ok := c.Get(addr[:]); !ok {
					b.Error("miss on the hot key")
					return
				}
			}
		})
	})

	b.Run("lockfree/bytelru", func(b *testing.B) {
		c := NewByteLRU(8*datasize.MB, func(_ uint64, e addrCodeHashEntry) int64 {
			return int64(addrToCodeHashEntryBytes)
		})
		c.Add(addrSlot(addr), entry)
		b.ResetTimer()
		b.RunParallel(func(pb *testing.PB) {
			for pb.Next() {
				if e, ok := c.Get(addrSlot(addr)); !ok || e.addr != addr {
					b.Error("miss on the hot key")
					return
				}
			}
		})
	})
}
