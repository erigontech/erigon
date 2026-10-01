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

package crypto

import (
	"bytes"
	"fmt"
	"hash/maphash"
	"math/rand/v2"
	"os"
	"strings"
	"sync/atomic"
	"time"
	"unsafe"

	"golang.org/x/sys/unix"

	"github.com/erigontech/erigon/common/dbg"

	keccak "github.com/erigontech/fastkeccak"

	"github.com/erigontech/erigon/common"
)

// keccakCacheMaxInput keeps a bucket at 128 bytes: tag 8 + length 1 + input 87 + hash 32.
const keccakCacheMaxInput = 87

const (
	keccakBucketLocked = 1 << 0
	keccakBucketAlive  = 1 << 1
)

// keccakBucket holds its entry in place, so the table allocates nothing and the GC never scans it.
// tag is both a pre-filter and a lock: readers and writers own the fields only between a
// CompareAndSwap that sets keccakBucketLocked and the Store that clears it.
type keccakBucket struct {
	tag  atomic.Uint64
	n    uint8
	in   [keccakCacheMaxInput]byte
	hash common.Hash
}

var (
	keccakCacheSeed    = maphash.MakeSeed()
	keccakCacheBuckets = allocBuckets()
	kcAdmit            = dbg.EnvBool("KECCAK_CACHE_ADMIT", false)
	kcInsertMask       = uint32(1)<<dbg.EnvInt("KECCAK_CACHE_INSERT_SHIFT", 0) - 1
	kcSeen             [1 << 14]atomic.Uint64 // 2^20 bits, 128 KB
	kcSeenAdds         atomic.Uint64
)

func allocBuckets() []keccakBucket {
	n := 1 << dbg.EnvInt("KECCAK_CACHE_BITS", 17)
	size := n * int(unsafe.Sizeof(keccakBucket{}))
	if dbg.EnvBool("KECCAK_CACHE_HUGE", false) {
		mem, err := unix.Mmap(-1, 0, size, unix.PROT_READ|unix.PROT_WRITE, unix.MAP_PRIVATE|unix.MAP_ANON)
		if err == nil {
			_ = unix.Madvise(mem, 14) // MADV_HUGEPAGE on linux
			return unsafe.Slice((*keccakBucket)(unsafe.Pointer(&mem[0])), n)
		}
	}
	return make([]keccakBucket, n)
}

// admitted reports whether key was seen before; the first sighting only marks it.
func admitted(key uint64) bool {
	w, bit := &kcSeen[(key>>20)&(uint64(len(kcSeen))-1)], uint64(1)<<(key>>40&63)
	if w.Load()&bit != 0 {
		return true
	}
	w.Or(bit)
	if kcSeenAdds.Add(1)&(1<<18-1) == 0 {
		for i := range kcSeen {
			kcSeen[i].Store(0)
		}
	}
	return false
}

// Keccak256Hash calc Keccak256. Short inputs are memoized in a direct-mapped table; a bucket
// another goroutine holds is treated as a miss, so a lookup never waits.
func Keccak256Hash(data []byte) common.Hash {
	if kcStats {
		kcHist[min(len(data), len(kcHist)-1)].Add(1)
	}
	if kcVariant == "off" || len(data) == 0 || len(data) > keccakCacheMaxInput {
		return keccak.Sum256(data)
	}
	key := maphash.Bytes(keccakCacheSeed, data)
	if kcVariant == "hashonly" {
		kcSink.Store(key)
		return keccak.Sum256(data)
	}
	b := &keccakCacheBuckets[key&(uint64(len(keccakCacheBuckets))-1)]
	tag := (key | keccakBucketAlive) &^ keccakBucketLocked
	if st := b.tag.Load(); st == tag && b.tag.CompareAndSwap(st, st|keccakBucketLocked) {
		hit := bytes.Equal(b.in[:b.n], data)
		h := b.hash
		b.tag.Store(st)
		if hit {
			if kcStats {
				kcHits.Add(1)
			}
			return h
		}
	}
	h := keccak.Sum256(data)
	if kcVariant == "lookup" || (kcAdmit && !admitted(key)) || (kcInsertMask != 0 && rand.Uint32()&kcInsertMask != 0) {
		return h
	}
	if st := b.tag.Load(); st&keccakBucketLocked == 0 && b.tag.CompareAndSwap(st, st|keccakBucketLocked) {
		b.n = uint8(len(data))
		copy(b.in[:], data)
		b.hash = h
		b.tag.Store(tag)
	}
	return h
}

var (
	kcVariant = dbg.EnvString("KECCAK_CACHE_VARIANT", "full")
	kcStats   = dbg.EnvBool("KECCAK_CACHE_STATS", false)
	kcHist    [137]atomic.Uint64
	kcSink    atomic.Uint64
	kcHits    atomic.Uint64
)

func init() {
	if !kcStats {
		return
	}
	go func() {
		for now := range time.Tick(200 * time.Millisecond) {
			var sb strings.Builder
			var total uint64
			for i := range kcHist {
				if n := kcHist[i].Load(); n > 0 {
					total += n
					fmt.Fprintf(&sb, " %d:%d", i, n)
				}
			}
			fmt.Fprintf(os.Stderr, "[kcvar] pid=%d ts=%d variant=%s calls=%d hits=%d len:count%s\n", os.Getpid(), now.UnixMilli(), kcVariant, total, kcHits.Load(), sb.String())
		}
	}()
}
