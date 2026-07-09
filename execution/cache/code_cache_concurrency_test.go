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
	"sync"
	"testing"

	"github.com/c2h5oh/datasize"
	"github.com/stretchr/testify/require"

	"github.com/erigontech/erigon/common/crypto"
	"github.com/erigontech/erigon/common/maphash"
)

// TestCodeCache_ConcurrentPutSameCode_NoSizeDrift guards against the size
// accounting drift that the pre-LoadOrStore code had: parallel workers Putting
// the same cold code all missed the membership check, all passed the cap gate,
// and all added to the byte counter, leaving a permanent positive surplus that
// eventually wedged the cap. With the atomic LoadOrStore insert, only the
// goroutine that actually inserts accounts the size, so the counters must equal
// exactly one entry regardless of how many concurrent Puts raced.
func TestCodeCache_ConcurrentPutSameCode_NoSizeDrift(t *testing.T) {
	cc := NewCodeCache(64*datasize.MB, 16*datasize.MB)

	addr := make([]byte, 20)
	addr[0] = 0xab
	code := []byte("some non-trivial contract bytecode payload xyz")
	codeHash := crypto.Keccak256(code)

	const workers = 64
	var wg sync.WaitGroup
	wg.Add(workers)
	for i := 0; i < workers; i++ {
		go func() {
			defer wg.Done()
			cc.PutWithCodeHash(addr, code, codeHash, 1)
		}()
	}
	wg.Wait()

	require.Equal(t, int64(8+len(code)), cc.codeSize.Load(),
		"hashToCode size must reflect exactly one insert after concurrent same-code Puts")
	require.Equal(t, int64(len(codeHash)+len(code)), cc.codeHashCodeSize.Load(),
		"codeHashToCode size must reflect exactly one insert after concurrent same-code Puts")
	require.Equal(t, int64(1), cc.codeSizeEntries.Load(),
		"codeSizeByCodeHash must hold exactly one entry after concurrent same-code Puts")

	got, ok := cc.GetByCodeHash(codeHash)
	require.True(t, ok)
	require.Equal(t, code, got)
}

// TestCodeCache_ByteCheckRejectsForeignKeyHash verifies the collision guard:
// an entry whose stored keyHash differs from the requested codeHash is treated
// as a miss, so a 64-bit maphash collision can never serve the wrong code.
func TestCodeCache_ByteCheckRejectsForeignKeyHash(t *testing.T) {
	cc := NewCodeCache(64*datasize.MB, 16*datasize.MB)

	code := []byte("contract A bytecode")
	realHash := crypto.Keccak256(code)
	cc.PutWithCodeHash(nil, code, realHash, 1)

	// Sanity: the real hash hits.
	_, ok := cc.GetByCodeHash(realHash)
	require.True(t, ok)

	// Simulate a foreign 32-byte codeHash that collapses to the same maphash
	// bucket by storing a colliding entry directly under a different keyHash.
	foreign := make([]byte, 32)
	copy(foreign, realHash)
	foreign[0] ^= 0xff // different 32-byte key
	cc.codeHashToCode.Add(maphash.Hash(foreign), codeEntry{code: code, keyHash: hash32(realHash), txNum: 1, epoch: cc.coh.Epoch()})

	// The stored entry's keyHash is realHash, not foreign — Get must reject it.
	_, ok = cc.GetByCodeHash(foreign)
	require.False(t, ok, "byte-check must reject an entry whose keyHash differs from the requested codeHash")
}

// TestCodeCache_ConcurrentDistinctPuts_RespectCap drives many workers putting
// distinct codes whose combined size far exceeds a tiny cap. The freelru layer
// evicts the coldest entries to stay within its entry cap (no freeze), and the
// OnEvict-maintained byte counter must never drift negative under concurrency.
func TestCodeCache_ConcurrentDistinctPuts_RespectCap(t *testing.T) {
	const codeCap = 4 * datasize.KB
	cc := NewCodeCache(codeCap, 16*datasize.MB)

	const workers = 128
	var wg sync.WaitGroup
	wg.Add(workers)
	for i := 0; i < workers; i++ {
		go func(n int) {
			defer wg.Done()
			code := make([]byte, 256)
			code[0], code[1] = byte(n), byte(n>>8) // distinct code per worker
			cc.PutWithCodeHash(nil, code, crypto.Keccak256(code), 1)
		}(i)
	}
	wg.Wait()

	// The entry cap (codeCap/avgCodeEntryBytes) is the hard bound; residency
	// settled far below the 128 distinct puts rather than freezing at the first.
	require.Less(t, cc.codeHashToCode.Len(), workers,
		"freelru must evict to its entry cap, not hold all 128 distinct codes")
	require.GreaterOrEqual(t, cc.codeHashCodeSize.Load(), int64(0),
		"byte counter must stay non-negative (OnEvict accounting must not double-subtract)")
}

// Same atomicity requirement for the addr→code binding: a concurrent
// authoritative Put must win over a conditional prefetch put in every
// interleaving.
func TestCodeCache_PutIfAbsentAtomicWithPut(t *testing.T) {
	cc := NewCodeCache(64*datasize.MB, 16*datasize.MB)
	addr := make([]byte, 20)
	addr[0] = 0xcd
	fresh := []byte{0xaa, 1, 2, 3}
	stale := []byte{0xbb, 4, 5, 6}
	for round := 0; round < 20000; round++ {
		binary.BigEndian.PutUint64(addr[1:], uint64(round)) // a fresh addr each round, so both writers race on the bind
		var wg sync.WaitGroup
		wg.Add(2)
		go func() { defer wg.Done(); cc.Put(addr, fresh, 20) }()
		go func() { defer wg.Done(); cc.PutIfAbsent(addr, stale, 10) }()
		wg.Wait()
		v, ok := cc.Get(addr)
		require.True(t, ok)
		require.Equal(t, fresh, v, "round %d: PutIfAbsent raced past a concurrent Put", round)
	}
}
