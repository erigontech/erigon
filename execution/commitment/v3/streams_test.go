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

package v3

import (
	"bytes"
	"encoding/binary"
	"fmt"
	"math/rand"
	"testing"

	"github.com/erigontech/erigon/common"
	"github.com/erigontech/erigon/common/empty"
	"github.com/erigontech/erigon/common/length"
	"github.com/erigontech/erigon/execution/commitment"
	"github.com/erigontech/erigon/internal/commitmenttest"
	"github.com/holiman/uint256"
	"github.com/stretchr/testify/require"
)

func TestSharedGeneratorStreams(t *testing.T) {
	t.Run("integer_keys_and_values", func(t *testing.T) {
		for i := range 100001 {
			require.Equal(t, oldBenchAddr(i), benchAddr(i))
			require.Equal(t, oldBenchSlot(i), benchSlot(i))
			require.Equal(t, oldParityAddress(i), parityAddress(i))
			require.Equal(t, oldParitySlot(i), paritySlot(i))
			require.Equal(t, oldParityFuzzAddress(i), parityFuzzAddress(i))
			require.Equal(t, oldIncrAddress(i), incrAddress(i))
			require.Equal(t, oldIncrSlot(oldIncrAddress(i), i), incrSlot(incrAddress(i), i))
			require.Equal(t, oldAccountParityUpdate(i), accountParityUpdate(i))
			require.Equal(t, oldStorageParityUpdate(i), storageParityUpdate(i))
			require.Equal(t, oldPlainAccount(i), plainAccount(i))
			for _, plane := range []byte{planeAccount, planeStorage} {
				require.Equal(t, oldFoldKey(plane, i), foldKey(plane, i))
				require.Equal(t, oldFoldUpdate(plane, i), foldUpdate(plane, i))
			}
		}
	})
	t.Run("bench_entries", func(t *testing.T) {
		for _, shape := range []string{"accounts", "storage", "whale", "whale_mixed", "mixed"} {
			for _, n := range []int{0, 1, 6, 64, 128, 256, 300, 512, 3 * defaultStorageFanOutMin, 10000, 100000} {
				require.Equal(t, oldBenchEntries(shape, n), benchEntries(shape, n), "%s/%d", shape, n)
			}
		}
	})
	t.Run("fold_seeds", func(t *testing.T) {
		for _, plane := range []byte{planeAccount, planeStorage} {
			for _, count := range []int{1, 2, 16, 1000} {
				for _, seed := range []int64{int64(count) + int64(plane), 91 + int64(plane)} {
					oldKeys, oldPaths := oldDistinctKeysAndPaths(plane, count, seed)
					keys, paths := distinctKeysAndPaths(plane, count, seed)
					require.Equal(t, oldKeys, keys)
					require.Equal(t, oldPaths, paths)
				}
			}
		}
	})
	t.Run("incremental_batches", func(t *testing.T) {
		oldBatches, oldFinal := oldIncrementalBatches()
		batches, final := incrementalBatches()
		require.Equal(t, oldBatches, batches)
		require.Equal(t, oldFinal, final)
		for i := range uint64(256) {
			require.Equal(t, oldIncrementalAccountUpdate(i, i*10), incrementalAccountUpdate(i, i*10))
			require.Equal(t, oldIncrementalStorageUpdate(byte(i)), incrementalStorageUpdate(byte(i)))
			require.Equal(t, oldFullAccountUpdate(i, i*10, common.Hash{byte(i)}), fullAccountUpdate(i, i*10, common.Hash{byte(i)}))
		}
		require.Equal(t, oldIncrementalDeleteUpdate(), incrementalDeleteUpdate())
	})
	t.Run("random_values", func(t *testing.T) {
		for seed := int64(1); seed <= 4; seed++ {
			a, b := rand.New(rand.NewSource(seed)), rand.New(rand.NewSource(seed))
			for range 10000 {
				require.Equal(t, oldIncrAccountUpdate(a), incrAccountUpdate(b))
				require.Equal(t, oldIncrStorageUpdate(a), incrStorageUpdate(b))
			}
		}
		for _, seed := range []int64{7, 9} {
			a, b := rand.New(rand.NewSource(seed)), rand.New(rand.NewSource(seed))
			for i := range 200000 {
				var au, bu *commitment.Update
				var ar, br []byte
				if i%100 < 12 {
					au, ar = oldSizeContract(i, a)
					bu, br = sizeContract(i, b)
				} else {
					au, ar = oldSizeEOA(i, a)
					bu, br = sizeEOA(i, b)
				}
				require.Equal(t, au, bu)
				require.Equal(t, ar, br)
			}
		}
	})
	t.Run("paths_and_records", func(t *testing.T) {
		for depth := range 65 {
			prefix := bytes.Repeat([]byte{byte(depth % 16)}, depth)
			require.Equal(t, oldSlotPath(prefix...), slotPath(prefix...))
			require.Equal(t, oldSlotValue(oldSlotPath(prefix...)), slotValue(slotPath(prefix...)))
		}
		for flags := range 256 {
			leaves := map[int]leafFixture{1: {suffix: []byte{0x12}, value: []byte{3, 4}}}
			require.Equal(t, oldRecordFixture(byte(flags), 61, 7, 2, 4, []byte{0}, map[int][]byte{2: {0}}, leaves), recordFixture(byte(flags), 61, 7, 2, 4, []byte{0}, map[int][]byte{2: {0}}, leaves))
		}
	})
	t.Run("size_fixtures", func(t *testing.T) {
		a, b := rand.New(rand.NewSource(7)), rand.New(rand.NewSource(7))
		for range 200000 {
			au, ar := oldSizeEOA(0, a)
			bu, br := sizeEOA(0, b)
			require.Equal(t, au, bu)
			require.Equal(t, ar, br)
		}
		au, ar := oldSizeContract(3, rand.New(rand.NewSource(9)))
		bu, br := sizeContract(3, rand.New(rand.NewSource(9)))
		require.Equal(t, au, bu)
		require.Equal(t, ar, br)
	})
}

func oldAccountParityUpdate(i int) *commitment.Update {
	balance := uint256.NewInt(uint64(i + 1))
	return &commitment.Update{
		Flags:    commitment.BalanceUpdate | commitment.NonceUpdate | commitment.CodeUpdate,
		Balance:  *balance,
		Nonce:    uint64(i + 1),
		CodeHash: common.HexToHash(fmt.Sprintf("0x%064x", i+1)),
	}
}

func oldStorageParityUpdate(i int) *commitment.Update {
	update := &commitment.Update{Flags: commitment.StorageUpdate}
	update.Storage[0] = byte(i)
	update.Storage[1] = byte(i >> 8)
	update.StorageLen = 2
	return update
}

func oldParityAddress(i int) []byte {
	address := make([]byte, length.Addr)
	for j := range address {
		address[j] = byte(i*17 + j)
	}
	return address
}

func oldParitySlot(i int) []byte {
	slot := make([]byte, length.Hash)
	for j := range slot {
		slot[j] = byte(i*29 + j*3)
	}
	return slot
}

func oldParityFuzzAddress(i int) []byte {
	key := make([]byte, length.Addr)
	binary.BigEndian.PutUint64(key[length.Addr-8:], uint64(i))
	return key
}

func oldBenchAddr(i int) []byte {
	a := make([]byte, length.Addr)
	binary.BigEndian.PutUint64(a[:8], uint64(i)*0x9E3779B97F4A7C15)
	binary.BigEndian.PutUint64(a[8:16], uint64(i))
	return a
}

func oldBenchSlot(i int) []byte {
	s := make([]byte, length.Hash)
	binary.BigEndian.PutUint64(s[:8], uint64(i)*0xC2B2AE3D27D4EB4F)
	binary.BigEndian.PutUint64(s[8:16], uint64(i))
	return s
}

func oldBenchEntries(shape string, n int) []parityUpdate {
	switch shape {
	case "accounts":
		out := make([]parityUpdate, n)
		for i := range out {
			out[i] = parityUpdate{key: oldBenchAddr(i), update: oldAccountParityUpdate(i)}
		}
		return out
	case "storage":
		out := make([]parityUpdate, 0, n*2)
		for i := range n {
			out = append(out,
				parityUpdate{key: oldBenchAddr(i), update: oldAccountParityUpdate(i)},
				parityUpdate{key: append(oldBenchAddr(i), oldBenchSlot(i)...), update: oldStorageParityUpdate(i)})
		}
		return out
	case "whale":
		rnd := rand.New(rand.NewSource(424242))
		addr := make([]byte, length.Addr)
		rnd.Read(addr)
		out := make([]parityUpdate, 0, n+1)
		out = append(out, parityUpdate{key: addr, update: oldAccountParityUpdate(1)})
		for i := range n {
			slot := make([]byte, length.Hash)
			rnd.Read(slot)
			out = append(out, parityUpdate{key: append(append([]byte{}, addr...), slot...), update: oldStorageParityUpdate(i)})
		}
		return out
	case "whale_mixed":
		rnd := rand.New(rand.NewSource(99))
		out := make([]parityUpdate, 0, n+2000)
		for i := range 1000 {
			out = append(out, parityUpdate{key: oldBenchAddr(i), update: oldAccountParityUpdate(i)})
		}
		waddr := make([]byte, length.Addr)
		rnd.Read(waddr)
		out = append(out, parityUpdate{key: waddr, update: oldAccountParityUpdate(7)})
		for i := range n {
			slot := make([]byte, length.Hash)
			rnd.Read(slot)
			out = append(out, parityUpdate{key: append(append([]byte{}, waddr...), slot...), update: oldStorageParityUpdate(i)})
		}
		for i := range 1000 {
			out = append(out, parityUpdate{key: oldBenchAddr(500000 + i), update: oldAccountParityUpdate(i)})
		}
		return out
	default:
		out := make([]parityUpdate, 0, n*2)
		for i := range n {
			out = append(out,
				parityUpdate{key: oldBenchAddr(i), update: oldAccountParityUpdate(i)},
				parityUpdate{key: append(oldBenchAddr(i), oldBenchSlot(i)...), update: oldStorageParityUpdate(i)})
		}
		return out
	}
}

func oldDistinctKeysAndPaths(plane byte, count int, seed int64) ([][]byte, [][]byte) {
	rng := rand.New(rand.NewSource(seed))
	keys := make([][]byte, 0, count)
	paths := make([][]byte, 0, count)
	seen := make(map[string]struct{}, count)
	for len(paths) < count {
		key := make([]byte, length.Addr)
		if plane == planeStorage {
			key = make([]byte, length.Addr)
		}
		rng.Read(key)
		if _, ok := seen[string(key)]; ok {
			continue
		}
		seen[string(key)] = struct{}{}
		keys = append(keys, key)
		path := commitment.KeyToHexNibbleHash(key)
		paths = append(paths, path)
	}
	return keys, paths
}

func oldFoldKey(plane byte, number int) []byte {
	key := make([]byte, length.Addr)
	if plane == planeStorage {
		key = make([]byte, length.Addr)
	}
	for i := range key {
		key[i] = byte(number + i*17)
	}
	return key
}

func oldFoldUpdate(plane byte, number int) commitment.Update {
	if plane == planeStorage {
		var storage [32]byte
		storage[0] = byte(number)
		storage[1] = byte(number >> 8)
		return commitment.Update{Flags: commitment.StorageUpdate, StorageLen: 2, Storage: storage}
	}
	return commitment.Update{CodeHash: empty.CodeHash, Flags: commitment.CodeUpdate | commitment.NonceUpdate | commitment.BalanceUpdate, Nonce: uint64(number), Balance: *uint256.NewInt(uint64(number * 3))}
}

func oldIncrementalBatches() ([][]incrementalOp, []incrementalOp) {
	addressA := bytes.Repeat([]byte{0x11}, 20)
	addressB := bytes.Repeat([]byte{0x22}, 20)
	slotA1 := append(append([]byte(nil), addressA...), bytes.Repeat([]byte{0x31}, 32)...)
	slotA2 := append(append([]byte(nil), addressA...), bytes.Repeat([]byte{0x32}, 32)...)
	slotA3 := append(append([]byte(nil), addressA...), bytes.Repeat([]byte{0x33}, 32)...)
	slotB1 := append(append([]byte(nil), addressB...), bytes.Repeat([]byte{0x41}, 32)...)

	batch1 := []incrementalOp{
		{key: addressA, update: oldIncrementalAccountUpdate(1, 10)},
		{key: addressB, update: oldIncrementalAccountUpdate(2, 20)},
		{key: slotA1, update: oldIncrementalStorageUpdate(1)},
		{key: slotB1, update: oldIncrementalStorageUpdate(2)},
	}
	batch2 := []incrementalOp{
		{key: addressA, update: oldIncrementalAccountUpdate(3, 30)},
		{key: addressA, update: oldIncrementalAccountUpdate(4, 40)},
		{key: slotA2, update: oldIncrementalStorageUpdate(3)},
		{key: slotA3, update: oldIncrementalDeleteUpdate()},
		{key: slotA3, update: oldIncrementalStorageUpdate(4)},
	}
	batch3 := []incrementalOp{
		{key: addressA, read: true},
		{key: slotA1, read: true},
		{key: addressB, update: oldIncrementalDeleteUpdate()},
	}
	final := []incrementalOp{
		{key: addressA, update: oldIncrementalAccountUpdate(4, 40)},
		{key: slotA1, update: oldIncrementalStorageUpdate(1)},
		{key: slotA2, update: oldIncrementalStorageUpdate(3)},
		{key: slotA3, update: oldIncrementalStorageUpdate(4)},
	}
	return [][]incrementalOp{batch1, batch2, batch3}, final
}

func oldIncrementalAccountUpdate(nonce, balance uint64) *commitment.Update {
	value := uint256.NewInt(balance)
	return &commitment.Update{
		Flags:    commitment.BalanceUpdate | commitment.NonceUpdate | commitment.CodeUpdate,
		Balance:  *value,
		Nonce:    nonce,
		CodeHash: common.HexToHash("0x1234"),
	}
}

func oldIncrementalStorageUpdate(value byte) *commitment.Update {
	update := &commitment.Update{Flags: commitment.StorageUpdate, StorageLen: 1}
	update.Storage[0] = value
	return update
}

func oldIncrementalDeleteUpdate() *commitment.Update {
	return &commitment.Update{Flags: commitment.DeleteUpdate}
}

func oldIncrAddress(i int) []byte {
	addr := make([]byte, length.Addr)
	binary.BigEndian.PutUint64(addr[length.Addr-8:], uint64(i))
	return addr
}

func oldIncrSlot(addr []byte, j int) []byte {
	key := make([]byte, 0, length.Addr+length.Hash)
	key = append(key, addr...)
	slot := make([]byte, length.Hash)
	binary.BigEndian.PutUint64(slot[length.Hash-8:], uint64(j))
	return append(key, slot...)
}

func oldPlainAccount(i int) *commitment.Update {
	u := &commitment.Update{Flags: commitment.BalanceUpdate | commitment.NonceUpdate | commitment.CodeUpdate}
	u.Nonce = uint64(i + 1)
	u.Balance = *uint256.NewInt(uint64(i + 1))
	u.CodeHash = empty.CodeHash
	return u
}

func oldIncrAccountUpdate(rng *rand.Rand) *commitment.Update {
	u := &commitment.Update{Flags: commitment.BalanceUpdate | commitment.NonceUpdate | commitment.CodeUpdate}
	u.Nonce = uint64(rng.Intn(1 << 20))
	u.Balance = *uint256.NewInt(rng.Uint64())
	u.CodeHash = empty.CodeHash
	if rng.Intn(3) == 0 {
		u.CodeHash = common.BigToHash(uint256.NewInt(rng.Uint64()).ToBig())
	}
	return u
}

func oldIncrStorageUpdate(rng *rand.Rand) *commitment.Update {
	u := &commitment.Update{Flags: commitment.StorageUpdate}
	n := 1 + rng.Intn(length.Hash)
	rng.Read(u.Storage[:n])
	u.Storage[0] |= 1
	u.StorageLen = int8(n)
	return u
}

func oldSizeEOA(i int, rnd *rand.Rand) (*commitment.Update, []byte) {
	u := &commitment.Update{Flags: commitment.BalanceUpdate | commitment.NonceUpdate}
	u.Nonce = uint64(rnd.Intn(500))
	u.Balance = *uint256.NewInt(uint64(rnd.Int63n(4e18)))
	u.CodeHash = empty.CodeHash
	return u, empty.RootHash[:]
}

func oldSizeContract(i int, rnd *rand.Rand) (*commitment.Update, []byte) {
	u := &commitment.Update{Flags: commitment.BalanceUpdate | commitment.NonceUpdate | commitment.CodeUpdate}
	u.Nonce = 1
	u.Balance = *uint256.NewInt(uint64(rnd.Int63n(1e15)))
	u.CodeHash = common.HexToHash(fmt.Sprintf("0x%064x", i+7))
	root := make([]byte, 32)
	rnd.Read(root)
	return u, root
}

func oldFullAccountUpdate(nonce, balance uint64, codeHash common.Hash) commitment.Update {
	return commitment.Update{
		Flags:    commitment.BalanceUpdate | commitment.NonceUpdate | commitment.CodeUpdate,
		Nonce:    nonce,
		Balance:  *uint256.NewInt(balance),
		CodeHash: codeHash,
	}
}

func oldSlotPath(prefix ...byte) []byte {
	p := make([]byte, 0, 64)
	p = append(p, prefix...)
	fill := byte(0xd)
	if len(prefix) != 0 {
		fill = (prefix[len(prefix)-1] + 7) & 0x0f
	}
	for len(p) < 64 {
		p = append(p, fill)
	}
	return p
}

func oldSlotValue(path []byte) []byte {
	return []byte{path[0] + 1, path[1] + 1, path[2] + 1, path[3] + 1}
}

func oldRecordFixture(flags byte, depth int, child, leaf, ext uint16, self []byte, extAt map[int][]byte, leafAt map[int]leafFixture) []byte {
	if flags&hdrIsLeafRoot != 0 {
		return append([]byte{flags}, append(make([]byte, 32), 0)...)
	}
	rec := []byte{flags}
	if flags&hdrHasSelfExt != 0 {
		rec = append(rec, self...)
	}
	mask := make([]byte, 4)
	mask[0] = byte(child >> 8)
	mask[1] = byte(child)
	mask[2] = byte(leaf >> 8)
	mask[3] = byte(leaf)
	rec = append(rec, mask...)
	if flags&hdrHasChildExt != 0 {
		rec = append(rec, byte(ext>>8), byte(ext))
	}
	tree := child &^ leaf
	for nib := range 16 {
		if tree&(uint16(1)<<nib) == 0 {
			continue
		}
		for range 32 {
			rec = append(rec, byte(nib))
		}
	}
	for nib := range 16 {
		if ext&(uint16(1)<<nib) != 0 {
			rec = append(rec, extAt[nib]...)
		}
	}
	for nib := range 16 {
		if leaf&(uint16(1)<<nib) != 0 {
			entry := leafAt[nib]
			rec = append(rec, entry.suffix...)
			rec = append(rec, byte(len(entry.value)))
			rec = append(rec, entry.value...)
		}
	}
	return rec
}

func oldWhaleRounds(seed int64, sizes ...int) [][]parityUpdate {
	rng := rand.New(rand.NewSource(seed))
	addr := make([]byte, length.Addr)
	rng.Read(addr)
	rounds := make([][]parityUpdate, len(sizes))
	next := 0
	for round, count := range sizes {
		if round == 0 {
			rounds[round] = append(rounds[round], parityUpdate{key: addr, update: oldAccountParityUpdate(1)})
		}
		for range count {
			slot := make([]byte, length.Hash)
			rng.Read(slot)
			rounds[round] = append(rounds[round], parityUpdate{key: append(append([]byte{}, addr...), slot...), update: oldStorageParityUpdate(next)})
			next++
		}
	}
	return rounds
}

func oldOneSlotRounds(seed int64, n1, n2 int, rewrite bool) [][]parityUpdate {
	rnd := rand.New(rand.NewSource(seed))
	addrs, slots := make([][]byte, n1+n2), make([][]byte, n1+n2)
	for i := range addrs {
		addrs[i], slots[i] = make([]byte, length.Addr), make([]byte, length.Hash)
		rnd.Read(addrs[i])
		rnd.Read(slots[i])
	}
	build := func(lo, hi, gen int) []parityUpdate {
		var out []parityUpdate
		for i := lo; i < hi; i++ {
			out = append(out, parityUpdate{key: addrs[i], update: oldAccountParityUpdate(i)}, parityUpdate{key: append(append([]byte{}, addrs[i]...), slots[i]...), update: oldStorageParityUpdate(i + gen*1000)})
		}
		return out
	}
	rounds := [][]parityUpdate{build(0, n1, 0), build(n1, n1+n2, 0)}
	if rewrite {
		rounds[1] = build(0, n2, 1)
	}
	return rounds
}

func TestSharedSequenceStreams(t *testing.T) {
	check := func(seed int64, spec commitmenttest.SequenceSpec, want [][]parityUpdate) {
		t.Helper()
		c, err := commitmenttest.Generate(commitmenttest.MathRand(seed), spec)
		require.NoError(t, err)
		require.Len(t, c.Rounds, len(want))
		for i, round := range c.Rounds {
			require.Equal(t, want[i], parityEntries(round), "seed=%d spec=%+v round=%d", seed, spec, i)
		}
	}
	for seed := range int64(200) {
		check(seed, commitmenttest.SequenceSpec{Kind: "whale", Count: 2}, oldWhaleRounds(seed, 2))
	}
	for _, seed := range []int64{424242, 1, 2, 3, 7, 99, 12345, 777, 31337, 5150} {
		for n := 1; n <= 80; n++ {
			check(seed, commitmenttest.SequenceSpec{Kind: "whale", Count: n}, oldWhaleRounds(seed, n))
		}
	}
	for seed := int64(1); seed <= 40; seed++ {
		for _, b1 := range []int{2, 3, 5} {
			for _, b2 := range []int{1, 2, 3} {
				check(seed, commitmenttest.SequenceSpec{Kind: "whale", BatchSizes: []int{b1, b2}}, oldWhaleRounds(seed, b1, b2))
			}
		}
		for _, n1 := range []int{2, 5, 20} {
			for _, n2 := range []int{1, 2, 5} {
				for _, rewrite := range []bool{false, true} {
					check(seed, commitmenttest.SequenceSpec{Kind: "one-slot", BatchSizes: []int{n1, n2}, Rewrite: rewrite}, oldOneSlotRounds(seed, n1, n2, rewrite))
				}
			}
		}
	}
}

func oldStressRounds(seed int64, blocks, addrs, slots, opsPerBlock int) [][]parityUpdate {
	rng := rand.New(rand.NewSource(seed))
	rounds := make([][]parityUpdate, 0, blocks)
	live := make(map[string]struct{})
	for range blocks {
		count := 1 + rng.Intn(opsPerBlock)
		touched := make(map[string]struct{}, count)
		entries := make([]parityUpdate, 0, count)
		for range count {
			addr := oldIncrAddress(rng.Intn(addrs))
			if _, busy := touched[string(addr)]; busy {
				continue
			}
			_, alive := live[string(addr)]
			switch {
			case !alive:
				touched[string(addr)] = struct{}{}
				live[string(addr)] = struct{}{}
				entries = append(entries, parityUpdate{key: addr, update: oldIncrAccountUpdate(rng)})
			case rng.Intn(16) == 0:
				touched[string(addr)] = struct{}{}
				delete(live, string(addr))
				entries = append(entries, parityUpdate{key: addr, update: &commitment.Update{Flags: commitment.DeleteUpdate}})
			default:
				touched[string(addr)] = struct{}{}
				if rng.Intn(4) == 0 {
					entries = append(entries, parityUpdate{key: addr, update: oldIncrAccountUpdate(rng)})
				}
				for range 1 + rng.Intn(3) {
					key := oldIncrSlot(addr, rng.Intn(slots))
					if rng.Intn(5) == 0 {
						entries = append(entries, parityUpdate{key: key, update: &commitment.Update{Flags: commitment.DeleteUpdate}})
						continue
					}
					entries = append(entries, parityUpdate{key: key, update: oldIncrStorageUpdate(rng)})
				}
			}
		}
		rounds = append(rounds, entries)
	}
	return rounds
}

func oldCollapseRounds(seed int64, blocks, addrs int) [][]parityUpdate {
	rng := rand.New(rand.NewSource(seed))
	rounds := make([][]parityUpdate, 0, blocks)
	live := make(map[int]struct{})
	for n := range blocks {
		entries := make([]parityUpdate, 0, 4)
		touched := make(map[int]struct{})
		for i := range addrs {
			if _, ok := live[i]; ok {
				continue
			}
			if rng.Intn(2) == 0 {
				live[i] = struct{}{}
				touched[i] = struct{}{}
				entries = append(entries, parityUpdate{key: oldIncrAddress(i), update: oldPlainAccount(i*31 + n)})
			}
		}
		for i := range addrs {
			if _, ok := live[i]; !ok {
				continue
			}
			if _, busy := touched[i]; busy {
				continue
			}
			if rng.Intn(3) != 0 {
				continue
			}
			delete(live, i)
			touched[i] = struct{}{}
			entries = append(entries, parityUpdate{key: oldIncrAddress(i), update: &commitment.Update{Flags: commitment.DeleteUpdate}})
		}
		rounds = append(rounds, entries)
	}
	return rounds
}

func TestSharedStressStreams(t *testing.T) {
	check := func(seed int64, spec commitmenttest.SequenceSpec, want [][]parityUpdate) {
		t.Helper()
		c, err := commitmenttest.Generate(commitmenttest.MathRand(seed), spec)
		require.NoError(t, err)
		require.Len(t, c.Rounds, len(want))
		for i, round := range c.Rounds {
			require.Equal(t, want[i], parityEntries(round), "seed=%d spec=%+v round=%d", seed, spec, i)
		}
	}
	for _, spec := range []commitmenttest.SequenceSpec{
		{Kind: "stress", Rounds: 120, Accounts: 6, Slots: 4, OpsPerRound: 3},
		{Kind: "stress", Rounds: 120, Accounts: 24, Slots: 10, OpsPerRound: 5},
		{Kind: "stress", Rounds: 60, Accounts: 200, Slots: 40, OpsPerRound: 8},
	} {
		for seed := int64(1); seed <= 4; seed++ {
			check(seed, spec, oldStressRounds(seed, spec.Rounds, spec.Accounts, spec.Slots, spec.OpsPerRound))
		}
	}
	for _, accounts := range []int{2, 3, 5, 8} {
		for seed := int64(1); seed <= 30; seed++ {
			check(seed, commitmenttest.SequenceSpec{Kind: "root-collapse", Rounds: 120, Accounts: accounts}, oldCollapseRounds(seed, 120, accounts))
		}
	}
}
