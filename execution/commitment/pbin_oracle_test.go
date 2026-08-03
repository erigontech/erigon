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
	"encoding/binary"
	"encoding/hex"
	"fmt"
	"math/rand"
	"slices"
	"sync"
	"testing"

	"github.com/stretchr/testify/require"
	"golang.org/x/crypto/sha3"

	"github.com/erigontech/erigon/common/empty"
	"github.com/erigontech/erigon/common/length"
)

// The reference implementation of EIP-8297's binary tree (eip:112-222),
// transcribed from the spec's Python with no optimisation — no memoised hashes,
// no shared buffers, one bit per byte — because it is the ground truth the
// engine is diffed against and has to stay recognisably the same algorithm. Its
// Keccak comes from x/crypto, not the fastkeccak the engine uses, so a hasher
// bug cannot cancel out on both sides.

const (
	pbinOracleMaxKeyLength = 8192
	pbinOracleLeafTag      = 0x00
	pbinOracleBranchTag    = 0x01
)

type pbinOracleNode interface{ pbinOracleNodeKind() }

type pbinOracleLeaf struct {
	key   []byte
	value []byte
}

// prefix holds one bit per byte, mirroring the spec's list[int].
type pbinOracleBranch struct {
	prefix      []byte
	left, right pbinOracleNode
}

func (*pbinOracleLeaf) pbinOracleNodeKind()   {}
func (*pbinOracleBranch) pbinOracleNodeKind() {}

type pbinOracleTree struct {
	root pbinOracleNode
}

func pbinOracleBytesToBits(data []byte) []byte {
	bits := make([]byte, 0, len(data)*8)
	for _, b := range data {
		for i := range 8 {
			bits = append(bits, (b>>(7-i))&1)
		}
	}
	return bits
}

func (t *pbinOracleTree) insert(key, value []byte) {
	if len(key) < 1 || len(key) > pbinOracleMaxKeyLength {
		panic(fmt.Sprintf("pbin oracle: key length %d out of range", len(key)))
	}
	if len(value) != pbinValueLength {
		panic(fmt.Sprintf("pbin oracle: value of %d bytes, want %d", len(value), pbinValueLength))
	}
	if t.root == nil {
		t.root = &pbinOracleLeaf{key: slices.Clone(key), value: slices.Clone(value)}
		return
	}
	t.root = pbinOracleInsert(t.root, pbinOracleBytesToBits(key), key, value, 0)
}

func pbinOracleInsert(node pbinOracleNode, bits, key, value []byte, depth int) pbinOracleNode {
	if leaf, ok := node.(*pbinOracleLeaf); ok {
		if bytes.Equal(leaf.key, key) {
			leaf.value = slices.Clone(value)
			return leaf
		}
		otherBits := pbinOracleBytesToBits(leaf.key)
		limit := min(len(bits), len(otherBits))
		run := 0
		for depth+run < limit && bits[depth+run] == otherBits[depth+run] {
			run++
		}
		if depth+run >= limit {
			panic("pbin oracle: insert violates prefix-freedom")
		}
		newLeaf := &pbinOracleLeaf{key: slices.Clone(key), value: slices.Clone(value)}
		branch := &pbinOracleBranch{prefix: slices.Clone(bits[depth : depth+run])}
		if bits[depth+run] == 0 {
			branch.left, branch.right = newLeaf, leaf
		} else {
			branch.left, branch.right = leaf, newLeaf
		}
		return branch
	}

	branch := node.(*pbinOracleBranch)
	matched := 0
	for matched < len(branch.prefix) && depth+matched < len(bits) && bits[depth+matched] == branch.prefix[matched] {
		matched++
	}
	if depth+matched >= len(bits) {
		panic("pbin oracle: insert violates prefix-freedom")
	}
	if matched == len(branch.prefix) {
		split := depth + matched
		if bits[split] == 0 {
			branch.left = pbinOracleInsert(branch.left, bits, key, value, split+1)
		} else {
			branch.right = pbinOracleInsert(branch.right, bits, key, value, split+1)
		}
		return branch
	}

	// The key diverges inside the prefix (eip:171-182). The survivor keeps the
	// bits after the divergence, dropping the bit the new branch consumes.
	survivor := &pbinOracleBranch{
		prefix: slices.Clone(branch.prefix[matched+1:]),
		left:   branch.left,
		right:  branch.right,
	}
	newLeaf := &pbinOracleLeaf{key: slices.Clone(key), value: slices.Clone(value)}
	newBranch := &pbinOracleBranch{prefix: slices.Clone(branch.prefix[:matched])}
	if bits[depth+matched] == 0 {
		newBranch.left, newBranch.right = newLeaf, survivor
	} else {
		newBranch.left, newBranch.right = survivor, newLeaf
	}
	return newBranch
}

// pbinOracleEncodeBitPrefix is the spec's encode_bit_prefix (eip:196-201).
func pbinOracleEncodeBitPrefix(prefix []byte) []byte {
	if len(prefix) >= 1<<16 {
		panic(fmt.Sprintf("pbin oracle: prefix of %d bits exceeds the encodable count", len(prefix)))
	}
	out := make([]byte, 2+(len(prefix)+7)/8)
	binary.BigEndian.PutUint16(out, uint16(len(prefix)))
	for i, bit := range prefix {
		out[2+i/8] |= bit << (7 - i%8)
	}
	return out
}

func pbinOracleMerkelize(node pbinOracleNode) [32]byte {
	return pbinOracleMerkelizeWith(node, nil)
}

// pbinOracleMerkelizeWith merkelizes under an explicit H. A nil sum means
// Keccak-256; the reference's vectors are replayed by passing BLAKE3.
func pbinOracleMerkelizeWith(node pbinOracleNode, sum func([]byte) [32]byte) [32]byte {
	var out [32]byte
	if node == nil {
		return out
	}
	if sum != nil {
		var pre []byte
		switch n := node.(type) {
		case *pbinOracleLeaf:
			pre = append(pre, pbinOracleLeafTag)
			pre = append(pre, n.key...)
			pre = append(pre, n.value...)
		case *pbinOracleBranch:
			left := pbinOracleMerkelizeWith(n.left, sum)
			right := pbinOracleMerkelizeWith(n.right, sum)
			pre = append(pre, pbinOracleBranchTag)
			pre = append(pre, pbinOracleEncodeBitPrefix(n.prefix)...)
			pre = append(pre, left[:]...)
			pre = append(pre, right[:]...)
		}
		return sum(pre)
	}
	h := sha3.NewLegacyKeccak256()
	switch n := node.(type) {
	case *pbinOracleLeaf:
		h.Write([]byte{pbinOracleLeafTag})
		h.Write(n.key)
		h.Write(n.value)
	case *pbinOracleBranch:
		left, right := pbinOracleMerkelize(n.left), pbinOracleMerkelize(n.right)
		h.Write([]byte{pbinOracleBranchTag})
		h.Write(pbinOracleEncodeBitPrefix(n.prefix))
		h.Write(left[:])
		h.Write(right[:])
	}
	copy(out[:], h.Sum(nil))
	return out
}

func (t *pbinOracleTree) rootHash() [32]byte { return pbinOracleMerkelize(t.root) }

type pbinOracleEntry struct {
	key   []byte
	value []byte
}

type pbinOracleCorpus struct {
	name    string
	entries []pbinOracleEntry
}

func pbinOracleRoot(entries []pbinOracleEntry) [32]byte {
	var tree pbinOracleTree
	for _, e := range entries {
		tree.insert(e.key, e.value)
	}
	return tree.rootHash()
}

func pbinOracleSharedBits(a, b []byte) int {
	aBits, bBits := pbinOracleBytesToBits(a), pbinOracleBytesToBits(b)
	n := 0
	for n < len(aBits) && n < len(bBits) && aBits[n] == bBits[n] {
		n++
	}
	return n
}

func pbinOracleValue(seed uint64) []byte {
	v := make([]byte, pbinValueLength)
	binary.BigEndian.PutUint64(v, 0xA5A5A5A5A5A5A5A5)
	binary.BigEndian.PutUint64(v[24:], seed)
	return v
}

func pbinOracleAddr(seed uint64) []byte {
	addr := make([]byte, length.Addr)
	binary.BigEndian.PutUint64(addr[12:], seed)
	return addr
}

func pbinOracleSlot(v uint64) []byte {
	slot := make([]byte, length.Hash)
	binary.BigEndian.PutUint64(slot[24:], v)
	return slot
}

func pbinOracleCorpora() []pbinOracleCorpus {
	return []pbinOracleCorpus{
		pbinOracleCorpusEmpty(),
		pbinOracleCorpusSingleKey(),
		pbinOracleCorpusSplitAtBit0(),
		pbinOracleCorpusSplitAtLastBit(),
		pbinOracleCorpusSplitInsidePrefix(),
		pbinOracleCorpusOneAccount(),
		pbinOracleCorpusDeepSharedPrefix(),
	}
}

func pbinOracleCorpusEmpty() pbinOracleCorpus {
	return pbinOracleCorpus{name: "empty"}
}

func pbinOracleCorpusSingleKey() pbinOracleCorpus {
	return pbinOracleCorpus{
		name: "single key",
		entries: []pbinOracleEntry{
			{key: pbinTreeKeyAccount(pbinOracleAddr(1), pbinBasicDataLeafKey), value: pbinOracleValue(1)},
		},
	}
}

// pbinOracleCorpusSplitAtBit0 diverges on the zone byte, so the root branch
// carries an empty prefix.
func pbinOracleCorpusSplitAtBit0() pbinOracleCorpus {
	addr := pbinOracleAddr(2)
	return pbinOracleCorpus{
		name: "split at bit 0",
		entries: []pbinOracleEntry{
			{key: pbinTreeKeyAccount(addr, pbinBasicDataLeafKey), value: pbinOracleValue(1)},
			{key: pbinTreeKeyStorage(addr, pbinOracleSlot(1000)), value: pbinOracleValue(2)},
		},
	}
}

// pbinOracleCorpusSplitAtLastBit picks two slots in one storage group whose
// sub-indices differ in their low bit, the deepest split 528-bit keys admit.
func pbinOracleCorpusSplitAtLastBit() pbinOracleCorpus {
	addr := pbinOracleAddr(3)
	return pbinOracleCorpus{
		name: "split at bit 527",
		entries: []pbinOracleEntry{
			{key: pbinTreeKeyStorage(addr, pbinOracleSlot(256)), value: pbinOracleValue(1)},
			{key: pbinTreeKeyStorage(addr, pbinOracleSlot(257)), value: pbinOracleValue(2)},
		},
	}
}

// pbinOracleCorpusSplitInsidePrefix uses synthetic account-zone keys, not
// digests, so the divergence bit is exact: the third key leaves the prefix the
// first two share, forcing insert down the survivor path.
func pbinOracleCorpusSplitInsidePrefix() pbinOracleCorpus {
	return pbinOracleCorpus{
		name: "split inside prefix",
		entries: []pbinOracleEntry{
			{key: pbinOracleSyntheticAccountKey(0x00), value: pbinOracleValue(1)},
			{key: pbinOracleSyntheticAccountKey(0x01), value: pbinOracleValue(2)},
			{key: pbinOracleSyntheticAccountKey(0x40), value: pbinOracleValue(3)},
		},
	}
}

func pbinOracleSyntheticAccountKey(stemByte byte) []byte {
	key := make([]byte, pbinAccountKeyLength)
	key[1] = stemByte
	return key
}

// pbinOracleCorpusOneAccount is the realistic shape: header leaves plus header-
// and storage-zone slots for one address, all sharing a stem.
func pbinOracleCorpusOneAccount() pbinOracleCorpus {
	addr := pbinOracleAddr(4)
	entries := []pbinOracleEntry{
		{key: pbinTreeKeyAccount(addr, pbinBasicDataLeafKey), value: pbinOracleValue(1)},
		{key: pbinTreeKeyAccount(addr, pbinCodeHashLeafKey), value: pbinOracleValue(2)},
	}
	for i, slot := range []uint64{0, 1, 63, 64, 65, 255, 256, 1000} {
		entries = append(entries, pbinOracleEntry{
			key:   pbinTreeKeyStorage(addr, pbinOracleSlot(slot)),
			value: pbinOracleValue(uint64(10 + i)),
		})
	}
	return pbinOracleCorpus{name: "one account", entries: entries}
}

const (
	pbinOracleMinedPrefixBits = 20
	pbinOracleMinedCluster    = 4
)

func pbinOracleCorpusDeepSharedPrefix() pbinOracleCorpus {
	entries := make([]pbinOracleEntry, 0, pbinOracleMinedCluster)
	for i, addr := range pbinOracleMinedAddrs() {
		entries = append(entries, pbinOracleEntry{
			key:   pbinTreeKeyAccount(addr, pbinBasicDataLeafKey),
			value: pbinOracleValue(uint64(i)),
		})
	}
	return pbinOracleCorpus{name: "mined deep shared prefix", entries: entries}
}

var pbinOracleMinedAddrs = sync.OnceValue(func() [][]byte {
	return pbinOracleMineSharedStems(pbinOracleMinedPrefixBits, pbinOracleMinedCluster)
})

// pbinOracleMineSharedStems finds addresses whose account keys agree on the
// leading bits by trial: the stem is a digest, so it cannot be constructed.
func pbinOracleMineSharedStems(shared, n int) [][]byte {
	const limit = 1 << 24
	var target []byte
	found := make([][]byte, 0, n)
	for i := uint64(0); i < limit && len(found) < n; i++ {
		addr := pbinOracleAddr(i)
		key := pbinTreeKeyAccount(addr, pbinBasicDataLeafKey)
		if target == nil {
			target, found = key, append(found, addr)
			continue
		}
		if pbinOracleSharedBits(target, key) >= shared {
			found = append(found, addr)
		}
	}
	if len(found) < n {
		panic(fmt.Sprintf("pbin oracle: found only %d of %d addresses sharing %d bits", len(found), n, shared))
	}
	return found
}

func TestPBinOracleEncodeBitPrefix(t *testing.T) {
	t.Parallel()

	for _, tc := range []struct {
		name   string
		prefix []byte
		want   string
	}{
		{name: "empty prefix is a bare count", prefix: nil, want: "0000"},
		{name: "one zero bit", prefix: []byte{0}, want: "000100"},
		{name: "one set bit lands in the MSB", prefix: []byte{1}, want: "000180"},
		{name: "three bits", prefix: []byte{1, 0, 1}, want: "0003a0"},
		{name: "seven bits pad low", prefix: []byte{1, 1, 1, 1, 1, 1, 1}, want: "0007fe"},
		{name: "full byte", prefix: []byte{1, 0, 1, 0, 1, 0, 1, 0}, want: "0008aa"},
		{name: "nine bits open a second byte", prefix: []byte{1, 0, 1, 0, 1, 0, 1, 0, 1}, want: "0009aa80"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			require.Equal(t, tc.want, hex.EncodeToString(pbinOracleEncodeBitPrefix(tc.prefix)))
		})
	}
}

func TestPBinOracleEncodeBitPrefixLongRun(t *testing.T) {
	t.Parallel()

	// 528 bits of 1: count 0x0210 followed by 66 0xFF bytes.
	prefix := bytes.Repeat([]byte{1}, pbinMaxPathBits)
	got := pbinOracleEncodeBitPrefix(prefix)
	require.Len(t, got, 2+66)
	require.Equal(t, []byte{0x02, 0x10}, got[:2])
	require.Equal(t, bytes.Repeat([]byte{0xFF}, 66), got[2:])
}

// The empty tree is 32 zero bytes (eip:208), not the empty-MPT root the rest of
// erigon uses.
func TestPBinOracleEmptyTreeHash(t *testing.T) {
	t.Parallel()

	var tree pbinOracleTree
	root := tree.rootHash()
	require.Equal(t, make([]byte, 32), root[:])
	require.NotEqual(t, empty.RootHash[:], root[:])
}

func TestPBinOracleSingleKeyRootIsLeafHash(t *testing.T) {
	t.Parallel()

	corpus := pbinOracleCorpusSingleKey()
	require.Len(t, corpus.entries, 1)
	e := corpus.entries[0]

	var tree pbinOracleTree
	tree.insert(e.key, e.value)

	require.IsType(t, &pbinOracleLeaf{}, tree.root, "a one-key tree's root is the leaf itself (eip:133-135)")

	want := pbinTestKeccak(t, []byte{0x00}, e.key, e.value)
	got := tree.rootHash()
	require.Equal(t, want, got[:])
}

func TestPBinOracleTwoKeyRootIsBranchHash(t *testing.T) {
	t.Parallel()

	corpus := pbinOracleCorpusSplitAtBit0()
	require.Len(t, corpus.entries, 2)
	a, b := corpus.entries[0], corpus.entries[1]

	var tree pbinOracleTree
	tree.insert(a.key, a.value)
	tree.insert(b.key, b.value)

	branch, ok := tree.root.(*pbinOracleBranch)
	require.True(t, ok)
	require.Empty(t, branch.prefix, "keys diverging at bit 0 leave the root prefix empty")

	left := pbinTestKeccak(t, []byte{0x00}, a.key, a.value)
	right := pbinTestKeccak(t, []byte{0x00}, b.key, b.value)
	want := pbinTestKeccak(t, []byte{0x01}, pbinOracleEncodeBitPrefix(nil), left, right)

	got := tree.rootHash()
	require.Equal(t, want, got[:])
}

func TestPBinOracleSplitAtLastBit(t *testing.T) {
	t.Parallel()

	corpus := pbinOracleCorpusSplitAtLastBit()
	require.Len(t, corpus.entries, 2)
	a, b := corpus.entries[0], corpus.entries[1]
	require.Equal(t, pbinMaxPathBits-1, pbinOracleSharedBits(a.key, b.key))

	var tree pbinOracleTree
	tree.insert(a.key, a.value)
	tree.insert(b.key, b.value)

	branch, ok := tree.root.(*pbinOracleBranch)
	require.True(t, ok)
	require.Len(t, branch.prefix, pbinMaxPathBits-1)

	left := pbinTestKeccak(t, []byte{0x00}, a.key, a.value)
	right := pbinTestKeccak(t, []byte{0x00}, b.key, b.value)
	want := pbinTestKeccak(t, []byte{0x01}, pbinOracleEncodeBitPrefix(branch.prefix), left, right)

	got := tree.rootHash()
	require.Equal(t, want, got[:])
}

// Pins the shape of the split-inside-prefix branch (eip:171-182): the bit the
// new branch consumes must not reappear in the survivor below it.
func TestPBinOracleSplitInsidePrefix(t *testing.T) {
	t.Parallel()

	corpus := pbinOracleCorpusSplitInsidePrefix()
	require.Len(t, corpus.entries, 3)
	a, b, c := corpus.entries[0], corpus.entries[1], corpus.entries[2]

	var pair pbinOracleTree
	pair.insert(a.key, a.value)
	pair.insert(b.key, b.value)
	pairRoot, ok := pair.root.(*pbinOracleBranch)
	require.True(t, ok)
	require.Len(t, pairRoot.prefix, 15, "a and b must share a prefix long enough to split inside")

	var tree pbinOracleTree
	tree.insert(a.key, a.value)
	tree.insert(b.key, b.value)
	tree.insert(c.key, c.value)

	root, ok := tree.root.(*pbinOracleBranch)
	require.True(t, ok)
	require.Len(t, root.prefix, 9, "the new branch keeps the bits before the divergence")

	// c has a 1 bit where the old prefix had 0, so the new leaf takes the right
	// side and the survivor keeps the left.
	survivor, ok := root.left.(*pbinOracleBranch)
	require.True(t, ok)
	require.Len(t, survivor.prefix, 5, "the survivor drops the divergence bit itself")
	require.Equal(t, pairRoot.prefix[10:], survivor.prefix)
	require.IsType(t, &pbinOracleLeaf{}, root.right)
}

func TestPBinOracleDuplicateKeyUpdatesValue(t *testing.T) {
	t.Parallel()

	corpus := pbinOracleCorpusSplitAtBit0()
	a, b := corpus.entries[0], corpus.entries[1]
	updated := pbinOracleValue(0xDEAD)

	var tree pbinOracleTree
	tree.insert(a.key, a.value)
	tree.insert(b.key, b.value)
	tree.insert(a.key, updated)

	var want pbinOracleTree
	want.insert(a.key, updated)
	want.insert(b.key, b.value)

	require.Equal(t, want.rootHash(), tree.rootHash())
	require.NotEqual(t, pbinOracleRoot(corpus.entries), tree.rootHash())
}

func TestPBinOracleRejectsInvalidInsert(t *testing.T) {
	t.Parallel()

	key := pbinOracleCorpusSingleKey().entries[0].key

	t.Run("value must be 32 bytes", func(t *testing.T) {
		t.Parallel()
		var tree pbinOracleTree
		require.Panics(t, func() { tree.insert(key, make([]byte, 31)) })
	})
	t.Run("key must be non-empty", func(t *testing.T) {
		t.Parallel()
		var tree pbinOracleTree
		require.Panics(t, func() { tree.insert(nil, pbinOracleValue(0)) })
	})
	t.Run("key must fit MAX_KEY_LENGTH", func(t *testing.T) {
		t.Parallel()
		var tree pbinOracleTree
		require.Panics(t, func() { tree.insert(make([]byte, pbinOracleMaxKeyLength+1), pbinOracleValue(0)) })
	})
	t.Run("a key that is a prefix of another is rejected", func(t *testing.T) {
		t.Parallel()
		var tree pbinOracleTree
		tree.insert(key, pbinOracleValue(0))
		require.Panics(t, func() { tree.insert(key[:8], pbinOracleValue(1)) })
	})
	t.Run("a key extending another is rejected", func(t *testing.T) {
		t.Parallel()
		var tree pbinOracleTree
		tree.insert(key[:8], pbinOracleValue(0))
		require.Panics(t, func() { tree.insert(key, pbinOracleValue(1)) })
	})
}

// Every corpus must satisfy the prefix-freedom insert asserts, so that a later
// differential failure is a tree bug and not a malformed corpus.
func TestPBinOracleCorporaArePrefixFree(t *testing.T) {
	t.Parallel()

	for _, corpus := range pbinOracleCorpora() {
		t.Run(corpus.name, func(t *testing.T) {
			t.Parallel()
			for i, a := range corpus.entries {
				require.Contains(t, []int{pbinAccountKeyLength, pbinStorageKeyLength}, len(a.key),
					"key %d has no zone-fixed length", i)
				require.Len(t, a.value, pbinValueLength)
				for j, b := range corpus.entries {
					if i == j {
						continue
					}
					require.False(t, bytes.HasPrefix(b.key, a.key),
						"key %d is a prefix of key %d", i, j)
				}
			}
		})
	}
}

// The property that makes the oracle usable as ground truth: the root depends
// on the key/value set, not on the order entries arrive in.
func TestPBinOraclePermutationIndependence(t *testing.T) {
	t.Parallel()

	for _, corpus := range pbinOracleCorpora() {
		t.Run(corpus.name, func(t *testing.T) {
			t.Parallel()
			want := pbinOracleRoot(corpus.entries)
			for name, order := range pbinOracleOrderings(corpus.entries) {
				require.Equal(t, want, pbinOracleRoot(order), "ordering %s", name)
			}
		})
	}
}

// The mined cluster has to really share a deep prefix — otherwise the corpus
// never exercises a split far from the root.
func TestPBinOracleDeepSharedPrefixCorpus(t *testing.T) {
	t.Parallel()

	corpus := pbinOracleCorpusDeepSharedPrefix()
	require.GreaterOrEqual(t, len(corpus.entries), 4)

	first := corpus.entries[0].key
	for _, e := range corpus.entries[1:] {
		require.GreaterOrEqual(t, pbinOracleSharedBits(first, e.key), pbinOracleMinedPrefixBits)
	}

	var tree pbinOracleTree
	for _, e := range corpus.entries {
		tree.insert(e.key, e.value)
	}
	root, ok := tree.root.(*pbinOracleBranch)
	require.True(t, ok)
	require.GreaterOrEqual(t, len(root.prefix), pbinOracleMinedPrefixBits-1)
}

// One account's storage-zone keys must land under a shared stem: they agree on
// the 8+256 zone+stem bits.
func TestPBinOracleStemSharedCorpus(t *testing.T) {
	t.Parallel()

	corpus := pbinOracleCorpusOneAccount()
	var storage [][]byte
	for _, e := range corpus.entries {
		if len(e.key) == pbinStorageKeyLength {
			storage = append(storage, e.key)
		}
	}
	require.GreaterOrEqual(t, len(storage), 2)
	for _, k := range storage[1:] {
		require.GreaterOrEqual(t, pbinOracleSharedBits(storage[0], k), 8+256)
	}
}

func pbinOracleOrderings(entries []pbinOracleEntry) map[string][]pbinOracleEntry {
	byKeyAsc := slices.Clone(entries)
	slices.SortFunc(byKeyAsc, func(a, b pbinOracleEntry) int { return bytes.Compare(a.key, b.key) })
	byKeyDesc := slices.Clone(byKeyAsc)
	slices.Reverse(byKeyDesc)
	reversed := slices.Clone(entries)
	slices.Reverse(reversed)

	shuffled := slices.Clone(entries)
	rnd := rand.New(rand.NewSource(0x8297))
	rnd.Shuffle(len(shuffled), func(i, j int) { shuffled[i], shuffled[j] = shuffled[j], shuffled[i] })

	return map[string][]pbinOracleEntry{
		"reversed":       reversed,
		"key ascending":  byKeyAsc,
		"key descending": byKeyDesc,
		"shuffled":       shuffled,
	}
}
