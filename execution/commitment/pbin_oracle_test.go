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
	"fmt"
	"math/rand"
	"slices"
	"sync"

	"github.com/erigontech/erigon/common"
	"github.com/erigontech/erigon/common/length"
	"github.com/erigontech/erigon/execution/commitment/eip8297"
)

const (
	pbinOracleMaxKeyLength    = 8192
	pbinOracleMinedPrefixBits = 20
	pbinOracleMinedCluster    = 4
	pbinOracleLeafTag         = eip8297.LeafTag
	pbinOracleBranchTag       = eip8297.BranchTag
)

type pbinOracleNode = eip8297.Node
type pbinOracleLeaf = eip8297.Leaf
type pbinOracleBranch = eip8297.Branch

type pbinOracleTree struct {
	root pbinOracleNode
}

func (t *pbinOracleTree) insert(key, value []byte) {
	tree := eip8297.Tree{Root: t.root}
	tree.Insert(key, value)
	t.root = tree.Root
}

func pbinOracleBytesToBits(data []byte) []byte {
	return eip8297.BitsFromBytes(data)
}

func pbinOracleEncodeBitPrefix(prefix []byte) []byte {
	return eip8297.EncodeBitPrefix(prefix)
}

func pbinOracleMerkelize(node pbinOracleNode) [32]byte {
	return [32]byte(eip8297.MerkelizeWith(node, nil))
}

func pbinOracleMerkelizeWith(node pbinOracleNode, sum func([]byte) [32]byte) [32]byte {
	var hash eip8297.HashFn
	if sum != nil {
		hash = func(preimage []byte) common.Hash { return common.Hash(sum(preimage)) }
	}
	return [32]byte(eip8297.MerkelizeWith(node, hash))
}

func (t *pbinOracleTree) rootHash() [32]byte {
	return pbinOracleMerkelize(t.root)
}

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
	for _, entry := range entries {
		tree.insert(entry.key, entry.value)
	}
	return tree.rootHash()
}

func pbinOracleSharedBits(a, b []byte) int {
	aBits, bBits := pbinOracleBytesToBits(a), pbinOracleBytesToBits(b)
	shared := 0
	for shared < len(aBits) && shared < len(bBits) && aBits[shared] == bBits[shared] {
		shared++
	}
	return shared
}

func pbinOracleValue(seed uint64) []byte {
	value := make([]byte, pbinValueLength)
	for i := range 8 {
		value[i] = 0xA5
		value[24+i] = byte(seed >> (56 - 8*i))
	}
	return value
}

func pbinOracleAddr(seed uint64) []byte {
	address := make([]byte, length.Addr)
	binary.BigEndian.PutUint64(address[12:], seed)
	return address
}

func pbinOracleSlot(seed uint64) []byte {
	slot := make([]byte, length.Hash)
	binary.BigEndian.PutUint64(slot[24:], seed)
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
		entries: []pbinOracleEntry{{
			key: pbinTreeKeyAccount(pbinOracleAddr(1), pbinBasicDataLeafKey), value: pbinOracleValue(1),
		}},
	}
}

func pbinOracleCorpusSplitAtBit0() pbinOracleCorpus {
	address := pbinOracleAddr(2)
	return pbinOracleCorpus{
		name: "split at bit 0",
		entries: []pbinOracleEntry{
			{key: pbinTreeKeyAccount(address, pbinBasicDataLeafKey), value: pbinOracleValue(1)},
			{key: pbinTreeKeyStorage(address, pbinOracleSlot(1000)), value: pbinOracleValue(2)},
		},
	}
}

func pbinOracleCorpusSplitAtLastBit() pbinOracleCorpus {
	address := pbinOracleAddr(3)
	return pbinOracleCorpus{
		name: "split at bit 527",
		entries: []pbinOracleEntry{
			{key: pbinTreeKeyStorage(address, pbinOracleSlot(256)), value: pbinOracleValue(1)},
			{key: pbinTreeKeyStorage(address, pbinOracleSlot(257)), value: pbinOracleValue(2)},
		},
	}
}

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

func pbinOracleCorpusOneAccount() pbinOracleCorpus {
	address := pbinOracleAddr(4)
	entries := []pbinOracleEntry{
		{key: pbinTreeKeyAccount(address, pbinBasicDataLeafKey), value: pbinOracleValue(1)},
		{key: pbinTreeKeyAccount(address, pbinCodeHashLeafKey), value: pbinOracleValue(2)},
	}
	for i, slot := range []uint64{0, 1, 63, 64, 65, 255, 256, 1000} {
		entries = append(entries, pbinOracleEntry{
			key: pbinTreeKeyStorage(address, pbinOracleSlot(slot)), value: pbinOracleValue(uint64(10 + i)),
		})
	}
	return pbinOracleCorpus{name: "one account", entries: entries}
}

func pbinOracleCorpusDeepSharedPrefix() pbinOracleCorpus {
	entries := make([]pbinOracleEntry, 0, pbinOracleMinedCluster)
	for i, address := range pbinOracleMinedAddrs() {
		entries = append(entries, pbinOracleEntry{
			key: pbinTreeKeyAccount(address, pbinBasicDataLeafKey), value: pbinOracleValue(uint64(i)),
		})
	}
	return pbinOracleCorpus{name: "mined deep shared prefix", entries: entries}
}

var pbinOracleMinedAddrs = sync.OnceValue(func() [][]byte {
	return pbinOracleMineSharedStems(pbinOracleMinedPrefixBits, pbinOracleMinedCluster)
})

func pbinOracleMineSharedStems(shared, count int) [][]byte {
	const limit = 1 << 24
	var target []byte
	found := make([][]byte, 0, count)
	for i := uint64(0); i < limit && len(found) < count; i++ {
		address := pbinOracleAddr(i)
		key := pbinTreeKeyAccount(address, pbinBasicDataLeafKey)
		if target == nil {
			target = key
			found = append(found, address)
			continue
		}
		if pbinOracleSharedBits(target, key) >= shared {
			found = append(found, address)
		}
	}
	if len(found) < count {
		panic(fmt.Sprintf("pbin oracle: found only %d of %d addresses sharing %d bits", len(found), count, shared))
	}
	return found
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
		"reversed": reversed, "key ascending": byKeyAsc, "key descending": byKeyDesc, "shuffled": shuffled,
	}
}
