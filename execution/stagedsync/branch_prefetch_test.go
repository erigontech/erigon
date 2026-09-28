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

package stagedsync

import (
	"bytes"
	"fmt"
	"hash/maphash"
	"runtime/debug"
	"sort"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/erigontech/erigon/db/kv"
	"github.com/erigontech/erigon/execution/commitment"
	"github.com/erigontech/erigon/execution/commitment/eip8297"
	pbt "github.com/erigontech/erigon/execution/commitment/v3/pbt"
)

func TestBranchPrefetchRejectsChangedRecordBytes(t *testing.T) {
	p := &branchPrefetcher{seed: maphash.MakeSeed()}
	for i := range p.shards {
		p.shards[i].records = make(map[string]prefetchedRecord)
	}
	key := []byte{0x08}
	data := []byte{0x01, 0x02}
	p.shard(key).records[prefetchRecordKey(kv.CommitmentDomain, key)] = prefetchedRecord{
		data: bytes.Clone(data),
		refs: &commitment.LeafRefs{Mask: 1, Refs: make([][32]byte, 1)},
	}
	changed := bytes.Clone(data)
	changed[0]++
	require.Nil(t, p.leafRefs(key, changed))
}

func TestBranchPrefetchBudgetChargesCompleteLeafRefs(t *testing.T) {
	p := newTestBranchPrefetcher()
	ops := make([]pbt.Op, 16)
	for slot := range ops {
		ops[slot] = pbt.Op{Key: branchPrefetchCodeKey(byte(slot<<4), 0, byte(slot+1)), Value: branchPrefetchValue(byte(slot + 1))}
	}
	sort.Slice(ops, func(i, j int) bool { return bytes.Compare(ops[i].Key, ops[j].Key) < 0 })
	ctx := newBranchPrefetchTrieContext()
	_, err := pbt.NewTrie(ctx).Process(ops)
	require.NoError(t, err)
	var key []byte
	var data []byte
	var refs *commitment.LeafRefs
	for rawKey, rawData := range ctx.records {
		candidate := pbt.ComputeLeafRefs([]byte(rawKey), rawData)
		if candidate != nil && len(candidate.PBinInternal) != 0 {
			key, data, refs = []byte(rawKey), rawData, candidate
			break
		}
	}
	require.NotEmpty(t, key)
	require.NotNil(t, refs)
	p.putDomain(kv.CommitmentDomain, key, data, 1)
	want := int64(len(key) + len(data) + 32*len(refs.Refs))
	for _, prefix := range refs.Prefixes {
		want += int64(len(prefix))
	}
	for i := range refs.PBinInternal {
		ref := &refs.PBinInternal[i]
		want += int64(2 + 2 + 2 + 32 + 32 + 32 + len(ref.Prefix))
	}
	require.Equal(t, want, p.bytes.Load())
}

func TestBranchPrefetchFoldRejectsStaleRecordRefs(t *testing.T) {
	ops := make([]pbt.Op, 32)
	for slot := range 16 {
		ops[2*slot] = pbt.Op{Key: branchPrefetchCodeKey(byte(slot<<4), 0, 1), Value: branchPrefetchValue(1)}
		ops[2*slot+1] = pbt.Op{Key: branchPrefetchCodeKey(byte(slot<<4), 0, 2), Value: branchPrefetchValue(2)}
	}
	sort.Slice(ops, func(i, j int) bool { return bytes.Compare(ops[i].Key, ops[j].Key) < 0 })
	plain := newBranchPrefetchTrieContext()
	cached := newBranchPrefetchTrieContext()
	plainTrie := pbt.NewTrie(plain)
	cachedTrie := pbt.NewTrie(cached)
	_, err := plainTrie.Process(ops)
	require.NoError(t, err)
	cached.records = cloneBranchPrefetchRecords(plain.records)
	p := newTestBranchPrefetcher()
	for key, data := range cached.records {
		p.putDomain(kv.CommitmentDomain, []byte(key), data, 1)
	}
	cached.prefetcher = p
	insert := pbt.Op{Key: branchPrefetchCodeKey(1, 0, 3), Value: branchPrefetchValue(3)}
	update := pbt.Op{Key: branchPrefetchCodeKey(0xf0, 0, 1), Value: branchPrefetchValue(9)}
	_, err = plainTrie.Process([]pbt.Op{insert})
	require.NoError(t, err)
	_, err = cachedTrie.Process([]pbt.Op{insert})
	require.NoError(t, err)
	plainRoot, err := plainTrie.Process([]pbt.Op{update})
	require.NoError(t, err)
	cachedRoot, err := cachedTrie.Process([]pbt.Op{update})
	require.NoError(t, err)
	require.Equal(t, plainRoot, cachedRoot)
	require.Equal(t, plain.records, cached.records)
}

func newTestBranchPrefetcher() *branchPrefetcher {
	p := &branchPrefetcher{seed: maphash.MakeSeed(), bin: map[kv.Domain]bool{kv.CommitmentDomain: true}}
	for i := range p.shards {
		p.shards[i].records = make(map[string]prefetchedRecord)
	}
	return p
}

type branchPrefetchTrieContext struct {
	records    map[string][]byte
	prefetcher *branchPrefetcher
}

func newBranchPrefetchTrieContext() *branchPrefetchTrieContext {
	return &branchPrefetchTrieContext{records: make(map[string][]byte)}
}

func (c *branchPrefetchTrieContext) Branch(key []byte) ([]byte, kv.Step, error) {
	return bytes.Clone(c.records[string(key)]), 0, nil
}

func (c *branchPrefetchTrieContext) PutBranch(key, data, prev []byte) error {
	if !bytes.Equal(c.records[string(key)], prev) {
		return fmt.Errorf("previous record mismatch for %x", key)
	}
	if len(data) == 0 {
		delete(c.records, string(key))
	} else {
		c.records[string(key)] = bytes.Clone(data)
	}
	return nil
}

func (c *branchPrefetchTrieContext) Account([]byte) (*commitment.Update, error) {
	return nil, fmt.Errorf("unexpected account read")
}

func (c *branchPrefetchTrieContext) Storage([]byte) (*commitment.Update, error) {
	return nil, fmt.Errorf("unexpected storage read")
}

func (c *branchPrefetchTrieContext) LeafRefs(key, data []byte) *commitment.LeafRefs {
	if c.prefetcher == nil {
		return nil
	}
	return c.prefetcher.leafRefsDomain(kv.CommitmentDomain, key, data)
}

func cloneBranchPrefetchRecords(records map[string][]byte) map[string][]byte {
	clone := make(map[string][]byte, len(records))
	for key, data := range records {
		clone[key] = bytes.Clone(data)
	}
	return clone
}

func branchPrefetchCodeKey(first, second, seed byte) []byte {
	key := make([]byte, eip8297.CodeKeyLength)
	key[0] = eip8297.CodeZone
	key[1] = first
	key[2] = second
	key[len(key)-1] = seed
	return key
}

func branchPrefetchValue(seed byte) [eip8297.ValueLength]byte {
	var value [eip8297.ValueLength]byte
	value[len(value)-1] = seed
	return value
}

func TestPrefetchedBranchesYieldToMemBatch(t *testing.T) {
	_, tx, doms := setupStepTest(t)
	p := &branchPrefetcher{seed: maphash.MakeSeed()}
	for i := range p.shards {
		p.shards[i].records = make(map[string]prefetchedRecord)
	}
	r := &asOfStateReader{sd: doms, roTx: tx, commitmentDomain: kv.CommitmentDomain, prefetched: p}

	flushed, untouched, absent := []byte{0x40, 0x12, 0x02}, []byte{0x40, 0x34, 0x02}, []byte{0x40, 0x56, 0x02}
	p.put(flushed, []byte("prefetched-flushed"), 3)
	p.put(untouched, []byte("prefetched-untouched"), 3)
	require.NoError(t, doms.DomainPut(kv.CommitmentDomain, tx, flushed, []byte("mem-flushed"), 5, nil))

	got, _, err := r.Read(kv.CommitmentDomain, flushed, 16)
	require.NoError(t, err)
	require.Equal(t, []byte("mem-flushed"), got)

	got, step, err := r.Read(kv.CommitmentDomain, untouched, 16)
	require.NoError(t, err)
	require.Equal(t, []byte("prefetched-untouched"), got)
	require.Equal(t, kv.Step(3), step)

	got, _, err = r.Read(kv.CommitmentDomain, absent, 16)
	require.NoError(t, err)
	require.Empty(t, got)
}

func TestBranchPrefetchUsesBinaryTreeKeys(t *testing.T) {
	_, tx, _ := setupStepTest(t)
	p := &branchPrefetcher{
		seed:    maphash.MakeSeed(),
		domains: []kv.Domain{kv.CommitmentDomain},
		bin:     map[kv.Domain]bool{kv.CommitmentDomain: true},
	}
	for i := range p.shards {
		p.shards[i].records = make(map[string]prefetchedRecord)
	}
	address := [20]byte{0x46}
	slot := [32]byte{0x80}
	p.touch(tx, prefetchItem{address: address, plainSlot: slot, storage: true})

	treeKey := eip8297.TreeKeyStorage(address[:], slot[:])
	path := eip8297.PathFromBits(treeKey, 268)
	rowKey, err := pbt.EncodeRowKey(&path)
	require.NoError(t, err)
	_, _, ok := p.getDomain(kv.CommitmentDomain, rowKey)
	require.True(t, ok)
}

func TestRaiseGCPercentRestores(t *testing.T) {
	prev := debug.SetGCPercent(150)
	defer debug.SetGCPercent(prev)
	restore := raiseGCPercent()
	require.Equal(t, computeGCPercent, debug.SetGCPercent(computeGCPercent))
	restore()
	require.Equal(t, 150, debug.SetGCPercent(150))

	debug.SetGCPercent(-1)
	raiseGCPercent()()
	require.Equal(t, -1, debug.SetGCPercent(-1))
}
