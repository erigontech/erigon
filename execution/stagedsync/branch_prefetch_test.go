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
	"hash/maphash"
	"runtime/debug"
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
