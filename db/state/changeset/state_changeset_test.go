// Copyright 2024 The Erigon Authors
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

package changeset_test

import (
	"context"
	"fmt"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/erigontech/erigon/common"
	"github.com/erigontech/erigon/common/log/v3"
	"github.com/erigontech/erigon/db/datadir"
	"github.com/erigontech/erigon/db/kv"
	"github.com/erigontech/erigon/db/kv/dbcfg"
	"github.com/erigontech/erigon/db/kv/mdbx"
	"github.com/erigontech/erigon/db/state/changeset"
	"github.com/erigontech/erigon/node/ethconfig"
)

func TestNoOverflowPages(t *testing.T) {
	dirs := datadir.New(t.TempDir())
	db := mdbx.New(dbcfg.ChainDB, log.Root()).InMem(t, dirs.Chaindata).PageSize(ethconfig.DefaultChainDBPageSize).MustOpen()
	t.Cleanup(db.Close)

	ctx := context.Background()
	tx, err := db.BeginRw(ctx)
	require.NoError(t, err)
	defer tx.Rollback()
	k, v := make([]byte, changeset.DiffChunkKeyLen), make([]byte, changeset.DiffChunkLen)
	k[0] = 0
	_ = tx.Put(kv.ChangeSets3, k, v)
	k[0] = 1
	_ = tx.Put(kv.ChangeSets3, k, v)
	st, err := tx.(*mdbx.MdbxTx).BucketStat(kv.ChangeSets3)
	require.NoError(t, err)

	// no ofverflow pages: no problems with FreeList maintainance costs
	require.Equal(t, 0, int(st.OverflowPages))
	require.Equal(t, 1, int(st.LeafPages))
	require.Equal(t, 2, int(st.Entries))
}

func TestSerializeDeserializeDiff(t *testing.T) {
	t.Parallel()

	d := []kv.DomainEntryDiff{
		{Key: "key188888888", Value: []byte("value1")},
		{Key: "key288888888", Value: []byte("value2")},
		{Key: "key388888888", Value: []byte("value3")},
		{Key: "key388888888", Value: []byte("value3")},
	}

	serialized := changeset.SerializeDiffSet(d, nil)
	fmt.Println(len(serialized))
	deserialized := changeset.DeserializeDiffSet(serialized)

	require.Equal(t, d, deserialized)
}

func TestSerializeDeserializeDiffEmpty(t *testing.T) {
	t.Parallel()

	var empty []kv.DomainEntryDiff
	serialized := changeset.SerializeDiffSet(empty, nil)
	require.Equal(t, []byte{0, 0, 0, 0}, serialized) // count (4 bytes, zero entries)
	deserialized := changeset.DeserializeDiffSet(serialized)
	require.Empty(t, deserialized)
}

func TestMergeDiffSet(t *testing.T) {
	t.Parallel()

	d1 := []kv.DomainEntryDiff{
		{Key: "key188888888", Value: []byte("value1")},
		{Key: "key288888888", Value: []byte("value2")},
		{Key: "key388888888", Value: []byte("value3")},
	}

	d2 := []kv.DomainEntryDiff{
		{Key: "key188888888", Value: []byte("value5")},
		{Key: "key388888888", Value: []byte("value6")},
		{Key: "key488888888", Value: []byte("value4")},
	}

	merged := changeset.MergeDiffSets(d1, d2)
	require.Len(t, merged, 4)

	require.Equal(t, d2[0], merged[0])
	require.Equal(t, d1[1], merged[1])
	require.Equal(t, d2[1], merged[2])
	require.Equal(t, d2[2], merged[3])
}

func BenchmarkSerializeDiffSet(b *testing.B) {
	// Create a realistic diffSet with varying sizes
	var d []kv.DomainEntryDiff
	for i := 0; i < 1000; i++ {
		key := fmt.Sprintf("key%08d_padding", i)
		value := make([]byte, 32+i%64) // varying value sizes
		d = append(d, kv.DomainEntryDiff{
			Key:   key,
			Value: value,
		})
	}

	out := make([]byte, 0, 128*1024)
	b.ResetTimer()
	b.ReportAllocs()

	for b.Loop() {
		out = changeset.SerializeDiffSet(d, out[:0])
	}
}

func BenchmarkWriteDiffSet(b *testing.B) {
	dirs := datadir.New(b.TempDir())
	db := mdbx.New(dbcfg.ChainDB, log.Root()).InMem(b, dirs.Chaindata).PageSize(ethconfig.DefaultChainDBPageSize).MustOpen()
	b.Cleanup(db.Close)

	// Create a realistic StateChangeSet
	diffSet := createTestDiffSet(b, 10, 100, 10, 100)

	blockHash := common.Hash{0x01, 0x02, 0x03}

	b.ResetTimer()
	b.ReportAllocs()

	for i := 0; b.Loop(); i++ {
		ctx := context.Background()
		tx, err := db.BeginRw(ctx)
		if err != nil {
			b.Fatal(err)
		}
		defer tx.Rollback()
		if err := changeset.WriteDiffSet(tx, uint64(i), blockHash, diffSet); err != nil {
			tx.Rollback()
			b.Fatal(err)
		}
		tx.Rollback() // Don't commit to avoid filling up the DB
	}
}

func BenchmarkWriteDiffSetLarge(b *testing.B) {
	dirs := datadir.New(b.TempDir())
	db := mdbx.New(dbcfg.ChainDB, log.Root()).InMem(b, dirs.Chaindata).PageSize(ethconfig.DefaultChainDBPageSize).MustOpen()
	b.Cleanup(db.Close)

	// Create a large StateChangeSet (simulating a heavy block)
	diffSet := createTestDiffSet(b, 1000, 5000, 10, 10_000)

	blockHash := common.Hash{0x01, 0x02, 0x03}

	b.ResetTimer()
	b.ReportAllocs()

	for i := 0; b.Loop(); i++ {
		ctx := context.Background()
		tx, err := db.BeginRw(ctx)
		if err != nil {
			b.Fatal(err)
		}
		defer tx.Rollback()
		if err := changeset.WriteDiffSet(tx, uint64(i), blockHash, diffSet); err != nil {
			tx.Rollback()
			b.Fatal(err)
		}
		tx.Rollback()
	}
}

// createTestDiffSet creates a StateChangeSet with realistic data
func createTestDiffSet(tb testing.TB, numAccounts, numStorage, numCode, numCommitment int) *changeset.StateChangeSet {
	tb.Helper()

	diffSet := &changeset.StateChangeSet{}

	// Accounts domain - 20 byte addresses with account data
	for i := 0; i < numAccounts; i++ {
		key := make([]byte, 20)
		key[0] = byte(i >> 8)
		key[1] = byte(i)
		value := make([]byte, 70) // typical account encoding size
		diffSet.Diffs[kv.AccountsDomain].DomainUpdate(key, kv.Step(100), value)
	}

	// Storage domain - 20 byte address + 32 byte location
	for i := 0; i < numStorage; i++ {
		key := make([]byte, 52)
		key[0] = byte(i >> 16)
		key[1] = byte(i >> 8)
		key[2] = byte(i)
		value := make([]byte, 32) // storage value
		diffSet.Diffs[kv.StorageDomain].DomainUpdate(key, kv.Step(100), value)
	}

	// Code domain - 20 byte address with code hash
	for i := 0; i < numCode; i++ {
		key := make([]byte, 20)
		key[0] = byte(i >> 8)
		key[1] = byte(i)
		value := make([]byte, 32) // code hash
		diffSet.Diffs[kv.CodeDomain].DomainUpdate(key, kv.Step(100), value)
	}

	// Commitment domain - variable key with trie node data
	for i := 0; i < numCommitment; i++ {
		key := make([]byte, 8+i%32) // variable length keys
		key[0] = byte(i >> 8)
		key[1] = byte(i)
		value := make([]byte, 64+i%64) // variable length values
		diffSet.Diffs[kv.CommitmentDomain].DomainUpdate(key, kv.Step(100), value)
	}

	return diffSet
}
