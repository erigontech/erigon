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
	"encoding/binary"
	"fmt"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/erigontech/erigon/common"
	"github.com/erigontech/erigon/common/log/v3"
	"github.com/erigontech/erigon/db/datadir"
	"github.com/erigontech/erigon/db/kv"
	"github.com/erigontech/erigon/db/kv/dbcfg"
	"github.com/erigontech/erigon/db/kv/dbutils"
	"github.com/erigontech/erigon/db/kv/mdbx"
	"github.com/erigontech/erigon/db/kv/mdbx/mdbxtest"
	"github.com/erigontech/erigon/db/state/changeset"
	"github.com/erigontech/erigon/node/ethconfig"
)

func TestNoOverflowPages(t *testing.T) {
	dirs := datadir.New(t.TempDir())
	db := mdbxtest.InMem(t, mdbx.New(dbcfg.ChainDB, log.Root()), dirs.Chaindata).PageSize(ethconfig.DefaultChainDBPageSize).MustOpen()
	t.Cleanup(db.Close)

	ctx := t.Context()
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
	require.Equal(t, []byte{0, 1, 0, 0, 0, 0}, serialized) // version [0,1] + count (4 bytes, zero entries)
	deserialized := changeset.DeserializeDiffSet(serialized)
	require.Empty(t, deserialized)
}

func TestDeserializeOldFormatEmpty(t *testing.T) {
	t.Parallel()

	// Old empty format: dictLen=0, diffSetLen=0
	oldEmpty := []byte{0, 0, 0, 0, 0}
	deserialized := changeset.DeserializeDiffSet(oldEmpty)
	require.Empty(t, deserialized)
}

func TestDeserializeOldFormatNonEmpty(t *testing.T) {
	t.Parallel()

	// Construct old dictionary-based format bytes:
	// dictLen(1) = 1
	// dict entry: 8 bytes step + 1 byte (unused) = 9 bytes
	// diffSetLen(4) = 2
	// entry 1: keyLen(4) + key + valLen(4) + val + dictIdx(1)
	// entry 2: keyLen(4) + key + valLen(4=0) + dictIdx(1) (valueLen=0 → nil)

	var buf []byte
	// dictLen = 1
	buf = append(buf, 1)
	// dict entry: step=42 (8 bytes big-endian) + 1 byte unused
	step := make([]byte, 8)
	binary.BigEndian.PutUint64(step, 42)
	buf = append(buf, step...)
	buf = append(buf, 0) // unused byte

	// diffSetLen = 2
	diffSetLen := make([]byte, 4)
	binary.BigEndian.PutUint32(diffSetLen, 2)
	buf = append(buf, diffSetLen...)

	// entry 1: key="abc", value="xyz", dictIdx=0
	keyLen := make([]byte, 4)
	binary.BigEndian.PutUint32(keyLen, 3)
	buf = append(buf, keyLen...)
	buf = append(buf, "abc"...)
	valLen := make([]byte, 4)
	binary.BigEndian.PutUint32(valLen, 3)
	buf = append(buf, valLen...)
	buf = append(buf, "xyz"...)
	buf = append(buf, 0) // dictIdx

	// entry 2: key="def", valueLen=0 (→ nil), dictIdx=0
	binary.BigEndian.PutUint32(keyLen, 3)
	buf = append(buf, keyLen...)
	buf = append(buf, "def"...)
	binary.BigEndian.PutUint32(valLen, 0)
	buf = append(buf, valLen...)
	buf = append(buf, 0) // dictIdx

	deserialized := changeset.DeserializeDiffSet(buf)
	require.Len(t, deserialized, 2)
	require.Equal(t, "abc", deserialized[0].Key)
	require.Equal(t, []byte("xyz"), deserialized[0].Value)
	require.Equal(t, "def", deserialized[1].Key)
	require.Nil(t, deserialized[1].Value)
}

func TestDeserializeNilValue(t *testing.T) {
	t.Parallel()

	d := []kv.DomainEntryDiff{
		{Key: "key1_padding", Value: []byte("value1")},
		{Key: "key2_padding", Value: nil},
		{Key: "key3_padding", Value: []byte{}},
	}

	serialized := changeset.SerializeDiffSet(d, nil)
	deserialized := changeset.DeserializeDiffSet(serialized)

	require.Len(t, deserialized, 3)
	require.Equal(t, "key1_padding", deserialized[0].Key)
	require.Equal(t, []byte("value1"), deserialized[0].Value)
	require.Equal(t, "key2_padding", deserialized[1].Key)
	require.Nil(t, deserialized[1].Value)
	require.Equal(t, "key3_padding", deserialized[2].Key)
	require.Equal(t, []byte{}, deserialized[2].Value)
}

func TestStateChangeSetFramingRoundTrip(t *testing.T) {
	db := newChangesetTestDB(t)
	blockHash := common.Hash{1}
	diffSet := &changeset.StateChangeSet{}
	diffSet.Diffs[kv.AccountsDomain].DomainUpdate([]byte("account"), kv.Step(3), []byte("previous"))

	err := db.Update(t.Context(), func(tx kv.RwTx) error {
		return changeset.WriteDiffSet(tx, 1, blockHash, diffSet)
	})
	require.NoError(t, err)

	tx, err := db.BeginRo(t.Context())
	require.NoError(t, err)
	defer tx.Rollback()

	diffs, found, err := changeset.ReadDiffSet(tx, 1, blockHash)
	require.NoError(t, err)
	require.True(t, found)
	require.Len(t, diffs[kv.AccountsDomain], 1)
	require.Equal(t, []byte("previous"), diffs[kv.AccountsDomain][0].Value)
}

func TestStateChangeSetFramingShortDomainCount(t *testing.T) {
	db := newChangesetTestDB(t)
	blockHash := common.Hash{2}
	payload := frameDiffDomains(
		changeset.SerializeDiffSet([]kv.DomainEntryDiff{{Key: "account", Value: []byte("value")}}, nil),
		changeset.SerializeDiffSet(nil, nil),
	)
	payload = append([]byte{1, 2}, payload...)
	writeRawDiffSet(t, db, 2, blockHash, payload)

	tx, err := db.BeginRo(t.Context())
	require.NoError(t, err)
	defer tx.Rollback()

	diffs, found, err := changeset.ReadDiffSet(tx, 2, blockHash)
	require.NoError(t, err)
	require.True(t, found)
	require.Len(t, diffs[0], 1)
	require.Nil(t, diffs[2])
	require.Nil(t, diffs[kv.DomainLen-1])
}

func TestStateChangeSetFramingLegacy(t *testing.T) {
	db := newChangesetTestDB(t)
	blockHash := common.Hash{3}
	domains := make([][]byte, int(kv.DomainLen))
	for i := range domains {
		domains[i] = changeset.SerializeDiffSet(nil, nil)
	}
	domains[kv.CommitmentDomain] = changeset.SerializeDiffSet([]kv.DomainEntryDiff{{Key: "state", Value: []byte("root")}}, nil)
	writeRawDiffSet(t, db, 3, blockHash, frameDiffDomains(domains...))

	tx, err := db.BeginRo(t.Context())
	require.NoError(t, err)
	defer tx.Rollback()

	diffs, found, err := changeset.ReadDiffSet(tx, 3, blockHash)
	require.NoError(t, err)
	require.True(t, found)
	require.Len(t, diffs[kv.CommitmentDomain], 1)
	require.Equal(t, []byte("root"), diffs[kv.CommitmentDomain][0].Value)
}

func TestStateChangeSetFramingTruncated(t *testing.T) {
	db := newChangesetTestDB(t)
	blockHash := common.Hash{4}
	writeRawDiffSet(t, db, 4, blockHash, []byte{1, byte(kv.DomainLen), 0, 0, 0, 4})

	tx, err := db.BeginRo(t.Context())
	require.NoError(t, err)
	defer tx.Rollback()

	_, found, err := changeset.ReadDiffSet(tx, 4, blockHash)
	require.Error(t, err)
	require.False(t, found)
}

func newChangesetTestDB(tb testing.TB) kv.RwDB {
	tb.Helper()
	dirs := datadir.New(tb.TempDir())
	db := mdbxtest.InMem(tb, mdbx.New(dbcfg.ChainDB, log.Root()), dirs.Chaindata).PageSize(ethconfig.DefaultChainDBPageSize).MustOpen()
	tb.Cleanup(db.Close)
	return db
}

func writeRawDiffSet(tb testing.TB, db kv.RwDB, blockNumber uint64, blockHash common.Hash, payload []byte) {
	tb.Helper()
	err := db.Update(tb.Context(), func(tx kv.RwTx) error {
		blockKey := dbutils.BlockBodyKey(blockNumber, blockHash)
		if err := tx.Put(kv.ChangeSets3, blockKey, dbutils.EncodeBlockNumber(1)); err != nil {
			return err
		}
		chunkKey := make([]byte, len(blockKey)+8)
		copy(chunkKey, blockKey)
		return tx.Put(kv.ChangeSets3, chunkKey, payload)
	})
	require.NoError(tb, err)
}

func frameDiffDomains(domains ...[]byte) []byte {
	var payload []byte
	for _, domain := range domains {
		payload = binary.BigEndian.AppendUint32(payload, uint32(len(domain)))
		payload = append(payload, domain...)
	}
	return payload
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
	for i := range 1000 {
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
	db := mdbxtest.InMem(b, mdbx.New(dbcfg.ChainDB, log.Root()), dirs.Chaindata).PageSize(ethconfig.DefaultChainDBPageSize).MustOpen()
	b.Cleanup(db.Close)

	// Create a realistic StateChangeSet
	diffSet := createTestDiffSet(b, 10, 100, 10, 100)

	blockHash := common.Hash{0x01, 0x02, 0x03}

	b.ResetTimer()
	b.ReportAllocs()

	for i := 0; b.Loop(); i++ {
		ctx := b.Context()
		tx, err := db.BeginRw(ctx)
		if err != nil {
			b.Fatal(err)
		}
		defer tx.Rollback() //nolint:gocritic
		if err := changeset.WriteDiffSet(tx, uint64(i), blockHash, diffSet); err != nil {
			tx.Rollback()
			b.Fatal(err)
		}
		tx.Rollback() // Don't commit to avoid filling up the DB
	}
}

func BenchmarkWriteDiffSetLarge(b *testing.B) {
	dirs := datadir.New(b.TempDir())
	db := mdbxtest.InMem(b, mdbx.New(dbcfg.ChainDB, log.Root()), dirs.Chaindata).PageSize(ethconfig.DefaultChainDBPageSize).MustOpen()
	b.Cleanup(db.Close)

	// Create a large StateChangeSet (simulating a heavy block)
	diffSet := createTestDiffSet(b, 1000, 5000, 10, 10_000)

	blockHash := common.Hash{0x01, 0x02, 0x03}

	b.ResetTimer()
	b.ReportAllocs()

	for i := 0; b.Loop(); i++ {
		ctx := b.Context()
		tx, err := db.BeginRw(ctx)
		if err != nil {
			b.Fatal(err)
		}
		defer tx.Rollback() //nolint:gocritic
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
	for i := range numAccounts {
		key := make([]byte, 20)
		key[0] = byte(i >> 8)
		key[1] = byte(i)
		value := make([]byte, 70) // typical account encoding size
		diffSet.Diffs[kv.AccountsDomain].DomainUpdate(key, kv.Step(100), value)
	}

	// Storage domain - 20 byte address + 32 byte location
	for i := range numStorage {
		key := make([]byte, 52)
		key[0] = byte(i >> 16)
		key[1] = byte(i >> 8)
		key[2] = byte(i)
		value := make([]byte, 32) // storage value
		diffSet.Diffs[kv.StorageDomain].DomainUpdate(key, kv.Step(100), value)
	}

	// Code domain - 20 byte address with code hash
	for i := range numCode {
		key := make([]byte, 20)
		key[0] = byte(i >> 8)
		key[1] = byte(i)
		value := make([]byte, 32) // code hash
		diffSet.Diffs[kv.CodeDomain].DomainUpdate(key, kv.Step(100), value)
	}

	// Commitment domain - variable key with trie node data
	for i := range numCommitment {
		key := make([]byte, 8+i%32) // variable length keys
		key[0] = byte(i >> 8)
		key[1] = byte(i)
		value := make([]byte, 64+i%64) // variable length values
		diffSet.Diffs[kv.CommitmentDomain].DomainUpdate(key, kv.Step(100), value)
	}

	return diffSet
}
