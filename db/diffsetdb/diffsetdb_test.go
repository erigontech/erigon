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

package diffsetdb_test

import (
	"context"
	"os"
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/erigontech/erigon/common"
	"github.com/erigontech/erigon/db/datadir"
	"github.com/erigontech/erigon/db/diffsetdb"
	"github.com/erigontech/erigon/db/kv"
	"github.com/erigontech/erigon/db/state/changeset"
)

func TestDiffsetDatabaseWriteRead(t *testing.T) {
	dirs := datadir.New(t.TempDir())
	db := diffsetdb.Open(dirs)
	hash := common.HexToHash("0x0102030405060708090a0b0c0d0e0f101112131415161718191a1b1c1d1e1f20")
	diffSet := makeDiffset()

	require.NoError(t, db.WriteDiffSet(10, hash, diffSet))
	got, ok, err := db.ReadDiffSet(10, hash)
	require.NoError(t, err)
	require.True(t, ok)

	require.Equal(t, diffSet.Diffs[kv.AccountsDomain].GetDiffSet(), got[kv.AccountsDomain])
	require.Equal(t, diffSet.Diffs[kv.StorageDomain].GetDiffSet(), got[kv.StorageDomain])
	require.Empty(t, got[kv.CodeDomain])
	require.Empty(t, got[kv.CommitmentDomain])
}

func TestDiffsetDatabaseReadMissing(t *testing.T) {
	dirs := datadir.New(t.TempDir())
	db := diffsetdb.Open(dirs)
	hash := common.HexToHash("0x1111111111111111111111111111111111111111111111111111111111111111")
	_, ok, err := db.ReadDiffSet(1, hash)
	require.NoError(t, err)
	require.False(t, ok)
}

func TestDiffsetDatabaseReadLowestUnwindableBlock(t *testing.T) {
	dirs := datadir.New(t.TempDir())
	db := diffsetdb.Open(dirs)
	diffSet := makeDiffset()

	require.NoError(t, db.WriteDiffSet(7, common.HexToHash("0xaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"), diffSet))
	require.NoError(t, db.WriteDiffSet(3, common.HexToHash("0xbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb"), diffSet))

	junkPath := filepath.Join(db.Dir(), "not-a-diffset")
	require.NoError(t, os.WriteFile(junkPath, []byte("junk"), 0o644))
	badHashPath := filepath.Join(db.Dir(), "4+deadbeef.diffset")
	require.NoError(t, os.WriteFile(badHashPath, []byte("junk"), 0o644))

	minBlock, err := db.ReadLowestUnwindableBlock()
	require.NoError(t, err)
	require.Equal(t, uint64(3), minBlock)
}

func TestDiffsetDatabasePruneBefore(t *testing.T) {
	dirs := datadir.New(t.TempDir())
	db := diffsetdb.Open(dirs)
	diffSet := makeDiffset()

	require.NoError(t, db.WriteDiffSet(1, common.HexToHash("0x1000000000000000000000000000000000000000000000000000000000000000"), diffSet))
	require.NoError(t, db.WriteDiffSet(2, common.HexToHash("0x2000000000000000000000000000000000000000000000000000000000000000"), diffSet))
	require.NoError(t, db.WriteDiffSet(3, common.HexToHash("0x3000000000000000000000000000000000000000000000000000000000000000"), diffSet))

	removed, err := db.PruneBefore(context.Background(), 3, -1, 0)
	require.NoError(t, err)
	require.Equal(t, 2, removed)

	_, ok, err := db.ReadDiffSet(1, common.HexToHash("0x1000000000000000000000000000000000000000000000000000000000000000"))
	require.NoError(t, err)
	require.False(t, ok)

	_, ok, err = db.ReadDiffSet(3, common.HexToHash("0x3000000000000000000000000000000000000000000000000000000000000000"))
	require.NoError(t, err)
	require.True(t, ok)
}

func TestDiffsetDatabaseClear(t *testing.T) {
	dirs := datadir.New(t.TempDir())
	db := diffsetdb.Open(dirs)
	diffSet := makeDiffset()
	hash := common.HexToHash("0x0100000000000000000000000000000000000000000000000000000000000000")

	require.NoError(t, db.WriteDiffSet(5, hash, diffSet))
	require.NoError(t, db.Clear())

	_, ok, err := db.ReadDiffSet(5, hash)
	require.NoError(t, err)
	require.False(t, ok)
}

func TestDiffsetDatabaseWriteNil(t *testing.T) {
	dirs := datadir.New(t.TempDir())
	db := diffsetdb.Open(dirs)
	err := db.WriteDiffSet(1, common.HexToHash("0x9999999999999999999999999999999999999999999999999999999999999999"), nil)
	require.Error(t, err)
}

func TestDiffsetDatabaseReadLowestUnwindableBlockEmptyDir(t *testing.T) {
	dirs := datadir.New(t.TempDir())
	db := diffsetdb.Open(dirs)
	require.NoError(t, db.Clear())

	minBlock, err := db.ReadLowestUnwindableBlock()
	require.NoError(t, err)
	require.Equal(t, uint64(^uint64(0)), minBlock)
}

func TestDiffsetDatabasePruneLimitAndCancelled(t *testing.T) {
	dirs := datadir.New(t.TempDir())
	db := diffsetdb.Open(dirs)
	diffSet := makeDiffset()

	require.NoError(t, db.WriteDiffSet(1, common.HexToHash("0x4000000000000000000000000000000000000000000000000000000000000000"), diffSet))
	require.NoError(t, db.WriteDiffSet(2, common.HexToHash("0x5000000000000000000000000000000000000000000000000000000000000000"), diffSet))

	removed, err := db.PruneBefore(context.Background(), 3, 1, 0)
	require.NoError(t, err)
	require.Equal(t, 1, removed)

	_, ok, err := db.ReadDiffSet(1, common.HexToHash("0x4000000000000000000000000000000000000000000000000000000000000000"))
	require.NoError(t, err)
	require.False(t, ok)

	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	_, err = db.PruneBefore(ctx, 3, -1, 0)
	require.Error(t, err)
}

func TestDiffsetDatabaseNoDatadirErrors(t *testing.T) {
	db := diffsetdb.Open(datadir.Dirs{})
	diffSet := makeDiffset()

	require.Error(t, db.WriteDiffSet(1, common.Hash{}, diffSet))
	_, _, err := db.ReadDiffSet(1, common.Hash{})
	require.Error(t, err)
	_, err = db.ReadLowestUnwindableBlock()
	require.Error(t, err)
	_, err = db.PruneBefore(context.Background(), 1, 1, 0)
	require.Error(t, err)
	require.Error(t, db.Clear())
}

func makeDiffset() *changeset.StateChangeSet {
	var diffSet changeset.StateChangeSet
	diffSet.Diffs[kv.AccountsDomain].DomainUpdate([]byte("acct"), kv.Step(2), []byte("prev-acct"), kv.Step(1))
	diffSet.Diffs[kv.StorageDomain].DomainUpdate([]byte("slot"), kv.Step(3), []byte("prev-slot"), kv.Step(2))
	return &diffSet
}

// makeLargeDiffset creates a diffset with many entries for benchmarking.
func makeLargeDiffset(numEntries int) *changeset.StateChangeSet {
	var diffSet changeset.StateChangeSet
	for i := 0; i < numEntries; i++ {
		key := make([]byte, 20)
		key[0] = byte(i >> 24)
		key[1] = byte(i >> 16)
		key[2] = byte(i >> 8)
		key[3] = byte(i)
		value := make([]byte, 32)
		value[0] = byte(i)
		diffSet.Diffs[kv.AccountsDomain].DomainUpdate(key, kv.Step(uint64(i+2)), value, kv.Step(uint64(i+1)))
	}
	for i := 0; i < numEntries*2; i++ {
		key := make([]byte, 52) // address + slot
		key[0] = byte(i >> 24)
		key[1] = byte(i >> 16)
		key[2] = byte(i >> 8)
		key[3] = byte(i)
		value := make([]byte, 32)
		value[0] = byte(i)
		diffSet.Diffs[kv.StorageDomain].DomainUpdate(key, kv.Step(uint64(i+2)), value, kv.Step(uint64(i+1)))
	}
	return &diffSet
}

func BenchmarkWriteDiffSet(b *testing.B) {
	dirs := datadir.New(b.TempDir())
	db := diffsetdb.Open(dirs)
	diffSet := makeLargeDiffset(100)
	hash := common.HexToHash("0x0102030405060708090a0b0c0d0e0f101112131415161718191a1b1c1d1e1f20")

	b.ResetTimer()
	b.ReportAllocs()
	for i := 0; i < b.N; i++ {
		_ = db.WriteDiffSet(uint64(i), hash, diffSet)
	}
}

func BenchmarkReadDiffSet(b *testing.B) {
	dirs := datadir.New(b.TempDir())
	db := diffsetdb.Open(dirs)
	diffSet := makeLargeDiffset(100)
	hash := common.HexToHash("0x0102030405060708090a0b0c0d0e0f101112131415161718191a1b1c1d1e1f20")

	if err := db.WriteDiffSet(1, hash, diffSet); err != nil {
		b.Fatal(err)
	}

	b.ResetTimer()
	b.ReportAllocs()
	for i := 0; i < b.N; i++ {
		_, _, _ = db.ReadDiffSet(1, hash)
	}
}
