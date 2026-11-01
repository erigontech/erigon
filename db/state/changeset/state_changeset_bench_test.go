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
	"fmt"
	"testing"

	"github.com/erigontech/erigon/common"
	"github.com/erigontech/erigon/common/log/v3"
	"github.com/erigontech/erigon/db/datadir"
	"github.com/erigontech/erigon/db/kv"
	"github.com/erigontech/erigon/db/kv/dbcfg"
	"github.com/erigontech/erigon/db/kv/mdbx"
	"github.com/erigontech/erigon/db/kv/mdbx/mdbxtest"
	"github.com/erigontech/erigon/db/state/changeset"
	"github.com/erigontech/erigon/node/ethconfig"
)

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
