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

//go:build linux

package state

import (
	"bytes"
	"encoding/binary"
	"fmt"
	"math/rand/v2"
	"os"
	"path/filepath"
	"testing"

	"github.com/c2h5oh/datasize"
	mdbxgo "github.com/erigontech/mdbx-go/mdbx"
	"github.com/stretchr/testify/require"
	"golang.org/x/sys/unix"

	"github.com/erigontech/erigon/common/log/v3"
	"github.com/erigontech/erigon/db/config3"
	"github.com/erigontech/erigon/db/datadir"
	"github.com/erigontech/erigon/db/kv"
	"github.com/erigontech/erigon/db/kv/dbcfg"
	"github.com/erigontech/erigon/db/kv/mdbx"
	"github.com/erigontech/erigon/db/state/statecfg"
)

var domainCodeBenchResult []byte

func BenchmarkDomainGetLatestCode(b *testing.B) {
	for _, size := range []int{1024, 4096, 8192, 24576, 65536} {
		for _, cold := range []bool{false, true} {
			b.Run(fmt.Sprintf("size=%d/cold=%t", size, cold), func(b *testing.B) {
				benchmarkDomainGetLatestCode(b, size, cold)
			})
		}
	}
}

func benchmarkDomainGetLatestCode(b *testing.B, size int, cold bool) {
	const count = 256
	dirs := datadir.New(b.TempDir())
	logger := log.New()
	cfg := statecfg.Schema.CodeDomain
	d, err := NewDomain(cfg, 16, config3.DefaultStepsInFrozenFile, dirs, logger)
	require.NoError(b, err)
	defer d.Close()
	// Large databases disable automatic readahead when they exceed available RAM.
	opts := mdbx.New(dbcfg.ChainDB, logger).
		Path(dirs.Chaindata).
		MapSize(128 * datasize.MB).
		GrowthStep(16 * datasize.MB).
		AddFlags(mdbxgo.NoReadahead)
	db := opts.MustOpen()
	defer func() { db.Close() }()
	keys := make([][20]byte, count)
	code := make([]byte, size)
	for i := range code {
		code[i] = byte(i)
	}
	require.NoError(b, db.Update(b.Context(), func(tx kv.RwTx) error {
		for i := range keys {
			binary.BigEndian.PutUint64(keys[i][:], uint64(i))
			key := make([]byte, len(keys[i])+8)
			copy(key, keys[i][:])
			binary.BigEndian.PutUint64(key[len(keys[i]):], ^uint64(0))
			if err := tx.Put(cfg.ValuesTable, key, code); err != nil {
				return err
			}
		}
		return nil
	}))
	rng := rand.New(rand.NewPCG(1, 2))
	rng.Shuffle(len(keys), func(i, j int) { keys[i], keys[j] = keys[j], keys[i] })
	f, err := os.Open(filepath.Join(dirs.Chaindata, "mdbx.dat"))
	require.NoError(b, err)
	defer f.Close()
	b.SetBytes(int64(count * size))
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		b.StopTimer()
		if cold {
			db.Close()
			require.NoError(b, unix.Fadvise(int(f.Fd()), 0, 0, unix.FADV_DONTNEED))
			db = opts.MustOpen()
		}
		func() {
			tx, err := db.BeginRo(b.Context())
			require.NoError(b, err)
			defer tx.Rollback()
			dt := d.beginForTests()
			defer dt.Close()
			b.StartTimer()
			for j := range keys {
				value, _, found, err := dt.GetLatest(keys[j][:], tx)
				if err != nil || !found {
					b.Fatalf("code lookup: found=%t err=%v", found, err)
				}
				domainCodeBenchResult = bytes.Clone(value)
			}
			b.StopTimer()
			require.Equal(b, code, domainCodeBenchResult)
		}()
		b.StartTimer()
	}
}
