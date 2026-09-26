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

package mdbx_test

import (
	"context"
	"testing"

	"github.com/c2h5oh/datasize"
	"github.com/stretchr/testify/require"
	"golang.org/x/sys/unix"

	"github.com/erigontech/erigon/common/log/v3"
	"github.com/erigontech/erigon/db/kv"
	"github.com/erigontech/erigon/db/kv/dbcfg"
	"github.com/erigontech/erigon/db/kv/mdbx"
)

func TestValueFileRegionResolvesOverflowPages(t *testing.T) {
	path := t.TempDir()
	logger := log.New()
	table := "T"
	db := mdbx.New(dbcfg.ChainDB, logger).Path(path).
		WithTableCfg(func(kv.TableCfg) kv.TableCfg { return kv.TableCfg{table: kv.TableCfgItem{}} }).
		MapSize(1 * datasize.GB).MustOpen()
	defer db.Close()

	val := make([]byte, 64*1024)
	for i := range val {
		val[i] = byte(i*7 + 3)
	}
	require.NoError(t, db.Update(context.Background(), func(tx kv.RwTx) error {
		return tx.Put(table, []byte("k"), val)
	}))

	require.NoError(t, db.View(context.Background(), func(tx kv.Tx) error {
		got, err := tx.GetOne(table, []byte("k"))
		require.NoError(t, err)
		require.Len(t, got, len(val))

		fd, off, ok := db.(*mdbx.MdbxKV).ValueFileRegion(got)
		require.True(t, ok, "a 64KiB value must live on overflow pages inside the data mapping")

		onDisk := make([]byte, len(got))
		_, err = unix.Pread(int(fd), onDisk, off)
		require.NoError(t, err)
		require.Equal(t, got, onDisk, "computed file offset must contain the value bytes")
		return nil
	}))
}
