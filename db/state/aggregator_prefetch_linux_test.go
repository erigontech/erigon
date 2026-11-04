// Copyright 2022 The Erigon Authors
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
	"encoding/binary"
	"os"
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/require"
	"golang.org/x/sys/unix"

	"github.com/erigontech/erigon/common/log/v3"
	"github.com/erigontech/erigon/common/mmap"
	"github.com/erigontech/erigon/db/kv"
)

// A batch prefetch over cold domain files changes no latest value and leaves no probe on the readers.
func TestPrefetchLatestOverColdFilesKeepsLatestValues(t *testing.T) {
	prev := mmap.BatchPrefetch
	mmap.BatchPrefetch = true
	t.Cleanup(func() { mmap.BatchPrefetch = prev })
	db, d, txs := filledDomain(t, log.New())
	require.NoError(t, db.UpdateNosync(t.Context(), func(tx kv.RwTx) error {
		collateAndMerge(t, tx, d, txs)
		return nil
	}))
	files, err := filepath.Glob(filepath.Join(d.dirs.SnapDomain, "*"))
	require.NoError(t, err)
	require.NotEmpty(t, files)
	for _, p := range files {
		f, err := os.Open(p)
		require.NoError(t, err)
		require.NoError(t, unix.Fadvise(int(f.Fd()), 0, 0, unix.FADV_DONTNEED))
		require.NoError(t, f.Close())
	}

	roTx, err := db.BeginRo(t.Context())
	require.NoError(t, err)
	defer roTx.Rollback()
	keys := make([][]byte, 0, 31)
	for keyNum := uint64(1); keyNum <= 31; keyNum++ {
		keys = append(keys, binary.BigEndian.AppendUint64(nil, keyNum))
	}
	dt := d.beginForTests()
	defer dt.Close()
	at := &AggregatorRoTx{}
	at.d[kv.AccountsDomain] = dt
	at.PrefetchLatest(t.Context(), kv.AccountsDomain, keys)
	require.Nil(t, dt.probe)
	for _, r := range dt.dataReaders {
		if r != nil {
			require.Nil(t, r.Probe())
		}
	}

	ref := d.beginForTests()
	defer ref.Close()
	for _, k := range keys {
		want, _, wantOK, err := ref.GetLatest(k, roTx)
		require.NoError(t, err)
		got, _, gotOK, err := dt.GetLatest(k, roTx)
		require.NoError(t, err)
		require.Equal(t, wantOK, gotOK, "%x", k)
		require.Equal(t, want, got, "%x", k)
	}
}
