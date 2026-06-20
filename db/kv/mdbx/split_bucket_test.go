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

package mdbx

import (
	"bytes"
	"encoding/binary"
	"sort"
	"testing"

	"github.com/c2h5oh/datasize"
	"github.com/stretchr/testify/require"

	"github.com/erigontech/erigon/common/dbg"
	"github.com/erigontech/erigon/common/log/v3"
	"github.com/erigontech/erigon/db/kv"
	"github.com/erigontech/erigon/db/kv/dbcfg"
	"github.com/erigontech/erigon/db/kv/order"
)

// Keys sharing a 32-byte prefix and differing only in a 4-byte suffix: the
// clustered shape that byte interpolation over the first 32 bytes can't split.
func clusteredKey(i int) []byte {
	k := make([]byte, 36)
	for j := 0; j < 32; j++ {
		k[j] = 0xCC
	}
	binary.BigEndian.PutUint32(k[32:], uint32(i))
	return k
}

func TestSplitBucketByCount(t *testing.T) {
	const table, n, chunks = "T", 100_000, 16
	db := New(dbcfg.ChainDB, log.New()).InMem(t, t.TempDir()).WithTableCfg(func(_ kv.TableCfg) kv.TableCfg {
		return kv.TableCfg{table: kv.TableCfgItem{}}
	}).MapSize(512 * datasize.MB).MustOpen()
	t.Cleanup(db.Close)

	require.NoError(t, db.Update(t.Context(), func(tx kv.RwTx) error {
		c, err := tx.RwCursor(table)
		require.NoError(t, err)
		defer c.Close()
		for i := 0; i < n; i++ {
			require.NoError(t, c.Append(clusteredKey(i), []byte{1}))
		}
		return nil
	}))

	require.NoError(t, db.View(t.Context(), func(tx kv.Tx) error {
		bounds, err := tx.(kv.BucketSplitter).SplitBucketByCount(table, nil, chunks)
		require.NoError(t, err)

		require.Greater(t, len(bounds), 2, "must split clustered keys into many ranges, not one")
		require.Nil(t, bounds[0])
		require.Nil(t, bounds[len(bounds)-1])
		require.True(t, sort.SliceIsSorted(bounds[1:len(bounds)-1], func(a, b int) bool {
			return bytes.Compare(bounds[1+a], bounds[1+b]) < 0
		}), "interior boundaries must be strictly increasing")

		per, total := n/(len(bounds)-1), 0
		for i := 0; i+1 < len(bounds); i++ {
			it, err := tx.Range(table, bounds[i], bounds[i+1], order.Asc, -1)
			require.NoError(t, err)
			cnt := 0
			for it.HasNext() {
				_, _, err := it.Next()
				require.NoError(t, err)
				cnt++
			}
			it.Close()
			total += cnt
			require.InDelta(t, per, cnt, float64(per), "range %d holds %d keys, want ~%d", i, cnt, per)
		}
		require.Equal(t, n, total, "ranges must cover every key exactly once")
		return nil
	}))
}

func TestWarmupTable(t *testing.T) {
	const table, n = "T", 50_000
	db := New(dbcfg.ChainDB, log.New()).InMem(t, t.TempDir()).WithTableCfg(func(_ kv.TableCfg) kv.TableCfg {
		return kv.TableCfg{table: kv.TableCfgItem{}}
	}).MapSize(512 * datasize.MB).MustOpen()
	t.Cleanup(db.Close)

	require.NoError(t, db.Update(t.Context(), func(tx kv.RwTx) error {
		c, err := tx.RwCursor(table)
		require.NoError(t, err)
		defer c.Close()
		for i := 0; i < n; i++ {
			require.NoError(t, c.Append(clusteredKey(i), []byte{1}))
		}
		return nil
	}))

	old := dbg.WarmupTableWorkers
	dbg.WarmupTableWorkers = 8
	t.Cleanup(func() { dbg.WarmupTableWorkers = old })

	db.WarmupTable(t.Context(), table) // count-balanced fan-out must warm a clustered table without panicking
}

// A from near the table end leaves the range with far fewer positions than the
// requested cursor count, so DistributeCursors leaves surplus cursors unset.
// Reading an unset cursor reports ENODATA, which must be treated as "no more
// positions", not propagated as an error.
func TestSplitBucketByCountFewerPositions(t *testing.T) {
	const table, n = "T", 200_000
	db := New(dbcfg.ChainDB, log.New()).InMem(t, t.TempDir()).WithTableCfg(func(_ kv.TableCfg) kv.TableCfg {
		return kv.TableCfg{table: kv.TableCfgItem{}}
	}).MapSize(512 * datasize.MB).MustOpen()
	t.Cleanup(db.Close)

	require.NoError(t, db.Update(t.Context(), func(tx kv.RwTx) error {
		c, err := tx.RwCursor(table)
		require.NoError(t, err)
		defer c.Close()
		k := make([]byte, 8)
		for i := 0; i < n; i++ {
			binary.BigEndian.PutUint64(k, uint64(i))
			require.NoError(t, c.Append(k, []byte{1}))
		}
		return nil
	}))

	require.NoError(t, db.View(t.Context(), func(tx kv.Tx) error {
		from := make([]byte, 8)
		binary.BigEndian.PutUint64(from, uint64(n-5)) // only ~5 positions left, but n_cap is far larger
		bounds, err := tx.(kv.BucketSplitter).SplitBucketByCount(table, from, 4096)
		require.NoError(t, err) // unset surplus cursors must not surface as an error
		require.GreaterOrEqual(t, len(bounds), 2)
		require.Equal(t, from, bounds[0])
		require.Nil(t, bounds[len(bounds)-1])
		return nil
	}))
}
