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

package state_test

import (
	"bytes"
	"encoding/binary"
	"strings"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/erigontech/erigon/common/log/v3"
	"github.com/erigontech/erigon/db/kv"
	"github.com/erigontech/erigon/db/kv/order"
	"github.com/erigontech/erigon/db/state"
	"github.com/erigontech/erigon/db/state/execctx"
)

func TestScanHistory(t *testing.T) {
	const stepSize, txs, keys = 2, 40, 20
	ctx := t.Context()
	db, agg := testDbAndAggregatorv3(t, stepSize)
	key := func(i int) []byte { return binary.BigEndian.AppendUint64(make([]byte, 44), uint64(i)) }

	tx, err := db.BeginTemporalRw(ctx)
	require.NoError(t, err)
	defer tx.Rollback()
	domains, err := execctx.NewSharedDomains(ctx, tx, log.New())
	require.NoError(t, err)
	defer domains.Close()
	for txNum := range uint64(txs) {
		for i := range keys {
			switch {
			case i%3 == int(txNum%3):
				require.NoError(t, domains.DomainDel(kv.StorageDomain, tx, key(i), txNum, nil))
			case i%2 == int(txNum%2):
				require.NoError(t, domains.DomainPut(kv.StorageDomain, tx, key(i), binary.BigEndian.AppendUint64([]byte("v"), txNum), txNum, nil))
			}
		}
	}
	require.NoError(t, domains.Flush(ctx, tx))
	domains.Close()
	require.NoError(t, tx.Commit())
	require.NoError(t, agg.BuildFiles(db, txs, unboundedFinalityCtx))

	type change struct {
		key    string
		txNum  uint64
		before string
	}
	roTx, err := db.BeginTemporalRo(ctx)
	require.NoError(t, err)
	defer roTx.Rollback()
	var files int
	for _, f := range roTx.Debug().DomainFiles(kv.StorageDomain) {
		if !strings.HasSuffix(f.Fullpath(), ".ef") {
			continue
		}
		files++
		from, to := f.StartRootNum(), f.EndRootNum()

		var want []change
		it, err := roTx.Debug().HistoryKeyTxNumRange(kv.StorageDomain, int(from), int(to), order.Asc, -1)
		require.NoError(t, err)
		for it.HasNext() {
			k, txNum, err := it.Next()
			require.NoError(t, err)
			v, ok, err := roTx.HistorySeek(kv.StorageDomain, k, txNum)
			require.NoError(t, err)
			require.True(t, ok)
			want = append(want, change{string(k), txNum, string(v)})
		}
		it.Close()

		var got []change
		require.NoError(t, state.AggTx(roTx).ScanHistory(ctx, kv.StorageDomain, from, to, func(k []byte, txNum uint64, before []byte) error {
			got = append(got, change{string(k), txNum, string(before)})
			return nil
		}))
		require.NotEmpty(t, got)
		require.Equal(t, want, got)

		var gotKeys, wantKeys [][]byte
		for _, c := range got {
			if len(wantKeys) == 0 || !bytes.Equal(wantKeys[len(wantKeys)-1], []byte(c.key)) {
				wantKeys = append(wantKeys, []byte(c.key))
			}
		}
		require.NoError(t, state.AggTx(roTx).ScanHistoryKeys(ctx, kv.StorageDomain, from, to, func(k []byte) error {
			gotKeys = append(gotKeys, bytes.Clone(k))
			return nil
		}))
		require.Equal(t, wantKeys, gotKeys)
	}
	require.Greater(t, files, 1)
	require.Error(t, state.AggTx(roTx).ScanHistory(ctx, kv.StorageDomain, 1, 3, func([]byte, uint64, []byte) error { return nil }))
}
