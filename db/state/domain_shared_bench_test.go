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

package state

import (
	"context"
	"encoding/binary"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/erigontech/erigon-lib/common/length"
	"github.com/erigontech/erigon-lib/log/v3"
	"github.com/erigontech/erigon/db/kv"
)

func Benchmark_SharedDomains_GetLatest(t *testing.B) {
	stepSize := uint64(100)
	_db, agg := testDbAndAggregatorBench(t, stepSize)
	db := wrapDbWithCtx(_db, agg)

	ctx := context.Background()
	rwTx, err := db.BeginTemporalRw(ctx)
	require.NoError(t, err)
	defer rwTx.Rollback()

	domains, err := NewSharedDomains(rwTx, log.New())
	require.NoError(t, err)
	defer domains.Close()
	maxTx := stepSize * 258

	rnd := newRnd(4500)

	keys := make([][]byte, 8)
	for i := 0; i < len(keys); i++ {
		keys[i] = make([]byte, length.Addr)
		rnd.Read(keys[i])
	}

	var txNum, blockNum uint64
	for i := uint64(0); i < maxTx; i++ {
		txNum = i
		domains.SetTxNum(txNum)
		v := make([]byte, 8)
		binary.BigEndian.PutUint64(v, i)
		for j := 0; j < len(keys); j++ {
			err := domains.DomainPut(kv.AccountsDomain, rwTx, keys[j], v, txNum, nil, 0)
			require.NoError(t, err)
		}

		if i%stepSize == 0 {
			_, err := domains.ComputeCommitment(ctx, true, blockNum, txNum, "")
			require.NoError(t, err)
			err = domains.Flush(ctx, rwTx)
			require.NoError(t, err)
			if i/stepSize > 3 {
				err = agg.BuildFiles(i - (2 * stepSize))
				require.NoError(t, err)
			}
		}
	}
	_, err = domains.ComputeCommitment(ctx, true, blockNum, txNum, "")
	require.NoError(t, err)
	err = domains.Flush(ctx, rwTx)
	require.NoError(t, err)
	err = rwTx.Commit()
	require.NoError(t, err)

	rwTx, err = db.BeginTemporalRw(ctx)
	require.NoError(t, err)
	defer rwTx.Rollback()

	latest := make([]byte, 8)
	binary.BigEndian.PutUint64(latest, maxTx-1)

	t.Run("GetLatest", func(b *testing.B) {
		t.ReportAllocs()
		for ik := 0; ik < t.N; ik++ {
			for i := 0; i < len(keys); i++ {
				v, _, err := rwTx.GetLatest(kv.AccountsDomain, keys[i])
				require.Equalf(t, latest, v, "unexpected %d, wanted %d", binary.BigEndian.Uint64(v), maxTx-1)
				require.NoError(t, err)
			}
		}
	})
	t.Run("HistorySeek", func(b *testing.B) {
		t.ReportAllocs()
		for ik := 0; ik < t.N; ik++ {
			for i := 0; i < len(keys); i++ {
				ts := uint64(rnd.IntN(int(maxTx)))
				v, ok, err := rwTx.HistorySeek(kv.AccountsDomain, keys[i], ts)

				require.True(t, ok)
				require.NotNil(t, v)
				//require.EqualValuesf(t, latest, v, "unexpected %d, wanted %d", binary.BigEndian.Uint64(v), maxTx-1)
				require.NoError(t, err)
			}
		}
	})
}

func BenchmarkSharedDomains_ComputeCommitment(b *testing.B) {
	stepSize := uint64(100)
	_db, agg := testDbAndAggregatorBench(b, stepSize)
	db := wrapDbWithCtx(_db, agg)

	ctx := context.Background()
	rwTx, err := db.BeginTemporalRw(ctx)
	require.NoError(b, err)
	defer rwTx.Rollback()

	domains, err := NewSharedDomains(rwTx, log.New())
	require.NoError(b, err)
	defer domains.Close()

	maxTx := stepSize * 17
	data := generateTestDataForDomainCommitment(b, length.Addr, length.Addr+length.Hash, maxTx, 15, 100)
	require.NotNil(b, data)

	var txNum, blockNum uint64
	for domName, d := range data {
		fom := kv.AccountsDomain
		if domName == "storage" {
			fom = kv.StorageDomain
		}
		for key, upd := range d {
			for _, u := range upd {
				txNum = u.txNum
				domains.SetTxNum(txNum)
				err := domains.DomainPut(fom, rwTx, []byte(key), u.value, txNum, nil, 0)
				require.NoError(b, err)
			}
		}
	}

	b.Run("ComputeCommitment", func(b *testing.B) {
		for i := 0; i < b.N; i++ {
			_, err := domains.ComputeCommitment(ctx, true, blockNum, txNum, "")
			require.NoError(b, err)
		}
	})
}
