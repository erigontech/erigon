package state

import (
	"context"
	"encoding/binary"
	"math/rand"
	"testing"

	"github.com/ledgerwatch/log/v3"
	"github.com/stretchr/testify/require"

	"github.com/ledgerwatch/erigon-lib/common/length"
	"github.com/ledgerwatch/erigon-lib/kv"
)

func Benchmark_SharedDomains_GetLatest(t *testing.B) {
	stepSize := uint64(100)
	db, agg := testDbAndAggregatorBench(t, stepSize)

	ctx := context.Background()
	rwTx, err := db.BeginRw(ctx)
	require.NoError(t, err)
	defer rwTx.Rollback()

	ac := agg.BeginFilesRo()
	defer ac.Close()

	domains, err := NewSharedDomains(WrapTxWithCtx(rwTx, ac), log.New())
	require.NoError(t, err)
	defer domains.Close()
	maxTx := stepSize * 258

	seed := int64(4500)
	rnd := rand.New(rand.NewSource(seed))

	keys := make([][]byte, 8)
	for i := 0; i < len(keys); i++ {
		keys[i] = make([]byte, length.Addr)
		rnd.Read(keys[i])
	}

	for i := uint64(0); i < maxTx; i++ {
		domains.SetTxNum(i)
		v := make([]byte, 8)
		binary.BigEndian.PutUint64(v, i)
		for j := 0; j < len(keys); j++ {
			err := domains.DomainPut(kv.AccountsDomain, keys[j], nil, v, nil, 0)
			require.NoError(t, err)
		}

		if i%stepSize == 0 {
			_, err := domains.ComputeCommitment(ctx, true, domains.BlockNum(), "")
			require.NoError(t, err)
			err = domains.Flush(ctx, rwTx)
			require.NoError(t, err)
			if i/stepSize > 3 {
				err = agg.BuildFiles(i - (2 * stepSize))
				require.NoError(t, err)
			}
		}
	}
	_, err = domains.ComputeCommitment(ctx, true, domains.BlockNum(), "")
	require.NoError(t, err)
	err = domains.Flush(ctx, rwTx)
	require.NoError(t, err)
	err = rwTx.Commit()
	require.NoError(t, err)

	rwTx, err = db.BeginRw(ctx)
	require.NoError(t, err)
	defer rwTx.Rollback()

	ac2 := agg.BeginFilesRo()
	defer ac2.Close()

	latest := make([]byte, 8)
	binary.BigEndian.PutUint64(latest, maxTx-1)
	//t.Run("GetLatest", func(t *testing.B) {
	for ik := 0; ik < t.N; ik++ {
		for i := 0; i < len(keys); i++ {
			v, _, ok, err := ac2.GetLatest(kv.AccountsDomain, keys[i], nil, rwTx)

			require.True(t, ok)
			require.EqualValuesf(t, latest, v, "unexpected %d, wanted %d", binary.BigEndian.Uint64(v), maxTx-1)
			require.NoError(t, err)
		}
	}

	for ik := 0; ik < t.N; ik++ {
		for i := 0; i < len(keys); i++ {
			ts := uint64(rnd.Intn(int(maxTx)))
			v, ok, err := ac2.HistorySeek(kv.AccountsHistory, keys[i], ts, rwTx)

			require.True(t, ok)
			require.NotNil(t, v)
			//require.EqualValuesf(t, latest, v, "unexpected %d, wanted %d", binary.BigEndian.Uint64(v), maxTx-1)
			require.NoError(t, err)
		}
	}
}

func BenchmarkSharedDomains_ComputeCommitment(b *testing.B) {
	b.StopTimer()

	stepSize := uint64(100)
	db, agg := testDbAndAggregatorBench(b, stepSize)

	ctx := context.Background()
	rwTx, err := db.BeginRw(ctx)
	require.NoError(b, err)
	defer rwTx.Rollback()

	ac := agg.BeginFilesRo()
	defer ac.Close()

	domains, err := NewSharedDomains(WrapTxWithCtx(rwTx, ac), log.New())
	require.NoError(b, err)
	defer domains.Close()

	maxTx := stepSize * 17
	data := generateTestDataForDomainCommitment(b, length.Addr, length.Addr+length.Hash, maxTx, 15, 100)
	require.NotNil(b, data)

	for domName, d := range data {
		fom := kv.AccountsDomain
		if domName == "storage" {
			fom = kv.StorageDomain
		}
		for key, upd := range d {
			for _, u := range upd {
				domains.SetTxNum(u.txNum)
				err := domains.DomainPut(fom, []byte(key), nil, u.value, nil, 0)
				require.NoError(b, err)
			}
		}
	}

	b.StartTimer()
	for i := 0; i < b.N; i++ {
		_, err := domains.ComputeCommitment(ctx, true, domains.BlockNum(), "")
		require.NoError(b, err)
	}
}
