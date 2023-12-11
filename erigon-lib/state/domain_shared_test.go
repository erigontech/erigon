package state

import (
	"context"
	"math/rand"
	"testing"

	"github.com/holiman/uint256"
	"github.com/ledgerwatch/erigon-lib/common/hexutility"
	"github.com/ledgerwatch/erigon-lib/kv"
	"github.com/stretchr/testify/require"

	"github.com/ledgerwatch/erigon-lib/common/length"
	"github.com/ledgerwatch/erigon-lib/types"
)

func TestSharedDomain_Unwind(t *testing.T) {
	stepSize := uint64(100)
	db, agg := testDbAndAggregatorv3(t, stepSize)

	ctx := context.Background()
	rwTx, err := db.BeginRw(ctx)
	require.NoError(t, err)
	defer rwTx.Rollback()

	ac := agg.MakeContext()
	defer ac.Close()

	domains := NewSharedDomains(WrapTxWithCtx(rwTx, ac))
	defer domains.Close()

	maxTx := stepSize
	hashes := make([][]byte, maxTx)
	count := 10
	rnd := rand.New(rand.NewSource(0))
	ac.Close()
	err = rwTx.Commit()
	require.NoError(t, err)

Loop:
	rwTx, err = db.BeginRw(ctx)
	require.NoError(t, err)
	defer rwTx.Rollback()

	ac = agg.MakeContext()
	defer ac.Close()
	domains = NewSharedDomains(WrapTxWithCtx(rwTx, ac))
	defer domains.Close()

	i := 0
	k0 := make([]byte, length.Addr)
	commitStep := 3

	for ; i < int(maxTx); i++ {
		domains.SetTxNum(ctx, uint64(i))
		for accs := 0; accs < 256; accs++ {
			v := types.EncodeAccountBytesV3(uint64(i), uint256.NewInt(uint64(i*10e6)+uint64(accs*10e2)), nil, 0)
			k0[0] = byte(accs)
			pv, err := domains.LatestAccount(k0)
			require.NoError(t, err)

			err = domains.DomainPut(kv.AccountsDomain, k0, nil, v, pv)
			require.NoError(t, err)
		}

		if i%commitStep == 0 {
			rh, err := domains.ComputeCommitment(ctx, true, false, domains.BlockNum(), "")
			require.NoError(t, err)
			if hashes[uint64(i)] != nil {
				require.Equal(t, hashes[uint64(i)], rh)
			}
			require.NotNil(t, rh)
			hashes[uint64(i)] = rh
		}
	}

	err = domains.Flush(ctx, rwTx)
	require.NoError(t, err)

	unwindTo := uint64(commitStep * rnd.Intn(int(maxTx)/commitStep))

	acu := agg.MakeContext()
	err = domains.Unwind(ctx, rwTx, unwindTo)
	require.NoError(t, err)
	acu.Close()

	err = rwTx.Commit()
	require.NoError(t, err)
	if count > 0 {
		count--
	}
	domains.FinishWrites()
	domains.Close()
	ac.Close()
	if count == 0 {
		return
	}

	goto Loop
}

func TestSharedDomain_IteratePrefix(t *testing.T) {
	stepSize := uint64(8)
	db, agg := testDbAndAggregatorv3(t, stepSize)

	ac := agg.MakeContext()
	defer ac.Close()
	ctx := context.Background()

	rwTx, err := db.BeginRw(ctx)
	require.NoError(t, err)
	defer rwTx.Rollback()

	ac = agg.MakeContext()
	defer ac.Close()
	domains := NewSharedDomains(WrapTxWithCtx(rwTx, ac))
	defer domains.Close()

	for i := uint64(0); i < stepSize*2; i++ {
		if err = domains.DomainPut(kv.StorageDomain, hexutility.EncodeTs(i), nil, hexutility.EncodeTs(i), nil); err != nil {
			panic(err)
		}
	}

	err = domains.Flush(ctx, rwTx)
	require.NoError(t, err)
	domains.Close()

	domains = NewSharedDomains(WrapTxWithCtx(rwTx, ac))
	defer domains.Close()
	{ // no deletes
		var list [][]byte
		require.NoError(t, domains.IterateStoragePrefix(nil, func(k []byte, v []byte) error {
			list = append(list, k)
			return nil
		}))
		require.Equal(t, int(stepSize*2), len(list))
	}
	{ // delete marker is in RAM
		if err := domains.DomainDel(kv.StorageDomain, hexutility.EncodeTs(1), nil, nil); err != nil {
			panic(err)
		}
		var list [][]byte
		require.NoError(t, domains.IterateStoragePrefix(nil, func(k []byte, v []byte) error {
			list = append(list, k)
			return nil
		}))
		require.Equal(t, int(stepSize*2-1), len(list))
	}
	{ // delete marker is in DB
		require.NoError(t, domains.Flush(ctx, rwTx))
		var list [][]byte
		require.NoError(t, domains.IterateStoragePrefix(nil, func(k []byte, v []byte) error {
			list = append(list, k)
			return nil
		}))
		require.Equal(t, int(stepSize*2-1), len(list))
	}
	domains.Close()
	ac.Close()
	require.NoError(t, agg.BuildFiles(stepSize*2))
	ac = agg.MakeContext()
	defer ac.Close()
	require.NoError(t, ac.Prune(ctx, rwTx))

	domains = NewSharedDomains(WrapTxWithCtx(rwTx, ac))
	defer domains.Close()
	{ //delete marker is in Files
		var list [][]byte
		require.NoError(t, domains.IterateStoragePrefix(nil, func(k []byte, v []byte) error {
			list = append(list, k)
			return nil
		}))
		require.Equal(t, int(stepSize*2-1), len(list))
	}
}
