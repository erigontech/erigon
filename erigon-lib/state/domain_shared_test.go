package state

import (
	"context"
	"encoding/binary"
	"math/rand"
	"testing"

	"github.com/holiman/uint256"
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
		domains.SetTxNum(uint64(i))
		for accs := 0; accs < 256; accs++ {
			v := types.EncodeAccountBytesV3(uint64(i), uint256.NewInt(uint64(i*10e6)+uint64(accs*10e2)), nil, 0)
			k0[0] = byte(accs)
			pv, err := domains.LatestAccount(k0)
			require.NoError(t, err)

			err = domains.DomainPut(kv.AccountsDomain, k0, nil, v, pv)
			require.NoError(t, err)
		}

		if i%commitStep == 0 {
			rh, err := domains.ComputeCommitment(ctx, true, domains.BlockNum(), "")
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
	require := require.New(t)
	db, agg := testDbAndAggregatorv3(t, stepSize)

	iterCount := func(domains *SharedDomains) int {
		var list [][]byte
		require.NoError(domains.IterateStoragePrefix(nil, func(k []byte, v []byte) error {
			list = append(list, k)
			return nil
		}))
		return len(list)
	}

	ac := agg.MakeContext()
	defer ac.Close()
	ctx := context.Background()

	rwTx, err := db.BeginRw(ctx)
	require.NoError(err)
	defer rwTx.Rollback()

	ac = agg.MakeContext()
	defer ac.Close()
	domains := NewSharedDomains(WrapTxWithCtx(rwTx, ac))
	defer domains.Close()

	acc := func(i uint64) []byte {
		buf := make([]byte, 20)
		binary.BigEndian.PutUint64(buf[20-8:], i)
		return buf
	}
	st := func(i uint64) []byte {
		buf := make([]byte, 32)
		binary.BigEndian.PutUint64(buf[32-8:], i)
		return buf
	}
	addr := acc(1)
	for i := uint64(0); i < stepSize; i++ {
		domains.SetTxNum(i)
		if err = domains.DomainPut(kv.AccountsDomain, addr, nil, acc(i), nil); err != nil {
			panic(err)
		}
		if err = domains.DomainPut(kv.StorageDomain, addr, st(i), acc(i), nil); err != nil {
			panic(err)
		}
	}

	{ // no deletes
		err = domains.Flush(ctx, rwTx)
		require.NoError(err)
		domains.Close()

		domains = NewSharedDomains(WrapTxWithCtx(rwTx, ac))
		defer domains.Close()
		require.Equal(int(stepSize), iterCount(domains))
	}
	{ // delete marker is in RAM
		require.NoError(domains.Flush(ctx, rwTx))
		domains.Close()
		domains = NewSharedDomains(WrapTxWithCtx(rwTx, ac))
		defer domains.Close()
		require.Equal(int(stepSize), iterCount(domains))

		domains.SetTxNum(stepSize)
		if err := domains.DomainDel(kv.StorageDomain, addr, st(1), nil); err != nil {
			panic(err)
		}
		if err := domains.DomainDel(kv.StorageDomain, addr, st(2), nil); err != nil {
			panic(err)
		}
		for i := uint64(stepSize); i < stepSize*2; i++ {
			domains.SetTxNum(i)
			if err = domains.DomainPut(kv.AccountsDomain, addr, nil, acc(i), nil); err != nil {
				panic(err)
			}
			if err = domains.DomainPut(kv.StorageDomain, addr, st(i), acc(i), nil); err != nil {
				panic(err)
			}
		}
		require.Equal(int(stepSize*2-2), iterCount(domains))
	}
	{ // delete marker is in DB
		require.NoError(domains.Flush(ctx, rwTx))
		domains.Close()
		domains = NewSharedDomains(WrapTxWithCtx(rwTx, ac))
		defer domains.Close()
		require.Equal(int(stepSize*2-2), iterCount(domains))
	}
	{ //delete marker is in Files
		domains.Close()
		ac.Close()
		err = rwTx.Commit() // otherwise agg.BuildFiles will not see data
		require.NoError(err)
		require.NoError(agg.BuildFiles(stepSize * 2))
		//require.NoError(agg.BuildFiles(stepSize * 2))
		require.Equal(1, agg.storage.files.Len())

		ac = agg.MakeContext()
		defer ac.Close()
		rwTx, err = db.BeginRw(ctx)
		require.NoError(err)
		defer rwTx.Rollback()
		require.NoError(ac.Prune(ctx, rwTx))
		domains = NewSharedDomains(WrapTxWithCtx(rwTx, ac))
		defer domains.Close()
		require.Equal(int(stepSize*2-2), iterCount(domains))
	}

	{ // delete/update more keys in RAM
		require.NoError(domains.Flush(ctx, rwTx))
		domains.Close()
		domains = NewSharedDomains(WrapTxWithCtx(rwTx, ac))
		defer domains.Close()

		domains.SetTxNum(stepSize * 2)
		if err := domains.DomainDel(kv.StorageDomain, addr, st(4), nil); err != nil {
			panic(err)
		}
		if err := domains.DomainPut(kv.StorageDomain, addr, st(5), acc(5), nil); err != nil {
			panic(err)
		}
		require.Equal(int(stepSize*2-3), iterCount(domains))
	}
	{ // flush delete/updates to DB
		err = domains.Flush(ctx, rwTx)
		require.NoError(err)
		domains.Close()

		domains = NewSharedDomains(WrapTxWithCtx(rwTx, ac))
		defer domains.Close()
		require.Equal(int(stepSize*2-3), iterCount(domains))
	}
	{ // delete everything - must see 0
		err = domains.Flush(ctx, rwTx)
		require.NoError(err)
		domains.Close()

		domains = NewSharedDomains(WrapTxWithCtx(rwTx, ac))
		defer domains.Close()
		err := domains.DomainDelPrefix(kv.StorageDomain, []byte{})
		require.NoError(err)
		require.Equal(0, iterCount(domains))
	}
}
