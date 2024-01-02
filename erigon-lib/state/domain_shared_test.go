package state

import (
	"context"
	"encoding/binary"
	"fmt"
	"math/rand"
	"testing"
	"time"

	"github.com/holiman/uint256"
	"github.com/ledgerwatch/erigon-lib/kv"
	"github.com/ledgerwatch/erigon-lib/kv/rawdbv3"
	"github.com/ledgerwatch/log/v3"
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

	domains := NewSharedDomains(WrapTxWithCtx(rwTx, ac), log.New())
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
	domains = NewSharedDomains(WrapTxWithCtx(rwTx, ac), log.New())
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
	agg.keepInDB = 0

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
	for i := uint64(0); i < stepSize*2; i++ {
		blockNum := i
		maxTxNum := blockNum*2 - 1
		err = rawdbv3.TxNums.Append(rwTx, blockNum, maxTxNum)
		require.NoError(err)
	}

	ac = agg.MakeContext()
	defer ac.Close()
	domains := NewSharedDomains(WrapTxWithCtx(rwTx, ac), log.New())
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

		domains = NewSharedDomains(WrapTxWithCtx(rwTx, ac), log.New())
		defer domains.Close()
		require.Equal(int(stepSize), iterCount(domains))
	}
	{ // delete marker is in RAM
		require.NoError(domains.Flush(ctx, rwTx))
		domains.Close()
		domains = NewSharedDomains(WrapTxWithCtx(rwTx, ac), log.New())
		defer domains.Close()
		require.Equal(int(stepSize), iterCount(domains))

		domains.SetTxNum(stepSize)
		if err := domains.DomainDel(kv.StorageDomain, addr, st(1), nil); err != nil {
			panic(err)
		}
		if err := domains.DomainDel(kv.StorageDomain, addr, st(2), nil); err != nil {
			panic(err)
		}
		for i := stepSize; i < stepSize*2+2; i++ {
			domains.SetTxNum(i)
			if err = domains.DomainPut(kv.AccountsDomain, addr, nil, acc(i), nil); err != nil {
				panic(err)
			}
			if err = domains.DomainPut(kv.StorageDomain, addr, st(i), acc(i), nil); err != nil {
				panic(err)
			}
		}
		require.Equal(int(stepSize*2+2-2), iterCount(domains))
	}
	{ // delete marker is in DB
		_, err = domains.ComputeCommitment(ctx, true, domains.TxNum()/2, "")
		require.NoError(err)
		err = domains.Flush(ctx, rwTx)
		require.NoError(err)
		domains.Close()

		domains = NewSharedDomains(WrapTxWithCtx(rwTx, ac), log.New())
		defer domains.Close()
		require.Equal(int(stepSize*2+2-2), iterCount(domains))
	}
	{ //delete marker is in Files
		domains.Close()
		ac.Close()
		err = rwTx.Commit() // otherwise agg.BuildFiles will not see data
		require.NoError(err)
		require.NoError(agg.BuildFiles(stepSize * 2))
		require.Equal(1, agg.storage.files.Len())

		ac = agg.MakeContext()
		defer ac.Close()
		rwTx, err = db.BeginRw(ctx)
		require.NoError(err)
		defer rwTx.Rollback()
		require.NoError(ac.Prune(ctx, rwTx))
		domains = NewSharedDomains(WrapTxWithCtx(rwTx, ac), log.New())
		defer domains.Close()
		require.Equal(int(stepSize*2+2-2), iterCount(domains))
	}

	{ // delete/update more keys in RAM
		require.NoError(domains.Flush(ctx, rwTx))
		domains.Close()
		domains = NewSharedDomains(WrapTxWithCtx(rwTx, ac), log.New())
		defer domains.Close()

		domains.SetTxNum(stepSize*2 + 1)
		if err := domains.DomainDel(kv.StorageDomain, addr, st(4), nil); err != nil {
			panic(err)
		}
		if err := domains.DomainPut(kv.StorageDomain, addr, st(5), acc(5), nil); err != nil {
			panic(err)
		}
		require.Equal(int(stepSize*2+2-3), iterCount(domains))
	}
	{ // flush delete/updates to DB
		_, err = domains.ComputeCommitment(ctx, true, domains.TxNum()/2, "")
		require.NoError(err)
		err = domains.Flush(ctx, rwTx)
		require.NoError(err)
		domains.Close()

		domains = NewSharedDomains(WrapTxWithCtx(rwTx, ac), log.New())
		defer domains.Close()
		require.Equal(int(stepSize*2+2-3), iterCount(domains))
	}
	{ // delete everything - must see 0
		err = domains.Flush(ctx, rwTx)
		require.NoError(err)
		domains.Close()

		domains = NewSharedDomains(WrapTxWithCtx(rwTx, ac), log.New())
		defer domains.Close()
		domains.SetTxNum(domains.TxNum() + 1)
		err := domains.DomainDelPrefix(kv.StorageDomain, []byte{})
		require.NoError(err)
		require.Equal(0, iterCount(domains))
	}
}

func TestSharedDomain_StorageIter(t *testing.T) {
	log.Root().SetHandler(log.LvlFilterHandler(log.LvlDebug, log.StderrHandler))

	stepSize := uint64(10)
	db, agg := testDbAndAggregatorv3(t, stepSize)

	ctx := context.Background()
	rwTx, err := db.BeginRw(ctx)
	require.NoError(t, err)
	defer rwTx.Rollback()

	ac := agg.MakeContext()
	defer ac.Close()

	domains := NewSharedDomains(WrapTxWithCtx(rwTx, ac), log.New())
	defer domains.Close()

	maxTx := 3*stepSize + 10
	hashes := make([][]byte, maxTx)

	domains = NewSharedDomains(WrapTxWithCtx(rwTx, ac), log.New())
	defer domains.Close()

	i := 0
	k0 := make([]byte, length.Addr)
	l0 := make([]byte, length.Hash)
	commitStep := 3
	accounts := 1

	for ; i < int(maxTx); i++ {
		domains.SetTxNum(uint64(i))
		for accs := 0; accs < accounts; accs++ {
			v := types.EncodeAccountBytesV3(uint64(i), uint256.NewInt(uint64(i*10e6)+uint64(accs*10e2)), nil, 0)
			k0[0] = byte(accs)

			pv, err := domains.LatestAccount(k0)
			require.NoError(t, err)

			err = domains.DomainPut(kv.AccountsDomain, k0, nil, v, pv)
			require.NoError(t, err)
			binary.BigEndian.PutUint64(l0[16:24], uint64(accs))

			for locs := 0; locs < 15000; locs++ {
				binary.BigEndian.PutUint64(l0[24:], uint64(locs))
				pv, err := domains.LatestStorage(append(k0, l0...))
				require.NoError(t, err)

				err = domains.DomainPut(kv.StorageDomain, k0, l0, l0[24:], pv)
				require.NoError(t, err)
			}
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
	fmt.Printf("calling build files step %d\n", maxTx/stepSize)
	err = domains.Flush(ctx, rwTx)
	require.NoError(t, err)
	err = rwTx.Commit()
	require.NoError(t, err)

	err = agg.BuildFiles(maxTx - stepSize)
	require.NoError(t, err)

	err = db.Update(ctx, func(tx kv.RwTx) error {
		return ac.PruneWithTimeout(ctx, 60*time.Minute, tx)
	})
	require.NoError(t, err)

	ac.Close()

	ac = agg.MakeContext()
	defer ac.Close()
	domains.Close()

	rwTx, err = db.BeginRw(ctx)
	require.NoError(t, err)

	domains = NewSharedDomains(WrapTxWithCtx(rwTx, ac), log.New())
	defer domains.Close()

	for accs := 0; accs < accounts; accs++ {
		k0[0] = byte(accs)
		pv, err := domains.LatestAccount(k0)
		require.NoError(t, err)

		existed := make(map[string]struct{})
		err = domains.IterateStoragePrefix(k0, func(k []byte, v []byte) error {
			existed[string(k)] = struct{}{}
			return nil
		})
		require.NoError(t, err)

		missed := 0
		err = domains.IterateStoragePrefix(k0, func(k []byte, v []byte) error {
			if _, been := existed[string(k)]; !been {
				missed++
			}
			return nil
		})
		require.NoError(t, err)
		require.Zero(t, missed)

		err = domains.deleteAccount(k0, pv)
		require.NoError(t, err)

		notRemoved := 0
		err = domains.IterateStoragePrefix(k0, func(k []byte, v []byte) error {
			notRemoved++
			if _, been := existed[string(k)]; !been {
				missed++
			}
			return nil
		})
		require.NoError(t, err)
		require.Zero(t, missed)
		require.Zero(t, notRemoved)
	}
	fmt.Printf("deleted\n")

	err = domains.Flush(ctx, rwTx)
	require.NoError(t, err)
	rwTx.Rollback()

	domains.Close()
	ac.Close()
}
