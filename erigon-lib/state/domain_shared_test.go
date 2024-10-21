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
	"fmt"
	"math/rand"
	"testing"
	"time"

	"github.com/holiman/uint256"
	"github.com/stretchr/testify/require"

	"github.com/erigontech/erigon-lib/kv"
	"github.com/erigontech/erigon-lib/kv/rawdbv3"
	"github.com/erigontech/erigon-lib/log/v3"

	"github.com/erigontech/erigon-lib/common/length"
	"github.com/erigontech/erigon-lib/types"
)

func TestSharedDomain_CommitmentKeyReplacement(t *testing.T) {
	t.Parallel()

	stepSize := uint64(100)
	db, agg := testDbAndAggregatorv3(t, stepSize)

	ctx := context.Background()
	rwTx, err := db.BeginRw(ctx)
	require.NoError(t, err)
	defer rwTx.Rollback()

	ac := agg.BeginFilesRo()
	defer ac.Close()

	domains, err := NewSharedDomains(WrapTxWithCtx(rwTx, ac), log.New())
	require.NoError(t, err)
	defer domains.Close()

	rnd := rand.New(rand.NewSource(2342))
	maxTx := stepSize * 8

	// 1. generate data
	data := generateSharedDomainsUpdates(t, domains, maxTx, rnd, length.Addr, 10, stepSize)
	fillRawdbTxNumsIndexForSharedDomains(t, rwTx, maxTx, stepSize)

	err = domains.Flush(ctx, rwTx)
	require.NoError(t, err)

	// 2. remove just one key and compute commitment
	removedKey := []byte{}
	for key := range data {
		removedKey = []byte(key)[:length.Addr]
		domains.SetTxNum(maxTx + 1)
		err = domains.DomainDel(kv.AccountsDomain, removedKey, nil, nil, 0)
		require.NoError(t, err)
		break
	}

	// 3. calculate commitment with all data +removed key
	expectedHash, err := domains.ComputeCommitment(context.Background(), false, domains.txNum/stepSize, "")
	require.NoError(t, err)
	domains.Close()

	err = rwTx.Commit()
	require.NoError(t, err)

	t.Logf("expected hash: %x", expectedHash)
	t.Logf("valueTransform enabled: %t", agg.commitmentValuesTransform)
	err = agg.BuildFiles(stepSize * 16)
	require.NoError(t, err)

	ac.Close()

	ac = agg.BeginFilesRo()
	rwTx, err = db.BeginRw(ctx)
	require.NoError(t, err)
	defer rwTx.Rollback()

	// 4. restart on same (replaced keys) files
	domains, err = NewSharedDomains(WrapTxWithCtx(rwTx, ac), log.New())
	require.NoError(t, err)
	defer domains.Close()

	// 5. delete same key. commitment should be the same
	domains.SetTxNum(maxTx + 1)
	err = domains.DomainDel(kv.AccountsDomain, removedKey, nil, nil, 0)
	require.NoError(t, err)

	resultHash, err := domains.ComputeCommitment(context.Background(), false, domains.txNum/stepSize, "")
	require.NoError(t, err)

	t.Logf("result hash: %x", resultHash)
	require.Equal(t, expectedHash, resultHash)
}

func TestSharedDomain_Unwind(t *testing.T) {
	t.Parallel()

	stepSize := uint64(100)
	db, agg := testDbAndAggregatorv3(t, stepSize)

	ctx := context.Background()
	rwTx, err := db.BeginRw(ctx)
	require.NoError(t, err)
	defer rwTx.Rollback()

	ac := agg.BeginFilesRo()
	defer ac.Close()

	domains, err := NewSharedDomains(WrapTxWithCtx(rwTx, ac), log.New())
	require.NoError(t, err)
	defer domains.Close()

	stateChangeset := &StateChangeSet{}
	domains.SetChangesetAccumulator(stateChangeset)

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

	ac = agg.BeginFilesRo()
	defer ac.Close()
	domains, err = NewSharedDomains(WrapTxWithCtx(rwTx, ac), log.New())
	require.NoError(t, err)
	defer domains.Close()

	i := 0
	k0 := make([]byte, length.Addr)
	commitStep := 3

	for ; i < int(maxTx); i++ {
		domains.SetTxNum(uint64(i))
		for accs := 0; accs < 256; accs++ {
			v := types.EncodeAccountBytesV3(uint64(i), uint256.NewInt(uint64(i*10e6)+uint64(accs*10e2)), nil, 0)
			k0[0] = byte(accs)
			pv, step, err := domains.DomainGet(kv.AccountsDomain, k0, nil)
			require.NoError(t, err)

			err = domains.DomainPut(kv.AccountsDomain, k0, nil, v, pv, step)
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
	domains.currentChangesAccumulator = nil

	acu := agg.BeginFilesRo()
	var a [kv.DomainLen][]DomainEntryDiff
	for idx, d := range stateChangeset.Diffs {
		a[idx] = d.GetDiffSet()
	}
	err = domains.Unwind(ctx, rwTx, 0, unwindTo, &a)
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
	t.Parallel()

	stepSize := uint64(8)
	require := require.New(t)
	db, agg := testDbAndAggregatorv3(t, stepSize)

	iterCount := func(domains *SharedDomains) int {
		var list [][]byte
		require.NoError(domains.IterateStoragePrefix(nil, func(k []byte, v []byte, step uint64) error {
			list = append(list, k)
			return nil
		}))
		return len(list)
	}

	ac := agg.BeginFilesRo()
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

	ac = agg.BeginFilesRo()
	defer ac.Close()
	domains, err := NewSharedDomains(WrapTxWithCtx(rwTx, ac), log.New())
	require.NoError(err)
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
		if err = domains.DomainPut(kv.AccountsDomain, addr, nil, acc(i), nil, 0); err != nil {
			panic(err)
		}
		if err = domains.DomainPut(kv.StorageDomain, addr, st(i), acc(i), nil, 0); err != nil {
			panic(err)
		}
	}

	{ // no deletes
		err = domains.Flush(ctx, rwTx)
		require.NoError(err)
		domains.Close()

		domains, err = NewSharedDomains(WrapTxWithCtx(rwTx, ac), log.New())
		require.NoError(err)
		defer domains.Close()
		require.Equal(int(stepSize), iterCount(domains))
	}
	{ // delete marker is in RAM
		require.NoError(domains.Flush(ctx, rwTx))
		domains.Close()
		domains, err = NewSharedDomains(WrapTxWithCtx(rwTx, ac), log.New())
		require.NoError(err)
		defer domains.Close()
		require.Equal(int(stepSize), iterCount(domains))

		domains.SetTxNum(stepSize)
		if err := domains.DomainDel(kv.StorageDomain, addr, st(1), nil, 0); err != nil {
			panic(err)
		}
		if err := domains.DomainDel(kv.StorageDomain, addr, st(2), nil, 0); err != nil {
			panic(err)
		}
		for i := stepSize; i < stepSize*2+2; i++ {
			domains.SetTxNum(i)
			if err = domains.DomainPut(kv.AccountsDomain, addr, nil, acc(i), nil, 0); err != nil {
				panic(err)
			}
			if err = domains.DomainPut(kv.StorageDomain, addr, st(i), acc(i), nil, 0); err != nil {
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

		domains, err = NewSharedDomains(WrapTxWithCtx(rwTx, ac), log.New())
		require.NoError(err)
		defer domains.Close()
		require.Equal(int(stepSize*2+2-2), iterCount(domains))
	}
	{ //delete marker is in Files
		domains.Close()
		ac.Close()
		err = rwTx.Commit() // otherwise agg.BuildFiles will not see data
		require.NoError(err)
		require.NoError(agg.BuildFiles(stepSize * 2))
		require.Equal(1, agg.d[kv.StorageDomain].dirtyFiles.Len())

		ac = agg.BeginFilesRo()
		defer ac.Close()
		rwTx, err = db.BeginRw(ctx)
		require.NoError(err)
		defer rwTx.Rollback()

		_, err := ac.Prune(ctx, rwTx, 0, nil)
		require.NoError(err)
		domains, err = NewSharedDomains(WrapTxWithCtx(rwTx, ac), log.New())
		require.NoError(err)
		defer domains.Close()
		require.Equal(int(stepSize*2+2-2), iterCount(domains))
	}

	{ // delete/update more keys in RAM
		require.NoError(domains.Flush(ctx, rwTx))
		domains.Close()
		domains, err = NewSharedDomains(WrapTxWithCtx(rwTx, ac), log.New())
		require.NoError(err)
		defer domains.Close()

		domains.SetTxNum(stepSize*2 + 1)
		if err := domains.DomainDel(kv.StorageDomain, addr, st(4), nil, 0); err != nil {
			panic(err)
		}
		if err := domains.DomainPut(kv.StorageDomain, addr, st(5), acc(5), nil, 0); err != nil {
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

		domains, err = NewSharedDomains(WrapTxWithCtx(rwTx, ac), log.New())
		require.NoError(err)
		defer domains.Close()
		require.Equal(int(stepSize*2+2-3), iterCount(domains))
	}
	{ // delete everything - must see 0
		err = domains.Flush(ctx, rwTx)
		require.NoError(err)
		domains.Close()

		domains, err = NewSharedDomains(WrapTxWithCtx(rwTx, ac), log.New())
		require.NoError(err)
		defer domains.Close()
		domains.SetTxNum(domains.TxNum() + 1)
		err := domains.DomainDelPrefix(kv.StorageDomain, []byte{})
		require.NoError(err)
		require.Equal(0, iterCount(domains))
	}
}

func TestSharedDomain_StorageIter(t *testing.T) {
	t.Parallel()

	log.Root().SetHandler(log.LvlFilterHandler(log.LvlWarn, log.StderrHandler))

	stepSize := uint64(10)
	db, agg := testDbAndAggregatorv3(t, stepSize)

	ctx := context.Background()
	rwTx, err := db.BeginRw(ctx)
	require.NoError(t, err)
	defer rwTx.Rollback()

	ac := agg.BeginFilesRo()
	defer ac.Close()

	domains, err := NewSharedDomains(WrapTxWithCtx(rwTx, ac), log.New())
	require.NoError(t, err)
	defer domains.Close()

	maxTx := 3*stepSize + 10
	hashes := make([][]byte, maxTx)

	domains, err = NewSharedDomains(WrapTxWithCtx(rwTx, ac), log.New())
	require.NoError(t, err)
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

			pv, step, err := domains.DomainGet(kv.AccountsDomain, k0, nil)
			require.NoError(t, err)

			err = domains.DomainPut(kv.AccountsDomain, k0, nil, v, pv, step)
			require.NoError(t, err)
			binary.BigEndian.PutUint64(l0[16:24], uint64(accs))

			for locs := 0; locs < 15000; locs++ {
				binary.BigEndian.PutUint64(l0[24:], uint64(locs))
				pv, step, err := domains.DomainGet(kv.AccountsDomain, append(k0, l0...), nil)
				require.NoError(t, err)

				err = domains.DomainPut(kv.StorageDomain, k0, l0, l0[24:], pv, step)
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
	domains.Close()

	err = rwTx.Commit()
	require.NoError(t, err)

	err = agg.BuildFiles(maxTx - stepSize)
	require.NoError(t, err)

	ac.Close()
	ac = agg.BeginFilesRo()

	//err = db.Update(ctx, func(tx kv.RwTx) error {
	//	_, err = ac.PruneSmallBatches(ctx, 1*time.Minute, tx)
	//	return err
	//})
	_, err = ac.PruneSmallBatchesDb(ctx, 1*time.Minute, db)
	require.NoError(t, err)

	ac.Close()

	ac = agg.BeginFilesRo()
	defer ac.Close()

	rwTx, err = db.BeginRw(ctx)
	require.NoError(t, err)

	domains, err = NewSharedDomains(WrapTxWithCtx(rwTx, ac), log.New())
	require.NoError(t, err)
	defer domains.Close()

	for accs := 0; accs < accounts; accs++ {
		k0[0] = byte(accs)
		pv, step, err := domains.DomainGet(kv.AccountsDomain, k0, nil)
		require.NoError(t, err)

		existed := make(map[string]struct{})
		err = domains.IterateStoragePrefix(k0, func(k []byte, v []byte, step uint64) error {
			existed[string(k)] = struct{}{}
			return nil
		})
		require.NoError(t, err)

		missed := 0
		err = domains.IterateStoragePrefix(k0, func(k []byte, v []byte, step uint64) error {
			if _, been := existed[string(k)]; !been {
				missed++
			}
			return nil
		})
		require.NoError(t, err)
		require.Zero(t, missed)

		err = domains.deleteAccount(k0, pv, step)
		require.NoError(t, err)

		notRemoved := 0
		err = domains.IterateStoragePrefix(k0, func(k []byte, v []byte, step uint64) error {
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
