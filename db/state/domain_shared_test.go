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
	"testing"
	"time"

	"github.com/holiman/uint256"
	"github.com/stretchr/testify/require"

	"github.com/erigontech/erigon-lib/common"
	"github.com/erigontech/erigon-lib/common/length"
	"github.com/erigontech/erigon-lib/log/v3"
	"github.com/erigontech/erigon/db/datadir"
	"github.com/erigontech/erigon/db/kv"
	"github.com/erigontech/erigon/db/kv/memdb"
	"github.com/erigontech/erigon/db/kv/rawdbv3"
	accounts3 "github.com/erigontech/erigon/execution/types/accounts"
)

func TestSharedDomain_CommitmentKeyReplacement(t *testing.T) {
	t.Parallel()

	stepSize := uint64(5)
	_db, agg := testDbAndAggregatorv3(t, stepSize)
	db := wrapDbWithCtx(_db, agg)

	ctx := context.Background()
	rwTx, err := db.BeginTemporalRw(ctx)
	require.NoError(t, err)
	defer rwTx.Rollback()

	domains, err := NewSharedDomains(rwTx, log.New())
	require.NoError(t, err)
	defer domains.Close()

	rnd := newRnd(2342)
	maxTx := stepSize * 8

	// 1. generate data
	data := generateSharedDomainsUpdates(t, domains, rwTx, maxTx, rnd, length.Addr, 10, stepSize)
	fillRawdbTxNumsIndexForSharedDomains(t, rwTx, maxTx, stepSize)

	err = domains.Flush(ctx, rwTx)
	require.NoError(t, err)

	// 2. remove just one key and compute commitment
	var txNum uint64
	removedKey := []byte{}
	for key := range data {
		removedKey = []byte(key)[:length.Addr]
		txNum = maxTx + 1
		domains.SetTxNum(txNum)
		err = domains.DomainDel(kv.AccountsDomain, rwTx, removedKey, maxTx+1, nil, 0)
		require.NoError(t, err)
		break
	}

	// 3. calculate commitment with all data +removed key
	expectedHash, err := domains.ComputeCommitment(context.Background(), false, txNum/stepSize, txNum, "")
	require.NoError(t, err)
	domains.Close()

	err = rwTx.Commit()
	require.NoError(t, err)

	t.Logf("expected hash: %x", expectedHash)
	t.Logf("key referencing enabled: %t", agg.d[kv.CommitmentDomain].ReplaceKeysInValues)
	err = agg.BuildFiles(stepSize * 16)
	require.NoError(t, err)

	err = rwTx.Commit()
	require.NoError(t, err)

	rwTx, err = db.BeginTemporalRw(ctx)
	require.NoError(t, err)
	defer rwTx.Rollback()

	// 4. restart on same (replaced keys) files
	domains, err = NewSharedDomains(rwTx, log.New())
	require.NoError(t, err)
	defer domains.Close()

	// 5. delete same key. commitment should be the same
	txNum = maxTx + 1
	domains.SetTxNum(txNum)
	err = domains.DomainDel(kv.AccountsDomain, rwTx, removedKey, maxTx+1, nil, 0)
	require.NoError(t, err)

	resultHash, err := domains.ComputeCommitment(context.Background(), false, txNum/stepSize, txNum, "")
	require.NoError(t, err)

	t.Logf("result hash: %x", resultHash)
	require.Equal(t, expectedHash, resultHash)
}

func TestSharedDomain_Unwind(t *testing.T) {
	if testing.Short() {
		t.Skip()
	}

	t.Parallel()

	stepSize := uint64(100)
	_db, agg := testDbAndAggregatorv3(t, stepSize)
	db := wrapDbWithCtx(_db, agg)

	ctx := context.Background()
	rwTx, err := db.BeginTemporalRw(ctx)
	require.NoError(t, err)
	defer rwTx.Rollback()

	domains, err := NewSharedDomains(rwTx, log.New())
	require.NoError(t, err)
	defer domains.Close()

	stateChangeset := &StateChangeSet{}
	domains.SetChangesetAccumulator(stateChangeset)

	maxTx := stepSize
	hashes := make([][]byte, maxTx)
	count := 10
	rnd := newRnd(0)

	err = rwTx.Commit()
	require.NoError(t, err)

Loop:
	rwTx, err = db.BeginTemporalRw(ctx)
	require.NoError(t, err)
	defer rwTx.Rollback()

	domains, err = NewSharedDomains(rwTx, log.New())
	require.NoError(t, err)
	defer domains.Close()

	i := 0
	k0 := make([]byte, length.Addr)
	commitStep := 3

	var blockNum uint64
	for ; i < int(maxTx); i++ {
		txNum := uint64(i)
		domains.SetTxNum(txNum)
		for accs := 0; accs < 256; accs++ {
			acc := accounts3.Account{
				Nonce:       txNum,
				Balance:     *uint256.NewInt(uint64(i*10e6) + uint64(accs*10e2)),
				CodeHash:    common.Hash{},
				Incarnation: 0,
			}
			v := accounts3.SerialiseV3(&acc)
			k0[0] = byte(accs)
			pv, step, err := domains.GetLatest(kv.AccountsDomain, rwTx, k0)
			require.NoError(t, err)

			err = domains.DomainPut(kv.AccountsDomain, rwTx, k0, v, uint64(i), pv, step)
			require.NoError(t, err)
		}

		if i%commitStep == 0 {
			rh, err := domains.ComputeCommitment(ctx, true, blockNum, txNum, "")
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

	unwindTo := uint64(commitStep * rnd.IntN(int(maxTx)/commitStep))
	domains.currentChangesAccumulator = nil

	var a [kv.DomainLen][]kv.DomainEntryDiff
	for idx, d := range stateChangeset.Diffs {
		a[idx] = d.GetDiffSet()
	}
	err = rwTx.Unwind(ctx, unwindTo, &a)
	require.NoError(t, err)

	err = rwTx.Commit()
	require.NoError(t, err)
	if count > 0 {
		count--
	}
	domains.Close()
	if count == 0 {
		return
	}

	goto Loop
}

func composite(k, k2 []byte) []byte {
	return append(common.Copy(k), k2...)
}
func TestSharedDomain_IteratePrefix(t *testing.T) {
	if testing.Short() {
		t.Skip()
	}

	t.Parallel()

	stepSize := uint64(8)
	require := require.New(t)
	_db, agg := testDbAndAggregatorv3(t, stepSize)
	db := wrapDbWithCtx(_db, agg)

	ctx := context.Background()
	rwTx, err := db.BeginTemporalRw(ctx)
	require.NoError(err)
	defer rwTx.Rollback()

	iterCount := func(domains *SharedDomains) int {
		var list [][]byte
		require.NoError(domains.IterateStoragePrefix(nil, rwTx, func(k []byte, v []byte, step kv.Step) (bool, error) {
			list = append(list, k)
			return true, nil
		}))
		return len(list)
	}

	for i := uint64(0); i < stepSize*2; i++ {
		blockNum := i
		maxTxNum := blockNum*2 - 1
		err = rawdbv3.TxNums.Append(rwTx, blockNum, maxTxNum)
		require.NoError(err)
	}

	domains, err := NewSharedDomains(rwTx, log.New())
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
		if err = domains.DomainPut(kv.AccountsDomain, rwTx, addr, acc(i), i, nil, 0); err != nil {
			panic(err)
		}
		if err = domains.DomainPut(kv.StorageDomain, rwTx, composite(addr, st(i)), acc(i), i, nil, 0); err != nil {
			panic(err)
		}
	}

	{ // no deletes
		err = domains.Flush(ctx, rwTx)
		require.NoError(err)
		domains.Close()

		domains, err = NewSharedDomains(rwTx, log.New())
		require.NoError(err)
		defer domains.Close()
		require.Equal(int(stepSize), iterCount(domains))
	}
	var txNum uint64
	{ // delete marker is in RAM
		require.NoError(domains.Flush(ctx, rwTx))
		domains.Close()
		domains, err = NewSharedDomains(rwTx, log.New())
		require.NoError(err)
		defer domains.Close()
		require.Equal(int(stepSize), iterCount(domains))

		txNum = stepSize
		domains.SetTxNum(stepSize)
		if err := domains.DomainDel(kv.StorageDomain, rwTx, append(addr, st(1)...), stepSize, nil, 0); err != nil {
			panic(err)
		}
		if err := domains.DomainDel(kv.StorageDomain, rwTx, append(addr, st(2)...), stepSize, nil, 0); err != nil {
			panic(err)
		}
		for i := stepSize; i < stepSize*2+2; i++ {
			txNum = i
			domains.SetTxNum(txNum)
			if err = domains.DomainPut(kv.AccountsDomain, rwTx, addr, acc(i), i, nil, 0); err != nil {
				panic(err)
			}
			if err = domains.DomainPut(kv.StorageDomain, rwTx, composite(addr, st(i)), acc(i), i, nil, 0); err != nil {
				panic(err)
			}
		}
		require.Equal(int(stepSize*2+2-2), iterCount(domains))
	}
	{ // delete marker is in DB
		_, err = domains.ComputeCommitment(ctx, true, txNum/2, txNum, "")
		require.NoError(err)
		err = domains.Flush(ctx, rwTx)
		require.NoError(err)
		domains.Close()

		domains, err = NewSharedDomains(rwTx, log.New())
		require.NoError(err)
		defer domains.Close()
		require.Equal(int(stepSize*2+2-2), iterCount(domains))
	}
	{ //delete marker is in Files
		domains.Close()
		err = rwTx.Commit() // otherwise agg.BuildFiles will not see data
		require.NoError(err)
		require.NoError(agg.BuildFiles(stepSize * 2))
		require.Equal(1, agg.d[kv.StorageDomain].dirtyFiles.Len())

		rwTx, err = db.BeginTemporalRw(ctx)
		require.NoError(err)
		defer rwTx.Rollback()

		ac := AggTx(rwTx)
		require.Equal(int(stepSize*2), int(ac.TxNumsInFiles(kv.StateDomains...)))

		_, err := ac.prune(ctx, rwTx, 0, nil)
		require.NoError(err)

		domains, err = NewSharedDomains(rwTx, log.New())
		require.NoError(err)
		defer domains.Close()
		require.Equal(int(stepSize*2+2-2), iterCount(domains))
	}

	{ // delete/update more keys in RAM
		require.NoError(domains.Flush(ctx, rwTx))
		domains.Close()
		domains, err = NewSharedDomains(rwTx, log.New())
		require.NoError(err)
		defer domains.Close()

		txNum = stepSize*2 + 1
		domains.SetTxNum(txNum)
		if err := domains.DomainDel(kv.StorageDomain, rwTx, append(addr, st(4)...), txNum, nil, 0); err != nil {
			panic(err)
		}
		if err := domains.DomainPut(kv.StorageDomain, rwTx, append(addr, st(5)...), acc(5), txNum, nil, 0); err != nil {
			panic(err)
		}
		require.Equal(int(stepSize*2+2-3), iterCount(domains))
	}
	{ // flush delete/updates to DB
		_, err = domains.ComputeCommitment(ctx, true, txNum/2, txNum, "")
		require.NoError(err)
		err = domains.Flush(ctx, rwTx)
		require.NoError(err)
		domains.Close()

		domains, err = NewSharedDomains(rwTx, log.New())
		require.NoError(err)
		defer domains.Close()
		require.Equal(int(stepSize*2+2-3), iterCount(domains))
	}
	{ // delete everything - must see 0
		err = domains.Flush(ctx, rwTx)
		require.NoError(err)
		domains.Close()

		domains, err = NewSharedDomains(rwTx, log.New())
		require.NoError(err)
		defer domains.Close()
		domains.SetTxNum(domains.TxNum() + 1)
		err := domains.DomainDelPrefix(kv.StorageDomain, rwTx, []byte{}, domains.TxNum()+1)
		require.NoError(err)
		require.Equal(0, iterCount(domains))
	}
}

func TestSharedDomain_StorageIter(t *testing.T) {
	if testing.Short() {
		t.Skip()
	}

	t.Parallel()

	log.Root().SetHandler(log.LvlFilterHandler(log.LvlWarn, log.StderrHandler))

	stepSize := uint64(4)
	_db, agg := testDbAndAggregatorv3(t, stepSize)
	db := wrapDbWithCtx(_db, agg)

	ctx := context.Background()
	rwTx, err := db.BeginTemporalRw(ctx)
	require.NoError(t, err)
	defer rwTx.Rollback()

	domains, err := NewSharedDomains(rwTx, log.New())
	require.NoError(t, err)
	defer domains.Close()

	maxTx := 3*stepSize + 10
	hashes := make([][]byte, maxTx)

	i := 0
	k0 := make([]byte, length.Addr)
	l0 := make([]byte, length.Hash)
	commitStep := 3
	accounts := 1

	var blockNum uint64
	for ; i < int(maxTx); i++ {
		txNum := uint64(i)
		domains.SetTxNum(txNum)
		for accs := 0; accs < accounts; accs++ {
			acc := accounts3.Account{
				Nonce:       uint64(i),
				Balance:     *uint256.NewInt(uint64(i*10e6) + uint64(accs*10e2)),
				CodeHash:    common.Hash{},
				Incarnation: 0,
			}
			v := accounts3.SerialiseV3(&acc)
			k0[0] = byte(accs)

			pv, step, err := domains.GetLatest(kv.AccountsDomain, rwTx, k0)
			require.NoError(t, err)

			err = domains.DomainPut(kv.AccountsDomain, rwTx, k0, v, txNum, pv, step)
			require.NoError(t, err)
			binary.BigEndian.PutUint64(l0[16:24], uint64(accs))

			for locs := 0; locs < 1000; locs++ {
				binary.BigEndian.PutUint64(l0[24:], uint64(locs))
				pv, step, err := domains.GetLatest(kv.AccountsDomain, rwTx, append(k0, l0...))
				require.NoError(t, err)

				err = domains.DomainPut(kv.StorageDomain, rwTx, composite(k0, l0), l0[24:], txNum, pv, step)
				require.NoError(t, err)
			}
		}

		if i%commitStep == 0 {
			rh, err := domains.ComputeCommitment(ctx, true, blockNum, txNum, "")
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

	{ //prune
		rwTx, err = db.BeginTemporalRw(ctx)
		require.NoError(t, err)
		defer rwTx.Rollback()
		_, err = rwTx.PruneSmallBatches(ctx, 1*time.Minute)
		require.NoError(t, err)
		err = rwTx.Commit()
		require.NoError(t, err)
	}

	rwTx, err = db.BeginTemporalRw(ctx)
	require.NoError(t, err)

	domains, err = NewSharedDomains(rwTx, log.New())
	require.NoError(t, err)
	defer domains.Close()

	txNum := domains.txNum
	for accs := 0; accs < accounts; accs++ {
		k0[0] = byte(accs)
		pv, step, err := domains.GetLatest(kv.AccountsDomain, rwTx, k0)
		require.NoError(t, err)

		existed := make(map[string]struct{})
		err = domains.IterateStoragePrefix(k0, rwTx, func(k []byte, v []byte, step kv.Step) (bool, error) {
			existed[string(k)] = struct{}{}
			return true, nil
		})
		require.NoError(t, err)

		missed := 0
		err = domains.IterateStoragePrefix(k0, rwTx, func(k []byte, v []byte, step kv.Step) (bool, error) {
			if _, been := existed[string(k)]; !been {
				missed++
			}
			return true, nil
		})
		require.NoError(t, err)
		require.Zero(t, missed)

		err = domains.deleteAccount(rwTx, string(k0), txNum, pv, step)
		require.NoError(t, err)

		notRemoved := 0
		err = domains.IterateStoragePrefix(k0, rwTx, func(k []byte, v []byte, step kv.Step) (bool, error) {
			notRemoved++
			if _, been := existed[string(k)]; !been {
				missed++
			}
			return true, nil
		})
		require.NoError(t, err)
		require.Zero(t, missed)
		require.Zero(t, notRemoved)
	}

	err = domains.Flush(ctx, rwTx)
	require.NoError(t, err)
	rwTx.Rollback()
}

func TestSharedDomain_HasPrefix_StorageDomain(t *testing.T) {
	t.Parallel()

	ctx, cancel := context.WithCancel(context.Background())
	t.Cleanup(cancel)

	logger := log.New()
	logger.SetHandler(log.LvlFilterHandler(log.LvlTrace, log.StderrHandler))

	mdbxDb := memdb.NewTestDB(t, kv.ChainDB)
	dirs := datadir.New(t.TempDir())
	_, err := GetStateIndicesSalt(dirs, true /* genNew */, logger) // gen salt needed by aggregator
	require.NoError(t, err)
	aggStep := uint64(1)
	agg, err := NewAggregator(ctx, dirs, aggStep, mdbxDb, logger)
	require.NoError(t, err)
	t.Cleanup(agg.Close)
	temporalDb, err := New(mdbxDb, agg)
	require.NoError(t, err)
	t.Cleanup(temporalDb.Close)

	rwTtx1, err := temporalDb.BeginTemporalRw(ctx)
	require.NoError(t, err)
	t.Cleanup(rwTtx1.Rollback)
	sd, err := NewSharedDomains(rwTtx1, logger)
	require.NoError(t, err)
	t.Cleanup(sd.Close)

	acc1 := common.HexToAddress("0x1234567890123456789012345678901234567890")
	acc1slot1 := common.HexToHash("0x0000000000000000000000000000000000000000000000000000000000000001")
	storageK1 := append(append([]byte{}, acc1.Bytes()...), acc1slot1.Bytes()...)
	acc2 := common.HexToAddress("0x1234567890123456789012345678901234567891")
	acc2slot2 := common.HexToHash("0x0000000000000000000000000000000000000000000000000000000000000002")
	storageK2 := append(append([]byte{}, acc2.Bytes()...), acc2slot2.Bytes()...)

	// --- check 1: non-existing storage ---
	{
		firstKey, firstVal, ok, err := sd.HasPrefix(kv.StorageDomain, acc1.Bytes(), rwTtx1)
		require.NoError(t, err)
		require.False(t, ok)
		require.Nil(t, firstKey)
		require.Nil(t, firstVal)
	}

	// --- check 2: storage exists in DB - SharedDomains.HasPrefix should catch this ---
	{
		// write to storage
		err = sd.DomainPut(kv.StorageDomain, rwTtx1, storageK1, []byte{1}, 1, nil, 0)
		require.NoError(t, err)
		// check before flush
		firstKey, firstVal, ok, err := sd.HasPrefix(kv.StorageDomain, acc1.Bytes(), rwTtx1)
		require.NoError(t, err)
		require.True(t, ok)
		require.Equal(t, append(append([]byte{}, acc1.Bytes()...), acc1slot1.Bytes()...), firstKey)
		require.Equal(t, []byte{1}, firstVal)
		// check after flush
		err = sd.Flush(ctx, rwTtx1)
		require.NoError(t, err)
		err = rwTtx1.Commit()
		require.NoError(t, err)

		// make sure it is indeed in db using a db tx
		dbRoTx1, err := mdbxDb.BeginRo(ctx)
		require.NoError(t, err)
		t.Cleanup(dbRoTx1.Rollback)
		c1, err := dbRoTx1.CursorDupSort(kv.TblStorageVals)
		require.NoError(t, err)
		t.Cleanup(c1.Close)
		k, v, err := c1.Next()
		require.NoError(t, err)
		require.Equal(t, append(append([]byte{}, acc1.Bytes()...), acc1slot1.Bytes()...), k)
		wantValueBytes := make([]byte, 8)                      // 8 bytes for uint64 step num
		binary.BigEndian.PutUint64(wantValueBytes, ^uint64(1)) // step num
		wantValueBytes = append(wantValueBytes, byte(1))       // value we wrote to the storage slot
		require.Equal(t, wantValueBytes, v)
		k, v, err = c1.Next()
		require.NoError(t, err)
		require.Nil(t, k)
		require.Nil(t, v)

		// all good
		// now move on to SharedDomains
		roTtx1, err := temporalDb.BeginTemporalRo(ctx)
		require.NoError(t, err)
		t.Cleanup(roTtx1.Rollback)

		// make sure there are no files yet and we are only hitting the DB
		require.Equal(t, uint64(0), roTtx1.Debug().TxNumsInFiles(kv.StorageDomain))

		// finally, verify SharedDomains.HasPrefix returns true
		sd.SetTxNum(2) // needed for HasPrefix (in-mem has to be ahead of tx num)
		firstKey, firstVal, ok, err = sd.HasPrefix(kv.StorageDomain, acc1.Bytes(), roTtx1)
		require.NoError(t, err)
		require.True(t, ok)
		require.Equal(t, append(append([]byte{}, acc1.Bytes()...), acc1slot1.Bytes()...), firstKey)
		require.Equal(t, []byte{1}, firstVal)

		// check some other non-existing storages for non-existence after write operation
		firstKey, firstVal, ok, err = sd.HasPrefix(kv.StorageDomain, acc2.Bytes(), roTtx1)
		require.NoError(t, err)
		require.False(t, ok)
		require.Nil(t, firstKey)
		require.Nil(t, firstVal)

		roTtx1.Rollback()
	}

	// --- check 3: storage exists in files only - SharedDomains.HasPrefix should catch this
	{
		// move data to files and trigger prune (need one more step for prune so write to some other storage)
		rwTtx2, err := temporalDb.BeginTemporalRw(ctx)
		require.NoError(t, err)
		t.Cleanup(rwTtx2.Rollback)
		err = sd.DomainPut(kv.StorageDomain, rwTtx2, storageK2, []byte{2}, 2, nil, 0)
		require.NoError(t, err)
		// check before flush
		firstKey, firstVal, ok, err := sd.HasPrefix(kv.StorageDomain, acc1.Bytes(), rwTtx2)
		require.NoError(t, err)
		require.True(t, ok)
		require.Equal(t, append(append([]byte{}, acc1.Bytes()...), acc1slot1.Bytes()...), firstKey)
		require.Equal(t, []byte{1}, firstVal)
		// check after flush
		err = sd.Flush(ctx, rwTtx2)
		require.NoError(t, err)
		err = rwTtx2.Commit()
		require.NoError(t, err)

		// build files
		err = agg.BuildFiles(2)
		require.NoError(t, err)
		rwTtx3, err := temporalDb.BeginTemporalRw(ctx)
		require.NoError(t, err)
		t.Cleanup(rwTtx3.Rollback)

		// prune
		haveMore, err := rwTtx3.PruneSmallBatches(ctx, time.Minute)
		require.NoError(t, err)
		require.False(t, haveMore)
		err = rwTtx3.Commit()
		require.NoError(t, err)

		// double check acc1 storage data not in the mdbx DB
		dbRoTx2, err := mdbxDb.BeginRo(ctx)
		require.NoError(t, err)
		t.Cleanup(dbRoTx2.Rollback)
		c2, err := dbRoTx2.CursorDupSort(kv.TblStorageVals)
		require.NoError(t, err)
		t.Cleanup(c2.Close)
		k, v, err := c2.Next() // acc2 storage from step 2 will be there
		require.NoError(t, err)
		require.Equal(t, append(append([]byte{}, acc2.Bytes()...), acc2slot2.Bytes()...), k)
		wantValueBytes := make([]byte, 8)                      // 8 bytes for uint64 step num
		binary.BigEndian.PutUint64(wantValueBytes, ^uint64(2)) // step num
		wantValueBytes = append(wantValueBytes, byte(2))       // value we wrote to the storage slot
		require.Equal(t, wantValueBytes, v)
		k, v, err = c2.Next() // acc1 storage from step 1 must not be there
		require.NoError(t, err)
		require.Nil(t, k)
		require.Nil(t, v)

		// double check files for 2 steps have been created
		roTtx2, err := temporalDb.BeginTemporalRo(ctx)
		require.NoError(t, err)
		t.Cleanup(roTtx2.Rollback)
		require.Equal(t, uint64(2), roTtx2.Debug().TxNumsInFiles(kv.StorageDomain))

		// finally, verify SharedDomains.HasPrefix returns true
		firstKey, firstVal, ok, err = sd.HasPrefix(kv.StorageDomain, acc1.Bytes(), roTtx2)
		require.NoError(t, err)
		require.True(t, ok)
		require.Equal(t, append(append([]byte{}, acc1.Bytes()...), acc1slot1.Bytes()...), firstKey)
		require.Equal(t, []byte{1}, firstVal)
		roTtx2.Rollback()
	}

	// --- check 4: delete storage - SharedDomains.HasPrefix should catch this and say it does not exist
	{
		rwTtx4, err := temporalDb.BeginTemporalRw(ctx)
		require.NoError(t, err)
		t.Cleanup(rwTtx4.Rollback)
		err = sd.DomainDelPrefix(kv.StorageDomain, rwTtx4, acc1.Bytes(), 3)
		require.NoError(t, err)
		// check before flush
		firstKey, firstVal, ok, err := sd.HasPrefix(kv.StorageDomain, acc1.Bytes(), rwTtx4)
		require.NoError(t, err)
		require.False(t, ok)
		require.Nil(t, firstKey)
		require.Nil(t, firstVal)
		// check after flush
		err = sd.Flush(ctx, rwTtx4)
		require.NoError(t, err)
		err = rwTtx4.Commit()
		require.NoError(t, err)

		roTtx3, err := temporalDb.BeginTemporalRo(ctx)
		require.NoError(t, err)
		t.Cleanup(roTtx3.Rollback)
		sd.SetTxNum(4) // needed for HasPrefix (in-mem has to be ahead of tx num)
		firstKey, firstVal, ok, err = sd.HasPrefix(kv.StorageDomain, acc1.Bytes(), roTtx3)
		require.NoError(t, err)
		require.False(t, ok)
		require.Nil(t, firstKey)
		require.Nil(t, firstVal)
		roTtx3.Rollback()
	}

	// --- check 5: write to it again after deletion - SharedDomains.HasPrefix should catch
	{
		rwTtx5, err := temporalDb.BeginTemporalRw(ctx)
		require.NoError(t, err)
		t.Cleanup(rwTtx5.Rollback)
		err = sd.DomainPut(kv.StorageDomain, rwTtx5, storageK1, []byte{3}, 4, nil, 0)
		require.NoError(t, err)
		// check before flush
		firstKey, firstVal, ok, err := sd.HasPrefix(kv.StorageDomain, acc1.Bytes(), rwTtx5)
		require.NoError(t, err)
		require.True(t, ok)
		require.Equal(t, append(append([]byte{}, acc1.Bytes()...), acc1slot1.Bytes()...), firstKey)
		require.Equal(t, []byte{3}, firstVal)
		// check after flush
		err = sd.Flush(ctx, rwTtx5)
		require.NoError(t, err)
		err = rwTtx5.Commit()
		require.NoError(t, err)

		roTtx4, err := temporalDb.BeginTemporalRo(ctx)
		require.NoError(t, err)
		t.Cleanup(roTtx4.Rollback)
		sd.SetTxNum(5) // needed for HasPrefix (in-mem has to be ahead of tx num)
		firstKey, firstVal, ok, err = sd.HasPrefix(kv.StorageDomain, acc1.Bytes(), roTtx4)
		require.NoError(t, err)
		require.True(t, ok)
		require.Equal(t, append(append([]byte{}, acc1.Bytes()...), acc1slot1.Bytes()...), firstKey)
		require.Equal(t, []byte{3}, firstVal)
		roTtx4.Rollback()
	}
}
