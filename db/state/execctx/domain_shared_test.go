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

package execctx_test

import (
	"context"
	"encoding/binary"
	"fmt"
	"math/rand/v2"
	"testing"
	"time"

	"github.com/c2h5oh/datasize"
	"github.com/holiman/uint256"
	"github.com/stretchr/testify/require"

	"github.com/erigontech/erigon/common"
	"github.com/erigontech/erigon/common/length"
	"github.com/erigontech/erigon/common/log/v3"
	"github.com/erigontech/erigon/db/datadir"
	"github.com/erigontech/erigon/db/kv"
	"github.com/erigontech/erigon/db/kv/dbcfg"
	"github.com/erigontech/erigon/db/kv/mdbx"
	"github.com/erigontech/erigon/db/kv/rawdbv3"
	"github.com/erigontech/erigon/db/kv/temporal"
	"github.com/erigontech/erigon/db/state"
	"github.com/erigontech/erigon/db/state/changeset"
	"github.com/erigontech/erigon/db/state/execctx"
	accounts3 "github.com/erigontech/erigon/execution/types/accounts"
)

func NewTest(dirs datadir.Dirs) state.AggOpts { //nolint:gocritic
	return state.New(dirs).DisableFsync().GenSaltIfNeed(true).ReorgBlockDepth(0)
}

func newTestDb(tb testing.TB, stepSize uint64) kv.TemporalRwDB {
	tb.Helper()
	logger := log.New()
	dirs := datadir.New(tb.TempDir())
	db := mdbx.New(dbcfg.ChainDB, logger).InMem(tb, dirs.Chaindata).GrowthStep(32 * datasize.MB).MapSize(2 * datasize.GB).MustOpen()
	tb.Cleanup(db.Close)

	agg := NewTest(dirs).StepSize(stepSize).Logger(logger).MustOpen(tb.Context(), db)
	tb.Cleanup(agg.Close)
	err := agg.OpenFolder()
	require.NoError(tb, err)
	tdb, err := temporal.New(db, agg, nil)
	require.NoError(tb, err)
	return tdb
}

func composite(k, k2 []byte) []byte {
	return append(common.Copy(k), k2...)
}

func TestSharedDomain_Unwind(t *testing.T) {
	if testing.Short() {
		t.Skip()
	}

	t.Parallel()

	stepSize := uint64(100)
	db := newTestDb(t, stepSize)
	//db := wrapDbWithCtx(_db, agg)

	ctx := context.Background()
	rwTx, err := db.BeginTemporalRw(ctx)
	require.NoError(t, err)
	defer rwTx.Rollback()

	domains, err := execctx.NewSharedDomains(rwTx, log.New())
	require.NoError(t, err)
	defer domains.Close()

	stateChangeset := &changeset.StateChangeSet{}
	domains.SetChangesetAccumulator(stateChangeset)

	maxTx := stepSize
	hashes := make([][]byte, maxTx)
	count := 10

	err = rwTx.Commit()
	require.NoError(t, err)

Loop:
	rwTx, err = db.BeginTemporalRw(ctx)
	require.NoError(t, err)
	defer rwTx.Rollback()

	domains, err = execctx.NewSharedDomains(rwTx, log.New())
	require.NoError(t, err)
	defer domains.Close()

	i := 0
	k0 := make([]byte, length.Addr)
	commitStep := 3

	var blockNum uint64
	for ; i < int(maxTx); i++ {
		txNum := uint64(i)
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
			rh, err := domains.ComputeCommitment(ctx, rwTx, true, blockNum, txNum, "", nil)
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

	unwindTo := uint64(commitStep * rand.IntN(int(maxTx)/commitStep))
	//domains.currentChangesAccumulator = nil

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

func TestSharedDomain_StorageIter(t *testing.T) {
	if testing.Short() {
		t.Skip()
	}

	t.Parallel()

	log.Root().SetHandler(log.LvlFilterHandler(log.LvlWarn, log.StderrHandler))

	stepSize := uint64(4)
	db := newTestDb(t, stepSize)
	//db := wrapDbWithCtx(_db, agg)

	ctx := context.Background()
	rwTx, err := db.BeginTemporalRw(ctx)
	require.NoError(t, err)
	defer rwTx.Rollback()

	domains, err := execctx.NewSharedDomains(rwTx, log.New())
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
			rh, err := domains.ComputeCommitment(ctx, rwTx, true, blockNum, txNum, "", nil)
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

	err = db.(state.HasAgg).Agg().(*state.Aggregator).BuildFiles(maxTx - stepSize)
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

	domains, err = execctx.NewSharedDomains(rwTx, log.New())
	require.NoError(t, err)
	defer domains.Close()

	txNum := domains.TxNum()
	for accs := 0; accs < accounts; accs++ {
		k0[0] = byte(accs)
		pv, step, err := domains.GetLatest(kv.AccountsDomain, rwTx, k0)
		require.NoError(t, err)

		existed := make(map[string]struct{})
		err = domains.IteratePrefix(kv.StorageDomain, k0, rwTx, func(k []byte, v []byte, step kv.Step) (bool, error) {
			existed[string(k)] = struct{}{}
			return true, nil
		})
		require.NoError(t, err)

		missed := 0
		err = domains.IteratePrefix(kv.StorageDomain, k0, rwTx, func(k []byte, v []byte, step kv.Step) (bool, error) {
			if _, been := existed[string(k)]; !been {
				missed++
			}
			return true, nil
		})
		require.NoError(t, err)
		require.Zero(t, missed)

		err = domains.DomainDel(kv.AccountsDomain, rwTx, k0, txNum, pv, step)
		require.NoError(t, err)

		notRemoved := 0
		err = domains.IteratePrefix(kv.StorageDomain, k0, rwTx, func(k []byte, v []byte, step kv.Step) (bool, error) {
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

func TestSharedDomain_IteratePrefix(t *testing.T) {
	if testing.Short() {
		t.Skip()
	}

	t.Parallel()

	stepSize := uint64(8)
	require := require.New(t)
	db := newTestDb(t, stepSize)

	ctx := context.Background()
	rwTx, err := db.BeginTemporalRw(ctx)
	require.NoError(err)
	defer rwTx.Rollback()

	iterCount := func(domains *execctx.SharedDomains) int {
		var list [][]byte
		require.NoError(domains.IteratePrefix(kv.StorageDomain, nil, rwTx, func(k []byte, v []byte, step kv.Step) (bool, error) {
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

	domains, err := execctx.NewSharedDomains(rwTx, log.New())
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
		txNum := i
		if err = domains.DomainPut(kv.AccountsDomain, rwTx, addr, acc(i), txNum, nil, 0); err != nil {
			panic(err)
		}
		if err = domains.DomainPut(kv.StorageDomain, rwTx, composite(addr, st(i)), acc(i), txNum, nil, 0); err != nil {
			panic(err)
		}
	}

	{ // no deletes
		err = domains.Flush(ctx, rwTx)
		require.NoError(err)
		domains.Close()

		domains, err = execctx.NewSharedDomains(rwTx, log.New())
		require.NoError(err)
		defer domains.Close()
		require.Equal(int(stepSize), iterCount(domains))
	}
	var txNum uint64
	{ // delete marker is in RAM
		require.NoError(domains.Flush(ctx, rwTx))
		domains.Close()
		domains, err = execctx.NewSharedDomains(rwTx, log.New())
		require.NoError(err)
		defer domains.Close()
		require.Equal(int(stepSize), iterCount(domains))

		txNum = stepSize
		if err := domains.DomainDel(kv.StorageDomain, rwTx, append(addr, st(1)...), txNum, nil, 0); err != nil {
			panic(err)
		}
		if err := domains.DomainDel(kv.StorageDomain, rwTx, append(addr, st(2)...), txNum, nil, 0); err != nil {
			panic(err)
		}
		for i := stepSize; i < stepSize*2+2; i++ {
			txNum = i
			if err = domains.DomainPut(kv.AccountsDomain, rwTx, addr, acc(i), txNum, nil, 0); err != nil {
				panic(err)
			}
			if err = domains.DomainPut(kv.StorageDomain, rwTx, composite(addr, st(i)), acc(i), txNum, nil, 0); err != nil {
				panic(err)
			}
		}
		require.Equal(int(stepSize*2+2-2), iterCount(domains))
	}
	{ // delete marker is in DB
		_, err = domains.ComputeCommitment(ctx, rwTx, true, txNum/2, txNum, "", nil)
		require.NoError(err)
		err = domains.Flush(ctx, rwTx)
		require.NoError(err)
		domains.Close()

		domains, err = execctx.NewSharedDomains(rwTx, log.New())
		require.NoError(err)
		defer domains.Close()
		require.Equal(int(stepSize*2+2-2), iterCount(domains))
	}
	{ //delete marker is in Files
		domains.Close()
		err = rwTx.Commit() // otherwise agg.BuildFiles will not see data
		require.NoError(err)
		require.NoError(db.(state.HasAgg).Agg().(*state.Aggregator).BuildFiles(stepSize * 2))

		rwTx, err = db.BeginTemporalRw(ctx)
		require.NoError(err)
		defer rwTx.Rollback()

		ac := state.AggTx(rwTx)
		require.Equal(int(stepSize*2), int(ac.TxNumsInFiles(kv.StateDomains...)))

		_, err := ac.PruneSmallBatches(ctx, time.Hour, rwTx)
		require.NoError(err)

		domains, err = execctx.NewSharedDomains(rwTx, log.New())
		require.NoError(err)
		defer domains.Close()
		require.Equal(int(stepSize*2+2-2), iterCount(domains))
	}

	{ // delete/update more keys in RAM
		require.NoError(domains.Flush(ctx, rwTx))
		domains.Close()
		domains, err = execctx.NewSharedDomains(rwTx, log.New())
		require.NoError(err)
		defer domains.Close()

		txNum = stepSize*2 + 1
		if err := domains.DomainDel(kv.StorageDomain, rwTx, append(addr, st(4)...), txNum, nil, 0); err != nil {
			panic(err)
		}
		if err := domains.DomainPut(kv.StorageDomain, rwTx, append(addr, st(5)...), acc(5), txNum, nil, 0); err != nil {
			panic(err)
		}
		require.Equal(int(stepSize*2+2-3), iterCount(domains))
	}
	{ // flush delete/updates to DB
		_, err = domains.ComputeCommitment(ctx, rwTx, true, txNum/2, txNum, "", nil)
		require.NoError(err)
		err = domains.Flush(ctx, rwTx)
		require.NoError(err)
		domains.Close()

		domains, err = execctx.NewSharedDomains(rwTx, log.New())
		require.NoError(err)
		defer domains.Close()
		require.Equal(int(stepSize*2+2-3), iterCount(domains))
	}
	{ // delete everything - must see 0
		err = domains.Flush(ctx, rwTx)
		require.NoError(err)
		domains.Close()

		domains, err = execctx.NewSharedDomains(rwTx, log.New())
		require.NoError(err)
		defer domains.Close()
		err := domains.DomainDelPrefix(kv.StorageDomain, rwTx, []byte{}, txNum+1)
		require.NoError(err)
		require.Equal(0, iterCount(domains))
	}
}

func TestSharedDomain_HasPrefix_StorageDomain(t *testing.T) {
	t.Parallel()

	ctx, cancel := context.WithCancel(context.Background())
	t.Cleanup(cancel)

	stepSize := uint64(1)
	db := newTestDb(t, stepSize)

	rwTtx1, err := db.BeginTemporalRw(ctx)
	require.NoError(t, err)
	t.Cleanup(rwTtx1.Rollback)
	sd, err := execctx.NewSharedDomains(rwTtx1, log.New())
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
		dbRoTx1, err := db.BeginRo(ctx)
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
		roTtx1, err := db.BeginTemporalRo(ctx)
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
		rwTtx2, err := db.BeginTemporalRw(ctx)
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
		err = db.(state.HasAgg).Agg().(*state.Aggregator).BuildFiles(2)
		require.NoError(t, err)
		rwTtx3, err := db.BeginTemporalRw(ctx)
		require.NoError(t, err)
		t.Cleanup(rwTtx3.Rollback)

		// prune
		haveMore, err := rwTtx3.PruneSmallBatches(ctx, time.Minute)
		require.NoError(t, err)
		require.False(t, haveMore)
		err = rwTtx3.Commit()
		require.NoError(t, err)

		// double check acc1 storage data not in the mdbx DB
		dbRoTx2, err := db.BeginRo(ctx)
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
		roTtx2, err := db.BeginTemporalRo(ctx)
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
		rwTtx4, err := db.BeginTemporalRw(ctx)
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

		roTtx3, err := db.BeginTemporalRo(ctx)
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
		rwTtx5, err := db.BeginTemporalRw(ctx)
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

		roTtx4, err := db.BeginTemporalRo(ctx)
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
