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

package state_test

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
	"github.com/erigontech/erigon/common/u256"
	"github.com/erigontech/erigon/common/crypto"
	"github.com/erigontech/erigon/db/datadir"
	"github.com/erigontech/erigon/db/kv"
	"github.com/erigontech/erigon/db/kv/dbcfg"
	"github.com/erigontech/erigon/db/kv/mdbx"
	"github.com/erigontech/erigon/db/kv/rawdbv3"
	"github.com/erigontech/erigon/db/kv/temporal"
	"github.com/erigontech/erigon/db/state"
	"github.com/erigontech/erigon/db/state/changeset"
	execstate "github.com/erigontech/erigon/execution/state"
	"github.com/erigontech/erigon/execution/types/accounts"
	accounts3 "github.com/erigontech/erigon/execution/types/accounts"
)

func NewTest(dirs datadir.Dirs) state.AggOpts { //nolint:gocritic
	return state.New(dirs).DisableFsync().GenSaltIfNeed(true).ReorgBlockDepth(0)
}

func newTestDb(tb testing.TB, stepSize uint64) kv.TemporalRwDB {
	tb.Helper()
	logger := log.New()
	dirs := datadir.New(tb.TempDir())
	db := mdbx.New(dbcfg.ChainDB, logger).InMem(dirs.Chaindata, false).GrowthStep(32 * datasize.MB).MapSize(2 * datasize.GB).MustOpen()
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

func TestExecutionContext_Unwind(t *testing.T) {
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

	domains, err := execstate.NewExecutionContext(ctx, rwTx, log.New())
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

	domains, err = execstate.NewExecutionContext(ctx, rwTx, log.New())
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
				CodeHash:    accounts.EmptyCodeHash,
				Incarnation: 0,
			}
			k0[0] = byte(accs)
			addr0 := accounts.BytesToAddress(k0)
			pv, step, _, err := domains.GetAccount(context.Background(), addr0, rwTx)
			require.NoError(t, err)

			err = domains.PutAccount(context.Background(), addr0, &acc, rwTx, uint64(i), execstate.ValueWithTxNum[*accounts.Account]{pv, step})
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

func TestExecutionContext_StorageIter(t *testing.T) {
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

	domains, err := execstate.NewExecutionContext(ctx, rwTx, log.New())
	require.NoError(t, err)
	defer domains.Close()

	maxTx := 3*stepSize + 10
	hashes := make([][]byte, maxTx)

	i := 0
	k0 := make([]byte, length.Addr)
	l0 := make([]byte, length.Hash)
	commitStep := 3
	noaccounts := 1

	var blockNum uint64
	for ; i < int(maxTx); i++ {
		txNum := uint64(i)
		for accs := 0; accs < noaccounts; accs++ {
			acc := accounts3.Account{
				Nonce:       uint64(i),
				Balance:     *uint256.NewInt(uint64(i*10e6) + uint64(accs*10e2)),
				CodeHash:    accounts.EmptyCodeHash,
				Incarnation: 0,
			}
			k0[0] = byte(accs)
			addr0 := accounts.BytesToAddress(k0)
			pv, step, _, err := domains.GetAccount(context.Background(), addr0, rwTx)
			require.NoError(t, err)

			err = domains.PutAccount(context.Background(), addr0, &acc, rwTx, txNum, execstate.ValueWithTxNum[*accounts.Account]{pv, step})
			require.NoError(t, err)
			binary.BigEndian.PutUint64(l0[16:24], uint64(accs))

			for locs := 0; locs < 1000; locs++ {
				binary.BigEndian.PutUint64(l0[24:], uint64(locs))
				p, txNum, ok, err := domains.GetStorage(context.Background(), addr0, accounts.BytesToKey(l0), rwTx)
				require.NoError(t, err)

				var v = u256.U64(uint64(locs + 1)) // zero not a valid storage value (==deleted)
				var pv []execstate.ValueWithTxNum[uint256.Int]

				if ok {
					pv = []execstate.ValueWithTxNum[uint256.Int]{{Value: p, TxNum: txNum}}
				}

				err = domains.PutStorage(context.Background(), addr0, accounts.BytesToKey(l0), v, rwTx, txNum, pv...)
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

	domains, err = execstate.NewExecutionContext(ctx, rwTx, log.New())
	require.NoError(t, err)
	defer domains.Close()

	txNum := domains.TxNum()
	for accs := 0; accs < noaccounts; accs++ {
		k0[0] = byte(accs)
		pv, pTxNum, _, err := domains.GetAccount(ctx, accounts.BytesToAddress(k0), rwTx)
		require.NoError(t, err)

		existed := make(map[accounts.StorageKey]struct{})
		err = domains.IterateStorage(ctx, accounts.BytesToAddress(k0),
			func(k accounts.StorageKey, v uint256.Int, step kv.Step) (bool, error) {
				existed[k] = struct{}{}
				return true, nil
			}, rwTx)
		require.NoError(t, err)

		missed := 0
		err = domains.IterateStorage(ctx, accounts.BytesToAddress(k0), func(k accounts.StorageKey, v uint256.Int, step kv.Step) (bool, error) {
			if _, been := existed[k]; !been {
				missed++
			}
			return true, nil
		}, rwTx)
		require.NoError(t, err)
		require.Zero(t, missed)

		var prev []execstate.ValueWithTxNum[*accounts.Account]
		if pv != nil {
			prev = []execstate.ValueWithTxNum[*accounts.Account]{{Value: pv, TxNum: pTxNum}}
		}
		err = domains.DelAccount(ctx, accounts.BytesToAddress(k0), rwTx, txNum, prev...)
		require.NoError(t, err)

		notRemoved := 0
		err = domains.IterateStorage(ctx, accounts.BytesToAddress(k0), func(k accounts.StorageKey, v uint256.Int, step kv.Step) (bool, error) {
			notRemoved++
			if _, been := existed[k]; !been {
				missed++
			}
			return true, nil
		}, rwTx)
		require.NoError(t, err)
		require.Zero(t, missed)
		require.Zero(t, notRemoved)
	}

	err = domains.Flush(ctx, rwTx)
	require.NoError(t, err)
	rwTx.Rollback()
}

func TestExecutionContext_IterateStorage(t *testing.T) {
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

	iterCount := func(domains *execstate.ExecutionContext) int {
		var list []accounts.StorageKey
		require.NoError(domains.IterateStorage(ctx, accounts.NilAddress, func(k accounts.StorageKey, v uint256.Int, step kv.Step) (bool, error) {
			list = append(list, k)
			return true, nil
		}, rwTx))
		return len(list)
	}

	for i := uint64(0); i < stepSize*2; i++ {
		blockNum := i
		maxTxNum := blockNum*2 - 1
		err = rawdbv3.TxNums.Append(rwTx, blockNum, maxTxNum)
		require.NoError(err)
	}

	domains, err := execstate.NewExecutionContext(ctx, rwTx, log.New())
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
	addr := accounts.BytesToAddress(acc(1))
	for i := uint64(0); i < stepSize; i++ {
		txNum := i
		if err = domains.PutAccount(ctx, addr, &accounts.Account{Nonce: i}, rwTx, txNum); err != nil {
			panic(err)
		}

		if i == 0 {
			// you can't insert 0(empty) - it should use del - check it returns an error
			var v uint256.Int
			err = domains.PutStorage(ctx, addr, accounts.BytesToKey(st(i)), v, rwTx, txNum)
			require.Error(err)
		}

		var v uint256.Int
		v.SetBytes(acc(i + 1))
		if err = domains.PutStorage(ctx, addr, accounts.BytesToKey(st(i)), v, rwTx, txNum); err != nil {
			panic(err)
		}
	}

	{ // no deletes
		err = domains.Flush(ctx, rwTx)
		require.NoError(err)
		domains.Close()

		domains, err = execstate.NewExecutionContext(ctx, rwTx, log.New())
		require.NoError(err)
		defer domains.Close()
		require.Equal(int(stepSize), iterCount(domains))
	}
	var txNum uint64
	{ // delete marker is in RAM
		require.NoError(domains.Flush(ctx, rwTx))
		domains.Close()
		domains, err = execstate.NewExecutionContext(ctx, rwTx, log.New())
		require.NoError(err)
		defer domains.Close()
		require.Equal(int(stepSize), iterCount(domains))

		txNum = stepSize
		if err := domains.DelStorage(ctx, addr, accounts.BytesToKey(st(1)), rwTx, txNum); err != nil {
			panic(err)
		}
		if err := domains.DelStorage(ctx, addr, accounts.BytesToKey(st(2)), rwTx, txNum); err != nil {
			panic(err)
		}
		for i := stepSize; i < stepSize*2+2; i++ {
			txNum = i
			if err = domains.PutAccount(ctx, addr, &accounts.Account{Nonce: i}, rwTx, txNum); err != nil {
				panic(err)
			}
			var v uint256.Int
			v.SetBytes(acc(i))
			if err = domains.PutStorage(ctx, addr, accounts.BytesToKey(st(i)), v, rwTx, txNum); err != nil {
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

		domains, err = execstate.NewExecutionContext(ctx, rwTx, log.New())
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

		domains, err = execstate.NewExecutionContext(ctx, rwTx, log.New())
		require.NoError(err)
		defer domains.Close()
		require.Equal(int(stepSize*2+2-2), iterCount(domains))
	}

	{ // delete/update more keys in RAM
		require.NoError(domains.Flush(ctx, rwTx))
		domains.Close()
		domains, err = execstate.NewExecutionContext(ctx, rwTx, log.New())
		require.NoError(err)
		defer domains.Close()

		txNum = stepSize*2 + 1
		if err := domains.DelStorage(ctx, addr, accounts.BytesToKey(st(4)), rwTx, txNum); err != nil {
			panic(err)
		}
		var v uint256.Int
		v.SetBytes(acc(5))
		if err := domains.PutStorage(ctx, addr, accounts.BytesToKey(st(5)), v, rwTx, txNum); err != nil {
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

		domains, err = execstate.NewExecutionContext(ctx, rwTx, log.New())
		require.NoError(err)
		defer domains.Close()
		require.Equal(int(stepSize*2+2-3), iterCount(domains))
	}
	{ // delete everything - must see 0
		err = domains.Flush(ctx, rwTx)
		require.NoError(err)
		domains.Close()

		domains, err = execstate.NewExecutionContext(ctx, rwTx, log.New())
		require.NoError(err)
		defer domains.Close()
		err := domains.DelStorage(ctx, accounts.NilAddress, accounts.NilKey, rwTx, txNum+1)
		require.NoError(err)
		require.Equal(0, iterCount(domains))
	}
}

func TestExecutionContext_HasPrefix_StorageDomain(t *testing.T) {
	t.Parallel()

	ctx, cancel := context.WithCancel(context.Background())
	t.Cleanup(cancel)

	stepSize := uint64(1)
	db := newTestDb(t, stepSize)

	rwTtx1, err := db.BeginTemporalRw(ctx)
	require.NoError(t, err)
	t.Cleanup(rwTtx1.Rollback)
	sd, err := execstate.NewExecutionContext(ctx, rwTtx1, log.New())
	require.NoError(t, err)
	t.Cleanup(sd.Close)

	acc1 := common.HexToAddress("0x1234567890123456789012345678901234567890")
	acc1slot1 := common.HexToHash("0x0000000000000000000000000000000000000000000000000000000000000001")
	acc2 := common.HexToAddress("0x1234567890123456789012345678901234567891")
	acc2slot2 := common.HexToHash("0x0000000000000000000000000000000000000000000000000000000000000002")

	// --- check 1: non-existing storage ---
	{
		ok, err := sd.HasStorage(ctx, accounts.InternAddress(acc1), rwTtx1)
		require.NoError(t, err)
		require.False(t, ok)
		var firstKey accounts.StorageKey
		var firstVal uint256.Int
		err = sd.IterateStorage(ctx, accounts.InternAddress(acc1),
			func(k accounts.StorageKey, v uint256.Int, step kv.Step) (cont bool, err error) {
				firstKey = k
				firstVal = v
				return false, nil
			}, rwTtx1)
		require.NoError(t, err)
		require.True(t, firstKey.IsNil(), "first key unexpectedly set")
		require.Equal(t, firstVal.ByteLen(), 0)
	}

	// --- check 2: storage exists in DB - ExecutionContexts.HasStorage should catch this ---
	{
		// write to storage
		var i uint256.Int
		i.SetBytes([]byte{1})
		err = sd.PutStorage(ctx, accounts.InternAddress(acc1), accounts.InternKey(acc1slot1), i, rwTtx1, 1)
		require.NoError(t, err)
		// check before flush
		ok, err := sd.HasStorage(ctx, accounts.InternAddress(acc1), rwTtx1)
		require.NoError(t, err)
		require.True(t, ok)
		var firstKey accounts.StorageKey
		var firstVal uint256.Int
		sd.IterateStorage(ctx, accounts.InternAddress(acc1),
			func(k accounts.StorageKey, v uint256.Int, step kv.Step) (cont bool, err error) {
				firstKey = k
				firstVal = v
				return false, nil
			}, rwTtx1)
		require.False(t, firstKey.IsNil(), "first key not set")
		require.Equal(t, acc1slot1, firstKey.Value())
		require.Equal(t, u256.Num1, firstVal)

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
		// now move on to ExecutionContexts
		roTtx1, err := db.BeginTemporalRo(ctx)
		require.NoError(t, err)
		t.Cleanup(roTtx1.Rollback)

		// make sure there are no files yet and we are only hitting the DB
		require.Equal(t, uint64(0), roTtx1.Debug().TxNumsInFiles(kv.StorageDomain))

		// finally, verify ExecutionContexts.HasStorage returns true
		sd.SetTxNum(2) // needed for HasPrefix (in-mem has to be ahead of tx num)
		ok, err = sd.HasStorage(ctx, accounts.InternAddress(acc1), roTtx1)
		require.NoError(t, err)
		require.True(t, ok)
		sd.IterateStorage(ctx, accounts.InternAddress(acc1),
			func(k accounts.StorageKey, v uint256.Int, step kv.Step) (cont bool, err error) {
				firstKey = k
				firstVal = v
				return false, nil
			}, roTtx1)
		require.False(t, firstKey.IsNil(), "first key not set")
		require.Equal(t, acc1slot1, firstKey.Value())
		require.Equal(t, u256.Num1, firstVal)

		// check some other non-existing storages for non-existence after write operation
		ok, err = sd.HasStorage(ctx, accounts.InternAddress(acc2), roTtx1)
		require.NoError(t, err)
		require.False(t, ok)
		firstKey = accounts.NilKey
		firstVal = uint256.Int{}
		sd.IterateStorage(ctx, accounts.InternAddress(acc2),
			func(k accounts.StorageKey, v uint256.Int, step kv.Step) (cont bool, err error) {
				firstKey = k
				firstVal = v
				return false, nil
			}, roTtx1)
		require.True(t, firstKey.IsNil(), "first key unexpectedly set")
		require.Equal(t, firstVal.ByteLen(), 0)

		roTtx1.Rollback()
	}

	// --- check 3: storage exists in files only - ExecutionContexts.HasStorage should catch this
	{
		// move data to files and trigger prune (need one more step for prune so write to some other storage)
		rwTtx2, err := db.BeginTemporalRw(ctx)
		require.NoError(t, err)
		t.Cleanup(rwTtx2.Rollback)
		var i uint256.Int
		i.SetBytes([]byte{2})
		err = sd.PutStorage(ctx, accounts.InternAddress(acc2), accounts.InternKey(acc2slot2), i, rwTtx2, 2)
		require.NoError(t, err)
		// check before flush
		ok, err := sd.HasStorage(ctx, accounts.InternAddress(acc1), rwTtx2)
		require.NoError(t, err)
		require.True(t, ok)
		var firstKey accounts.StorageKey
		var firstVal uint256.Int
		sd.IterateStorage(ctx, accounts.InternAddress(acc1),
			func(k accounts.StorageKey, v uint256.Int, step kv.Step) (cont bool, err error) {
				firstKey = k
				firstVal = v
				return false, nil
			}, rwTtx2)
		require.False(t, firstKey.IsNil(), "first key not set")
		require.Equal(t, acc1slot1, firstKey.Value())
		require.Equal(t, u256.Num1, firstVal)
		// check after flush
		err = sd.Flush(ctx, rwTtx2)
		require.NoError(t, err)
		err = rwTtx2.Commit()
		roTtx2, err := db.BeginTemporalRo(ctx)
		require.NoError(t, err)
		t.Cleanup(roTtx2.Rollback)
		require.NoError(t, err)
		firstKey = accounts.NilKey
		firstVal = uint256.Int{}
		sd.IterateStorage(ctx, accounts.InternAddress(acc1),
			func(k accounts.StorageKey, v uint256.Int, step kv.Step) (cont bool, err error) {
				firstKey = k
				firstVal = v
				return false, nil
			}, roTtx2)
		require.False(t, firstKey.IsNil(), "first key not set")
		require.Equal(t, acc1slot1, firstKey.Value())
		require.Equal(t, u256.Num1, firstVal)
		roTtx2.Rollback()

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
		roTtx2, err = db.BeginTemporalRo(ctx)
		require.NoError(t, err)
		t.Cleanup(roTtx2.Rollback)
		require.Equal(t, uint64(2), roTtx2.Debug().TxNumsInFiles(kv.StorageDomain))

		// finally, verify ExecutionContexts.HasStorage returns true
		ok, err = sd.HasStorage(ctx, accounts.InternAddress(acc1), roTtx2)
		require.NoError(t, err)
		require.True(t, ok)
		firstKey = accounts.NilKey
		firstVal = uint256.Int{}
		sd.IterateStorage(ctx, accounts.InternAddress(acc1),
			func(k accounts.StorageKey, v uint256.Int, step kv.Step) (cont bool, err error) {
				firstKey = k
				firstVal = v
				return false, nil
			}, roTtx2)
		require.False(t, firstKey.IsNil(), "first key not set")
		require.Equal(t, acc1slot1, firstKey.Value())
		require.Equal(t, u256.Num1, firstVal)
		roTtx2.Rollback()
	}

	// --- check 4: delete storage - ExecutionContexts.HasStorage should catch this and say it does not exist
	{
		rwTtx4, err := db.BeginTemporalRw(ctx)
		require.NoError(t, err)
		t.Cleanup(rwTtx4.Rollback)
		err = sd.DelStorage(ctx, accounts.InternAddress(acc1), accounts.NilKey, rwTtx4, 3)
		require.NoError(t, err)
		// check before flush
		ok, err := sd.HasStorage(ctx, accounts.InternAddress(acc1), rwTtx4)
		require.NoError(t, err)
		require.False(t, ok)
		var firstKey accounts.StorageKey
		var firstVal uint256.Int
		sd.IterateStorage(ctx, accounts.InternAddress(acc1),
			func(k accounts.StorageKey, v uint256.Int, step kv.Step) (cont bool, err error) {
				firstKey = k
				firstVal = v
				return false, nil
			}, rwTtx4)
		require.True(t, firstKey.IsNil(), "first key unexpectedly set")
		require.Equal(t, firstVal.ByteLen(), 0)
		// check after flush
		err = sd.Flush(ctx, rwTtx4)
		require.NoError(t, err)
		err = rwTtx4.Commit()
		require.NoError(t, err)

		roTtx3, err := db.BeginTemporalRo(ctx)
		require.NoError(t, err)
		t.Cleanup(roTtx3.Rollback)
		sd.SetTxNum(4) // needed for HasPrefix (in-mem has to be ahead of tx num)
		ok, err = sd.HasStorage(ctx, accounts.InternAddress(acc1), roTtx3)
		require.NoError(t, err)
		require.False(t, ok)
		sd.IterateStorage(ctx, accounts.InternAddress(acc1),
			func(k accounts.StorageKey, v uint256.Int, step kv.Step) (cont bool, err error) {
				firstKey = k
				firstVal = v
				return false, nil
			}, roTtx3)
		require.True(t, firstKey.IsNil(), "first key unexpectedly set")
		require.Equal(t, firstVal.ByteLen(), 0)
		roTtx3.Rollback()
	}

	// --- check 5: write to it again after deletion - ExecutionContexts.HasStorage should catch
	{
		rwTtx5, err := db.BeginTemporalRw(ctx)
		require.NoError(t, err)
		t.Cleanup(rwTtx5.Rollback)
		var i uint256.Int
		i.SetBytes([]byte{3})
		err = sd.PutStorage(ctx, accounts.InternAddress(acc1), accounts.InternKey(acc1slot1), i, rwTtx5, 4)
		require.NoError(t, err)
		// check before flush
		ok, err := sd.HasStorage(ctx, accounts.InternAddress(acc1), rwTtx5)
		require.NoError(t, err)
		require.True(t, ok)
		var firstKey accounts.StorageKey
		var firstVal uint256.Int
		sd.IterateStorage(ctx, accounts.InternAddress(acc1),
			func(k accounts.StorageKey, v uint256.Int, step kv.Step) (cont bool, err error) {
				firstKey = k
				firstVal = v
				return false, nil
			}, rwTtx5)
		require.False(t, firstKey.IsNil(), "first key not set")
		require.Equal(t, acc1slot1, firstKey.Value())
		require.Equal(t, u256.U64(3), firstVal)
		// check after flush
		err = sd.Flush(ctx, rwTtx5)
		require.NoError(t, err)
		err = rwTtx5.Commit()
		require.NoError(t, err)

		roTtx4, err := db.BeginTemporalRo(ctx)
		require.NoError(t, err)
		t.Cleanup(roTtx4.Rollback)
		sd.SetTxNum(5) // needed for HasPrefix (in-mem has to be ahead of tx num)
		ok, err = sd.HasStorage(ctx, accounts.InternAddress(acc1), roTtx4)
		require.NoError(t, err)
		require.True(t, ok)
		firstKey = accounts.NilKey
		firstVal = uint256.Int{}
		sd.IterateStorage(ctx, accounts.InternAddress(acc1),
			func(k accounts.StorageKey, v uint256.Int, step kv.Step) (cont bool, err error) {
				firstKey = k
				firstVal = v
				return false, nil
			}, roTtx4)
		require.False(t, firstKey.IsNil(), "first key not set")
		require.Equal(t, acc1slot1, firstKey.Value())
		require.Equal(t, u256.U64(3), firstVal)
		roTtx4.Rollback()
	}
}

func TestExecutionContext_GetAsOf(t *testing.T) {
	if testing.Short() {
		t.Skip()
	}
	t.Parallel()

	stepSize := uint64(100)
	db := newTestDb(t, stepSize)
	ctx := context.Background()

	rwTx, err := db.BeginTemporalRw(ctx)
	require.NoError(t, err)
	defer rwTx.Rollback()

	domains, err := execstate.NewExecutionContext(ctx, rwTx, log.New())
	require.NoError(t, err)
	defer domains.Close()

	// --- Setup: create test keys ---
	k0 := make([]byte, length.Addr)
	k0[0] = 0xAA
	addr0 := accounts.BytesToAddress(k0)

	k1 := make([]byte, length.Addr)
	k1[0] = 0xBB
	addr1 := accounts.BytesToAddress(k1)

	storageKey := make([]byte, length.Hash)
	storageKey[0] = 0xCC
	sKey := accounts.BytesToKey(storageKey)

	codeAddr := make([]byte, length.Addr)
	codeAddr[0] = 0xDD
	addrCode := accounts.BytesToAddress(codeAddr)

	// --- Write account data at multiple txNums ---
	// addr0: txNum=0 (nonce=0), txNum=5 (nonce=5), txNum=10 (nonce=10)
	for _, tc := range []struct {
		txNum uint64
		nonce uint64
	}{
		{0, 0},
		{5, 5},
		{10, 10},
	} {
		acc := accounts.Account{Nonce: tc.nonce, Balance: *uint256.NewInt(tc.nonce * 100), CodeHash: accounts.EmptyCodeHash}
		domains.SetTxNum(tc.txNum)
		err = domains.PutAccount(ctx, addr0, &acc, rwTx, tc.txNum)
		require.NoError(t, err)
	}

	// addr1: txNum=1 (nonce=1), txNum=6 (nonce=6)
	for _, tc := range []struct {
		txNum uint64
		nonce uint64
	}{
		{1, 1},
		{6, 6},
	} {
		acc := accounts.Account{Nonce: tc.nonce, Balance: *uint256.NewInt(tc.nonce * 200), CodeHash: accounts.EmptyCodeHash}
		domains.SetTxNum(tc.txNum)
		err = domains.PutAccount(ctx, addr1, &acc, rwTx, tc.txNum)
		require.NoError(t, err)
	}

	// --- Write storage data ---
	// addr0/sKey: txNum=2 (val=100), txNum=7 (val=200)
	domains.SetTxNum(2)
	err = domains.PutStorage(ctx, addr0, sKey, u256.U64(100), rwTx, 2)
	require.NoError(t, err)
	domains.SetTxNum(7)
	err = domains.PutStorage(ctx, addr0, sKey, u256.U64(200), rwTx, 7)
	require.NoError(t, err)

	// --- Write code data ---
	// addrCode: txNum=3
	testCode := []byte{0x60, 0x00, 0x60, 0x00, 0xFD} // PUSH0 PUSH0 REVERT
	codeHash := accounts.InternCodeHash(crypto.Keccak256Hash(testCode))
	domains.SetTxNum(3)
	// Put account first so code domain has context
	accWithCode := accounts.Account{Nonce: 1, CodeHash: codeHash}
	err = domains.PutAccount(ctx, addrCode, &accWithCode, rwTx, 3)
	require.NoError(t, err)
	err = domains.PutCode(ctx, addrCode, accounts.NilCodeHash, testCode, rwTx, 3)
	require.NoError(t, err)

	// =====================================================
	// Phase 1: Local lookups (values in d.updates)
	// =====================================================
	t.Run("Phase1_LocalLookups", func(t *testing.T) {
		// Account lookups via ExecutionContext.GetAsOf (byte interface)
		addrKey0 := addr0.Value()

		// ts=0: no value before txNum=0
		_, ok, err := domains.GetAsOf(kv.AccountsDomain, addrKey0[:], 0)
		require.NoError(t, err)
		require.False(t, ok, "should not find value before txNum=0")

		// ts=1: should return account written at txNum=0 (nonce=0)
		v, ok, err := domains.GetAsOf(kv.AccountsDomain, addrKey0[:], 1)
		require.NoError(t, err)
		require.True(t, ok, "should find value at ts=1")
		var a accounts.Account
		require.NoError(t, accounts.DeserialiseV3(&a, v))
		require.Equal(t, uint64(0), a.Nonce)

		// ts=5: should return account at txNum=0 (nonce=0), since ts > 0 and ts <= 5
		v, ok, err = domains.GetAsOf(kv.AccountsDomain, addrKey0[:], 5)
		require.NoError(t, err)
		require.True(t, ok)
		require.NoError(t, accounts.DeserialiseV3(&a, v))
		require.Equal(t, uint64(0), a.Nonce, "at ts=5 should get value from txNum=0")

		// ts=6: should return account at txNum=5 (nonce=5)
		v, ok, err = domains.GetAsOf(kv.AccountsDomain, addrKey0[:], 6)
		require.NoError(t, err)
		require.True(t, ok)
		require.NoError(t, accounts.DeserialiseV3(&a, v))
		require.Equal(t, uint64(5), a.Nonce)

		// ts=7: between txNum=5 and txNum=10, should return nonce=5
		v, ok, err = domains.GetAsOf(kv.AccountsDomain, addrKey0[:], 7)
		require.NoError(t, err)
		require.True(t, ok)
		require.NoError(t, accounts.DeserialiseV3(&a, v))
		require.Equal(t, uint64(5), a.Nonce)

		// ts=11: after last write, should return nonce=10
		v, ok, err = domains.GetAsOf(kv.AccountsDomain, addrKey0[:], 11)
		require.NoError(t, err)
		require.True(t, ok)
		require.NoError(t, accounts.DeserialiseV3(&a, v))
		require.Equal(t, uint64(10), a.Nonce)

		// addr1 lookups
		addrKey1 := addr1.Value()
		v, ok, err = domains.GetAsOf(kv.AccountsDomain, addrKey1[:], 2)
		require.NoError(t, err)
		require.True(t, ok)
		require.NoError(t, accounts.DeserialiseV3(&a, v))
		require.Equal(t, uint64(1), a.Nonce)

		v, ok, err = domains.GetAsOf(kv.AccountsDomain, addrKey1[:], 7)
		require.NoError(t, err)
		require.True(t, ok)
		require.NoError(t, accounts.DeserialiseV3(&a, v))
		require.Equal(t, uint64(6), a.Nonce)

		// Storage lookups via byte interface
		storageComposite := composite(addrKey0[:], storageKey)
		v, ok, err = domains.GetAsOf(kv.StorageDomain, storageComposite, 3)
		require.NoError(t, err)
		require.True(t, ok)
		var sval uint256.Int
		sval.SetBytes(v)
		require.Equal(t, u256.U64(100), sval)

		v, ok, err = domains.GetAsOf(kv.StorageDomain, storageComposite, 8)
		require.NoError(t, err)
		require.True(t, ok)
		sval.SetBytes(v)
		require.Equal(t, u256.U64(200), sval)

		// Code lookups via byte interface
		codeAddrKey := addrCode.Value()
		v, ok, err = domains.GetAsOf(kv.CodeDomain, codeAddrKey[:], 4)
		require.NoError(t, err)
		require.True(t, ok)
		require.Equal(t, testCode, v)
	})

	// =====================================================
	// Phase 2: After flush (updates cleared)
	// =====================================================
	t.Run("Phase2_AfterFlush", func(t *testing.T) {
		err = domains.Flush(ctx, rwTx)
		require.NoError(t, err)

		addrKey0 := addr0.Value()

		// After FlushUpdates, d.updates is cleared, so GetAsOf returns not-found
		_, ok, err := domains.GetAsOf(kv.AccountsDomain, addrKey0[:], 6)
		require.NoError(t, err)
		require.False(t, ok, "after flush, in-memory updates should be cleared")

		// But GetAccount (latest) should still work via cache/DB
		a, _, ok, err := domains.GetAccount(ctx, addr0, rwTx)
		require.NoError(t, err)
		require.True(t, ok)
		require.Equal(t, uint64(10), a.Nonce, "latest account should still be accessible after flush")
	})

	// Commit the transaction
	err = rwTx.Commit()
	require.NoError(t, err)

	// =====================================================
	// Phase 3: Historic reads via temporal tx
	// =====================================================
	t.Run("Phase3_HistoricReads", func(t *testing.T) {
		rwTx2, err := db.BeginTemporalRw(ctx)
		require.NoError(t, err)
		defer rwTx2.Rollback()

		addrKey0 := addr0.Value()

		// Account historic reads through temporal tx
		v, ok, err := rwTx2.GetAsOf(kv.AccountsDomain, addrKey0[:], 1)
		require.NoError(t, err)
		require.True(t, ok, "historic read at ts=1 should find value")
		var a accounts.Account
		require.NoError(t, accounts.DeserialiseV3(&a, v))
		require.Equal(t, uint64(0), a.Nonce)

		v, ok, err = rwTx2.GetAsOf(kv.AccountsDomain, addrKey0[:], 6)
		require.NoError(t, err)
		require.True(t, ok)
		require.NoError(t, accounts.DeserialiseV3(&a, v))
		require.Equal(t, uint64(5), a.Nonce)

		v, ok, err = rwTx2.GetAsOf(kv.AccountsDomain, addrKey0[:], 11)
		require.NoError(t, err)
		require.True(t, ok)
		require.NoError(t, accounts.DeserialiseV3(&a, v))
		require.Equal(t, uint64(10), a.Nonce)

		// Storage historic reads
		storageComposite := composite(addrKey0[:], storageKey)
		v, ok, err = rwTx2.GetAsOf(kv.StorageDomain, storageComposite, 3)
		require.NoError(t, err)
		require.True(t, ok)
		var sval uint256.Int
		sval.SetBytes(v)
		require.Equal(t, u256.U64(100), sval)

		v, ok, err = rwTx2.GetAsOf(kv.StorageDomain, storageComposite, 8)
		require.NoError(t, err)
		require.True(t, ok)
		sval.SetBytes(v)
		require.Equal(t, u256.U64(200), sval)

		// Code historic reads
		codeAddrKey := addrCode.Value()
		v, ok, err = rwTx2.GetAsOf(kv.CodeDomain, codeAddrKey[:], 4)
		require.NoError(t, err)
		require.True(t, ok)
		require.Equal(t, testCode, v)

		// Fresh ExecutionContext on new tx: GetAsOf returns not-found (empty updates)
		domains2, err := execstate.NewExecutionContext(ctx, rwTx2, log.New())
		require.NoError(t, err)
		defer domains2.Close()

		_, ok, err = domains2.GetAsOf(kv.AccountsDomain, addrKey0[:], 6)
		require.NoError(t, err)
		require.False(t, ok, "fresh ExecutionContext should have empty updates")

		// But GetAccount should return the latest value from DB
		a2, _, ok, err := domains2.GetAccount(ctx, addr0, rwTx2)
		require.NoError(t, err)
		require.True(t, ok)
		require.Equal(t, uint64(10), a2.Nonce)
	})
}
