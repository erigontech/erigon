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
	"encoding/hex"
	"math"
	"math/rand"
	"path/filepath"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/holiman/uint256"
	"github.com/stretchr/testify/require"

	"github.com/erigontech/erigon/common"
	"github.com/erigontech/erigon/common/dir"
	"github.com/erigontech/erigon/common/length"
	"github.com/erigontech/erigon/common/log/v3"
	"github.com/erigontech/erigon/db/config3"
	"github.com/erigontech/erigon/db/kv"
	"github.com/erigontech/erigon/db/kv/dbcfg"
	"github.com/erigontech/erigon/db/kv/mdbx"
	"github.com/erigontech/erigon/db/kv/order"
	"github.com/erigontech/erigon/db/kv/rawdbv3"
	"github.com/erigontech/erigon/db/kv/stream"
	"github.com/erigontech/erigon/db/kv/temporal"
	"github.com/erigontech/erigon/db/state"
	"github.com/erigontech/erigon/execution/commitment"
	"github.com/erigontech/erigon/execution/commitment/commitmentdb"
	"github.com/erigontech/erigon/execution/types/accounts"
)

func TestAggregatorV3_RestartOnFiles(t *testing.T) {
	if testing.Short() {
		t.Skip()
	}

	t.Parallel()

	logger := log.New()
	stepSize := uint64(100)
	ctx := context.Background()
	db, agg := testDbAndAggregatorv3(t, stepSize)
	dirs := agg.Dirs()

	tx, err := db.BeginTemporalRw(context.Background())
	require.NoError(t, err)
	defer tx.Rollback()

	domains, err := state.NewSharedDomains(tx, log.New())
	require.NoError(t, err)
	defer domains.Close()

	txs := stepSize * 5
	t.Logf("step=%d tx_count=%d\n", stepSize, txs)

	rnd := newRnd(0)
	keys := make([][]byte, txs)

	hph := commitment.NewHexPatriciaHashed(1, nil)

	for txNum := uint64(1); txNum <= txs; txNum++ {
		addr, loc := make([]byte, length.Addr), make([]byte, length.Hash)
		n, err := rnd.Read(addr)
		require.NoError(t, err)
		require.Equal(t, length.Addr, n)

		n, err = rnd.Read(loc)
		require.NoError(t, err)
		require.Equal(t, length.Hash, n)

		acc := accounts.Account{
			Nonce:       txNum,
			Balance:     *uint256.NewInt(1000000000000),
			CodeHash:    common.Hash{},
			Incarnation: 0,
		}
		buf := accounts.SerialiseV3(&acc)
		err = domains.DomainPut(kv.AccountsDomain, tx, addr, buf[:], txNum, nil, 0)
		require.NoError(t, err)

		err = domains.DomainPut(kv.StorageDomain, tx, composite(addr, loc), []byte{addr[0], loc[0]}, txNum, nil, 0)
		require.NoError(t, err)

		keys[txNum-1] = append(addr, loc...)

		if (txNum+1)%stepSize == 0 {
			trieState, err := hph.EncodeCurrentState(nil)
			require.NoError(t, err)
			cs := commitmentdb.NewCommitmentState(domains.TxNum(), 0, trieState)
			encodedState, err := cs.Encode()
			require.NoError(t, err)
			err = domains.DomainPut(kv.CommitmentDomain, tx, KeyCommitmentState, encodedState, txNum, nil, 0)
			require.NoError(t, err)
		}
	}

	// flush and build files
	err = domains.Flush(context.Background(), tx)
	require.NoError(t, err)

	progress := tx.Debug().DomainProgress(kv.AccountsDomain)
	require.Equal(t, 5, int(progress/stepSize))

	err = tx.Commit()
	require.NoError(t, err)

	err = agg.BuildFiles(txs)
	require.NoError(t, err)

	agg.Close()
	db.Close()

	// remove database files
	require.NoError(t, dir.RemoveAll(dirs.Chaindata))

	// open new db and aggregator instances
	newDb := mdbx.New(dbcfg.ChainDB, logger).InMem(t, dirs.Chaindata).MustOpen()
	t.Cleanup(newDb.Close)

	newAgg := state.New(agg.Dirs()).StepSize(stepSize).MustOpen(ctx, newDb)
	require.NoError(t, newAgg.OpenFolder())

	db, _ = temporal.New(newDb, newAgg)

	tx, err = db.BeginTemporalRw(context.Background())
	require.NoError(t, err)
	defer tx.Rollback()

	newDoms, err := state.NewSharedDomains(tx, log.New())
	require.NoError(t, err)
	defer newDoms.Close()

	err = newDoms.SeekCommitment(ctx, tx)
	require.NoError(t, err)
	latestTx := newDoms.TxNum()
	t.Logf("seek to latest_tx=%d", latestTx)

	miss := uint64(0)
	for i, key := range keys {
		if uint64(i+1) >= txs-stepSize {
			continue // finishtx always stores last agg step in db which we deleted, so missing  values which were not aggregated is expected
		}
		stored, _, err := tx.GetLatest(kv.AccountsDomain, key[:length.Addr])
		require.NoError(t, err)
		if len(stored) == 0 {
			miss++
			//fmt.Printf("%x [%d/%d]", key, miss, i+1) // txnum starts from 1
			continue
		}
		acc := accounts.Account{}
		err = accounts.DeserialiseV3(&acc, stored)
		require.NoError(t, err)

		require.Equal(t, i+1, int(acc.Nonce))

		storedV, _, err := tx.GetLatest(kv.StorageDomain, key)
		require.NoError(t, err)
		require.NotEmpty(t, storedV)
		_ = key[0]
		_ = storedV[0]
		require.Equal(t, key[0], storedV[0])
		require.Equal(t, key[length.Addr], storedV[1])
	}
	newAgg.Close()

	require.NoError(t, err)
}

func TestAggregatorV3_ReplaceCommittedKeys(t *testing.T) {
	if testing.Short() {
		t.Skip()
	}

	t.Parallel()
	ctx := context.Background()
	aggStep := uint64(20)

	db, _ := testDbAndAggregatorv3(t, aggStep)

	tx, err := db.BeginTemporalRw(context.Background())
	require.NoError(t, err)
	defer tx.Rollback()

	domains, err := state.NewSharedDomains(tx, log.New())
	require.NoError(t, err)
	defer domains.Close()

	var latestCommitTxNum uint64
	commit := func(txn uint64) error {
		err = domains.Flush(ctx, tx)
		require.NoError(t, err)

		err = tx.Commit()
		require.NoError(t, err)

		tx, err = db.BeginTemporalRw(context.Background())
		require.NoError(t, err)

		domains, err = state.NewSharedDomains(tx, log.New())
		require.NoError(t, err)
		atomic.StoreUint64(&latestCommitTxNum, txn)
		return nil
	}

	txs := (aggStep) * config3.StepsInFrozenFile
	t.Logf("step=%d tx_count=%d", aggStep, txs)

	rnd := newRnd(0)
	keys := make([][]byte, txs/2)

	var prev1, prev2 []byte
	var txNum uint64
	for txNum = uint64(1); txNum <= txs/2; txNum++ {
		addr, loc := make([]byte, length.Addr), make([]byte, length.Hash)
		n, err := rnd.Read(addr)
		require.NoError(t, err)
		require.Equal(t, length.Addr, n)

		n, err = rnd.Read(loc)
		require.NoError(t, err)
		require.Equal(t, length.Hash, n)
		keys[txNum-1] = append(addr, loc...)

		acc := accounts.Account{
			Nonce:       1,
			Balance:     *uint256.NewInt(0),
			CodeHash:    common.Hash{},
			Incarnation: 0,
		}
		buf := accounts.SerialiseV3(&acc)

		err = domains.DomainPut(kv.AccountsDomain, tx, addr, buf, txNum, prev1, 0)
		require.NoError(t, err)
		prev1 = buf

		err = domains.DomainPut(kv.StorageDomain, tx, composite(addr, loc), []byte{addr[0], loc[0]}, txNum, prev2, 0)
		require.NoError(t, err)
		prev2 = []byte{addr[0], loc[0]}

	}
	require.NoError(t, commit(txNum))

	half := txs / 2
	for txNum = txNum + 1; txNum <= txs; txNum++ {
		addr, loc := keys[txNum-1-half][:length.Addr], keys[txNum-1-half][length.Addr:]

		prev, step, err := tx.GetLatest(kv.AccountsDomain, keys[txNum-1-half])
		require.NoError(t, err)
		err = domains.DomainPut(kv.StorageDomain, tx, composite(addr, loc), []byte{addr[0], loc[0]}, txNum, prev, step)
		require.NoError(t, err)
	}

	err = tx.Commit()

	tx, err = db.BeginTemporalRw(context.Background())
	require.NoError(t, err)
	defer tx.Rollback()

	for i, key := range keys {

		storedV, _, err := tx.GetLatest(kv.StorageDomain, key)
		require.NotNil(t, storedV, "key %x not found %d", key, i)
		require.NoError(t, err)
		require.Equal(t, key[0], storedV[0])
		require.Equal(t, key[length.Addr], storedV[1])
	}
	require.NoError(t, err)
}

func TestAggregatorV3_Merge(t *testing.T) {
	if testing.Short() {
		t.Skip()
	}

	t.Parallel()
	db, agg := testDbAndAggregatorv3(t, 10)

	rwTx, err := db.BeginTemporalRw(context.Background())
	require.NoError(t, err)
	defer rwTx.Rollback()

	domains, err := state.NewSharedDomains(rwTx, log.New())
	require.NoError(t, err)
	defer domains.Close()

	txs := uint64(1000)
	rnd := rand.New(rand.NewSource(time.Now().UnixNano()))

	var (
		commKey1 = []byte("someCommKey")
		commKey2 = []byte("otherCommKey")
	)

	// keys are encodings of numbers 1..31
	// each key changes value on every txNum which is multiple of the key
	var maxWrite, otherMaxWrite uint64
	for txNum := uint64(1); txNum <= txs; txNum++ {

		addr, loc := make([]byte, length.Addr), make([]byte, length.Hash)

		n, err := rnd.Read(addr)
		require.NoError(t, err)
		require.Equal(t, length.Addr, n)

		n, err = rnd.Read(loc)
		require.NoError(t, err)
		require.Equal(t, length.Hash, n)
		acc := accounts.Account{
			Nonce:       1,
			Balance:     *uint256.NewInt(0),
			CodeHash:    common.Hash{},
			Incarnation: 0,
		}
		buf := accounts.SerialiseV3(&acc)
		err = domains.DomainPut(kv.AccountsDomain, rwTx, addr, buf, txNum, nil, 0)
		require.NoError(t, err)

		err = domains.DomainPut(kv.StorageDomain, rwTx, composite(addr, loc), []byte{addr[0], loc[0]}, txNum, nil, 0)
		require.NoError(t, err)

		var v [8]byte
		binary.BigEndian.PutUint64(v[:], txNum)
		if txNum%135 == 0 {
			pv, step, err := domains.GetLatest(kv.CommitmentDomain, rwTx, commKey2)
			require.NoError(t, err)

			err = domains.DomainPut(kv.CommitmentDomain, rwTx, commKey2, v[:], txNum, pv, step)
			require.NoError(t, err)
			otherMaxWrite = txNum
		} else {
			pv, step, err := domains.GetLatest(kv.CommitmentDomain, rwTx, commKey1)
			require.NoError(t, err)

			err = domains.DomainPut(kv.CommitmentDomain, rwTx, commKey1, v[:], txNum, pv, step)
			require.NoError(t, err)
			maxWrite = txNum
		}
		require.NoError(t, err)

	}

	err = domains.Flush(context.Background(), rwTx)
	require.NoError(t, err)

	require.NoError(t, err)
	err = rwTx.Commit()
	require.NoError(t, err)

	mustSeeFile := func(files []string, folderName, fileNameWithoutVersion string) bool { //file-version agnostic
		for _, f := range files {
			if strings.HasPrefix(f, folderName) && strings.HasSuffix(f, fileNameWithoutVersion) {
				return true
			}
		}
		return false
	}

	onChangeCalls, onDelCalls := 0, 0
	agg.OnFilesChange(func(newFiles []string) {
		if len(newFiles) == 0 {
			return
		}

		onChangeCalls++
		if onChangeCalls == 1 {
			mustSeeFile(newFiles, "domain", "accounts.0-2.kv") //TODO: when we build `accounts.0-1.kv` - we sending empty notifcation
			require.False(t, filepath.IsAbs(newFiles[0]))      // expecting non-absolute paths (relative as of snapshots dir)
		}
	}, func(deletedFiles []string) {
		if len(deletedFiles) == 0 {
			return
		}

		onDelCalls++
		if onDelCalls == 1 {
			mustSeeFile(deletedFiles, "domain", "accounts.0-1.kv")
			mustSeeFile(deletedFiles, "domain", "commitment.0-1.kv")
			mustSeeFile(deletedFiles, "history", "accounts.0-1.v")
			mustSeeFile(deletedFiles, "accessor", "accounts.0-1.vi")

			mustSeeFile(deletedFiles, "domain", "accounts.1-2.kv")
			require.False(t, filepath.IsAbs(deletedFiles[0])) // expecting non-absolute paths (relative as of snapshots dir)
		}
	})

	err = agg.BuildFiles(txs)
	require.NoError(t, err)
	require.Equal(t, 13, onChangeCalls)
	require.Equal(t, 14, onDelCalls)

	{ //prune
		rwTx, err = db.BeginTemporalRw(context.Background())
		require.NoError(t, err)
		defer rwTx.Rollback()

		_, err := state.AggTx(rwTx).PruneSmallBatches(context.Background(), time.Hour, rwTx)
		require.NoError(t, err)

		err = rwTx.Commit()
		require.NoError(t, err)
	}

	onChangeCalls, onDelCalls = 0, 0
	err = agg.MergeLoop(context.Background())
	require.NoError(t, err)
	require.Equal(t, 0, onChangeCalls)
	require.Equal(t, 0, onDelCalls)

	// Check the history
	roTx, err := db.BeginTemporalRo(context.Background())
	require.NoError(t, err)
	defer roTx.Rollback()

	v, _, err := roTx.GetLatest(kv.CommitmentDomain, commKey1)
	require.NoError(t, err)
	require.Equal(t, maxWrite, binary.BigEndian.Uint64(v[:]))

	v, _, err = roTx.GetLatest(kv.CommitmentDomain, commKey2)
	require.NoError(t, err)
	require.Equal(t, otherMaxWrite, binary.BigEndian.Uint64(v[:]))
}

func TestAggregatorV3_PruneSmallBatches(t *testing.T) {
	if testing.Short() {
		t.Skip()
	}

	t.Parallel()
	aggStep := uint64(2)
	db, agg := testDbAndAggregatorv3(t, aggStep)

	tx, err := db.BeginTemporalRw(context.Background())
	require.NoError(t, err)
	defer tx.Rollback()

	domains, err := state.NewSharedDomains(tx, log.New())
	require.NoError(t, err)
	defer domains.Close()

	maxTx := aggStep * 3
	t.Logf("step=%d tx_count=%d\n", aggStep, maxTx)

	rnd := newRnd(0)

	generateSharedDomainsUpdates(t, domains, tx, maxTx, rnd, length.Addr, 10, aggStep/2)

	// flush and build files
	err = domains.Flush(context.Background(), tx)
	require.NoError(t, err)

	var (
		// until pruning
		accountsRange    map[string][]byte
		storageRange     map[string][]byte
		codeRange        map[string][]byte
		accountHistRange map[string][]byte
		storageHistRange map[string][]byte
		codeHistRange    map[string][]byte
	)
	maxInt := math.MaxInt
	{
		it, err := tx.Debug().RangeLatest(kv.AccountsDomain, nil, nil, maxInt)
		require.NoError(t, err)
		accountsRange = extractKVErrIterator(t, it)

		it, err = tx.Debug().RangeLatest(kv.StorageDomain, nil, nil, maxInt)
		require.NoError(t, err)
		storageRange = extractKVErrIterator(t, it)

		it, err = tx.Debug().RangeLatest(kv.CodeDomain, nil, nil, maxInt)
		require.NoError(t, err)
		codeRange = extractKVErrIterator(t, it)

		its, err := tx.HistoryRange(kv.AccountsDomain, 0, int(maxTx), order.Asc, maxInt)
		require.NoError(t, err)
		accountHistRange = extractKVErrIterator(t, its)
		its, err = tx.HistoryRange(kv.CodeDomain, 0, int(maxTx), order.Asc, maxInt)
		require.NoError(t, err)
		codeHistRange = extractKVErrIterator(t, its)
		its, err = tx.HistoryRange(kv.StorageDomain, 0, int(maxTx), order.Asc, maxInt)
		require.NoError(t, err)
		storageHistRange = extractKVErrIterator(t, its)
	}

	err = tx.Commit()
	require.NoError(t, err)

	err = agg.BuildFiles(maxTx)
	require.NoError(t, err)

	buildTx, err := db.BeginTemporalRw(context.Background())
	require.NoError(t, err)
	defer buildTx.Rollback()

	for i := 0; i < 10; i++ {
		_, err = buildTx.PruneSmallBatches(context.Background(), time.Second*3)
		require.NoError(t, err)
	}
	err = buildTx.Commit()
	require.NoError(t, err)

	afterTx, err := db.BeginTemporalRw(context.Background())
	require.NoError(t, err)
	defer afterTx.Rollback()

	var (
		// after pruning
		accountsRangeAfter    map[string][]byte
		storageRangeAfter     map[string][]byte
		codeRangeAfter        map[string][]byte
		accountHistRangeAfter map[string][]byte
		storageHistRangeAfter map[string][]byte
		codeHistRangeAfter    map[string][]byte
	)

	{
		it, err := afterTx.Debug().RangeLatest(kv.AccountsDomain, nil, nil, maxInt)
		require.NoError(t, err)
		accountsRangeAfter = extractKVErrIterator(t, it)

		it, err = afterTx.Debug().RangeLatest(kv.StorageDomain, nil, nil, maxInt)
		require.NoError(t, err)
		storageRangeAfter = extractKVErrIterator(t, it)

		it, err = afterTx.Debug().RangeLatest(kv.CodeDomain, nil, nil, maxInt)
		require.NoError(t, err)
		codeRangeAfter = extractKVErrIterator(t, it)

		its, err := afterTx.HistoryRange(kv.AccountsDomain, 0, int(maxTx), order.Asc, maxInt)
		require.NoError(t, err)
		accountHistRangeAfter = extractKVErrIterator(t, its)
		its, err = afterTx.HistoryRange(kv.CodeDomain, 0, int(maxTx), order.Asc, maxInt)
		require.NoError(t, err)
		codeHistRangeAfter = extractKVErrIterator(t, its)
		its, err = afterTx.HistoryRange(kv.StorageDomain, 0, int(maxTx), order.Asc, maxInt)
		require.NoError(t, err)
		storageHistRangeAfter = extractKVErrIterator(t, its)
	}

	{
		// compare
		compareMapsBytes(t, accountsRange, accountsRangeAfter)
		compareMapsBytes(t, storageRange, storageRangeAfter)
		compareMapsBytes(t, codeRange, codeRangeAfter)
		compareMapsBytes(t, accountHistRange, accountHistRangeAfter)
		compareMapsBytes(t, storageHistRange, storageHistRangeAfter)
		compareMapsBytes(t, codeHistRange, codeHistRangeAfter)
	}

}

func TestSharedDomain_CommitmentKeyReplacement(t *testing.T) {
	t.Parallel()

	stepSize := uint64(5)
	db, agg := testDbAndAggregatorv3(t, stepSize)

	ctx := context.Background()
	rwTx, err := db.BeginTemporalRw(ctx)
	require.NoError(t, err)
	defer rwTx.Rollback()

	domains, err := state.NewSharedDomains(rwTx, log.New())
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
		err = domains.DomainDel(kv.AccountsDomain, rwTx, removedKey, txNum, nil, 0)
		require.NoError(t, err)
		break
	}

	// 3. calculate commitment with all data +removed key
	expectedHash, err := domains.ComputeCommitment(context.Background(), rwTx, false, txNum/stepSize, txNum, "", nil)
	require.NoError(t, err)
	domains.Close()

	err = rwTx.Commit()
	require.NoError(t, err)

	t.Logf("expected hash: %x", expectedHash)
	err = agg.BuildFiles(stepSize * 16)
	require.NoError(t, err)

	err = rwTx.Commit()
	require.NoError(t, err)

	rwTx, err = db.BeginTemporalRw(ctx)
	require.NoError(t, err)
	defer rwTx.Rollback()

	// 4. restart on same (replaced keys) files
	domains, err = state.NewSharedDomains(rwTx, log.New())
	require.NoError(t, err)
	defer domains.Close()

	// 5. delete same key. commitment should be the same
	txNum = maxTx + 1
	err = domains.DomainDel(kv.AccountsDomain, rwTx, removedKey, txNum, nil, 0)
	require.NoError(t, err)

	resultHash, err := domains.ComputeCommitment(context.Background(), rwTx, false, txNum/stepSize, txNum, "", nil)
	require.NoError(t, err)

	t.Logf("result hash: %x", resultHash)
	require.Equal(t, expectedHash, resultHash)
}

func TestAggregatorV3_MergeValTransform(t *testing.T) {
	if testing.Short() {
		t.Skip()
	}

	t.Parallel()
	db, agg := testDbAndAggregatorv3(t, 5)
	rwTx, err := db.BeginTemporalRw(context.Background())
	require.NoError(t, err)
	defer rwTx.Rollback()

	agg.ForTestReplaceKeysInValues(kv.CommitmentDomain, true)

	domains, err := state.NewSharedDomains(rwTx, log.New())
	require.NoError(t, err)
	defer domains.Close()

	txs := uint64(100)
	rnd := rand.New(rand.NewSource(time.Now().UnixNano()))

	state := make(map[string][]byte)

	// keys are encodings of numbers 1..31
	// each key changes value on every txNum which is multiple of the key
	//var maxWrite, otherMaxWrite uint64
	for txNum := uint64(1); txNum <= txs; txNum++ {

		addr, loc := make([]byte, length.Addr), make([]byte, length.Hash)

		n, err := rnd.Read(addr)
		require.NoError(t, err)
		require.Equal(t, length.Addr, n)

		n, err = rnd.Read(loc)
		require.NoError(t, err)
		require.Equal(t, length.Hash, n)
		acc := accounts.Account{
			Nonce:       1,
			Balance:     *uint256.NewInt(txNum * 1e6),
			CodeHash:    common.Hash{},
			Incarnation: 0,
		}
		buf := accounts.SerialiseV3(&acc)
		err = domains.DomainPut(kv.AccountsDomain, rwTx, addr, buf, txNum, nil, 0)
		require.NoError(t, err)

		err = domains.DomainPut(kv.StorageDomain, rwTx, composite(addr, loc), []byte{addr[0], loc[0]}, txNum, nil, 0)
		require.NoError(t, err)

		if (txNum+1)%agg.StepSize() == 0 {
			_, err := domains.ComputeCommitment(context.Background(), rwTx, true, txNum/10, txNum, "", nil)
			require.NoError(t, err)
		}

		state[string(addr)] = buf
		state[string(addr)+string(loc)] = []byte{addr[0], loc[0]}
	}

	err = domains.Flush(context.Background(), rwTx)
	require.NoError(t, err)

	err = rwTx.Commit()
	require.NoError(t, err)

	err = agg.BuildFiles(txs)
	require.NoError(t, err)

	rwTx, err = db.BeginTemporalRw(context.Background())
	require.NoError(t, err)
	defer rwTx.Rollback()

	_, err = rwTx.PruneSmallBatches(context.Background(), time.Hour)
	require.NoError(t, err)

	err = rwTx.Commit()
	require.NoError(t, err)

	err = agg.MergeLoop(context.Background())
	require.NoError(t, err)
}

func compareMapsBytes(t *testing.T, m1, m2 map[string][]byte) {
	t.Helper()
	for k, v := range m1 {
		if len(v) == 0 {
			require.Equal(t, []byte{}, v)
		} else {
			require.Equal(t, m2[k], v)
		}
		delete(m2, k)
	}
	require.Emptyf(t, m2, "m2 should be empty got %d: %v", len(m2), m2)
}

func fillRawdbTxNumsIndexForSharedDomains(t *testing.T, rwTx kv.RwTx, maxTx, commitEvery uint64) {
	t.Helper()

	for txn := uint64(1); txn <= maxTx; txn++ {
		err := rawdbv3.TxNums.Append(rwTx, txn, txn/commitEvery)
		require.NoError(t, err)
	}
}

func extractKVErrIterator(t *testing.T, it stream.KV) map[string][]byte {
	t.Helper()

	accounts := make(map[string][]byte)
	for it.HasNext() {
		k, v, err := it.Next()
		require.NoError(t, err)
		accounts[hex.EncodeToString(k)] = common.Copy(v)
	}

	return accounts
}

func generateSharedDomainsUpdates(t *testing.T, domains *state.SharedDomains, tx kv.TemporalTx, maxTxNum uint64, rnd *rndGen, keyMaxLen, keysCount, commitEvery uint64) map[string]struct{} {
	t.Helper()
	usedKeys := make(map[string]struct{}, keysCount*maxTxNum)
	for txNum := uint64(1); txNum <= maxTxNum; txNum++ {
		used := generateSharedDomainsUpdatesForTx(t, domains, tx, txNum, rnd, usedKeys, keyMaxLen, keysCount)
		for k := range used {
			usedKeys[k] = struct{}{}
		}
		if txNum%commitEvery == 0 {
			// domains.SetTrace(true)
			rh, err := domains.ComputeCommitment(context.Background(), tx, true, txNum/commitEvery, txNum, "", nil)
			require.NoErrorf(t, err, "txNum=%d", txNum)
			t.Logf("commitment %x txn=%d", rh, txNum)
		}
	}
	return usedKeys
}

func generateSharedDomainsUpdatesForTx(t *testing.T, domains *state.SharedDomains, tx kv.TemporalTx, txNum uint64, rnd *rndGen, prevKeys map[string]struct{}, keyMaxLen, keysCount uint64) map[string]struct{} {
	t.Helper()

	getKey := func() ([]byte, bool) {
		r := rnd.IntN(100)
		if r < 50 && len(prevKeys) > 0 {
			ri := rnd.IntN(len(prevKeys))
			for k := range prevKeys {
				if ri == 0 {
					return []byte(k), true
				}
				ri--
			}
		} else {
			return []byte(generateRandomKey(rnd, keyMaxLen)), false
		}
		panic("unreachable")
	}

	const maxStorageKeys = 10
	usedKeys := make(map[string]struct{}, keysCount)

	for j := uint64(0); j < keysCount; j++ {
		key, existed := getKey()

		r := rnd.IntN(101)
		switch {
		case r <= 33:
			acc := accounts.Account{
				Nonce:       txNum,
				Balance:     *uint256.NewInt(txNum * 100_000),
				CodeHash:    common.Hash{},
				Incarnation: 0,
			}
			buf := accounts.SerialiseV3(&acc)
			prev, step, err := domains.GetLatest(kv.AccountsDomain, tx, key)
			require.NoError(t, err)

			usedKeys[string(key)] = struct{}{}

			err = domains.DomainPut(kv.AccountsDomain, tx, key, buf, txNum, prev, step)
			require.NoError(t, err)

		case r > 33 && r <= 66:
			codeUpd := make([]byte, rnd.IntN(24576))
			_, err := rnd.Read(codeUpd)
			require.NoError(t, err)
			for limit := 1000; len(key) > length.Addr && limit > 0; limit-- {
				key, existed = getKey() //nolint
				if !existed {
					continue
				}
			}
			usedKeys[string(key)] = struct{}{}

			prev, step, err := domains.GetLatest(kv.CodeDomain, tx, key)
			require.NoError(t, err)

			err = domains.DomainPut(kv.CodeDomain, tx, key, codeUpd, txNum, prev, step)
			require.NoError(t, err)
		case r > 80:
			if !existed {
				continue
			}
			usedKeys[string(key)] = struct{}{}

			err := domains.DomainDel(kv.AccountsDomain, tx, key, txNum, nil, 0)
			require.NoError(t, err)

		case r > 66 && r <= 80:
			// need to create account because commitment trie requires it (accounts are upper part of trie)
			if len(key) > length.Addr {
				key = key[:length.Addr]
			}

			prev, step, err := domains.GetLatest(kv.AccountsDomain, tx, key)
			require.NoError(t, err)
			if prev == nil {
				usedKeys[string(key)] = struct{}{}
				acc := accounts.Account{
					Nonce:       txNum,
					Balance:     *uint256.NewInt(txNum * 100_000),
					CodeHash:    common.Hash{},
					Incarnation: 0,
				}
				buf := accounts.SerialiseV3(&acc)
				err = domains.DomainPut(kv.AccountsDomain, tx, key, buf, txNum, prev, step)
				require.NoError(t, err)
			}

			sk := make([]byte, length.Hash+length.Addr)
			copy(sk, key)

			for i := 0; i < maxStorageKeys; i++ {
				loc := generateRandomKeyBytes(rnd, 32)
				copy(sk[length.Addr:], loc)
				usedKeys[string(sk)] = struct{}{}

				prev, step, err := domains.GetLatest(kv.StorageDomain, tx, sk[:length.Addr])
				require.NoError(t, err)

				err = domains.DomainPut(kv.StorageDomain, tx, sk, uint256.NewInt(txNum).Bytes(), txNum, prev, step)
				require.NoError(t, err)
			}

		}
	}
	return usedKeys
}
