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
	"bytes"
	"context"
	"encoding/binary"
	"encoding/hex"
	"fmt"
	"math"
	"math/rand"
	"os"
	"path"
	"path/filepath"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/c2h5oh/datasize"

	"github.com/erigontech/erigon/erigon-lib/commitment"
	"github.com/erigontech/erigon/erigon-lib/common"
	"github.com/erigontech/erigon/erigon-lib/common/background"
	"github.com/erigontech/erigon/erigon-lib/common/datadir"
	"github.com/erigontech/erigon/erigon-lib/common/length"
	"github.com/erigontech/erigon/erigon-lib/etl"
	"github.com/erigontech/erigon/erigon-lib/kv"
	"github.com/erigontech/erigon/erigon-lib/kv/mdbx"
	"github.com/erigontech/erigon/erigon-lib/kv/order"
	"github.com/erigontech/erigon/erigon-lib/kv/rawdbv3"
	"github.com/erigontech/erigon/erigon-lib/kv/stream"
	"github.com/erigontech/erigon/erigon-lib/log/v3"
	"github.com/erigontech/erigon/erigon-lib/seg"
	"github.com/erigontech/erigon/erigon-lib/types"
	"github.com/holiman/uint256"
	"github.com/stretchr/testify/require"
)

func TestAggregatorV3_RestartOnDatadir(t *testing.T) {
	t.Parallel()
	//t.Skip()
	t.Run("BPlus", func(t *testing.T) {
		rc := runCfg{
			aggStep:  50,
			useBplus: true,
		}
		aggregatorV3_RestartOnDatadir(t, rc)
	})
	t.Run("B", func(t *testing.T) {
		rc := runCfg{
			aggStep: 50,
		}
		aggregatorV3_RestartOnDatadir(t, rc)
	})

}

type runCfg struct {
	aggStep      uint64
	useBplus     bool
	compressVals bool
	largeVals    bool
}

// here we create a bunch of updates for further aggregation.
// FinishTx should merge underlying files several times
// Expected that:
// - we could close first aggregator and open another with previous data still available
// - new aggregator SeekCommitment must return txNum equal to amount of total txns
func aggregatorV3_RestartOnDatadir(t *testing.T, rc runCfg) {
	t.Helper()
	ctx := context.Background()
	logger := log.New()
	aggStep := rc.aggStep
	db, agg := testDbAndAggregatorv3(t, aggStep)
	//if rc.useBplus {
	//	UseBpsTree = true
	//	defer func() { UseBpsTree = false }()
	//}

	tx, err := db.BeginRw(context.Background())
	require.NoError(t, err)
	defer func() {
		if tx != nil {
			tx.Rollback()
		}
	}()
	ac := agg.BeginFilesRo()
	defer ac.Close()

	domains, err := NewSharedDomains(WrapTxWithCtx(tx, ac), log.New())
	require.NoError(t, err)
	defer domains.Close()

	var latestCommitTxNum uint64
	rnd := newRnd(0)

	someKey := []byte("somekey")
	txs := (aggStep / 2) * 19
	t.Logf("step=%d tx_count=%d", aggStep, txs)
	var aux [8]byte
	// keys are encodings of numbers 1..31
	// each key changes value on every txNum which is multiple of the key
	var maxWrite uint64
	addr, loc := make([]byte, length.Addr), make([]byte, length.Hash)
	for txNum := uint64(1); txNum <= txs; txNum++ {
		domains.SetTxNum(txNum)
		binary.BigEndian.PutUint64(aux[:], txNum)

		n, err := rnd.Read(addr)
		require.NoError(t, err)
		require.EqualValues(t, length.Addr, n)

		n, err = rnd.Read(loc)
		require.NoError(t, err)
		require.EqualValues(t, length.Hash, n)
		//keys[txNum-1] = append(addr, loc...)

		buf := types.EncodeAccountBytesV3(1, uint256.NewInt(rnd.Uint64()), nil, 0)
		err = domains.DomainPut(kv.AccountsDomain, addr, nil, buf, nil, 0)
		require.NoError(t, err)

		err = domains.DomainPut(kv.StorageDomain, addr, loc, []byte{addr[0], loc[0]}, nil, 0)
		require.NoError(t, err)

		err = domains.DomainPut(kv.CommitmentDomain, someKey, nil, aux[:], nil, 0)
		require.NoError(t, err)
		maxWrite = txNum
	}
	_, err = domains.ComputeCommitment(ctx, true, domains.BlockNum(), "")
	require.NoError(t, err)

	err = domains.Flush(context.Background(), tx)
	require.NoError(t, err)
	err = tx.Commit()
	require.NoError(t, err)
	tx = nil

	err = agg.BuildFiles(txs)
	require.NoError(t, err)

	agg.Close()

	// Start another aggregator on same datadir
	anotherAgg, err := NewAggregator(context.Background(), agg.dirs, aggStep, db, logger)
	require.NoError(t, err)
	defer anotherAgg.Close()

	require.NoError(t, anotherAgg.OpenFolder())

	rwTx, err := db.BeginRw(context.Background())
	require.NoError(t, err)
	defer func() {
		if rwTx != nil {
			rwTx.Rollback()
		}
	}()

	//anotherAgg.SetTx(rwTx)
	startTx := anotherAgg.EndTxNumMinimax()
	ac2 := anotherAgg.BeginFilesRo()
	defer ac2.Close()
	dom2, err := NewSharedDomains(WrapTxWithCtx(rwTx, ac2), log.New())
	require.NoError(t, err)
	defer dom2.Close()

	_, err = dom2.SeekCommitment(ctx, rwTx)
	sstartTx := dom2.TxNum()

	require.NoError(t, err)
	require.GreaterOrEqual(t, sstartTx, startTx)
	require.GreaterOrEqual(t, sstartTx, latestCommitTxNum)
	_ = sstartTx
	rwTx.Rollback()
	rwTx = nil

	// Check the history
	roTx, err := db.BeginRo(context.Background())
	require.NoError(t, err)
	defer roTx.Rollback()

	dc := anotherAgg.BeginFilesRo()
	v, _, ex, err := dc.GetLatest(kv.CommitmentDomain, someKey, nil, roTx)
	require.NoError(t, err)
	require.True(t, ex)
	dc.Close()

	require.EqualValues(t, maxWrite, binary.BigEndian.Uint64(v[:]))
}

func TestNewBtIndex(t *testing.T) {
	t.Parallel()
	keyCount := 10000
	kvPath := generateKV(t, t.TempDir(), 20, 10, keyCount, log.New(), seg.CompressNone)

	indexPath := strings.TrimSuffix(kvPath, ".kv") + ".bt"

	kv, bt, err := OpenBtreeIndexAndDataFile(indexPath, kvPath, DefaultBtreeM, seg.CompressNone, false)
	require.NoError(t, err)
	defer bt.Close()
	defer kv.Close()
	require.NotNil(t, kv)
	require.NotNil(t, bt)
	require.Len(t, bt.bplus.mx, keyCount/int(DefaultBtreeM))

	for i := 1; i < len(bt.bplus.mx); i++ {
		require.NotZero(t, bt.bplus.mx[i].di)
		require.NotZero(t, bt.bplus.mx[i].off)
		require.NotEmpty(t, bt.bplus.mx[i].key)
	}
}

func TestAggregatorV3_PruneSmallBatches(t *testing.T) {
	t.Parallel()
	aggStep := uint64(10)
	db, agg := testDbAndAggregatorv3(t, aggStep)

	tx, err := db.BeginRw(context.Background())
	require.NoError(t, err)
	defer func() {
		if tx != nil {
			tx.Rollback()
		}
	}()

	ac := agg.BeginFilesRo()
	defer ac.Close()

	domains, err := NewSharedDomains(WrapTxWithCtx(tx, ac), log.New())
	require.NoError(t, err)
	defer domains.Close()

	maxTx := aggStep * 5
	t.Logf("step=%d tx_count=%d\n", aggStep, maxTx)

	rnd := newRnd(0)

	generateSharedDomainsUpdates(t, domains, maxTx, rnd, 20, 10, aggStep/2)

	// flush and build files
	err = domains.Flush(context.Background(), tx)
	require.NoError(t, err)

	var (
		// until pruning
		accountsRange    map[string][]byte
		storageRange     map[string][]byte
		codeRange        map[string][]byte
		accountHistRange map[string]vs
		storageHistRange map[string]vs
		codeHistRange    map[string]vs
	)
	maxInt := math.MaxInt
	{
		it, err := ac.RangeLatest(tx, kv.AccountsDomain, nil, nil, maxInt)
		require.NoError(t, err)
		accountsRange = extractKVErrIterator(t, it)

		it, err = ac.RangeLatest(tx, kv.StorageDomain, nil, nil, maxInt)
		require.NoError(t, err)
		storageRange = extractKVErrIterator(t, it)

		it, err = ac.RangeLatest(tx, kv.CodeDomain, nil, nil, maxInt)
		require.NoError(t, err)
		codeRange = extractKVErrIterator(t, it)

		its, err := ac.d[kv.AccountsDomain].ht.HistoryRange(0, int(maxTx), order.Asc, maxInt, tx)
		require.NoError(t, err)
		accountHistRange = extractKVSErrIterator(t, its)
		its, err = ac.d[kv.CodeDomain].ht.HistoryRange(0, int(maxTx), order.Asc, maxInt, tx)
		require.NoError(t, err)
		codeHistRange = extractKVSErrIterator(t, its)
		its, err = ac.d[kv.StorageDomain].ht.HistoryRange(0, int(maxTx), order.Asc, maxInt, tx)
		require.NoError(t, err)
		storageHistRange = extractKVSErrIterator(t, its)
	}

	err = tx.Commit()
	require.NoError(t, err)

	err = agg.BuildFiles(maxTx)
	require.NoError(t, err)

	buildTx, err := db.BeginRw(context.Background())
	require.NoError(t, err)
	defer func() {
		if buildTx != nil {
			buildTx.Rollback()
		}
	}()
	ac = agg.BeginFilesRo()
	for i := 0; i < 10; i++ {
		_, err = ac.PruneSmallBatches(context.Background(), time.Second*3, buildTx)
		require.NoError(t, err)
	}
	err = buildTx.Commit()
	require.NoError(t, err)

	afterTx, err := db.BeginRw(context.Background())
	require.NoError(t, err)
	defer func() {
		if afterTx != nil {
			afterTx.Rollback()
		}
	}()

	var (
		// after pruning
		accountsRangeAfter    map[string][]byte
		storageRangeAfter     map[string][]byte
		codeRangeAfter        map[string][]byte
		accountHistRangeAfter map[string]vs
		storageHistRangeAfter map[string]vs
		codeHistRangeAfter    map[string]vs
	)

	{
		it, err := ac.RangeLatest(afterTx, kv.AccountsDomain, nil, nil, maxInt)
		require.NoError(t, err)
		accountsRangeAfter = extractKVErrIterator(t, it)

		it, err = ac.RangeLatest(afterTx, kv.StorageDomain, nil, nil, maxInt)
		require.NoError(t, err)
		storageRangeAfter = extractKVErrIterator(t, it)

		it, err = ac.RangeLatest(afterTx, kv.CodeDomain, nil, nil, maxInt)
		require.NoError(t, err)
		codeRangeAfter = extractKVErrIterator(t, it)

		its, err := ac.d[kv.AccountsDomain].ht.HistoryRange(0, int(maxTx), order.Asc, maxInt, tx)
		require.NoError(t, err)
		accountHistRangeAfter = extractKVSErrIterator(t, its)
		its, err = ac.d[kv.CodeDomain].ht.HistoryRange(0, int(maxTx), order.Asc, maxInt, tx)
		require.NoError(t, err)
		codeHistRangeAfter = extractKVSErrIterator(t, its)
		its, err = ac.d[kv.StorageDomain].ht.HistoryRange(0, int(maxTx), order.Asc, maxInt, tx)
		require.NoError(t, err)
		storageHistRangeAfter = extractKVSErrIterator(t, its)
	}

	{
		// compare
		compareMapsBytes(t, accountsRange, accountsRangeAfter)
		compareMapsBytes(t, storageRange, storageRangeAfter)
		compareMapsBytes(t, codeRange, codeRangeAfter)
		compareMapsBytes2(t, accountHistRange, accountHistRangeAfter)
		compareMapsBytes2(t, storageHistRange, storageHistRangeAfter)
		compareMapsBytes2(t, codeHistRange, codeHistRangeAfter)
	}

}

func compareMapsBytes2(t *testing.T, m1, m2 map[string]vs) {
	t.Helper()
	for k, v := range m1 {
		v2, ok := m2[k]
		require.Truef(t, ok, "key %x not found", k)
		require.EqualValues(t, v.s, v2.s)
		if !bytes.Equal(v.v, v2.v) { // empty value==nil
			t.Logf("key %x expected '%x' but got '%x'\n", k, v, m2[k])
		}
		delete(m2, k)
	}
	require.Emptyf(t, m2, "m2 should be empty got %d: %v", len(m2), m2)
}

func compareMapsBytes(t *testing.T, m1, m2 map[string][]byte) {
	t.Helper()
	for k, v := range m1 {
		require.EqualValues(t, v, m2[k])
		delete(m2, k)
	}
	require.Emptyf(t, m2, "m2 should be empty got %d: %v", len(m2), m2)
}

type vs struct {
	v []byte
	s uint64
}

func extractKVSErrIterator(t *testing.T, it stream.KVS) map[string]vs {
	t.Helper()

	accounts := make(map[string]vs)
	for it.HasNext() {
		k, v, s, err := it.Next()
		require.NoError(t, err)
		accounts[hex.EncodeToString(k)] = vs{v: common.Copy(v), s: s}
	}

	return accounts
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

func fillRawdbTxNumsIndexForSharedDomains(t *testing.T, rwTx kv.RwTx, maxTx, commitEvery uint64) {
	t.Helper()

	for txn := uint64(1); txn <= maxTx; txn++ {
		err := rawdbv3.TxNums.Append(rwTx, txn, txn/commitEvery)
		require.NoError(t, err)
	}
}

func generateSharedDomainsUpdates(t *testing.T, domains *SharedDomains, maxTxNum uint64, rnd *rndGen, keyMaxLen, keysCount, commitEvery uint64) map[string]struct{} {
	t.Helper()
	usedKeys := make(map[string]struct{}, keysCount*maxTxNum)
	for txNum := uint64(1); txNum <= maxTxNum; txNum++ {
		used := generateSharedDomainsUpdatesForTx(t, domains, txNum, rnd, usedKeys, keyMaxLen, keysCount)
		for k := range used {
			usedKeys[k] = struct{}{}
		}
		if txNum%commitEvery == 0 {
			_, err := domains.ComputeCommitment(context.Background(), true, txNum/commitEvery, "")
			require.NoErrorf(t, err, "txNum=%d", txNum)
		}
	}
	return usedKeys
}

func generateSharedDomainsUpdatesForTx(t *testing.T, domains *SharedDomains, txNum uint64, rnd *rndGen, prevKeys map[string]struct{}, keyMaxLen, keysCount uint64) map[string]struct{} {
	t.Helper()
	domains.SetTxNum(txNum)

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

	const maxStorageKeys = 350
	usedKeys := make(map[string]struct{}, keysCount)

	for j := uint64(0); j < keysCount; j++ {
		key, existed := getKey()

		r := rnd.IntN(101)
		switch {
		case r <= 33:
			buf := types.EncodeAccountBytesV3(txNum, uint256.NewInt(txNum*100_000), nil, 0)
			prev, step, err := domains.GetLatest(kv.AccountsDomain, key, nil)
			require.NoError(t, err)

			usedKeys[string(key)] = struct{}{}

			err = domains.DomainPut(kv.AccountsDomain, key, nil, buf, prev, step)
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

			prev, step, err := domains.GetLatest(kv.CodeDomain, key, nil)
			require.NoError(t, err)

			err = domains.DomainPut(kv.CodeDomain, key, nil, codeUpd, prev, step)
			require.NoError(t, err)
		case r > 80:
			if !existed {
				continue
			}
			usedKeys[string(key)] = struct{}{}

			err := domains.DomainDel(kv.AccountsDomain, key, nil, nil, 0)
			require.NoError(t, err)

		case r > 66 && r <= 80:
			// need to create account because commitment trie requires it (accounts are upper part of trie)
			if len(key) > length.Addr {
				key = key[:length.Addr]
			}

			prev, step, err := domains.GetLatest(kv.AccountsDomain, key, nil)
			require.NoError(t, err)
			if prev == nil {
				usedKeys[string(key)] = struct{}{}
				buf := types.EncodeAccountBytesV3(txNum, uint256.NewInt(txNum*100_000), nil, 0)
				err = domains.DomainPut(kv.AccountsDomain, key, nil, buf, prev, step)
				require.NoError(t, err)
			}

			sk := make([]byte, length.Hash+length.Addr)
			copy(sk, key)

			for i := 0; i < maxStorageKeys; i++ {
				loc := generateRandomKeyBytes(rnd, 32)
				copy(sk[length.Addr:], loc)
				usedKeys[string(sk)] = struct{}{}

				prev, step, err := domains.GetLatest(kv.StorageDomain, sk[:length.Addr], sk[length.Addr:])
				require.NoError(t, err)

				err = domains.DomainPut(kv.StorageDomain, sk[:length.Addr], sk[length.Addr:], uint256.NewInt(txNum).Bytes(), prev, step)
				require.NoError(t, err)
			}

		}
	}
	return usedKeys
}

func TestAggregatorV3_RestartOnFiles(t *testing.T) {
	t.Parallel()

	logger := log.New()
	aggStep := uint64(100)
	ctx := context.Background()
	db, agg := testDbAndAggregatorv3(t, aggStep)
	dirs := agg.dirs

	tx, err := db.BeginRw(context.Background())
	require.NoError(t, err)
	defer func() {
		if tx != nil {
			tx.Rollback()
		}
	}()
	ac := agg.BeginFilesRo()
	defer ac.Close()
	domains, err := NewSharedDomains(WrapTxWithCtx(tx, ac), log.New())
	require.NoError(t, err)
	defer domains.Close()

	txs := aggStep * 5
	t.Logf("step=%d tx_count=%d\n", aggStep, txs)

	rnd := newRnd(0)
	keys := make([][]byte, txs)

	for txNum := uint64(1); txNum <= txs; txNum++ {
		domains.SetTxNum(txNum)

		addr, loc := make([]byte, length.Addr), make([]byte, length.Hash)
		n, err := rnd.Read(addr)
		require.NoError(t, err)
		require.EqualValues(t, length.Addr, n)

		n, err = rnd.Read(loc)
		require.NoError(t, err)
		require.EqualValues(t, length.Hash, n)

		buf := types.EncodeAccountBytesV3(txNum, uint256.NewInt(1000000000000), nil, 0)
		err = domains.DomainPut(kv.AccountsDomain, addr, nil, buf[:], nil, 0)
		require.NoError(t, err)

		err = domains.DomainPut(kv.StorageDomain, addr, loc, []byte{addr[0], loc[0]}, nil, 0)
		require.NoError(t, err)

		keys[txNum-1] = append(addr, loc...)
	}

	// flush and build files
	err = domains.Flush(context.Background(), tx)
	require.NoError(t, err)

	latestStepInDB := agg.d[kv.AccountsDomain].maxStepInDB(tx)
	require.Equal(t, 5, int(latestStepInDB))

	err = tx.Commit()
	require.NoError(t, err)

	err = agg.BuildFiles(txs)
	require.NoError(t, err)

	tx = nil
	agg.Close()
	db.Close()

	// remove database files
	require.NoError(t, os.RemoveAll(dirs.Chaindata))

	// open new db and aggregator instances
	newDb := mdbx.New(kv.ChainDB, logger).InMem(dirs.Chaindata).MustOpen()
	t.Cleanup(newDb.Close)

	newAgg, err := NewAggregator(context.Background(), agg.dirs, aggStep, newDb, logger)
	require.NoError(t, err)
	require.NoError(t, newAgg.OpenFolder())

	newTx, err := newDb.BeginRw(context.Background())
	require.NoError(t, err)
	defer newTx.Rollback()

	ac = newAgg.BeginFilesRo()
	defer ac.Close()
	newDoms, err := NewSharedDomains(WrapTxWithCtx(newTx, ac), log.New())
	require.NoError(t, err)
	defer newDoms.Close()

	_, err = newDoms.SeekCommitment(ctx, newTx)
	require.NoError(t, err)
	latestTx := newDoms.TxNum()
	t.Logf("seek to latest_tx=%d", latestTx)

	miss := uint64(0)
	for i, key := range keys {
		if uint64(i+1) >= txs-aggStep {
			continue // finishtx always stores last agg step in db which we deleted, so missing  values which were not aggregated is expected
		}
		stored, _, _, err := ac.GetLatest(kv.AccountsDomain, key[:length.Addr], nil, newTx)
		require.NoError(t, err)
		if len(stored) == 0 {
			miss++
			//fmt.Printf("%x [%d/%d]", key, miss, i+1) // txnum starts from 1
			continue
		}
		nonce, _, _ := types.DecodeAccountBytesV3(stored)

		require.EqualValues(t, i+1, int(nonce))

		storedV, _, found, err := ac.GetLatest(kv.StorageDomain, key[:length.Addr], key[length.Addr:], newTx)
		require.NoError(t, err)
		require.True(t, found)
		_ = key[0]
		_ = storedV[0]
		require.EqualValues(t, key[0], storedV[0])
		require.EqualValues(t, key[length.Addr], storedV[1])
	}
	newAgg.Close()

	require.NoError(t, err)
}

func TestAggregatorV3_ReplaceCommittedKeys(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	aggStep := uint64(500)

	db, agg := testDbAndAggregatorv3(t, aggStep)

	tx, err := db.BeginRw(context.Background())
	require.NoError(t, err)
	defer func() {
		if tx != nil {
			tx.Rollback()
		}
	}()

	ac := agg.BeginFilesRo()
	defer ac.Close()
	domains, err := NewSharedDomains(WrapTxWithCtx(tx, ac), log.New())
	require.NoError(t, err)
	defer domains.Close()

	var latestCommitTxNum uint64
	commit := func(txn uint64) error {
		domains.Flush(ctx, tx)
		ac.Close()
		err = tx.Commit()
		require.NoError(t, err)

		tx, err = db.BeginRw(context.Background())
		require.NoError(t, err)
		ac = agg.BeginFilesRo()
		domains, err = NewSharedDomains(WrapTxWithCtx(tx, ac), log.New())
		require.NoError(t, err)
		atomic.StoreUint64(&latestCommitTxNum, txn)
		return nil
	}

	txs := (aggStep) * StepsInColdFile
	t.Logf("step=%d tx_count=%d", aggStep, txs)

	rnd := newRnd(0)
	keys := make([][]byte, txs/2)

	var prev1, prev2 []byte
	var txNum uint64
	for txNum = uint64(1); txNum <= txs/2; txNum++ {
		domains.SetTxNum(txNum)

		addr, loc := make([]byte, length.Addr), make([]byte, length.Hash)
		n, err := rnd.Read(addr)
		require.NoError(t, err)
		require.EqualValues(t, length.Addr, n)

		n, err = rnd.Read(loc)
		require.NoError(t, err)
		require.EqualValues(t, length.Hash, n)
		keys[txNum-1] = append(addr, loc...)

		buf := types.EncodeAccountBytesV3(1, uint256.NewInt(0), nil, 0)

		err = domains.DomainPut(kv.AccountsDomain, addr, nil, buf, prev1, 0)
		require.NoError(t, err)
		prev1 = buf

		err = domains.DomainPut(kv.StorageDomain, addr, loc, []byte{addr[0], loc[0]}, prev2, 0)
		require.NoError(t, err)
		prev2 = []byte{addr[0], loc[0]}

	}
	require.NoError(t, commit(txNum))

	half := txs / 2
	for txNum = txNum + 1; txNum <= txs; txNum++ {
		domains.SetTxNum(txNum)

		addr, loc := keys[txNum-1-half][:length.Addr], keys[txNum-1-half][length.Addr:]

		prev, step, _, err := ac.d[kv.StorageDomain].GetLatest(addr, loc, tx)
		require.NoError(t, err)
		err = domains.DomainPut(kv.StorageDomain, addr, loc, []byte{addr[0], loc[0]}, prev, step)
		require.NoError(t, err)
	}

	ac.Close()
	err = tx.Commit()
	tx = nil

	tx, err = db.BeginRw(context.Background())
	require.NoError(t, err)

	aggCtx2 := agg.BeginFilesRo()
	defer aggCtx2.Close()

	for i, key := range keys {
		storedV, _, found, err := aggCtx2.d[kv.StorageDomain].GetLatest(key[:length.Addr], key[length.Addr:], tx)
		require.Truef(t, found, "key %x not found %d", key, i)
		require.NoError(t, err)
		require.EqualValues(t, key[0], storedV[0])
		require.EqualValues(t, key[length.Addr], storedV[1])
	}
	require.NoError(t, err)
}

func Test_EncodeCommitmentState(t *testing.T) {
	t.Parallel()
	cs := commitmentState{
		txNum:     rand.Uint64(),
		trieState: make([]byte, 1024),
	}
	n, err := rand.Read(cs.trieState)
	require.NoError(t, err)
	require.EqualValues(t, len(cs.trieState), n)

	buf, err := cs.Encode()
	require.NoError(t, err)
	require.NotEmpty(t, buf)

	var dec commitmentState
	err = dec.Decode(buf)
	require.NoError(t, err)
	require.EqualValues(t, cs.txNum, dec.txNum)
	require.EqualValues(t, cs.trieState, dec.trieState)
}

// takes first 100k keys from file
func pivotKeysFromKV(dataPath string) ([][]byte, error) {
	decomp, err := seg.NewDecompressor(dataPath)
	if err != nil {
		return nil, err
	}

	getter := decomp.MakeGetter()
	getter.Reset(0)

	key := make([]byte, 0, 64)

	listing := make([][]byte, 0, 1000)

	for getter.HasNext() {
		if len(listing) > 100000 {
			break
		}
		key, _ := getter.Next(key[:0])
		listing = append(listing, common.Copy(key))
		getter.Skip()
	}
	decomp.Close()

	return listing, nil
}

func generateKV(tb testing.TB, tmp string, keySize, valueSize, keyCount int, logger log.Logger, compressFlags seg.FileCompression) string {
	tb.Helper()

	rnd := newRnd(0)
	values := make([]byte, valueSize)

	dataPath := path.Join(tmp, fmt.Sprintf("%dk.kv", keyCount/1000))
	comp, err := seg.NewCompressor(context.Background(), "cmp", dataPath, tmp, seg.DefaultCfg, log.LvlDebug, logger)
	require.NoError(tb, err)

	bufSize := 8 * datasize.KB
	if keyCount > 1000 { // windows CI can't handle much small parallel disk flush
		bufSize = 1 * datasize.MB
	}
	collector := etl.NewCollector(BtreeLogPrefix+" genCompress", tb.TempDir(), etl.NewSortableBuffer(bufSize), logger)

	for i := 0; i < keyCount; i++ {
		key := make([]byte, keySize)
		n, err := rnd.Read(key[:])
		require.EqualValues(tb, keySize, n)
		binary.BigEndian.PutUint64(key[keySize-8:], uint64(i))
		require.NoError(tb, err)

		n, err = rnd.Read(values[:rnd.IntN(valueSize)+1])
		require.NoError(tb, err)

		err = collector.Collect(key, values[:n])
		require.NoError(tb, err)
	}

	writer := seg.NewWriter(comp, compressFlags)

	loader := func(k, v []byte, _ etl.CurrentTableReader, _ etl.LoadNextFunc) error {
		err = writer.AddWord(k)
		require.NoError(tb, err)
		err = writer.AddWord(v)
		require.NoError(tb, err)
		return nil
	}

	err = collector.Load(nil, "", loader, etl.TransformArgs{})
	require.NoError(tb, err)

	collector.Close()

	err = comp.Compress()
	require.NoError(tb, err)
	comp.Close()

	decomp, err := seg.NewDecompressor(dataPath)
	require.NoError(tb, err)
	defer decomp.Close()
	compPath := decomp.FilePath()
	ps := background.NewProgressSet()

	IndexFile := path.Join(tmp, fmt.Sprintf("%dk.bt", keyCount/1000))
	err = BuildBtreeIndexWithDecompressor(IndexFile, decomp, compressFlags, ps, tb.TempDir(), 777, logger, true)
	require.NoError(tb, err)

	return compPath
}

func testDbAndAggregatorv3(tb testing.TB, aggStep uint64) (kv.RwDB, *Aggregator) {
	tb.Helper()
	require, logger := require.New(tb), log.New()
	dirs := datadir.New(tb.TempDir())
	db := mdbx.New(kv.ChainDB, logger).InMem(dirs.Chaindata).GrowthStep(32 * datasize.MB).MapSize(2 * datasize.GB).WithTableCfg(func(defaultBuckets kv.TableCfg) kv.TableCfg {
		return kv.ChaindataTablesCfg
	}).MustOpen()
	tb.Cleanup(db.Close)

	agg, err := NewAggregator(context.Background(), dirs, aggStep, db, logger)
	require.NoError(err)
	tb.Cleanup(agg.Close)
	err = agg.OpenFolder()
	require.NoError(err)
	agg.DisableFsync()
	return db, agg
}

// generate test data for table tests, containing n; n < 20 keys of length 20 bytes and values of length <= 16 bytes
func generateInputData(tb testing.TB, keySize, valueSize, keyCount int) ([][]byte, [][]byte) {
	tb.Helper()

	rnd := newRnd(0)
	values := make([][]byte, keyCount)
	keys := make([][]byte, keyCount)

	bk, bv := make([]byte, keySize), make([]byte, valueSize)
	for i := 0; i < keyCount; i++ {
		n, err := rnd.Read(bk[:])
		require.EqualValues(tb, keySize, n)
		require.NoError(tb, err)
		keys[i] = common.Copy(bk[:n])

		n, err = rnd.Read(bv[:rnd.IntN(valueSize)+1])
		require.NoError(tb, err)

		values[i] = common.Copy(bv[:n])
	}
	return keys, values
}

func TestAggregatorV3_SharedDomains(t *testing.T) {
	t.Parallel()
	db, agg := testDbAndAggregatorv3(t, 20)
	ctx := context.Background()

	ac := agg.BeginFilesRo()
	defer ac.Close()

	rwTx, err := db.BeginRw(context.Background())
	require.NoError(t, err)
	defer rwTx.Rollback()

	domains, err := NewSharedDomains(WrapTxWithCtx(rwTx, ac), log.New())
	require.NoError(t, err)
	defer domains.Close()
	changesetAt5 := &StateChangeSet{}
	changesetAt3 := &StateChangeSet{}

	keys, vals := generateInputData(t, 20, 16, 10)
	keys = keys[:2]

	var i int
	roots := make([][]byte, 0, 10)
	var pruneFrom uint64 = 5

	mc := agg.BeginFilesRo()
	defer mc.Close()

	for i = 0; i < len(vals); i++ {
		domains.SetTxNum(uint64(i))
		if i == 3 {
			domains.SetChangesetAccumulator(changesetAt3)
		}
		if i == 5 {
			domains.SetChangesetAccumulator(changesetAt5)
		}

		for j := 0; j < len(keys); j++ {
			buf := types.EncodeAccountBytesV3(uint64(i), uint256.NewInt(uint64(i*100_000)), nil, 0)
			prev, step, err := domains.GetLatest(kv.AccountsDomain, keys[j], nil)
			require.NoError(t, err)

			err = domains.DomainPut(kv.AccountsDomain, keys[j], nil, buf, prev, step)
			//err = domains.UpdateAccountCode(keys[j], vals[i], nil)
			require.NoError(t, err)
		}
		rh, err := domains.ComputeCommitment(ctx, true, domains.BlockNum(), "")
		require.NoError(t, err)
		require.NotEmpty(t, rh)
		roots = append(roots, rh)
	}

	err = domains.Flush(context.Background(), rwTx)
	require.NoError(t, err)
	ac.Close()

	ac = agg.BeginFilesRo()
	defer ac.Close()
	domains, err = NewSharedDomains(WrapTxWithCtx(rwTx, ac), log.New())
	require.NoError(t, err)
	defer domains.Close()
	diffs := [kv.DomainLen][]DomainEntryDiff{}
	for idx := range changesetAt5.Diffs {
		diffs[idx] = changesetAt5.Diffs[idx].GetDiffSet()
	}
	err = domains.Unwind(context.Background(), rwTx, 0, pruneFrom, &diffs)
	require.NoError(t, err)

	domains.SetChangesetAccumulator(changesetAt3)
	for i = int(pruneFrom); i < len(vals); i++ {
		domains.SetTxNum(uint64(i))

		for j := 0; j < len(keys); j++ {
			buf := types.EncodeAccountBytesV3(uint64(i), uint256.NewInt(uint64(i*100_000)), nil, 0)
			prev, step, _, err := mc.GetLatest(kv.AccountsDomain, keys[j], nil, rwTx)
			require.NoError(t, err)

			err = domains.DomainPut(kv.AccountsDomain, keys[j], nil, buf, prev, step)
			require.NoError(t, err)
			//err = domains.UpdateAccountCode(keys[j], vals[i], nil)
			//require.NoError(t, err)
		}

		rh, err := domains.ComputeCommitment(ctx, true, domains.BlockNum(), "")
		require.NoError(t, err)
		require.NotEmpty(t, rh)
		require.EqualValues(t, roots[i], rh)
	}

	err = domains.Flush(context.Background(), rwTx)
	require.NoError(t, err)
	ac.Close()

	pruneFrom = 3

	ac = agg.BeginFilesRo()
	defer ac.Close()
	domains, err = NewSharedDomains(WrapTxWithCtx(rwTx, ac), log.New())
	require.NoError(t, err)
	defer domains.Close()
	for idx := range changesetAt3.Diffs {
		diffs[idx] = changesetAt3.Diffs[idx].GetDiffSet()
	}
	err = domains.Unwind(context.Background(), rwTx, 0, pruneFrom, &diffs)
	require.NoError(t, err)

	for i = int(pruneFrom); i < len(vals); i++ {
		domains.SetTxNum(uint64(i))

		for j := 0; j < len(keys); j++ {
			buf := types.EncodeAccountBytesV3(uint64(i), uint256.NewInt(uint64(i*100_000)), nil, 0)
			prev, step, _, err := mc.GetLatest(kv.AccountsDomain, keys[j], nil, rwTx)
			require.NoError(t, err)

			err = domains.DomainPut(kv.AccountsDomain, keys[j], nil, buf, prev, step)
			require.NoError(t, err)
			//err = domains.UpdateAccountCode(keys[j], vals[i], nil)
			//require.NoError(t, err)
		}

		rh, err := domains.ComputeCommitment(ctx, true, domains.BlockNum(), "")
		require.NoError(t, err)
		require.NotEmpty(t, rh)
		require.EqualValues(t, roots[i], rh)
	}
}

// also useful to decode given input into v3 account
func Test_helper_decodeAccountv3Bytes(t *testing.T) {
	t.Parallel()
	input, err := hex.DecodeString("000114000101")
	require.NoError(t, err)

	n, b, ch := types.DecodeAccountBytesV3(input)
	fmt.Printf("input %x nonce %d balance %d codeHash %d\n", input, n, b.Uint64(), ch)
}

func TestAggregator_RebuildCommitmentBasedOnFiles(t *testing.T) {
	db, agg := testDbAggregatorWithFiles(t, &testAggConfig{
		stepSize:                         20,
		disableCommitmentBranchTransform: false,
	})

	ac := agg.BeginFilesRo()
	roots := make([]common.Hash, 0)

	// collect latest root from each available file
	compression := ac.d[kv.CommitmentDomain].d.compression
	fnames := []string{}
	for _, f := range ac.d[kv.CommitmentDomain].files {
		k, stateVal, _, found, err := f.src.bindex.Get(keyCommitmentState, seg.NewReader(f.src.decompressor.MakeGetter(), compression))
		require.NoError(t, err)
		require.True(t, found)
		require.EqualValues(t, keyCommitmentState, k)
		rh, err := commitment.HexTrieExtractStateRoot(stateVal)
		require.NoError(t, err)

		roots = append(roots, common.BytesToHash(rh))
		fmt.Printf("file %s root %x\n", filepath.Base(f.src.decompressor.FilePath()), rh)
		fnames = append(fnames, f.src.decompressor.FilePath())
	}
	ac.Close()
	agg.d[kv.CommitmentDomain].closeFilesAfterStep(0) // close commitment files to remove

	// now clean all commitment files along with related db buckets
	rwTx, err := db.BeginRw(context.Background())
	require.NoError(t, err)
	defer rwTx.Rollback()

	buckets, err := rwTx.ListBuckets()
	require.NoError(t, err)
	for i, b := range buckets {
		if strings.Contains(strings.ToLower(b), "commitment") {
			size, err := rwTx.BucketSize(b)
			require.NoError(t, err)
			t.Logf("cleaned table #%d %s: %d keys", i, b, size)

			err = rwTx.ClearBucket(b)
			require.NoError(t, err)
		}
	}
	require.NoError(t, rwTx.Commit())

	for _, fn := range fnames {
		if strings.Contains(fn, "v1-commitment") {
			require.NoError(t, os.Remove(fn))
			t.Logf("removed file %s", filepath.Base(fn))
		}
	}
	err = agg.OpenFolder()
	require.NoError(t, err)

	ctx := context.Background()
	finalRoot, err := agg.RebuildCommitmentFiles(ctx, nil, &rawdbv3.TxNums)
	require.NoError(t, err)
	require.NotEmpty(t, finalRoot)
	require.NotEqualValues(t, commitment.EmptyRootHash, finalRoot)

	require.EqualValues(t, roots[len(roots)-1][:], finalRoot[:])
}
