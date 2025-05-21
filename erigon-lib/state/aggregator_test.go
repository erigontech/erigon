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
	"path/filepath"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/c2h5oh/datasize"
	"github.com/holiman/uint256"
	"github.com/stretchr/testify/require"

	"github.com/erigontech/erigon-lib/commitment"
	"github.com/erigontech/erigon-lib/common"
	"github.com/erigontech/erigon-lib/common/background"
	"github.com/erigontech/erigon-lib/common/datadir"
	"github.com/erigontech/erigon-lib/common/empty"
	"github.com/erigontech/erigon-lib/common/length"
	"github.com/erigontech/erigon-lib/config3"
	"github.com/erigontech/erigon-lib/etl"
	"github.com/erigontech/erigon-lib/kv"
	"github.com/erigontech/erigon-lib/kv/mdbx"
	"github.com/erigontech/erigon-lib/kv/order"
	"github.com/erigontech/erigon-lib/kv/rawdbv3"
	"github.com/erigontech/erigon-lib/kv/stream"
	"github.com/erigontech/erigon-lib/log/v3"
	"github.com/erigontech/erigon-lib/seg"
	"github.com/erigontech/erigon-lib/types/accounts"
)

func TestAggregatorV3_Merge(t *testing.T) {
	if testing.Short() {
		t.Skip()
	}

	t.Parallel()
	db, agg := testDbAndAggregatorv3(t, 10)
	rwTx, err := db.BeginRwNosync(context.Background())
	require.NoError(t, err)
	defer func() {
		if rwTx != nil {
			rwTx.Rollback()
		}
	}()

	ac := agg.BeginFilesRo()
	defer ac.Close()
	domains, err := NewSharedDomains(wrapTxWithCtx(rwTx, ac), log.New())
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
		domains.SetTxNum(txNum)

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
		err = domains.DomainPut(kv.AccountsDomain, rwTx, addr, nil, buf, nil, 0)
		require.NoError(t, err)

		err = domains.DomainPut(kv.StorageDomain, append(common.Copy(addr), loc...), nil, []byte{addr[0], loc[0]}, nil, 0)
		require.NoError(t, err)

		var v [8]byte
		binary.BigEndian.PutUint64(v[:], txNum)
		if txNum%135 == 0 {
			pv, step, err := domains.GetLatest(kv.CommitmentDomain, rwTx, commKey2)
			require.NoError(t, err)

			err = domains.DomainPut(kv.CommitmentDomain, rwTx, commKey2, nil, v[:], pv, step)
			require.NoError(t, err)
			otherMaxWrite = txNum
		} else {
			pv, step, err := domains.GetLatest(kv.CommitmentDomain, rwTx, commKey1)
			require.NoError(t, err)

			err = domains.DomainPut(kv.CommitmentDomain, rwTx, commKey1, nil, v[:], pv, step)
			require.NoError(t, err)
			maxWrite = txNum
		}
		require.NoError(t, err)

	}

	err = domains.Flush(context.Background(), rwTx, 0)
	require.NoError(t, err)

	require.NoError(t, err)
	err = rwTx.Commit()
	require.NoError(t, err)
	rwTx = nil

	err = agg.BuildFiles(txs)
	require.NoError(t, err)

	rwTx, err = db.BeginRw(context.Background())
	require.NoError(t, err)
	defer rwTx.Rollback()

	logEvery := time.NewTicker(30 * time.Second)
	defer logEvery.Stop()
	stat, err := ac.prune(context.Background(), rwTx, 0, logEvery)
	require.NoError(t, err)
	t.Logf("Prune: %s", stat)

	err = rwTx.Commit()
	require.NoError(t, err)

	err = agg.MergeLoop(context.Background())
	require.NoError(t, err)

	// Check the history
	roTx, err := db.BeginRo(context.Background())
	require.NoError(t, err)
	defer roTx.Rollback()

	dc := agg.BeginFilesRo()

	v, _, ex, err := dc.GetLatest(kv.CommitmentDomain, commKey1, roTx)
	require.NoError(t, err)
	require.Truef(t, ex, "key %x not found", commKey1)

	require.Equal(t, maxWrite, binary.BigEndian.Uint64(v[:]))

	v, _, ex, err = dc.GetLatest(kv.CommitmentDomain, commKey2, roTx)
	require.NoError(t, err)
	require.Truef(t, ex, "key %x not found", commKey2)
	dc.Close()

	require.Equal(t, otherMaxWrite, binary.BigEndian.Uint64(v[:]))
}

func TestAggregatorV3_DirtyFilesRo(t *testing.T) {
	if testing.Short() {
		t.Skip()
	}

	t.Parallel()
	db, agg := testDbAndAggregatorv3(t, 10)
	rwTx, err := db.BeginRwNosync(context.Background())
	require.NoError(t, err)
	defer func() {
		if rwTx != nil {
			rwTx.Rollback()
		}
	}()

	ac := agg.BeginFilesRo()
	defer ac.Close()
	domains, err := NewSharedDomains(wrapTxWithCtx(rwTx, ac), log.New())
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
	//var maxWrite, otherMaxWrite uint64
	for txNum := uint64(1); txNum <= txs; txNum++ {
		domains.SetTxNum(txNum)

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
		err = domains.DomainPut(kv.AccountsDomain, rwTx, addr, nil, buf, nil, 0)
		require.NoError(t, err)

		err = domains.DomainPut(kv.StorageDomain, composite(addr, loc), nil, []byte{addr[0], loc[0]}, nil, 0)
		require.NoError(t, err)

		var v [8]byte
		binary.BigEndian.PutUint64(v[:], txNum)
		if txNum%135 == 0 {
			pv, step, err := domains.GetLatest(kv.CommitmentDomain, rwTx, commKey2)
			require.NoError(t, err)

			err = domains.DomainPut(kv.CommitmentDomain, rwTx, commKey2, nil, v[:], pv, step)
			require.NoError(t, err)
			// otherMaxWrite = txNum
		} else {
			pv, step, err := domains.GetLatest(kv.CommitmentDomain, rwTx, commKey1)
			require.NoError(t, err)

			err = domains.DomainPut(kv.CommitmentDomain, rwTx, commKey1, nil, v[:], pv, step)
			require.NoError(t, err)
			// maxWrite = txNum
		}
		require.NoError(t, err)

	}

	err = domains.Flush(context.Background(), rwTx, 0)
	require.NoError(t, err)

	require.NoError(t, err)
	err = rwTx.Commit()
	require.NoError(t, err)
	rwTx = nil

	err = agg.BuildFiles(txs)
	require.NoError(t, err)

	checkDirtyFiles := func(dirtyFiles []*filesItem, expectedLen, expectedRefCnt int, disabled bool, name string) {
		if disabled {
			expectedLen = 0
		}

		require.Len(t, dirtyFiles, expectedLen, name)
		for _, f := range dirtyFiles {
			require.Equal(t, int32(expectedRefCnt), f.refcount.Load(), name)
		}
	}

	checkAllEntities := func(expectedLen, expectedRefCnt int) {
		for _, d := range agg.d {
			checkDirtyFiles(d.dirtyFiles.Items(), expectedLen, expectedRefCnt, d.disable, d.name.String())
			if d.snapshotsDisabled {
				continue
			}
			checkDirtyFiles(d.History.dirtyFiles.Items(), expectedLen, expectedRefCnt, d.disable, d.name.String())
			checkDirtyFiles(d.History.InvertedIndex.dirtyFiles.Items(), expectedLen, expectedRefCnt, d.disable, d.name.String())
		}

		for _, ii := range agg.iis {
			checkDirtyFiles(ii.dirtyFiles.Items(), expectedLen, expectedRefCnt, ii.disable, ii.filenameBase)
		}
	}

	checkAllEntities(3, 0)

	aggDirtyRoTx := agg.DebugBeginDirtyFilesRo()
	checkAllEntities(3, 1)

	aggDirtyRoTx2 := agg.DebugBeginDirtyFilesRo()
	checkAllEntities(3, 2)

	aggDirtyRoTx2.Close()
	checkAllEntities(3, 1)
	aggDirtyRoTx2.Close() // close again, should remain same refcnt
	checkAllEntities(3, 1)

	aggDirtyRoTx.Close()
	checkAllEntities(3, 0)
}

func TestAggregatorV3_MergeValTransform(t *testing.T) {
	if testing.Short() {
		t.Skip()
	}

	t.Parallel()
	db, agg := testDbAndAggregatorv3(t, 10)
	rwTx, err := db.BeginRwNosync(context.Background())
	require.NoError(t, err)
	defer func() {
		if rwTx != nil {
			rwTx.Rollback()
		}
	}()
	ac := agg.BeginFilesRo()
	defer ac.Close()
	domains, err := NewSharedDomains(wrapTxWithCtx(rwTx, ac), log.New())
	require.NoError(t, err)
	defer domains.Close()

	txs := uint64(1000)
	rnd := rand.New(rand.NewSource(time.Now().UnixNano()))

	agg.commitmentValuesTransform = true

	state := make(map[string][]byte)

	// keys are encodings of numbers 1..31
	// each key changes value on every txNum which is multiple of the key
	//var maxWrite, otherMaxWrite uint64
	for txNum := uint64(1); txNum <= txs; txNum++ {
		domains.SetTxNum(txNum)

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
		err = domains.DomainPut(kv.AccountsDomain, rwTx, addr, nil, buf, nil, 0)
		require.NoError(t, err)

		err = domains.DomainPut(kv.StorageDomain, composite(addr, loc), nil, []byte{addr[0], loc[0]}, nil, 0)
		require.NoError(t, err)

		if (txNum+1)%agg.StepSize() == 0 {
			_, err := domains.ComputeCommitment(context.Background(), rwTx, true, txNum/10, "")
			require.NoError(t, err)
		}

		state[string(addr)] = buf
		state[string(addr)+string(loc)] = []byte{addr[0], loc[0]}
	}

	err = domains.Flush(context.Background(), rwTx, 0)
	require.NoError(t, err)

	err = rwTx.Commit()
	require.NoError(t, err)
	rwTx = nil

	err = agg.BuildFiles(txs)
	require.NoError(t, err)

	ac.Close()
	ac = agg.BeginFilesRo()
	defer ac.Close()

	rwTx, err = db.BeginRwNosync(context.Background())
	require.NoError(t, err)
	defer func() {
		if rwTx != nil {
			rwTx.Rollback()
		}
	}()

	logEvery := time.NewTicker(30 * time.Second)
	defer logEvery.Stop()
	stat, err := ac.prune(context.Background(), rwTx, 0, logEvery)
	require.NoError(t, err)
	t.Logf("Prune: %s", stat)

	err = rwTx.Commit()
	require.NoError(t, err)

	err = agg.MergeLoop(context.Background())
	require.NoError(t, err)
}

func TestAggregatorV3_RestartOnDatadir(t *testing.T) {
	if testing.Short() {
		t.Skip()
	}

	t.Parallel()

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

	tx, err := db.BeginRw(context.Background())
	require.NoError(t, err)
	defer func() {
		if tx != nil {
			tx.Rollback()
		}
	}()
	ac := agg.BeginFilesRo()
	defer ac.Close()

	domains, err := NewSharedDomains(wrapTxWithCtx(tx, ac), log.New())
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
		require.Equal(t, length.Addr, n)

		n, err = rnd.Read(loc)
		require.NoError(t, err)
		require.Equal(t, length.Hash, n)
		//keys[txNum-1] = append(addr, loc...)
		acc := accounts.Account{
			Nonce:       1,
			Balance:     *uint256.NewInt(rnd.Uint64()),
			CodeHash:    common.Hash{},
			Incarnation: 0,
		}
		buf := accounts.SerialiseV3(&acc)
		err = domains.DomainPut(kv.AccountsDomain, tx, addr, nil, buf, nil, 0)
		require.NoError(t, err)

		err = domains.DomainPut(kv.StorageDomain, composite(addr, loc), nil, []byte{addr[0], loc[0]}, nil, 0)
		require.NoError(t, err)

		err = domains.DomainPut(kv.CommitmentDomain, tx, someKey, nil, aux[:], nil, 0)
		require.NoError(t, err)
		maxWrite = txNum
	}
	_, err = domains.ComputeCommitment(ctx, tx, true, domains.BlockNum(), "")
	require.NoError(t, err)

	err = domains.Flush(context.Background(), tx, 0)
	require.NoError(t, err)
	err = tx.Commit()
	require.NoError(t, err)
	tx = nil

	err = agg.BuildFiles(txs)
	require.NoError(t, err)

	agg.Close()

	// Start another aggregator on same datadir
	salt, err := GetStateIndicesSalt(agg.dirs, false, logger)
	require.NoError(t, err)
	require.NotNil(t, salt)
	anotherAgg, err := NewAggregator2(context.Background(), agg.dirs, aggStep, salt, db, logger)
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
	dom2, err := NewSharedDomains(wrapTxWithCtx(rwTx, ac2), log.New())
	require.NoError(t, err)
	defer dom2.Close()

	err = dom2.SeekCommitment(ctx, wrapTxWithCtx(rwTx, ac2))
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
	v, _, ex, err := dc.GetLatest(kv.CommitmentDomain, someKey, roTx)
	require.NoError(t, err)
	require.True(t, ex)
	dc.Close()

	require.Equal(t, maxWrite, binary.BigEndian.Uint64(v[:]))
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
	bplus := bt.bplus
	require.GreaterOrEqual(t, len(bplus.mx), keyCount/int(DefaultBtreeM))

	for i := 1; i < len(bt.bplus.mx); i++ {
		require.NotZero(t, bt.bplus.mx[i].di)
		require.NotZero(t, bt.bplus.mx[i].off)
		require.NotEmpty(t, bt.bplus.mx[i].key)
	}
}

func TestAggregatorV3_PruneSmallBatches(t *testing.T) {
	if testing.Short() {
		t.Skip()
	}

	t.Parallel()
	aggStep := uint64(2)
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

	domains, err := NewSharedDomains(wrapTxWithCtx(tx, ac), log.New())
	require.NoError(t, err)
	defer domains.Close()

	maxTx := aggStep * 3
	t.Logf("step=%d tx_count=%d\n", aggStep, maxTx)

	rnd := newRnd(0)

	generateSharedDomainsUpdates(t, domains, tx, maxTx, rnd, length.Addr, 10, aggStep/2)

	// flush and build files
	err = domains.Flush(context.Background(), tx, 0)
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
		it, err := ac.DebugRangeLatest(tx, kv.AccountsDomain, nil, nil, maxInt)
		require.NoError(t, err)
		accountsRange = extractKVErrIterator(t, it)

		it, err = ac.DebugRangeLatest(tx, kv.StorageDomain, nil, nil, maxInt)
		require.NoError(t, err)
		storageRange = extractKVErrIterator(t, it)

		it, err = ac.DebugRangeLatest(tx, kv.CodeDomain, nil, nil, maxInt)
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
		it, err := ac.DebugRangeLatest(afterTx, kv.AccountsDomain, nil, nil, maxInt)
		require.NoError(t, err)
		accountsRangeAfter = extractKVErrIterator(t, it)

		it, err = ac.DebugRangeLatest(afterTx, kv.StorageDomain, nil, nil, maxInt)
		require.NoError(t, err)
		storageRangeAfter = extractKVErrIterator(t, it)

		it, err = ac.DebugRangeLatest(afterTx, kv.CodeDomain, nil, nil, maxInt)
		require.NoError(t, err)
		codeRangeAfter = extractKVErrIterator(t, it)

		its, err := ac.d[kv.AccountsDomain].ht.HistoryRange(0, int(maxTx), order.Asc, maxInt, afterTx)
		require.NoError(t, err)
		accountHistRangeAfter = extractKVSErrIterator(t, its)
		its, err = ac.d[kv.CodeDomain].ht.HistoryRange(0, int(maxTx), order.Asc, maxInt, afterTx)
		require.NoError(t, err)
		codeHistRangeAfter = extractKVSErrIterator(t, its)
		its, err = ac.d[kv.StorageDomain].ht.HistoryRange(0, int(maxTx), order.Asc, maxInt, afterTx)
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
		require.Equal(t, v.s, v2.s)
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
		require.Equal(t, v, m2[k])
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

func generateSharedDomainsUpdates(t *testing.T, domains *SharedDomains, tx kv.Tx, maxTxNum uint64, rnd *rndGen, keyMaxLen, keysCount, commitEvery uint64) map[string]struct{} {
	t.Helper()
	usedKeys := make(map[string]struct{}, keysCount*maxTxNum)
	for txNum := uint64(1); txNum <= maxTxNum; txNum++ {
		used := generateSharedDomainsUpdatesForTx(t, domains, tx, txNum, rnd, usedKeys, keyMaxLen, keysCount)
		for k := range used {
			usedKeys[k] = struct{}{}
		}
		if txNum%commitEvery == 0 {
			// domains.SetTrace(true)
			rh, err := domains.ComputeCommitment(context.Background(), tx, true, txNum/commitEvery, "")
			require.NoErrorf(t, err, "txNum=%d", txNum)
			t.Logf("commitment %x txn=%d", rh, txNum)
		}
	}
	return usedKeys
}

func generateSharedDomainsUpdatesForTx(t *testing.T, domains *SharedDomains, tx kv.Tx, txNum uint64, rnd *rndGen, prevKeys map[string]struct{}, keyMaxLen, keysCount uint64) map[string]struct{} {
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

			err = domains.DomainPut(kv.AccountsDomain, tx, key, nil, buf, prev, step)
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

			err = domains.DomainPut(kv.CodeDomain, tx, key, nil, codeUpd, prev, step)
			require.NoError(t, err)
		case r > 80:
			if !existed {
				continue
			}
			usedKeys[string(key)] = struct{}{}

			err := domains.DomainDel(kv.AccountsDomain, tx, key, nil, 0)
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
				err = domains.DomainPut(kv.AccountsDomain, tx, key, nil, buf, prev, step)
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

				err = domains.DomainPut(kv.StorageDomain, sk, nil, uint256.NewInt(txNum).Bytes(), prev, step)
				require.NoError(t, err)
			}

		}
	}
	return usedKeys
}

func TestAggregatorV3_RestartOnFiles(t *testing.T) {
	if testing.Short() {
		t.Skip()
	}

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
	domains, err := NewSharedDomains(wrapTxWithCtx(tx, ac), log.New())
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
		err = domains.DomainPut(kv.AccountsDomain, tx, addr, nil, buf[:], nil, 0)
		require.NoError(t, err)

		err = domains.DomainPut(kv.StorageDomain, composite(addr, loc), nil, []byte{addr[0], loc[0]}, nil, 0)
		require.NoError(t, err)

		keys[txNum-1] = append(addr, loc...)
	}

	// flush and build files
	err = domains.Flush(context.Background(), tx, 0)
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

	salt, err := GetStateIndicesSalt(dirs, false, logger)
	require.NoError(t, err)
	require.NotNil(t, salt)
	newAgg, err := NewAggregator2(context.Background(), agg.dirs, aggStep, salt, newDb, logger)
	require.NoError(t, err)
	require.NoError(t, newAgg.OpenFolder())

	newTx, err := newDb.BeginRw(context.Background())
	require.NoError(t, err)
	defer newTx.Rollback()

	ac = newAgg.BeginFilesRo()
	defer ac.Close()
	tx2 := wrapTxWithCtx(newTx, ac)
	newDoms, err := NewSharedDomains(tx2, log.New())
	require.NoError(t, err)
	defer newDoms.Close()

	err = newDoms.SeekCommitment(ctx, tx2)
	require.NoError(t, err)
	latestTx := newDoms.TxNum()
	t.Logf("seek to latest_tx=%d", latestTx)

	miss := uint64(0)
	for i, key := range keys {
		if uint64(i+1) >= txs-aggStep {
			continue // finishtx always stores last agg step in db which we deleted, so missing  values which were not aggregated is expected
		}
		stored, _, _, err := ac.GetLatest(kv.AccountsDomain, key[:length.Addr], tx2)
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

		storedV, _, found, err := ac.GetLatest(kv.StorageDomain, key, tx2)
		require.NoError(t, err)
		require.True(t, found)
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
	domains, err := NewSharedDomains(wrapTxWithCtx(tx, ac), log.New())
	require.NoError(t, err)
	defer domains.Close()

	var latestCommitTxNum uint64
	commit := func(txn uint64) error {
		err = domains.Flush(ctx, tx, 0)
		require.NoError(t, err)
		ac.Close()
		err = tx.Commit()
		require.NoError(t, err)

		tx, err = db.BeginRw(context.Background())
		require.NoError(t, err)
		ac = agg.BeginFilesRo()
		domains, err = NewSharedDomains(wrapTxWithCtx(tx, ac), log.New())
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
		domains.SetTxNum(txNum)

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

		err = domains.DomainPut(kv.AccountsDomain, tx, addr, nil, buf, prev1, 0)
		require.NoError(t, err)
		prev1 = buf

		err = domains.DomainPut(kv.StorageDomain, composite(addr, loc), nil, []byte{addr[0], loc[0]}, prev2, 0)
		require.NoError(t, err)
		prev2 = []byte{addr[0], loc[0]}

	}
	require.NoError(t, commit(txNum))

	half := txs / 2
	for txNum = txNum + 1; txNum <= txs; txNum++ {
		domains.SetTxNum(txNum)

		addr, loc := keys[txNum-1-half][:length.Addr], keys[txNum-1-half][length.Addr:]

		prev, step, _, err := ac.d[kv.StorageDomain].GetLatest(keys[txNum-1-half], tx)
		require.NoError(t, err)
		err = domains.DomainPut(kv.StorageDomain, composite(addr, loc), nil, []byte{addr[0], loc[0]}, prev, step)
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
		storedV, _, found, err := aggCtx2.d[kv.StorageDomain].GetLatest(key, tx)
		require.Truef(t, found, "key %x not found %d", key, i)
		require.NoError(t, err)
		require.Equal(t, key[0], storedV[0])
		require.Equal(t, key[length.Addr], storedV[1])
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
	require.Equal(t, len(cs.trieState), n)

	buf, err := cs.Encode()
	require.NoError(t, err)
	require.NotEmpty(t, buf)

	var dec commitmentState
	err = dec.Decode(buf)
	require.NoError(t, err)
	require.Equal(t, cs.txNum, dec.txNum)
	require.Equal(t, cs.trieState, dec.trieState)
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

	dataPath := filepath.Join(tmp, fmt.Sprintf("%dk.kv", keyCount/1000))
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
		require.Equal(tb, keySize, n)
		binary.BigEndian.PutUint64(key[keySize-8:], uint64(i))
		require.NoError(tb, err)

		n, err = rnd.Read(values[:rnd.IntN(valueSize)+1])
		require.NoError(tb, err)

		err = collector.Collect(key, values[:n])
		require.NoError(tb, err)
	}

	writer := seg.NewWriter(comp, compressFlags)

	loader := func(k, v []byte, _ etl.CurrentTableReader, _ etl.LoadNextFunc) error {
		_, err = writer.Write(k)
		require.NoError(tb, err)
		_, err = writer.Write(v)
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

	IndexFile := filepath.Join(tmp, fmt.Sprintf("%dk.bt", keyCount/1000))
	err = BuildBtreeIndexWithDecompressor(IndexFile, decomp, compressFlags, ps, tb.TempDir(), 777, logger, true, AccessorBTree|AccessorExistence)
	require.NoError(tb, err)

	return compPath
}

func testDbAndAggregatorv3(tb testing.TB, aggStep uint64) (kv.RwDB, *Aggregator) {
	tb.Helper()
	require, logger := require.New(tb), log.New()
	dirs := datadir.New(tb.TempDir())
	db := mdbx.New(kv.ChainDB, logger).InMem(dirs.Chaindata).GrowthStep(32 * datasize.MB).MapSize(2 * datasize.GB).MustOpen()
	tb.Cleanup(db.Close)

	salt, err := GetStateIndicesSalt(dirs, true, logger)
	require.NoError(err)
	agg, err := NewAggregator2(context.Background(), dirs, aggStep, salt, db, logger)
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
		require.Equal(tb, keySize, n)
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

	domains, err := NewSharedDomains(wrapTxWithCtx(rwTx, ac), log.New())
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
			acc := accounts.Account{
				Nonce:       uint64(i),
				Balance:     *uint256.NewInt(uint64(i * 100_000)),
				CodeHash:    common.Hash{},
				Incarnation: 0,
			}
			buf := accounts.SerialiseV3(&acc)
			prev, step, err := domains.GetLatest(kv.AccountsDomain, rwTx, keys[j])
			require.NoError(t, err)

			err = domains.DomainPut(kv.AccountsDomain, rwTx, keys[j], nil, buf, prev, step)
			//err = domains.UpdateAccountCode(keys[j], vals[i], nil)
			require.NoError(t, err)
		}
		rh, err := domains.ComputeCommitment(ctx, rwTx, true, domains.BlockNum(), "")
		require.NoError(t, err)
		require.NotEmpty(t, rh)
		roots = append(roots, rh)
	}

	err = domains.Flush(context.Background(), rwTx, 0)
	require.NoError(t, err)
	ac.Close()

	ac = agg.BeginFilesRo()
	defer ac.Close()
	domains, err = NewSharedDomains(wrapTxWithCtx(rwTx, ac), log.New())
	require.NoError(t, err)
	defer domains.Close()
	diffs := [kv.DomainLen][]kv.DomainEntryDiff{}
	for idx := range changesetAt5.Diffs {
		diffs[idx] = changesetAt5.Diffs[idx].GetDiffSet()
	}
	err = domains.Unwind(context.Background(), wrapTxWithCtx(rwTx, ac), 0, pruneFrom, &diffs)
	require.NoError(t, err)

	domains.SetChangesetAccumulator(changesetAt3)
	for i = int(pruneFrom); i < len(vals); i++ {
		domains.SetTxNum(uint64(i))

		for j := 0; j < len(keys); j++ {
			acc := accounts.Account{
				Nonce:       uint64(i),
				Balance:     *uint256.NewInt(uint64(i * 100_000)),
				CodeHash:    common.Hash{},
				Incarnation: 0,
			}
			buf := accounts.SerialiseV3(&acc)
			prev, step, _, err := mc.GetLatest(kv.AccountsDomain, keys[j], rwTx)
			require.NoError(t, err)

			err = domains.DomainPut(kv.AccountsDomain, rwTx, keys[j], nil, buf, prev, step)
			require.NoError(t, err)
			//err = domains.UpdateAccountCode(keys[j], vals[i], nil)
			//require.NoError(t, err)
		}

		rh, err := domains.ComputeCommitment(ctx, rwTx, true, domains.BlockNum(), "")
		require.NoError(t, err)
		require.NotEmpty(t, rh)
		require.Equal(t, roots[i], rh)
	}

	err = domains.Flush(context.Background(), rwTx, 0)
	require.NoError(t, err)
	ac.Close()

	pruneFrom = 3

	ac = agg.BeginFilesRo()
	defer ac.Close()
	domains, err = NewSharedDomains(wrapTxWithCtx(rwTx, ac), log.New())
	require.NoError(t, err)
	defer domains.Close()
	for idx := range changesetAt3.Diffs {
		diffs[idx] = changesetAt3.Diffs[idx].GetDiffSet()
	}
	err = domains.Unwind(context.Background(), wrapTxWithCtx(rwTx, ac), 0, pruneFrom, &diffs)
	require.NoError(t, err)

	for i = int(pruneFrom); i < len(vals); i++ {
		domains.SetTxNum(uint64(i))

		for j := 0; j < len(keys); j++ {
			acc := accounts.Account{
				Nonce:       uint64(i),
				Balance:     *uint256.NewInt(uint64(i * 100_000)),
				CodeHash:    common.Hash{},
				Incarnation: 0,
			}
			buf := accounts.SerialiseV3(&acc)
			prev, step, _, err := mc.GetLatest(kv.AccountsDomain, keys[j], rwTx)
			require.NoError(t, err)

			err = domains.DomainPut(kv.AccountsDomain, rwTx, keys[j], nil, buf, prev, step)
			require.NoError(t, err)
			//err = domains.UpdateAccountCode(keys[j], vals[i], nil)
			//require.NoError(t, err)
		}

		rh, err := domains.ComputeCommitment(ctx, rwTx, true, domains.BlockNum(), "")
		require.NoError(t, err)
		require.NotEmpty(t, rh)
		require.Equal(t, roots[i], rh)
	}
}

// also useful to decode given input into v3 account
func Test_helper_decodeAccountv3Bytes(t *testing.T) {
	t.Parallel()
	input, err := hex.DecodeString("000114000101")
	require.NoError(t, err)

	acc := accounts.Account{}
	_ = accounts.DeserialiseV3(&acc, input)
	fmt.Printf("input %x nonce %d balance %d codeHash %d\n", input, acc.Nonce, acc.Balance.Uint64(), acc.CodeHash.Bytes())
}

// wrapTxWithCtx - deprecated copy of kv_temporal.go - visible only in tests
// need to move non-unit-tests to own package
func wrapTxWithCtx(tx kv.Tx, aggTx *AggregatorRoTx) *Tx {
	return &Tx{MdbxTx: tx.(*mdbx.MdbxTx), aggtx: aggTx}
}

// wrapTxWithCtx - deprecated copy of kv_temporal.go - visible only in tests
// need to move non-unit-tests to own package
func wrapDbWithCtx(db kv.RwDB, ctx *Aggregator) kv.TemporalRwDB {
	v, err := New(db, ctx)
	if err != nil {
		panic(err)
	}
	return v
}

func TestAggregator_RebuildCommitmentBasedOnFiles(t *testing.T) {
	if testing.Short() {
		t.Skip()
	}

	_db, agg := testDbAggregatorWithFiles(t, &testAggConfig{
		stepSize:                         20,
		disableCommitmentBranchTransform: false,
	})
	db := wrapDbWithCtx(_db, agg)

	ac := agg.BeginFilesRo()
	roots := make([]common.Hash, 0)

	// collect latest root from each available file
	compression := ac.d[kv.CommitmentDomain].d.Compression
	fnames := []string{}
	for _, f := range ac.d[kv.CommitmentDomain].files {
		var k, stateVal []byte
		if ac.d[kv.CommitmentDomain].d.Accessors.Has(AccessorHashMap) {
			idx := f.src.index.GetReaderFromPool()
			r := seg.NewReader(f.src.decompressor.MakeGetter(), compression)

			offset, ok := idx.TwoLayerLookup(keyCommitmentState)
			require.True(t, ok)
			r.Reset(offset)
			k, _ = r.Next(nil)
			stateVal, _ = r.Next(nil)
		} else {
			var found bool
			var err error
			k, stateVal, _, found, err = f.src.bindex.Get(keyCommitmentState, seg.NewReader(f.src.decompressor.MakeGetter(), compression))
			require.NoError(t, err)
			require.True(t, found)
			require.Equal(t, keyCommitmentState, k)
		}
		require.Equal(t, keyCommitmentState, k)
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

	buckets, err := rwTx.ListTables()
	require.NoError(t, err)
	for i, b := range buckets {
		if strings.Contains(strings.ToLower(b), kv.CommitmentDomain.String()) {
			size, err := rwTx.BucketSize(b)
			require.NoError(t, err)
			t.Logf("cleaned table #%d %s: %d keys", i, b, size)

			err = rwTx.ClearTable(b)
			require.NoError(t, err)
		}
	}
	require.NoError(t, rwTx.Commit())

	for _, fn := range fnames {
		if strings.Contains(fn, kv.CommitmentDomain.String()) {
			require.NoError(t, os.Remove(fn))
			t.Logf("removed file %s", filepath.Base(fn))
		}
	}
	err = agg.OpenFolder()
	require.NoError(t, err)

	ctx := context.Background()
	finalRoot, err := RebuildCommitmentFiles(ctx, db, &rawdbv3.TxNums, agg.logger)
	require.NoError(t, err)
	require.NotEmpty(t, finalRoot)
	require.NotEqual(t, empty.RootHash.Bytes(), finalRoot)

	require.Equal(t, roots[len(roots)-1][:], finalRoot[:])
}
