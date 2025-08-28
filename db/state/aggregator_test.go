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

	"github.com/erigontech/erigon-lib/common"
	"github.com/erigontech/erigon-lib/common/background"
	"github.com/erigontech/erigon-lib/common/dir"
	"github.com/erigontech/erigon-lib/common/empty"
	"github.com/erigontech/erigon-lib/common/length"
	"github.com/erigontech/erigon-lib/log/v3"
	"github.com/erigontech/erigon/db/config3"
	"github.com/erigontech/erigon/db/datadir"
	"github.com/erigontech/erigon/db/etl"
	"github.com/erigontech/erigon/db/kv"
	"github.com/erigontech/erigon/db/kv/mdbx"
	"github.com/erigontech/erigon/db/kv/order"
	"github.com/erigontech/erigon/db/kv/rawdbv3"
	"github.com/erigontech/erigon/db/kv/stream"
	"github.com/erigontech/erigon/db/seg"
	"github.com/erigontech/erigon/db/state/statecfg"
	"github.com/erigontech/erigon/db/version"
	"github.com/erigontech/erigon/execution/commitment"
	"github.com/erigontech/erigon/execution/types/accounts"
)

func TestAggregatorV3_Merge(t *testing.T) {
	if testing.Short() {
		t.Skip()
	}

	t.Parallel()
	_db, agg := testDbAndAggregatorv3(t, 10)
	db := wrapDbWithCtx(_db, agg)

	rwTx, err := db.BeginTemporalRw(context.Background())
	require.NoError(t, err)
	defer rwTx.Rollback()

	domains, err := NewSharedDomains(rwTx, log.New())
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

		logEvery := time.NewTicker(30 * time.Second)
		defer logEvery.Stop()
		stat, err := AggTx(rwTx).prune(context.Background(), rwTx, 0, logEvery)
		require.NoError(t, err)
		t.Logf("Prune: %s", stat)

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

func TestAggregatorV3_DirtyFilesRo(t *testing.T) {
	if testing.Short() {
		t.Skip()
	}

	t.Parallel()
	_db, agg := testDbAndAggregatorv3(t, 3)
	db := wrapDbWithCtx(_db, agg)

	rwTx, err := db.BeginTemporalRw(context.Background())
	require.NoError(t, err)
	defer rwTx.Rollback()

	domains, err := NewSharedDomains(rwTx, log.New())
	require.NoError(t, err)
	defer domains.Close()

	txs := uint64(300)
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
			// otherMaxWrite = txNum
		} else {
			pv, step, err := domains.GetLatest(kv.CommitmentDomain, rwTx, commKey1)
			require.NoError(t, err)

			err = domains.DomainPut(kv.CommitmentDomain, rwTx, commKey1, v[:], txNum, pv, step)
			require.NoError(t, err)
			// maxWrite = txNum
		}
		require.NoError(t, err)

	}

	err = domains.Flush(context.Background(), rwTx)
	require.NoError(t, err)

	require.NoError(t, err)
	err = rwTx.Commit()
	require.NoError(t, err)

	err = agg.BuildFiles(txs)
	require.NoError(t, err)

	checkDirtyFiles := func(dirtyFiles []*FilesItem, expectedLen, expectedRefCnt int, disabled bool, name string) {
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
			checkDirtyFiles(d.dirtyFiles.Items(), expectedLen, expectedRefCnt, d.Disable, d.Name.String())
			if d.SnapshotsDisabled {
				continue
			}
			checkDirtyFiles(d.History.dirtyFiles.Items(), expectedLen, expectedRefCnt, d.Disable, d.Name.String())
			checkDirtyFiles(d.History.InvertedIndex.dirtyFiles.Items(), expectedLen, expectedRefCnt, d.Disable, d.Name.String())
		}

		for _, ii := range agg.iis {
			checkDirtyFiles(ii.dirtyFiles.Items(), expectedLen, expectedRefCnt, ii.Disable, ii.FilenameBase)
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
	_db, agg := testDbAndAggregatorv3(t, 5)
	db := wrapDbWithCtx(_db, agg)
	rwTx, err := db.BeginTemporalRw(context.Background())
	require.NoError(t, err)
	defer rwTx.Rollback()

	agg.d[kv.CommitmentDomain].ReplaceKeysInValues = true

	domains, err := NewSharedDomains(rwTx, log.New())
	require.NoError(t, err)
	defer domains.Close()

	txs := uint64(100)
	rnd := rand.New(rand.NewSource(time.Now().UnixNano()))

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
		err = domains.DomainPut(kv.AccountsDomain, rwTx, addr, buf, txNum, nil, 0)
		require.NoError(t, err)

		err = domains.DomainPut(kv.StorageDomain, rwTx, composite(addr, loc), []byte{addr[0], loc[0]}, txNum, nil, 0)
		require.NoError(t, err)

		if (txNum+1)%agg.StepSize() == 0 {
			_, err := domains.ComputeCommitment(context.Background(), true, txNum/10, txNum, "")
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

	logEvery := time.NewTicker(30 * time.Second)
	defer logEvery.Stop()
	stat, err := AggTx(rwTx).prune(context.Background(), rwTx, 0, logEvery)
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
	_db, agg := testDbAndAggregatorv3(t, aggStep)
	db := wrapDbWithCtx(_db, agg)

	tx, err := db.BeginTemporalRw(context.Background())
	require.NoError(t, err)
	defer tx.Rollback()

	domains, err := NewSharedDomains(tx, log.New())
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
	var txNum, blockNum uint64
	for i := uint64(1); i <= txs; i++ {
		txNum = i
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
		err = domains.DomainPut(kv.AccountsDomain, tx, addr, buf, txNum, nil, 0)
		require.NoError(t, err)

		err = domains.DomainPut(kv.StorageDomain, tx, composite(addr, loc), []byte{addr[0], loc[0]}, txNum, nil, 0)
		require.NoError(t, err)

		err = domains.DomainPut(kv.CommitmentDomain, tx, someKey, aux[:], txNum, nil, 0)
		require.NoError(t, err)
		maxWrite = txNum
	}
	_, err = domains.ComputeCommitment(ctx, true, blockNum, txNum, "")
	require.NoError(t, err)

	err = domains.Flush(context.Background(), tx)
	require.NoError(t, err)
	err = tx.Commit()
	require.NoError(t, err)

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

	db = wrapDbWithCtx(db, anotherAgg)

	rwTx, err := db.BeginTemporalRw(context.Background())
	require.NoError(t, err)
	defer rwTx.Rollback()

	//anotherAgg.SetTx(rwTx)
	startTx := anotherAgg.EndTxNumMinimax()
	dom2, err := NewSharedDomains(rwTx, log.New())
	require.NoError(t, err)
	defer dom2.Close()

	err = dom2.SeekCommitment(ctx, rwTx)
	sstartTx := dom2.TxNum()

	require.NoError(t, err)
	require.GreaterOrEqual(t, sstartTx, startTx)
	require.GreaterOrEqual(t, sstartTx, latestCommitTxNum)
	_ = sstartTx
	rwTx.Rollback()

	// Check the history
	roTx, err := db.BeginTemporalRo(context.Background())
	require.NoError(t, err)
	defer roTx.Rollback()

	v, _, err := roTx.GetLatest(kv.CommitmentDomain, someKey)
	require.NoError(t, err)
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
	_db, agg := testDbAndAggregatorv3(t, aggStep)
	db := wrapDbWithCtx(_db, agg)

	tx, err := db.BeginTemporalRw(context.Background())
	require.NoError(t, err)
	defer tx.Rollback()

	domains, err := NewSharedDomains(tx, log.New())
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

		its, err := AggTx(tx).d[kv.AccountsDomain].ht.HistoryRange(0, int(maxTx), order.Asc, maxInt, tx)
		require.NoError(t, err)
		accountHistRange = extractKVErrIterator(t, its)
		its, err = AggTx(tx).d[kv.CodeDomain].ht.HistoryRange(0, int(maxTx), order.Asc, maxInt, tx)
		require.NoError(t, err)
		codeHistRange = extractKVErrIterator(t, its)
		its, err = AggTx(tx).d[kv.StorageDomain].ht.HistoryRange(0, int(maxTx), order.Asc, maxInt, tx)
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

		its, err := AggTx(afterTx).d[kv.AccountsDomain].ht.HistoryRange(0, int(maxTx), order.Asc, maxInt, afterTx)
		require.NoError(t, err)
		accountHistRangeAfter = extractKVErrIterator(t, its)
		its, err = AggTx(afterTx).d[kv.CodeDomain].ht.HistoryRange(0, int(maxTx), order.Asc, maxInt, afterTx)
		require.NoError(t, err)
		codeHistRangeAfter = extractKVErrIterator(t, its)
		its, err = AggTx(afterTx).d[kv.StorageDomain].ht.HistoryRange(0, int(maxTx), order.Asc, maxInt, afterTx)
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
			rh, err := domains.ComputeCommitment(context.Background(), true, txNum/commitEvery, txNum, "")
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

func TestAggregatorV3_RestartOnFiles(t *testing.T) {
	if testing.Short() {
		t.Skip()
	}

	t.Parallel()

	logger := log.New()
	aggStep := uint64(100)
	ctx := context.Background()
	_db, agg := testDbAndAggregatorv3(t, aggStep)
	db := wrapDbWithCtx(_db, agg)
	dirs := agg.dirs

	tx, err := db.BeginTemporalRw(context.Background())
	require.NoError(t, err)
	defer tx.Rollback()

	domains, err := NewSharedDomains(tx, log.New())
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
		err = domains.DomainPut(kv.AccountsDomain, tx, addr, buf[:], txNum, nil, 0)
		require.NoError(t, err)

		err = domains.DomainPut(kv.StorageDomain, tx, composite(addr, loc), []byte{addr[0], loc[0]}, txNum, nil, 0)
		require.NoError(t, err)

		keys[txNum-1] = append(addr, loc...)
	}

	// flush and build files
	err = domains.Flush(context.Background(), tx)
	require.NoError(t, err)

	latestStepInDB := agg.d[kv.AccountsDomain].maxStepInDB(tx)
	require.Equal(t, 5, int(latestStepInDB))

	latestStepInDBNoHist := agg.d[kv.AccountsDomain].maxStepInDBNoHistory(tx)
	require.Equal(t, 2, int(latestStepInDBNoHist))

	err = tx.Commit()
	require.NoError(t, err)

	err = agg.BuildFiles(txs)
	require.NoError(t, err)

	agg.Close()
	db.Close()

	// remove database files
	require.NoError(t, dir.RemoveAll(dirs.Chaindata))

	// open new db and aggregator instances
	newDb := mdbx.New(kv.ChainDB, logger).InMem(dirs.Chaindata).MustOpen()
	t.Cleanup(newDb.Close)

	salt, err := GetStateIndicesSalt(dirs, false, logger)
	require.NoError(t, err)
	require.NotNil(t, salt)
	newAgg, err := NewAggregator2(context.Background(), agg.dirs, aggStep, salt, newDb, logger)
	require.NoError(t, err)
	require.NoError(t, newAgg.OpenFolder())

	db = wrapDbWithCtx(newDb, newAgg)

	tx, err = db.BeginTemporalRw(context.Background())
	require.NoError(t, err)
	defer tx.Rollback()

	newDoms, err := NewSharedDomains(tx, log.New())
	require.NoError(t, err)
	defer newDoms.Close()

	err = newDoms.SeekCommitment(ctx, tx)
	require.NoError(t, err)
	latestTx := newDoms.TxNum()
	t.Logf("seek to latest_tx=%d", latestTx)

	miss := uint64(0)
	for i, key := range keys {
		if uint64(i+1) >= txs-aggStep {
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

	_db, agg := testDbAndAggregatorv3(t, aggStep)
	db := wrapDbWithCtx(_db, agg)

	tx, err := db.BeginTemporalRw(context.Background())
	require.NoError(t, err)
	defer tx.Rollback()

	domains, err := NewSharedDomains(tx, log.New())
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

		domains, err = NewSharedDomains(tx, log.New())
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
		domains.SetTxNum(txNum)

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
	r := seg.NewReader(decomp.MakeGetter(), compressFlags)
	err = BuildBtreeIndexWithDecompressor(IndexFile, r, ps, tb.TempDir(), 777, logger, true, statecfg.AccessorBTree|statecfg.AccessorExistence)
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
	_db, agg := testDbAndAggregatorv3(t, 20)
	db := wrapDbWithCtx(_db, agg)
	ctx := context.Background()

	rwTx, err := db.BeginTemporalRw(context.Background())
	require.NoError(t, err)
	defer rwTx.Rollback()

	domains, err := NewSharedDomains(rwTx, log.New())
	require.NoError(t, err)
	defer domains.Close()
	changesetAt5 := &StateChangeSet{}
	changesetAt3 := &StateChangeSet{}

	keys, vals := generateInputData(t, 20, 4, 10)
	keys = keys[:2]

	var i int
	roots := make([][]byte, 0, 10)
	var pruneFrom uint64 = 5

	blockNum := uint64(0)
	for i = 0; i < len(vals); i++ {
		txNum := uint64(i)
		domains.SetTxNum(txNum)
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

			err = domains.DomainPut(kv.AccountsDomain, rwTx, keys[j], buf, txNum, prev, step)
			//err = domains.UpdateAccountCode(keys[j], vals[i], nil)
			require.NoError(t, err)
		}
		rh, err := domains.ComputeCommitment(ctx, true, blockNum, txNum, "")
		require.NoError(t, err)
		require.NotEmpty(t, rh)
		roots = append(roots, rh)
	}

	err = domains.Flush(context.Background(), rwTx)
	require.NoError(t, err)
	err = rwTx.Commit()
	require.NoError(t, err)

	rwTx, err = db.BeginTemporalRw(context.Background())
	require.NoError(t, err)
	defer rwTx.Rollback()

	domains, err = NewSharedDomains(rwTx, log.New())
	require.NoError(t, err)
	defer domains.Close()
	diffs := [kv.DomainLen][]kv.DomainEntryDiff{}
	for idx := range changesetAt5.Diffs {
		diffs[idx] = changesetAt5.Diffs[idx].GetDiffSet()
	}
	err = rwTx.Unwind(ctx, pruneFrom, &diffs)
	domains.SetTxNum(pruneFrom)
	//err = domains.Unwind(context.Background(), rwTx, 0, pruneFrom, &diffs)
	require.NoError(t, err)

	domains.SetChangesetAccumulator(changesetAt3)
	for i = int(pruneFrom); i < len(vals); i++ {
		txNum := uint64(i)
		domains.SetTxNum(txNum)

		for j := 0; j < len(keys); j++ {
			acc := accounts.Account{
				Nonce:       uint64(i),
				Balance:     *uint256.NewInt(uint64(i * 100_000)),
				CodeHash:    common.Hash{},
				Incarnation: 0,
			}
			buf := accounts.SerialiseV3(&acc)
			prev, step, err := rwTx.GetLatest(kv.AccountsDomain, keys[j])
			require.NoError(t, err)

			err = domains.DomainPut(kv.AccountsDomain, rwTx, keys[j], buf, txNum, prev, step)
			require.NoError(t, err)
			//err = domains.UpdateAccountCode(keys[j], vals[i], nil)
			//require.NoError(t, err)
		}

		rh, err := domains.ComputeCommitment(ctx, true, blockNum, txNum, "")
		require.NoError(t, err)
		require.NotEmpty(t, rh)
		require.Equal(t, roots[i], rh)
	}

	err = domains.Flush(context.Background(), rwTx)
	require.NoError(t, err)

	pruneFrom = 3

	err = rwTx.Commit()
	require.NoError(t, err)

	rwTx, err = db.BeginTemporalRw(context.Background())
	require.NoError(t, err)
	defer rwTx.Rollback()

	domains, err = NewSharedDomains(rwTx, log.New())
	require.NoError(t, err)
	defer domains.Close()
	for idx := range changesetAt3.Diffs {
		diffs[idx] = changesetAt3.Diffs[idx].GetDiffSet()
	}
	err = rwTx.Unwind(context.Background(), pruneFrom, &diffs)
	domains.SetTxNum(pruneFrom)
	require.NoError(t, err)

	for i = int(pruneFrom); i < len(vals); i++ {
		txNum := uint64(i)
		domains.SetTxNum(txNum)

		for j := 0; j < len(keys); j++ {
			acc := accounts.Account{
				Nonce:       uint64(i),
				Balance:     *uint256.NewInt(uint64(i * 100_000)),
				CodeHash:    common.Hash{},
				Incarnation: 0,
			}
			buf := accounts.SerialiseV3(&acc)
			prev, step, err := rwTx.GetLatest(kv.AccountsDomain, keys[j])
			require.NoError(t, err)

			err = domains.DomainPut(kv.AccountsDomain, rwTx, keys[j], buf, txNum, prev, step)
			require.NoError(t, err)
			//err = domains.UpdateAccountCode(keys[j], vals[i], nil)
			//require.NoError(t, err)
		}

		rh, err := domains.ComputeCommitment(ctx, true, blockNum, txNum, "")
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

// wrapDbWithCtx - deprecated copy of kv_temporal.go - visible only in tests
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
		stepSize:                         10,
		disableCommitmentBranchTransform: false,
	})
	db := wrapDbWithCtx(_db, agg)

	tx, err := db.BeginTemporalRw(context.Background())
	require.NoError(t, err)
	defer tx.Rollback()
	ac := AggTx(tx)
	roots := make([]common.Hash, 0)

	// collect latest root from each available file
	dt := ac.d[kv.CommitmentDomain]
	fnames := []string{}
	for i, f := range dt.files {
		stateVal, ok, _, _ := dt.getLatestFromFile(i, keyCommitmentState)
		require.True(t, ok)
		rh, err := commitment.HexTrieExtractStateRoot(stateVal)
		require.NoError(t, err)

		roots = append(roots, common.BytesToHash(rh))
		//fmt.Printf("file %s root %x\n", filepath.Base(f.src.decompressor.FilePath()), rh)
		fnames = append(fnames, f.src.decompressor.FilePath())
	}
	tx.Rollback()
	agg.d[kv.CommitmentDomain].closeFilesAfterStep(0) // close commitment files to remove

	// now clean all commitment files along with related db buckets
	rwTx, err := db.BeginRw(context.Background())
	require.NoError(t, err)
	defer rwTx.Rollback()

	buckets, err := rwTx.ListTables()
	require.NoError(t, err)
	for _, b := range buckets {
		if strings.Contains(strings.ToLower(b), kv.CommitmentDomain.String()) {
			//size, err := rwTx.BucketSize(b)
			//require.NoError(t, err)
			//t.Logf("cleaned table %s: %d keys", b, size)

			err = rwTx.ClearTable(b)
			require.NoError(t, err)
		}
	}
	require.NoError(t, rwTx.Commit())

	for _, fn := range fnames {
		if strings.Contains(fn, kv.CommitmentDomain.String()) {
			require.NoError(t, dir.RemoveFile(fn))
			//t.Logf("removed file %s", filepath.Base(fn))
		}
	}
	err = agg.OpenFolder()
	require.NoError(t, err)

	ctx := context.Background()
	finalRoot, err := RebuildCommitmentFiles(ctx, db, &rawdbv3.TxNums, agg.logger, true)
	require.NoError(t, err)
	require.NotEmpty(t, finalRoot)
	require.NotEqual(t, empty.RootHash.Bytes(), finalRoot)

	require.Equal(t, roots[len(roots)-1][:], finalRoot[:])
}

func TestAggregator_CheckDependencyHistoryII(t *testing.T) {
	stepSize := uint64(10)
	db, agg := testDbAndAggregatorv3(t, stepSize)

	generateAccountsFile(t, agg.dirs, []testFileRange{{0, 1}, {1, 2}, {0, 2}})
	generateCodeFile(t, agg.dirs, []testFileRange{{0, 1}, {1, 2}, {0, 2}})
	generateStorageFile(t, agg.dirs, []testFileRange{{0, 1}, {1, 2}, {0, 2}})
	generateCommitmentFile(t, agg.dirs, []testFileRange{{0, 1}, {1, 2}})

	require.NoError(t, agg.OpenFolder())

	tdb := wrapDbWithCtx(db, agg)
	defer tdb.Close()
	tx, err := tdb.BeginTemporalRo(context.Background())
	require.NoError(t, err)
	defer tx.Rollback()

	aggTx := AggTx(tx)

	checkFn := func(files visibleFiles, merged bool) {
		if merged {
			require.Equal(t, 1, len(files))
			require.Equal(t, uint64(0), files[0].startTxNum/stepSize)
			require.Equal(t, uint64(2), files[0].endTxNum/stepSize)
		} else {
			require.Equal(t, 2, len(files))
			require.Equal(t, uint64(0), files[0].startTxNum/stepSize)
			require.Equal(t, uint64(1), files[0].endTxNum/stepSize)
			require.Equal(t, uint64(1), files[1].startTxNum/stepSize)
			require.Equal(t, uint64(2), files[1].endTxNum/stepSize)
		}
	}

	checkFn(aggTx.d[kv.AccountsDomain].ht.files, true)
	checkFn(aggTx.d[kv.CodeDomain].ht.files, true)
	checkFn(aggTx.d[kv.StorageDomain].ht.files, true)
	checkFn(aggTx.d[kv.AccountsDomain].ht.iit.files, true)
	checkFn(aggTx.d[kv.CodeDomain].ht.iit.files, true)
	checkFn(aggTx.d[kv.StorageDomain].ht.iit.files, true)

	tx.Rollback()

	// delete merged code history file
	codeMergedFile := filepath.Join(agg.dirs.SnapHistory, "v1.0-code.0-2.v")
	exist, err := dir.FileExist(codeMergedFile)
	require.NoError(t, err)
	require.True(t, exist)
	agg.closeDirtyFiles() // because windows

	require.NoError(t, dir.RemoveFile(codeMergedFile))

	require.NoError(t, agg.OpenFolder())
	tx, err = tdb.BeginTemporalRo(context.Background())
	require.NoError(t, err)
	defer tx.Rollback()

	aggTx = AggTx(tx)

	checkFn(aggTx.d[kv.AccountsDomain].ht.files, true)
	checkFn(aggTx.d[kv.CodeDomain].ht.files, false)
	checkFn(aggTx.d[kv.StorageDomain].ht.files, true)
	checkFn(aggTx.d[kv.AccountsDomain].ht.iit.files, true)
	checkFn(aggTx.d[kv.CodeDomain].ht.iit.files, false)
	checkFn(aggTx.d[kv.StorageDomain].ht.iit.files, true)
}

func TestAggregator_CheckDependencyBtwnDomains(t *testing.T) {
	stepSize := uint64(10)
	// testDbAggregatorWithNoFiles(t,  stepSize * 32, &testAggConfig{
	// 	stepSize:                         10,
	// 	disableCommitmentBranchTransform: false,
	// })
	db, agg := testDbAndAggregatorv3(t, stepSize)

	require.NotNil(t, agg.d[kv.AccountsDomain].checker)
	require.NotNil(t, agg.d[kv.StorageDomain].checker)
	require.NotNil(t, agg.checker)
	require.Nil(t, agg.d[kv.CommitmentDomain].checker)

	generateAccountsFile(t, agg.dirs, []testFileRange{{0, 1}, {1, 2}, {0, 2}})
	generateCodeFile(t, agg.dirs, []testFileRange{{0, 1}, {1, 2}, {0, 2}})
	generateStorageFile(t, agg.dirs, []testFileRange{{0, 1}, {1, 2}, {0, 2}})
	generateCommitmentFile(t, agg.dirs, []testFileRange{{0, 1}, {1, 2}})

	require.NoError(t, agg.OpenFolder())

	tdb := wrapDbWithCtx(db, agg)
	defer tdb.Close()
	tx, err := tdb.BeginTemporalRo(context.Background())
	require.NoError(t, err)
	defer tx.Rollback()

	aggTx := AggTx(tx)
	checkFn := func(files visibleFiles, merged bool) {
		if merged {
			require.Equal(t, 1, len(files))
			require.Equal(t, uint64(0), files[0].startTxNum/stepSize)
			require.Equal(t, uint64(2), files[0].endTxNum/stepSize)
		} else {
			require.Equal(t, 2, len(files))
			require.Equal(t, uint64(0), files[0].startTxNum/stepSize)
			require.Equal(t, uint64(1), files[0].endTxNum/stepSize)
			require.Equal(t, uint64(1), files[1].startTxNum/stepSize)
			require.Equal(t, uint64(2), files[1].endTxNum/stepSize)
		}
	}
	checkFn(aggTx.d[kv.AccountsDomain].files, false)
	checkFn(aggTx.d[kv.CodeDomain].files, true)
	checkFn(aggTx.d[kv.StorageDomain].files, false)
	checkFn(aggTx.d[kv.CommitmentDomain].files, false)
}

func TestReceiptFilesVersionAdjust(t *testing.T) {
	touchFn := func(t *testing.T, dirs datadir.Dirs, file string) {
		t.Helper()
		fullpath := filepath.Join(dirs.SnapDomain, file)
		ofile, err := os.Create(fullpath)
		require.NoError(t, err)
		ofile.Close()
	}

	t.Run("v1.0 files", func(t *testing.T) {
		// Schema is global and edited by subtests
		backup := statecfg.Schema
		t.Cleanup(func() {
			statecfg.Schema = backup
		})
		require, logger := require.New(t), log.New()
		dirs := datadir.New(t.TempDir())

		db := mdbx.New(kv.ChainDB, logger).InMem(dirs.Chaindata).GrowthStep(32 * datasize.MB).MapSize(2 * datasize.GB).MustOpen()
		t.Cleanup(db.Close)

		touchFn(t, dirs, "v1.0-receipt.0-2048.kv")
		touchFn(t, dirs, "v1.0-receipt.2048-2049.kv")

		salt, err := GetStateIndicesSalt(dirs, true, logger)
		require.NoError(err)
		agg, err := NewAggregator2(context.Background(), dirs, config3.DefaultStepSize, salt, db, logger)
		require.NoError(err)
		t.Cleanup(agg.Close)

		kv_versions := agg.d[kv.ReceiptDomain].Version.DataKV
		v_versions := agg.d[kv.ReceiptDomain].Hist.Version.DataV

		require.Equal(kv_versions.Current, version.V1_1)
		require.Equal(kv_versions.MinSupported, version.V1_0)
		require.Equal(v_versions.Current, version.V1_1)
		require.Equal(v_versions.MinSupported, version.V1_0)
	})

	t.Run("v1.1 files", func(t *testing.T) {
		backup := statecfg.Schema
		t.Cleanup(func() {
			statecfg.Schema = backup
		})
		require, logger := require.New(t), log.New()
		dirs := datadir.New(t.TempDir())

		db := mdbx.New(kv.ChainDB, logger).InMem(dirs.Chaindata).GrowthStep(32 * datasize.MB).MapSize(2 * datasize.GB).MustOpen()
		t.Cleanup(db.Close)

		touchFn(t, dirs, "v1.1-receipt.0-2048.kv")
		touchFn(t, dirs, "v1.1-receipt.2048-2049.kv")

		salt, err := GetStateIndicesSalt(dirs, true, logger)
		require.NoError(err)
		agg, err := NewAggregator2(context.Background(), dirs, config3.DefaultStepSize, salt, db, logger)
		require.NoError(err)
		t.Cleanup(agg.Close)

		kv_versions := agg.d[kv.ReceiptDomain].Version.DataKV
		v_versions := agg.d[kv.ReceiptDomain].Hist.Version.DataV

		require.Equal(kv_versions.Current, version.V1_1)
		require.Equal(kv_versions.MinSupported, version.V1_0)
		require.Equal(v_versions.Current, version.V1_1)
		require.Equal(v_versions.MinSupported, version.V1_0)
	})

	t.Run("v2.0 files", func(t *testing.T) {
		backup := statecfg.Schema
		t.Cleanup(func() {
			statecfg.Schema = backup
		})
		require, logger := require.New(t), log.New()
		dirs := datadir.New(t.TempDir())

		db := mdbx.New(kv.ChainDB, logger).InMem(dirs.Chaindata).GrowthStep(32 * datasize.MB).MapSize(2 * datasize.GB).MustOpen()
		t.Cleanup(db.Close)

		touchFn(t, dirs, "v2.0-receipt.0-2048.kv")
		touchFn(t, dirs, "v2.0-receipt.2048-2049.kv")

		salt, err := GetStateIndicesSalt(dirs, true, logger)
		require.NoError(err)
		agg, err := NewAggregator2(context.Background(), dirs, config3.DefaultStepSize, salt, db, logger)
		require.NoError(err)
		t.Cleanup(agg.Close)

		kv_versions := agg.d[kv.ReceiptDomain].Version.DataKV
		v_versions := agg.d[kv.ReceiptDomain].Hist.Version.DataV

		require.True(kv_versions.Current.Cmp(version.V2_1) >= 0)
		require.Equal(kv_versions.MinSupported, version.V1_0)
		require.True(v_versions.Current.Cmp(version.V2_1) >= 0)
		require.Equal(v_versions.MinSupported, version.V1_0)
	})

	t.Run("empty files", func(t *testing.T) {
		backup := statecfg.Schema
		t.Cleanup(func() {
			statecfg.Schema = backup
		})
		require, logger := require.New(t), log.New()
		dirs := datadir.New(t.TempDir())

		db := mdbx.New(kv.ChainDB, logger).InMem(dirs.Chaindata).GrowthStep(32 * datasize.MB).MapSize(2 * datasize.GB).MustOpen()
		t.Cleanup(db.Close)
		salt, err := GetStateIndicesSalt(dirs, true, logger)
		require.NoError(err)
		agg, err := NewAggregator2(context.Background(), dirs, config3.DefaultStepSize, salt, db, logger)
		require.NoError(err)
		t.Cleanup(agg.Close)

		kv_versions := agg.d[kv.ReceiptDomain].Version.DataKV
		v_versions := agg.d[kv.ReceiptDomain].Hist.Version.DataV

		require.True(kv_versions.Current.Cmp(version.V2_1) >= 0)
		require.Equal(kv_versions.MinSupported, version.V1_0)
		require.True(v_versions.Current.Cmp(version.V2_1) >= 0)
		require.Equal(v_versions.MinSupported, version.V1_0)
	})

}

func generateDomainFiles(t *testing.T, name string, dirs datadir.Dirs, ranges []testFileRange) {
	t.Helper()
	domainR := setupAggSnapRepo(t, dirs, func(stepSize uint64, dirs datadir.Dirs) (dn string, schema SnapNameSchema) {
		accessors := statecfg.AccessorBTree | statecfg.AccessorExistence
		schema = NewE3SnapSchemaBuilder(accessors, stepSize).
			Data(dirs.SnapDomain, name, DataExtensionKv, seg.CompressNone).
			BtIndex().Existence().
			Build()
		return name, schema
	})
	defer domainR.Close()
	populateFiles2(t, dirs, domainR, ranges)

	domainHR := setupAggSnapRepo(t, dirs, func(stepSize uint64, dirs datadir.Dirs) (dn string, schema SnapNameSchema) {
		accessors := statecfg.AccessorHashMap
		schema = NewE3SnapSchemaBuilder(accessors, stepSize).
			Data(dirs.SnapHistory, name, DataExtensionV, seg.CompressNone).
			Accessor(dirs.SnapAccessors).
			Build()
		return name, schema
	})
	defer domainHR.Close()
	populateFiles2(t, dirs, domainHR, ranges)

	domainII := setupAggSnapRepo(t, dirs, func(stepSize uint64, dirs datadir.Dirs) (dn string, schema SnapNameSchema) {
		accessors := statecfg.AccessorHashMap
		schema = NewE3SnapSchemaBuilder(accessors, stepSize).
			Data(dirs.SnapIdx, name, DataExtensionEf, seg.CompressNone).
			Accessor(dirs.SnapAccessors).
			Build()
		return name, schema
	})
	defer domainII.Close()
	populateFiles2(t, dirs, domainII, ranges)
}

func generateAccountsFile(t *testing.T, dirs datadir.Dirs, ranges []testFileRange) {
	t.Helper()
	generateDomainFiles(t, "accounts", dirs, ranges)
}

func generateCodeFile(t *testing.T, dirs datadir.Dirs, ranges []testFileRange) {
	t.Helper()
	generateDomainFiles(t, "code", dirs, ranges)
}

func generateStorageFile(t *testing.T, dirs datadir.Dirs, ranges []testFileRange) {
	t.Helper()
	generateDomainFiles(t, "storage", dirs, ranges)
}

func generateCommitmentFile(t *testing.T, dirs datadir.Dirs, ranges []testFileRange) {
	t.Helper()
	commitmentR := setupAggSnapRepo(t, dirs, func(stepSize uint64, dirs datadir.Dirs) (name string, schema SnapNameSchema) {
		accessors := statecfg.AccessorHashMap
		name = "commitment"
		schema = NewE3SnapSchemaBuilder(accessors, stepSize).
			Data(dirs.SnapDomain, name, DataExtensionKv, seg.CompressNone).
			Accessor(dirs.SnapDomain).
			Build()
		return name, schema
	})
	defer commitmentR.Close()
	populateFiles2(t, dirs, commitmentR, ranges)
}

func setupAggSnapRepo(t *testing.T, dirs datadir.Dirs, genRepo func(stepSize uint64, dirs datadir.Dirs) (name string, schema SnapNameSchema)) *SnapshotRepo {
	t.Helper()
	stepSize := uint64(10)
	name, schema := genRepo(stepSize, dirs)

	createConfig := SnapshotCreationConfig{
		RootNumPerStep: stepSize,
		MergeStages:    []uint64{20, 40, 80},
		MinimumSize:    10,
		SafetyMargin:   5,
	}
	d, err := kv.String2Domain(name)
	require.NoError(t, err)
	return NewSnapshotRepo(name, FromDomain(d), &SnapshotConfig{
		SnapshotCreationConfig: &createConfig,
		Schema:                 schema,
	}, log.New())
}
