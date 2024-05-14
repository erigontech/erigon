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
	"sync/atomic"
	"testing"
	"time"

	"github.com/ledgerwatch/erigon-lib/kv/rawdbv3"

	"github.com/ledgerwatch/erigon-lib/kv/iter"
	"github.com/ledgerwatch/erigon-lib/kv/order"

	"github.com/c2h5oh/datasize"
	"github.com/holiman/uint256"
	"github.com/ledgerwatch/log/v3"
	"github.com/stretchr/testify/require"

	"github.com/ledgerwatch/erigon-lib/common"
	"github.com/ledgerwatch/erigon-lib/common/datadir"
	"github.com/ledgerwatch/erigon-lib/common/length"
	"github.com/ledgerwatch/erigon-lib/etl"
	"github.com/ledgerwatch/erigon-lib/kv"
	"github.com/ledgerwatch/erigon-lib/kv/mdbx"
	"github.com/ledgerwatch/erigon-lib/seg"
	"github.com/ledgerwatch/erigon-lib/types"
)

func TestAggregatorV3_Merge(t *testing.T) {
	db, agg := testDbAndAggregatorv3(t, 1000)
	rwTx, err := db.BeginRwNosync(context.Background())
	require.NoError(t, err)
	defer func() {
		if rwTx != nil {
			rwTx.Rollback()
		}
	}()
	ac := agg.BeginFilesRo()
	defer ac.Close()
	domains, err := NewSharedDomains(WrapTxWithCtx(rwTx, ac), log.New())
	require.NoError(t, err)
	defer domains.Close()

	txs := uint64(100000)
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
		require.EqualValues(t, length.Addr, n)

		n, err = rnd.Read(loc)
		require.NoError(t, err)
		require.EqualValues(t, length.Hash, n)

		buf := types.EncodeAccountBytesV3(1, uint256.NewInt(0), nil, 0)
		err = domains.DomainPut(kv.AccountsDomain, addr, nil, buf, nil, 0)
		require.NoError(t, err)

		err = domains.DomainPut(kv.StorageDomain, addr, loc, []byte{addr[0], loc[0]}, nil, 0)
		require.NoError(t, err)

		var v [8]byte
		binary.BigEndian.PutUint64(v[:], txNum)
		if txNum%135 == 0 {
			pv, step, _, err := ac.GetLatest(kv.CommitmentDomain, commKey2, nil, rwTx)
			require.NoError(t, err)

			err = domains.DomainPut(kv.CommitmentDomain, commKey2, nil, v[:], pv, step)
			require.NoError(t, err)
			otherMaxWrite = txNum
		} else {
			pv, step, _, err := ac.GetLatest(kv.CommitmentDomain, commKey1, nil, rwTx)
			require.NoError(t, err)

			err = domains.DomainPut(kv.CommitmentDomain, commKey1, nil, v[:], pv, step)
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
	rwTx = nil

	err = agg.BuildFiles(txs)
	require.NoError(t, err)

	rwTx, err = db.BeginRw(context.Background())
	require.NoError(t, err)
	defer rwTx.Rollback()

	logEvery := time.NewTicker(30 * time.Second)
	defer logEvery.Stop()
	stat, err := ac.Prune(context.Background(), rwTx, 0, false, logEvery)
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

	v, _, ex, err := dc.GetLatest(kv.CommitmentDomain, commKey1, nil, roTx)
	require.NoError(t, err)
	require.Truef(t, ex, "key %x not found", commKey1)

	require.EqualValues(t, maxWrite, binary.BigEndian.Uint64(v[:]))

	v, _, ex, err = dc.GetLatest(kv.CommitmentDomain, commKey2, nil, roTx)
	require.NoError(t, err)
	require.Truef(t, ex, "key %x not found", commKey2)
	dc.Close()

	require.EqualValues(t, otherMaxWrite, binary.BigEndian.Uint64(v[:]))
}

func TestAggregatorV3_MergeValTransform(t *testing.T) {
	db, agg := testDbAndAggregatorv3(t, 1000)
	rwTx, err := db.BeginRwNosync(context.Background())
	require.NoError(t, err)
	defer func() {
		if rwTx != nil {
			rwTx.Rollback()
		}
	}()
	ac := agg.BeginFilesRo()
	defer ac.Close()
	domains, err := NewSharedDomains(WrapTxWithCtx(rwTx, ac), log.New())
	require.NoError(t, err)
	defer domains.Close()

	txs := uint64(100000)
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
		require.EqualValues(t, length.Addr, n)

		n, err = rnd.Read(loc)
		require.NoError(t, err)
		require.EqualValues(t, length.Hash, n)

		buf := types.EncodeAccountBytesV3(1, uint256.NewInt(txNum*1e6), nil, 0)
		err = domains.DomainPut(kv.AccountsDomain, addr, nil, buf, nil, 0)
		require.NoError(t, err)

		err = domains.DomainPut(kv.StorageDomain, addr, loc, []byte{addr[0], loc[0]}, nil, 0)
		require.NoError(t, err)

		if (txNum+1)%agg.StepSize() == 0 {
			_, err := domains.ComputeCommitment(context.Background(), true, txNum/10, "")
			require.NoError(t, err)
		}

		state[string(addr)] = buf
		state[string(addr)+string(loc)] = []byte{addr[0], loc[0]}
	}

	err = domains.Flush(context.Background(), rwTx)
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
	stat, err := ac.Prune(context.Background(), rwTx, 0, false, logEvery)
	require.NoError(t, err)
	t.Logf("Prune: %s", stat)

	err = rwTx.Commit()
	require.NoError(t, err)

	err = agg.MergeLoop(context.Background())
	require.NoError(t, err)
}

func TestAggregatorV3_RestartOnDatadir(t *testing.T) {
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
	rnd := rand.New(rand.NewSource(time.Now().Unix()))

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

	require.NoError(t, anotherAgg.OpenFolder(false))

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

func TestAggregatorV3_PruneSmallBatches(t *testing.T) {
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

	rnd := rand.New(rand.NewSource(0))

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
		it, err := ac.DomainRangeLatest(tx, kv.AccountsDomain, nil, nil, maxInt)
		require.NoError(t, err)
		accountsRange = extractKVErrIterator(t, it)

		it, err = ac.DomainRangeLatest(tx, kv.StorageDomain, nil, nil, maxInt)
		require.NoError(t, err)
		storageRange = extractKVErrIterator(t, it)

		it, err = ac.DomainRangeLatest(tx, kv.CodeDomain, nil, nil, maxInt)
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

	buildTx, err := db.BeginRw(context.Background())
	require.NoError(t, err)
	defer func() {
		if buildTx != nil {
			buildTx.Rollback()
		}
	}()

	err = agg.BuildFiles(maxTx)
	require.NoError(t, err)

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
		it, err := ac.DomainRangeLatest(afterTx, kv.AccountsDomain, nil, nil, maxInt)
		require.NoError(t, err)
		accountsRangeAfter = extractKVErrIterator(t, it)

		it, err = ac.DomainRangeLatest(afterTx, kv.StorageDomain, nil, nil, maxInt)
		require.NoError(t, err)
		storageRangeAfter = extractKVErrIterator(t, it)

		it, err = ac.DomainRangeLatest(afterTx, kv.CodeDomain, nil, nil, maxInt)
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

func extractKVSErrIterator(t *testing.T, it iter.KVS) map[string]vs {
	t.Helper()

	accounts := make(map[string]vs)
	for it.HasNext() {
		k, v, s, err := it.Next()
		require.NoError(t, err)
		accounts[hex.EncodeToString(k)] = vs{v: common.Copy(v), s: s}
	}

	return accounts
}

func extractKVErrIterator(t *testing.T, it iter.KV) map[string][]byte {
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

func generateSharedDomainsUpdates(t *testing.T, domains *SharedDomains, maxTxNum uint64, rnd *rand.Rand, keyMaxLen, keysCount, commitEvery uint64) map[string]struct{} {
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

func generateSharedDomainsUpdatesForTx(t *testing.T, domains *SharedDomains, txNum uint64, rnd *rand.Rand, prevKeys map[string]struct{}, keyMaxLen, keysCount uint64) map[string]struct{} {
	t.Helper()
	domains.SetTxNum(txNum)

	getKey := func() ([]byte, bool) {
		r := rnd.Intn(100)
		if r < 50 && len(prevKeys) > 0 {
			ri := rnd.Intn(len(prevKeys))
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

		r := rnd.Intn(101)
		switch {
		case r <= 33:
			buf := types.EncodeAccountBytesV3(txNum, uint256.NewInt(txNum*100_000), nil, 0)
			prev, step, err := domains.DomainGet(kv.AccountsDomain, key, nil)
			require.NoError(t, err)

			usedKeys[string(key)] = struct{}{}

			err = domains.DomainPut(kv.AccountsDomain, key, nil, buf, prev, step)
			require.NoError(t, err)

		case r > 33 && r <= 66:
			codeUpd := make([]byte, rnd.Intn(24576))
			_, err := rnd.Read(codeUpd)
			require.NoError(t, err)
			for limit := 1000; len(key) > length.Addr && limit > 0; limit-- {
				key, existed = getKey() //nolint
				if !existed {
					continue
				}
			}
			usedKeys[string(key)] = struct{}{}

			prev, step, err := domains.DomainGet(kv.CodeDomain, key, nil)
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

			prev, step, err := domains.DomainGet(kv.AccountsDomain, key, nil)
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

				prev, step, err := domains.DomainGet(kv.StorageDomain, sk[:length.Addr], sk[length.Addr:])
				require.NoError(t, err)

				err = domains.DomainPut(kv.StorageDomain, sk[:length.Addr], sk[length.Addr:], uint256.NewInt(txNum).Bytes(), prev, step)
				require.NoError(t, err)
			}

		}
	}
	return usedKeys
}

func TestAggregatorV3_RestartOnFiles(t *testing.T) {

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

	rnd := rand.New(rand.NewSource(0))
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

	latestStepInDB := agg.d[kv.AccountsDomain].LastStepInDB(tx)
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
	newDb := mdbx.NewMDBX(logger).InMem(dirs.Chaindata).WithTableCfg(func(defaultBuckets kv.TableCfg) kv.TableCfg {
		return kv.ChaindataTablesCfg
	}).MustOpen()
	t.Cleanup(newDb.Close)

	newAgg, err := NewAggregator(context.Background(), agg.dirs, aggStep, newDb, logger)
	require.NoError(t, err)
	require.NoError(t, newAgg.OpenFolder(false))

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

	rnd := rand.New(rand.NewSource(0))
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

func generateKV(tb testing.TB, tmp string, keySize, valueSize, keyCount int, logger log.Logger, compressFlags FileCompression) string {
	tb.Helper()

	args := BtIndexWriterArgs{
		IndexFile: path.Join(tmp, fmt.Sprintf("%dk.bt", keyCount/1000)),
		TmpDir:    tmp,
		KeyCount:  12,
	}

	iw, err := NewBtIndexWriter(args, logger)
	require.NoError(tb, err)

	defer iw.Close()
	rnd := rand.New(rand.NewSource(0))
	values := make([]byte, valueSize)

	dataPath := path.Join(tmp, fmt.Sprintf("%dk.kv", keyCount/1000))
	comp, err := seg.NewCompressor(context.Background(), "cmp", dataPath, tmp, seg.MinPatternScore, 1, log.LvlDebug, logger)
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

		n, err = rnd.Read(values[:rnd.Intn(valueSize)+1])
		require.NoError(tb, err)

		err = collector.Collect(key, values[:n])
		require.NoError(tb, err)
	}

	writer := NewArchiveWriter(comp, compressFlags)

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

	getter := NewArchiveGetter(decomp.MakeGetter(), compressFlags)
	getter.Reset(0)

	var pos uint64
	key := make([]byte, keySize)
	for i := 0; i < keyCount; i++ {
		if !getter.HasNext() {
			tb.Fatalf("not enough values at %d", i)
		}

		keys, _ := getter.Next(key[:0])
		err = iw.AddKey(keys[:], pos)

		pos, _ = getter.Skip()
		require.NoError(tb, err)
	}
	decomp.Close()

	require.NoError(tb, iw.Build())
	iw.Close()

	return decomp.FilePath()
}

func testDbAndAggregatorv3(t *testing.T, aggStep uint64) (kv.RwDB, *Aggregator) {
	t.Helper()
	require := require.New(t)
	dirs := datadir.New(t.TempDir())
	logger := log.New()
	db := mdbx.NewMDBX(logger).InMem(dirs.Chaindata).GrowthStep(32 * datasize.MB).MapSize(2 * datasize.GB).WithTableCfg(func(defaultBuckets kv.TableCfg) kv.TableCfg {
		return kv.ChaindataTablesCfg
	}).MustOpen()
	t.Cleanup(db.Close)

	agg, err := NewAggregator(context.Background(), dirs, aggStep, db, logger)
	require.NoError(err)
	t.Cleanup(agg.Close)
	err = agg.OpenFolder(false)
	require.NoError(err)
	agg.DisableFsync()
	return db, agg
}

// generate test data for table tests, containing n; n < 20 keys of length 20 bytes and values of length <= 16 bytes
func generateInputData(tb testing.TB, keySize, valueSize, keyCount int) ([][]byte, [][]byte) {
	tb.Helper()

	rnd := rand.New(rand.NewSource(0))
	values := make([][]byte, keyCount)
	keys := make([][]byte, keyCount)

	bk, bv := make([]byte, keySize), make([]byte, valueSize)
	for i := 0; i < keyCount; i++ {
		n, err := rnd.Read(bk[:])
		require.EqualValues(tb, keySize, n)
		require.NoError(tb, err)
		keys[i] = common.Copy(bk[:n])

		n, err = rnd.Read(bv[:rnd.Intn(valueSize)+1])
		require.NoError(tb, err)

		values[i] = common.Copy(bv[:n])
	}
	return keys, values
}

func TestAggregatorV3_SharedDomains(t *testing.T) {
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

	keys, vals := generateInputData(t, 20, 16, 10)
	keys = keys[:2]

	var i int
	roots := make([][]byte, 0, 10)
	var pruneFrom uint64 = 5

	mc := agg.BeginFilesRo()
	defer mc.Close()

	for i = 0; i < len(vals); i++ {
		domains.SetTxNum(uint64(i))

		for j := 0; j < len(keys); j++ {
			buf := types.EncodeAccountBytesV3(uint64(i), uint256.NewInt(uint64(i*100_000)), nil, 0)
			prev, step, err := domains.DomainGet(kv.AccountsDomain, keys[j], nil)
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
	err = domains.Unwind(context.Background(), rwTx, 0, pruneFrom)
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

	err = domains.Flush(context.Background(), rwTx)
	require.NoError(t, err)
	ac.Close()

	pruneFrom = 3

	ac = agg.BeginFilesRo()
	defer ac.Close()
	domains, err = NewSharedDomains(WrapTxWithCtx(rwTx, ac), log.New())
	require.NoError(t, err)
	defer domains.Close()

	err = domains.Unwind(context.Background(), rwTx, 0, pruneFrom)
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
	input, err := hex.DecodeString("000114000101")
	require.NoError(t, err)

	n, b, ch := types.DecodeAccountBytesV3(input)
	fmt.Printf("input %x nonce %d balance %d codeHash %d\n", input, n, b.Uint64(), ch)
}
