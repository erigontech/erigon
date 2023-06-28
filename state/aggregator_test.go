package state

import (
	"context"
	"encoding/binary"
	"encoding/hex"
	"fmt"
	"math/rand"
	"os"
	"path"
	"path/filepath"
	"sync/atomic"
	"testing"
	"time"

	"github.com/holiman/uint256"
	"github.com/ledgerwatch/log/v3"
	"github.com/stretchr/testify/require"

	"github.com/ledgerwatch/erigon-lib/common/background"

	"github.com/ledgerwatch/erigon-lib/commitment"
	"github.com/ledgerwatch/erigon-lib/common"
	"github.com/ledgerwatch/erigon-lib/common/length"
	"github.com/ledgerwatch/erigon-lib/compress"
	"github.com/ledgerwatch/erigon-lib/kv"
	"github.com/ledgerwatch/erigon-lib/kv/mdbx"
)

func testDbAndAggregator(t *testing.T, aggStep uint64) (string, kv.RwDB, *Aggregator) {
	t.Helper()
	path := t.TempDir()
	logger := log.New()
	db := mdbx.NewMDBX(logger).InMem(filepath.Join(path, "db4")).WithTableCfg(func(defaultBuckets kv.TableCfg) kv.TableCfg {
		return kv.ChaindataTablesCfg
	}).MustOpen()
	t.Cleanup(db.Close)
	agg, err := NewAggregator(filepath.Join(path, "e4"), filepath.Join(path, "e4tmp"), aggStep, CommitmentModeDirect, commitment.VariantHexPatriciaTrie, logger)
	require.NoError(t, err)
	return path, db, agg
}

func TestAggregatorV3_Merge(t *testing.T) {
	_, db, agg := testDbAndAggregatorv3(t, 1000)
	defer agg.Close()

	rwTx, err := db.BeginRwNosync(context.Background())
	require.NoError(t, err)
	defer func() {
		if rwTx != nil {
			rwTx.Rollback()
		}
	}()
	agg.SetTx(rwTx)
	agg.StartWrites()
	domains := agg.SharedDomains()
	domCtx := agg.MakeContext()

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
		agg.SetTxNum(txNum)

		addr, loc := make([]byte, length.Addr), make([]byte, length.Hash)

		n, err := rnd.Read(addr)
		require.NoError(t, err)
		require.EqualValues(t, length.Addr, n)

		n, err = rnd.Read(loc)
		require.NoError(t, err)
		require.EqualValues(t, length.Hash, n)

		buf := EncodeAccountBytes(1, uint256.NewInt(0), nil, 0)
		err = domains.UpdateAccountData(addr, buf, nil)
		require.NoError(t, err)

		err = domains.WriteAccountStorage(addr, loc, []byte{addr[0], loc[0]}, nil)
		require.NoError(t, err)

		var v [8]byte
		binary.BigEndian.PutUint64(v[:], txNum)
		if txNum%135 == 0 {
			pv, _, err := domCtx.GetLatest(kv.CommitmentDomain, commKey2, nil, rwTx)
			require.NoError(t, err)

			err = domains.UpdateCommitmentData(commKey2, v[:], pv)
			otherMaxWrite = txNum
		} else {
			pv, _, err := domCtx.GetLatest(kv.CommitmentDomain, commKey1, nil, rwTx)
			require.NoError(t, err)

			err = domains.UpdateCommitmentData(commKey1, v[:], pv)
			maxWrite = txNum
		}
		require.NoError(t, err)

	}
	err = agg.Flush(context.Background(), rwTx)
	require.NoError(t, err)
	agg.FinishWrites()

	require.NoError(t, err)
	err = rwTx.Commit()
	require.NoError(t, err)
	rwTx = nil

	// Check the history
	roTx, err := db.BeginRo(context.Background())
	require.NoError(t, err)
	defer roTx.Rollback()

	dc := agg.MakeContext()

	v, ex, err := dc.GetLatest(kv.CommitmentDomain, commKey1, nil, roTx)
	require.NoError(t, err)
	require.Truef(t, ex, "key %x not found", commKey1)

	require.EqualValues(t, maxWrite, binary.BigEndian.Uint64(v[:]))

	v, ex, err = dc.GetLatest(kv.CommitmentDomain, commKey2, nil, roTx)
	require.NoError(t, err)
	require.Truef(t, ex, "key %x not found", commKey2)
	dc.Close()

	require.EqualValues(t, otherMaxWrite, binary.BigEndian.Uint64(v[:]))
}

// here we create a bunch of updates for further aggregation.
// FinishTx should merge underlying files several times
// Expected that:
// - we could close first aggregator and open another with previous data still available
// - new aggregator SeekCommitment must return txNum equal to amount of total txns
func TestAggregatorV3_RestartOnDatadir(t *testing.T) {
	logger := log.New()
	aggStep := uint64(50)
	path, db, agg := testDbAndAggregatorv3(t, aggStep)

	tx, err := db.BeginRw(context.Background())
	require.NoError(t, err)
	defer func() {
		if tx != nil {
			tx.Rollback()
		}
	}()
	agg.SetTx(tx)
	agg.StartWrites()
	domains := agg.SharedDomains()

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
		agg.SetTxNum(txNum)
		binary.BigEndian.PutUint64(aux[:], txNum)

		n, err := rnd.Read(addr)
		require.NoError(t, err)
		require.EqualValues(t, length.Addr, n)

		n, err = rnd.Read(loc)
		require.NoError(t, err)
		require.EqualValues(t, length.Hash, n)
		//keys[txNum-1] = append(addr, loc...)

		buf := EncodeAccountBytes(1, uint256.NewInt(rnd.Uint64()), nil, 0)
		err = domains.UpdateAccountData(addr, buf, nil)
		require.NoError(t, err)

		err = domains.WriteAccountStorage(addr, loc, []byte{addr[0], loc[0]}, nil)
		require.NoError(t, err)

		err = domains.UpdateCommitmentData(someKey, aux[:], nil)
		require.NoError(t, err)
		maxWrite = txNum
	}
	_, err = domains.Commit(true, false)
	require.NoError(t, err)

	err = agg.Flush(context.Background(), tx)
	require.NoError(t, err)
	err = tx.Commit()
	require.NoError(t, err)
	tx = nil

	tx, err = db.BeginRw(context.Background())
	require.NoError(t, err)

	ac := agg.MakeContext()
	ac.IterateAccounts(tx, []byte{}, func(addr, val []byte) {
		fmt.Printf("addr=%x val=%x\n", addr, val)
	})
	ac.Close()
	tx.Rollback()

	err = agg.BuildFiles(txs)
	require.NoError(t, err)

	agg.FinishWrites()
	agg.Close()

	// Start another aggregator on same datadir
	anotherAgg, err := NewAggregatorV3(context.Background(), filepath.Join(path, "e4"), filepath.Join(path, "e4", "tmp2"), aggStep, db, logger)
	require.NoError(t, err)
	require.NoError(t, anotherAgg.OpenFolder())

	defer anotherAgg.Close()

	rwTx, err := db.BeginRw(context.Background())
	require.NoError(t, err)
	defer func() {
		if rwTx != nil {
			rwTx.Rollback()
		}
	}()

	anotherAgg.SetTx(rwTx)
	startTx := anotherAgg.EndTxNumMinimax()
	dom2 := anotherAgg.SharedDomains()

	_, sstartTx, err := dom2.SeekCommitment()

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

	dc := anotherAgg.MakeContext()
	v, ex, err := dc.GetLatest(kv.CommitmentDomain, someKey, nil, roTx)
	require.NoError(t, err)
	require.True(t, ex)
	dc.Close()

	require.EqualValues(t, maxWrite, binary.BigEndian.Uint64(v[:]))
}

func TestAggregatorV3_RestartOnFiles(t *testing.T) {
	//t.Skip("TODO: finish to fix this test")
	logger := log.New()
	aggStep := uint64(100)

	path, db, agg := testDbAndAggregatorv3(t, aggStep)

	tx, err := db.BeginRw(context.Background())
	require.NoError(t, err)
	defer func() {
		if tx != nil {
			tx.Rollback()
		}
	}()
	agg.SetTx(tx)
	agg.StartWrites()
	domains := agg.SharedDomains()

	txs := aggStep * 5
	t.Logf("step=%d tx_count=%d\n", aggStep, txs)

	rnd := rand.New(rand.NewSource(0))
	keys := make([][]byte, txs)

	for txNum := uint64(1); txNum <= txs; txNum++ {
		agg.SetTxNum(txNum)

		addr, loc := make([]byte, length.Addr), make([]byte, length.Hash)
		n, err := rnd.Read(addr)
		require.NoError(t, err)
		require.EqualValues(t, length.Addr, n)

		n, err = rnd.Read(loc)
		require.NoError(t, err)
		require.EqualValues(t, length.Hash, n)

		buf := EncodeAccountBytes(txNum, uint256.NewInt(1000000000000), nil, 0)
		err = domains.UpdateAccountData(addr, buf[:], nil)
		require.NoError(t, err)

		err = domains.WriteAccountStorage(addr, loc, []byte{addr[0], loc[0]}, nil)
		require.NoError(t, err)

		keys[txNum-1] = append(addr, loc...)
	}

	// flush and build files
	err = agg.Flush(context.Background(), tx)
	require.NoError(t, err)

	err = tx.Commit()
	require.NoError(t, err)
	agg.FinishWrites()

	err = agg.BuildFiles(txs)
	require.NoError(t, err)

	tx = nil
	agg.Close()
	db.Close()

	// remove database files
	require.NoError(t, os.RemoveAll(filepath.Join(path, "db4")))

	// open new db and aggregator instances
	newDb, err := mdbx.NewMDBX(logger).InMem(filepath.Join(path, "db4")).WithTableCfg(func(defaultBuckets kv.TableCfg) kv.TableCfg {
		return kv.ChaindataTablesCfg
	}).Open()
	require.NoError(t, err)
	t.Cleanup(newDb.Close)

	newTx, err := newDb.BeginRw(context.Background())
	require.NoError(t, err)
	defer newTx.Rollback()

	newAgg, err := NewAggregatorV3(context.Background(), filepath.Join(path, "e4"), filepath.Join(path, "e4", "tmp"), aggStep, newDb, logger)
	require.NoError(t, err)
	require.NoError(t, newAgg.OpenFolder())

	newAgg.SetTx(newTx)
	defer newAgg.StartWrites().FinishWrites()

	newDoms := newAgg.SharedDomains()
	defer newDoms.Close()

	_, latestTx, err := newDoms.SeekCommitment()
	require.NoError(t, err)
	t.Logf("seek to latest_tx=%d", latestTx)

	ctx := newAgg.MakeContext()
	defer ctx.Close()

	miss := uint64(0)
	for i, key := range keys {
		if uint64(i+1) >= txs-aggStep {
			continue // finishtx always stores last agg step in db which we deleted, so missing  values which were not aggregated is expected
		}
		stored, _, err := ctx.GetLatest(kv.AccountsDomain, key[:length.Addr], nil, newTx)
		require.NoError(t, err)
		if len(stored) == 0 {
			miss++
			fmt.Printf("%x [%d/%d]", key, miss, i+1) // txnum starts from 1
			continue
		}

		nonce, _, _ := DecodeAccountBytes(stored)
		require.EqualValues(t, i+1, nonce)

		storedV, _, err := ctx.GetLatest(kv.StorageDomain, key[:length.Addr], key[length.Addr:], newTx)
		require.NoError(t, err)
		require.EqualValues(t, key[0], storedV[0])
		require.EqualValues(t, key[length.Addr], storedV[1])
	}
	newAgg.Close()

	require.NoError(t, err)
}

func TestAggregator_ReplaceCommittedKeys(t *testing.T) {
	aggStep := uint64(500)

	_, db, agg := testDbAndAggregatorv3(t, aggStep)
	t.Cleanup(agg.Close)

	tx, err := db.BeginRw(context.Background())
	require.NoError(t, err)
	defer func() {
		if tx != nil {
			tx.Rollback()
		}
	}()
	agg.SetTx(tx)
	defer agg.StartUnbufferedWrites().FinishWrites()

	var latestCommitTxNum uint64
	commit := func(txn uint64) error {
		err = tx.Commit()
		require.NoError(t, err)
		tx, err = db.BeginRw(context.Background())
		require.NoError(t, err)
		t.Logf("commit to db txn=%d", txn)

		atomic.StoreUint64(&latestCommitTxNum, txn)
		agg.SetTx(tx)
		return nil
	}

	domains := agg.SharedDomains()

	txs := (aggStep) * StepsInBiggestFile
	t.Logf("step=%d tx_count=%d", aggStep, txs)

	rnd := rand.New(rand.NewSource(0))
	keys := make([][]byte, txs/2)

	ct := agg.MakeContext()
	defer ct.Close()

	var txNum uint64
	for txNum = uint64(1); txNum <= txs/2; txNum++ {
		agg.SetTxNum(txNum)

		addr, loc := make([]byte, length.Addr), make([]byte, length.Hash)
		n, err := rnd.Read(addr)
		require.NoError(t, err)
		require.EqualValues(t, length.Addr, n)

		n, err = rnd.Read(loc)
		require.NoError(t, err)
		require.EqualValues(t, length.Hash, n)
		keys[txNum-1] = append(addr, loc...)

		buf := EncodeAccountBytes(1, uint256.NewInt(0), nil, 0)

		prev, _, err := ct.accounts.GetLatest(addr, nil, tx)
		require.NoError(t, err)

		err = domains.UpdateAccountData(addr, buf, prev)
		require.NoError(t, err)

		prev, _, err = ct.storage.GetLatest(addr, loc, tx)
		require.NoError(t, err)
		err = domains.WriteAccountStorage(addr, loc, []byte{addr[0], loc[0]}, prev)
		require.NoError(t, err)

	}
	require.NoError(t, commit(txNum))

	half := txs / 2
	for txNum = txNum + 1; txNum <= txs; txNum++ {
		agg.SetTxNum(txNum)

		addr, loc := keys[txNum-1-half][:length.Addr], keys[txNum-1-half][length.Addr:]

		prev, _, err := ct.storage.GetLatest(addr, loc, tx)
		require.NoError(t, err)
		err = domains.WriteAccountStorage(addr, loc, []byte{addr[0], loc[0]}, prev)
		require.NoError(t, err)
	}

	err = tx.Commit()
	tx = nil

	tx, err = db.BeginRw(context.Background())
	require.NoError(t, err)

	ctx := agg.MakeContext()
	defer ctx.Close()

	for i, key := range keys {
		storedV, found, err := ctx.storage.GetLatest(key[:length.Addr], key[length.Addr:], tx)
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

func Test_BtreeIndex_Seek(t *testing.T) {
	tmp := t.TempDir()
	logger := log.New()
	keyCount, M := 120, 30

	t.Run("empty index", func(t *testing.T) {
		dataPath := generateCompressedKV(t, tmp, 52, 180 /*val size*/, 0, logger)
		indexPath := path.Join(tmp, filepath.Base(dataPath)+".bti")
		err := BuildBtreeIndex(dataPath, indexPath, logger)
		require.NoError(t, err)

		bt, err := OpenBtreeIndex(indexPath, dataPath, uint64(M))
		require.NoError(t, err)
		require.EqualValues(t, 0, bt.KeyCount())
	})
	dataPath := generateCompressedKV(t, tmp, 52, 180 /*val size*/, keyCount, logger)

	indexPath := path.Join(tmp, filepath.Base(dataPath)+".bti")
	err := BuildBtreeIndex(dataPath, indexPath, logger)
	require.NoError(t, err)

	bt, err := OpenBtreeIndex(indexPath, dataPath, uint64(M))
	require.NoError(t, err)
	require.EqualValues(t, bt.KeyCount(), keyCount)

	keys, err := pivotKeysFromKV(dataPath)
	require.NoError(t, err)

	t.Run("seek beyond the last key", func(t *testing.T) {
		_, _, err := bt.dataLookup(bt.keyCount + 1)
		require.ErrorIs(t, err, ErrBtIndexLookupBounds)

		_, _, err = bt.dataLookup(bt.keyCount)
		require.ErrorIs(t, err, ErrBtIndexLookupBounds)
		require.Error(t, err)

		_, _, err = bt.dataLookup(bt.keyCount - 1)
		require.NoError(t, err)

		cur, err := bt.Seek(common.FromHex("0xffffffffffffff")) //seek beyeon the last key
		require.NoError(t, err)
		require.Nil(t, cur)
	})

	for i := 0; i < len(keys); i++ {
		cur, err := bt.Seek(keys[i])
		require.NoErrorf(t, err, "i=%d", i)
		require.EqualValues(t, keys[i], cur.key)
		require.NotEmptyf(t, cur.Value(), "i=%d", i)
		// require.EqualValues(t, uint64(i), cur.Value())
	}
	for i := 1; i < len(keys); i++ {
		alt := common.Copy(keys[i])
		for j := len(alt) - 1; j >= 0; j-- {
			if alt[j] > 0 {
				alt[j] -= 1
				break
			}
		}
		cur, err := bt.Seek(keys[i])
		require.NoError(t, err)
		require.EqualValues(t, keys[i], cur.Key())
	}

	bt.Close()
}

func pivotKeysFromKV(dataPath string) ([][]byte, error) {
	decomp, err := compress.NewDecompressor(dataPath)
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

func generateCompressedKV(tb testing.TB, tmp string, keySize, valueSize, keyCount int, logger log.Logger) string {
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
	comp, err := compress.NewCompressor(context.Background(), "cmp", dataPath, tmp, compress.MinPatternScore, 1, log.LvlDebug, logger)
	require.NoError(tb, err)

	for i := 0; i < keyCount; i++ {
		key := make([]byte, keySize)
		n, err := rnd.Read(key[:])
		require.EqualValues(tb, keySize, n)
		binary.BigEndian.PutUint64(key[keySize-8:], uint64(i))
		require.NoError(tb, err)
		err = comp.AddWord(key[:])
		require.NoError(tb, err)

		n, err = rnd.Read(values[:rnd.Intn(valueSize)+1])
		require.NoError(tb, err)

		err = comp.AddWord(values[:n])
		require.NoError(tb, err)
	}

	err = comp.Compress()
	require.NoError(tb, err)
	comp.Close()

	decomp, err := compress.NewDecompressor(dataPath)
	require.NoError(tb, err)

	getter := decomp.MakeGetter()
	getter.Reset(0)

	var pos uint64
	key := make([]byte, keySize)
	for i := 0; i < keyCount; i++ {
		if !getter.HasNext() {
			tb.Fatalf("not enough values at %d", i)
			break
		}

		keys, _ := getter.Next(key[:0])
		err = iw.AddKey(keys[:], pos)

		pos = getter.Skip()
		require.NoError(tb, err)
	}
	decomp.Close()

	require.NoError(tb, iw.Build())
	iw.Close()

	return decomp.FilePath()
}

func Test_InitBtreeIndex(t *testing.T) {
	logger := log.New()
	tmp := t.TempDir()

	keyCount, M := 100, uint64(4)
	compPath := generateCompressedKV(t, tmp, 52, 300, keyCount, logger)
	decomp, err := compress.NewDecompressor(compPath)
	require.NoError(t, err)
	defer decomp.Close()

	err = BuildBtreeIndexWithDecompressor(filepath.Join(tmp, "a.bt"), decomp, &background.Progress{}, tmp, logger)
	require.NoError(t, err)

	bt, err := OpenBtreeIndexWithDecompressor(filepath.Join(tmp, "a.bt"), M, decomp)
	require.NoError(t, err)
	require.EqualValues(t, bt.KeyCount(), keyCount)
	bt.Close()
}

func testDbAndAggregatorv3(t *testing.T, aggStep uint64) (string, kv.RwDB, *AggregatorV3) {
	t.Helper()
	path := t.TempDir()
	logger := log.New()
	db := mdbx.NewMDBX(logger).InMem(filepath.Join(path, "db4")).WithTableCfg(func(defaultBuckets kv.TableCfg) kv.TableCfg {
		return kv.ChaindataTablesCfg
	}).MustOpen()
	t.Cleanup(db.Close)

	dir := filepath.Join(path, "e4")
	err := os.Mkdir(dir, 0740)
	require.NoError(t, err)

	agg, err := NewAggregatorV3(context.Background(), dir, filepath.Join(path, "e4", "tmp"), aggStep, db, logger)
	require.NoError(t, err)
	err = agg.OpenFolder()
	require.NoError(t, err)
	return path, db, agg
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
	_, db, agg := testDbAndAggregatorv3(t, 20)
	defer agg.Close()
	defer db.Close()

	domains := agg.SharedDomains()

	rwTx, err := db.BeginRw(context.Background())
	require.NoError(t, err)
	defer rwTx.Rollback()

	domains.SetTx(rwTx)
	agg.SetTx(rwTx)
	agg.StartWrites()

	//agg.StartUnbufferedWrites()
	defer agg.FinishWrites()
	defer domains.Close()

	keys, vals := generateInputData(t, 8, 16, 10)
	keys = keys[:2]

	var i int
	roots := make([][]byte, 0, 10)
	var pruneFrom uint64 = 5

	mc := agg.MakeContext()
	defer mc.Close()

	for i = 0; i < len(vals); i++ {
		domains.SetTxNum(uint64(i))
		fmt.Printf("txn=%d\n", i)

		for j := 0; j < len(keys); j++ {
			buf := EncodeAccountBytes(uint64(i), uint256.NewInt(uint64(i*100_000)), nil, 0)
			prev, err := domains.LatestAccount(keys[j])
			require.NoError(t, err)

			err = domains.UpdateAccountData(keys[j], buf, prev)
			//err = domains.UpdateAccountCode(keys[j], vals[i], nil)
			require.NoError(t, err)
		}
		rh, err := domains.Commit(true, false)
		require.NoError(t, err)
		require.NotEmpty(t, rh)
		roots = append(roots, rh)
	}

	err = agg.Flush(context.Background(), rwTx)
	require.NoError(t, err)

	err = agg.Unwind(context.Background(), pruneFrom)
	require.NoError(t, err)

	for i = int(pruneFrom); i < len(vals); i++ {
		domains.SetTxNum(uint64(i))

		fmt.Printf("txn=%d\n", i)
		for j := 0; j < len(keys); j++ {
			buf := EncodeAccountBytes(uint64(i), uint256.NewInt(uint64(i*100_000)), nil, 0)
			prev, _, err := mc.GetLatest(kv.AccountsDomain, keys[j], nil, rwTx)
			require.NoError(t, err)

			err = domains.UpdateAccountData(keys[j], buf, prev)
			require.NoError(t, err)
			//err = domains.UpdateAccountCode(keys[j], vals[i], nil)
			//require.NoError(t, err)
		}

		rh, err := domains.Commit(true, false)
		require.NoError(t, err)
		require.NotEmpty(t, rh)
		require.EqualValues(t, roots[i], rh)
	}

	err = agg.Flush(context.Background(), rwTx)
	require.NoError(t, err)

	pruneFrom = 3
	err = agg.Unwind(context.Background(), pruneFrom)
	require.NoError(t, err)

	for i = int(pruneFrom); i < len(vals); i++ {
		domains.SetTxNum(uint64(i))

		fmt.Printf("txn=%d\n", i)
		for j := 0; j < len(keys); j++ {
			buf := EncodeAccountBytes(uint64(i), uint256.NewInt(uint64(i*100_000)), nil, 0)
			prev, _, err := mc.GetLatest(kv.AccountsDomain, keys[j], nil, rwTx)
			require.NoError(t, err)

			err = domains.UpdateAccountData(keys[j], buf, prev)
			require.NoError(t, err)
			//err = domains.UpdateAccountCode(keys[j], vals[i], nil)
			//require.NoError(t, err)
		}

		rh, err := domains.Commit(true, false)
		require.NoError(t, err)
		require.NotEmpty(t, rh)
		require.EqualValues(t, roots[i], rh)
	}
}

func Test_helper_decodeAccountv3Bytes(t *testing.T) {
	input, err := hex.DecodeString("000114000101")
	require.NoError(t, err)

	n, b, ch := DecodeAccountBytes(input)
	fmt.Printf("input %x nonce %d balance %d codeHash %d\n", input, n, b.Uint64(), ch)

}
