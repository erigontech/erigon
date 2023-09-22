package state

import (
	"context"
	"encoding/binary"
	"fmt"
	"math/rand"
	"os"
	"path"
	"path/filepath"
	"sync/atomic"
	"testing"
	"time"

	"github.com/holiman/uint256"
	"github.com/ledgerwatch/erigon-lib/common/background"
	"github.com/ledgerwatch/log/v3"
	"github.com/stretchr/testify/require"

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

func TestAggregator_WinAccess(t *testing.T) {
	_, db, agg := testDbAndAggregator(t, 100)
	defer agg.Close()

	tx, err := db.BeginRwNosync(context.Background())
	require.NoError(t, err)
	defer func() {
		if tx != nil {
			tx.Rollback()
		}
	}()
	agg.SetTx(tx)

	agg.StartWrites()

	rnd := rand.New(rand.NewSource(time.Now().UnixNano()))
	for txNum := uint64(1); txNum <= 100; txNum++ {
		agg.SetTxNum(txNum)

		addr := make([]byte, length.Addr)
		n, err := rnd.Read(addr)
		require.NoError(t, err)
		require.EqualValues(t, length.Addr, n)

		buf := EncodeAccountBytes(1, uint256.NewInt(uint64(rand.Intn(10e9))), nil, 0)
		err = agg.UpdateAccountData(addr, buf)
		require.NoError(t, err)

		var v [8]byte
		binary.BigEndian.PutUint64(v[:], txNum)
		require.NoError(t, err)
		require.NoError(t, agg.FinishTx())
	}
	agg.FinishWrites()

	require.NoError(t, err)
	err = tx.Commit()
	require.NoError(t, err)
	tx = nil
}

func TestAggregator_Merge(t *testing.T) {
	_, db, agg := testDbAndAggregator(t, 1000)
	defer agg.Close()

	tx, err := db.BeginRwNosync(context.Background())
	require.NoError(t, err)
	defer func() {
		if tx != nil {
			tx.Rollback()
		}
	}()
	agg.SetTx(tx)

	agg.StartWrites()

	txs := uint64(10000)
	rnd := rand.New(rand.NewSource(time.Now().UnixNano()))

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
		//keys[txNum-1] = append(addr, loc...)

		buf := EncodeAccountBytes(1, uint256.NewInt(0), nil, 0)
		err = agg.UpdateAccountData(addr, buf)
		require.NoError(t, err)

		err = agg.WriteAccountStorage(addr, loc, []byte{addr[0], loc[0]})
		require.NoError(t, err)

		var v [8]byte
		binary.BigEndian.PutUint64(v[:], txNum)
		if txNum%135 == 0 {
			err = agg.UpdateCommitmentData([]byte("otherroothash"), v[:])
			otherMaxWrite = txNum
		} else {
			err = agg.UpdateCommitmentData([]byte("roothash"), v[:])
			maxWrite = txNum
		}
		require.NoError(t, err)
		require.NoError(t, agg.FinishTx())
	}
	agg.FinishWrites()
	require.NoError(t, err)
	err = tx.Commit()
	require.NoError(t, err)
	tx = nil

	// Check the history
	roTx, err := db.BeginRo(context.Background())
	require.NoError(t, err)
	defer roTx.Rollback()

	dc := agg.MakeContext()

	v, err := dc.ReadCommitment([]byte("roothash"), roTx)
	require.NoError(t, err)

	require.EqualValues(t, maxWrite, binary.BigEndian.Uint64(v[:]))

	v, err = dc.ReadCommitment([]byte("otherroothash"), roTx)
	require.NoError(t, err)
	dc.Close()

	require.EqualValues(t, otherMaxWrite, binary.BigEndian.Uint64(v[:]))
}

// here we create a bunch of updates for further aggregation.
// FinishTx should merge underlying files several times
// Expected that:
// - we could close first aggregator and open another with previous data still available
// - new aggregator SeekCommitment must return txNum equal to amount of total txns
func TestAggregator_RestartOnDatadir(t *testing.T) {
	logger := log.New()
	aggStep := uint64(50)
	path, db, agg := testDbAndAggregator(t, aggStep)

	tx, err := db.BeginRw(context.Background())
	require.NoError(t, err)
	defer func() {
		if tx != nil {
			tx.Rollback()
		}
	}()
	agg.SetTx(tx)
	agg.StartWrites()

	var latestCommitTxNum uint64

	rnd := rand.New(rand.NewSource(time.Now().Unix()))

	txs := (aggStep / 2) * 19
	t.Logf("step=%d tx_count=%d", aggStep, txs)
	var aux [8]byte
	// keys are encodings of numbers 1..31
	// each key changes value on every txNum which is multiple of the key
	var maxWrite uint64
	for txNum := uint64(1); txNum <= txs; txNum++ {
		agg.SetTxNum(txNum)
		binary.BigEndian.PutUint64(aux[:], txNum)

		addr, loc := make([]byte, length.Addr), make([]byte, length.Hash)
		n, err := rnd.Read(addr)
		require.NoError(t, err)
		require.EqualValues(t, length.Addr, n)

		n, err = rnd.Read(loc)
		require.NoError(t, err)
		require.EqualValues(t, length.Hash, n)
		//keys[txNum-1] = append(addr, loc...)

		buf := EncodeAccountBytes(1, uint256.NewInt(0), nil, 0)
		err = agg.UpdateAccountData(addr, buf)
		require.NoError(t, err)

		err = agg.WriteAccountStorage(addr, loc, []byte{addr[0], loc[0]})
		require.NoError(t, err)

		err = agg.UpdateCommitmentData([]byte("key"), aux[:])
		require.NoError(t, err)
		maxWrite = txNum

		require.NoError(t, agg.FinishTx())
	}
	agg.FinishWrites()
	agg.Close()

	err = tx.Commit()
	require.NoError(t, err)
	tx = nil

	// Start another aggregator on same datadir
	anotherAgg, err := NewAggregator(filepath.Join(path, "e4"), filepath.Join(path, "e4tmp"), aggStep, CommitmentModeDirect, commitment.VariantHexPatriciaTrie, logger)
	require.NoError(t, err)
	require.NoError(t, anotherAgg.ReopenFolder())

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
	_, sstartTx, err := anotherAgg.SeekCommitment()
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
	v, err := dc.ReadCommitment([]byte("key"), roTx)
	require.NoError(t, err)
	dc.Close()

	require.EqualValues(t, maxWrite, binary.BigEndian.Uint64(v[:]))
}

func TestAggregator_RestartOnFiles(t *testing.T) {
	logger := log.New()
	aggStep := uint64(100)

	path, db, agg := testDbAndAggregator(t, aggStep)

	tx, err := db.BeginRw(context.Background())
	require.NoError(t, err)
	defer func() {
		if tx != nil {
			tx.Rollback()
		}
	}()
	agg.SetTx(tx)
	agg.StartWrites()

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
		err = agg.UpdateAccountData(addr, buf[:])
		require.NoError(t, err)

		err = agg.WriteAccountStorage(addr, loc, []byte{addr[0], loc[0]})
		require.NoError(t, err)

		keys[txNum-1] = append(addr, loc...)

		err = agg.FinishTx()
		require.NoError(t, err)
	}
	agg.FinishWrites()

	err = tx.Commit()
	require.NoError(t, err)
	tx = nil
	db.Close()
	agg.Close()

	require.NoError(t, os.RemoveAll(filepath.Join(path, "db4")))

	newDb, err := mdbx.NewMDBX(logger).InMem(filepath.Join(path, "db4")).WithTableCfg(func(defaultBuckets kv.TableCfg) kv.TableCfg {
		return kv.ChaindataTablesCfg
	}).Open()
	require.NoError(t, err)
	t.Cleanup(newDb.Close)

	newTx, err := newDb.BeginRw(context.Background())
	require.NoError(t, err)
	defer newTx.Rollback()

	newAgg, err := NewAggregator(path, path, aggStep, CommitmentModeDirect, commitment.VariantHexPatriciaTrie, logger)
	require.NoError(t, err)
	require.NoError(t, newAgg.ReopenFolder())

	newAgg.SetTx(newTx)
	newAgg.StartWrites()

	_, latestTx, err := newAgg.SeekCommitment()
	require.NoError(t, err)
	t.Logf("seek to latest_tx=%d", latestTx)

	ctx := newAgg.defaultCtx
	miss := uint64(0)
	for i, key := range keys {
		if uint64(i+1) >= txs-aggStep {
			continue // finishtx always stores last agg step in db which we deleted, so missing  values which were not aggregated is expected
		}
		stored, err := ctx.ReadAccountData(key[:length.Addr], newTx)
		require.NoError(t, err)
		if len(stored) == 0 {
			miss++
			fmt.Printf("%x [%d/%d]", key, miss, i+1) // txnum starts from 1
			continue
		}

		nonce, _, _ := DecodeAccountBytes(stored)
		require.EqualValues(t, i+1, nonce)

		storedV, err := ctx.ReadAccountStorage(key[:length.Addr], key[length.Addr:], newTx)
		require.NoError(t, err)
		require.EqualValues(t, key[0], storedV[0])
		require.EqualValues(t, key[length.Addr], storedV[1])
	}
	newAgg.FinishWrites()
	ctx.Close()
	newAgg.Close()

	require.NoError(t, err)
}

func TestAggregator_ReplaceCommittedKeys(t *testing.T) {
	aggStep := uint64(500)

	_, db, agg := testDbAndAggregator(t, aggStep)
	t.Cleanup(agg.Close)

	tx, err := db.BeginRw(context.Background())
	require.NoError(t, err)
	defer func() {
		if tx != nil {
			tx.Rollback()
		}
	}()
	agg.SetTx(tx)
	defer agg.StartWrites().FinishWrites()

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

	roots := agg.AggregatedRoots()
	txs := (aggStep) * StepsInBiggestFile
	t.Logf("step=%d tx_count=%d", aggStep, txs)

	rnd := rand.New(rand.NewSource(0))
	keys := make([][]byte, txs/2)

	for txNum := uint64(1); txNum <= txs/2; txNum++ {
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
		err = agg.UpdateAccountData(addr, buf)
		require.NoError(t, err)

		err = agg.WriteAccountStorage(addr, loc, []byte{addr[0], loc[0]})
		require.NoError(t, err)

		err = agg.FinishTx()
		require.NoError(t, err)
		select {
		case <-roots:
			require.NoError(t, commit(txNum))
		default:
			continue
		}
	}

	half := txs / 2
	for txNum := txs/2 + 1; txNum <= txs; txNum++ {
		agg.SetTxNum(txNum)

		addr, loc := keys[txNum-1-half][:length.Addr], keys[txNum-1-half][length.Addr:]

		err = agg.WriteAccountStorage(addr, loc, []byte{addr[0], loc[0]})
		require.NoError(t, err)

		err = agg.FinishTx()
		require.NoError(t, err)
	}

	err = tx.Commit()
	tx = nil

	tx, err = db.BeginRw(context.Background())
	require.NoError(t, err)

	ctx := agg.defaultCtx
	for _, key := range keys {
		storedV, err := ctx.ReadAccountStorage(key[:length.Addr], key[length.Addr:], tx)
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

	keyCount, M := 120000, 1024
	dataPath := generateCompressedKV(t, tmp, 52, 180 /*val size*/, keyCount, logger)
	defer os.RemoveAll(tmp)

	indexPath := path.Join(tmp, filepath.Base(dataPath)+".bti")
	err := BuildBtreeIndex(dataPath, indexPath, logger)
	require.NoError(t, err)

	bt, err := OpenBtreeIndex(indexPath, dataPath, uint64(M))
	require.NoError(t, err)
	require.EqualValues(t, bt.KeyCount(), keyCount)

	keys, err := pivotKeysFromKV(dataPath)
	require.NoError(t, err)

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

		pos, _ = getter.Skip()
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

	err = BuildBtreeIndexWithDecompressor(tmp+".bt", decomp, &background.Progress{}, tmp, logger)
	require.NoError(t, err)

	bt, err := OpenBtreeIndexWithDecompressor(tmp+".bt", M, decomp)
	require.NoError(t, err)
	require.EqualValues(t, bt.KeyCount(), keyCount)
	bt.Close()
}
