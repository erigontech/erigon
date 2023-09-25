package state

import (
	"context"
	"encoding/binary"
	"encoding/hex"
	"fmt"
	"math/rand"
	"os"
	"path"
	"sync/atomic"
	"testing"
	"time"

	"github.com/ledgerwatch/erigon-lib/common/datadir"

	"github.com/c2h5oh/datasize"
	"github.com/holiman/uint256"
	"github.com/ledgerwatch/log/v3"
	"github.com/stretchr/testify/require"

	"github.com/ledgerwatch/erigon-lib/etl"

	"github.com/ledgerwatch/erigon-lib/common"
	"github.com/ledgerwatch/erigon-lib/common/length"
	"github.com/ledgerwatch/erigon-lib/compress"
	"github.com/ledgerwatch/erigon-lib/kv"
	"github.com/ledgerwatch/erigon-lib/kv/mdbx"
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
	agg.StartWrites()
	domCtx := agg.MakeContext()
	defer domCtx.Close()
	domains := agg.SharedDomains(domCtx)
	defer domains.Close()

	domains.SetTx(rwTx)

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
			require.NoError(t, err)
			otherMaxWrite = txNum
		} else {
			pv, _, err := domCtx.GetLatest(kv.CommitmentDomain, commKey1, nil, rwTx)
			require.NoError(t, err)

			err = domains.UpdateCommitmentData(commKey1, v[:], pv)
			require.NoError(t, err)
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

func TestAggregatorV3_RestartOnDatadir(t *testing.T) {
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
	logger := log.New()
	aggStep := rc.aggStep
	db, agg := testDbAndAggregatorv3(t, aggStep)
	if rc.useBplus {
		UseBpsTree = true
		defer func() { UseBpsTree = false }()
	}

	tx, err := db.BeginRw(context.Background())
	require.NoError(t, err)
	defer func() {
		if tx != nil {
			tx.Rollback()
		}
	}()
	agg.StartWrites()
	domCtx := agg.MakeContext()
	defer domCtx.Close()

	domains := agg.SharedDomains(domCtx)
	defer domains.Close()

	domains.SetTx(tx)

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

	//tx, err = db.BeginRw(context.Background())
	//require.NoError(t, err)
	//
	//ac := agg.MakeContext()
	//ac.IterateAccounts(tx, []byte{}, func(addr, val []byte) {
	//	fmt.Printf("addr=%x val=%x\n", addr, val)
	//})
	//ac.Close()
	//tx.Rollback()

	err = agg.BuildFiles(txs)
	require.NoError(t, err)

	agg.FinishWrites()
	agg.Close()

	// Start another aggregator on same datadir
	anotherAgg, err := NewAggregatorV3(context.Background(), agg.dirs, aggStep, db, logger)
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
	ac2 := anotherAgg.MakeContext()
	defer ac2.Close()
	dom2 := anotherAgg.SharedDomains(ac2)
	dom2.SetTx(rwTx)

	_, sstartTx, err := dom2.SeekCommitment(0, 1<<63-1)

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
	logger := log.New()
	aggStep := uint64(100)

	db, agg := testDbAndAggregatorv3(t, aggStep)
	dirs := agg.dirs

	tx, err := db.BeginRw(context.Background())
	require.NoError(t, err)
	defer func() {
		if tx != nil {
			tx.Rollback()
		}
	}()
	//agg.SetTx(tx)
	agg.StartWrites()
	domCtx := agg.MakeContext()
	defer domCtx.Close()
	domains := agg.SharedDomains(domCtx)
	defer domains.Close()
	domains.SetTx(tx)

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

	latestStepInDB := agg.accounts.LastStepInDB(tx)
	require.Equal(t, 5, int(latestStepInDB))

	err = tx.Commit()
	require.NoError(t, err)
	agg.FinishWrites()

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

	newAgg, err := NewAggregatorV3(context.Background(), agg.dirs, aggStep, newDb, logger)
	require.NoError(t, err)
	require.NoError(t, newAgg.OpenFolder())

	newTx, err := newDb.BeginRw(context.Background())
	require.NoError(t, err)
	defer newTx.Rollback()

	//newAgg.SetTx(newTx)
	defer newAgg.StartWrites().FinishWrites()

	ac := newAgg.MakeContext()
	defer ac.Close()
	newDoms := newAgg.SharedDomains(ac)
	defer newDoms.Close()
	newDoms.SetTx(newTx)

	_, latestTx, err := newDoms.SeekCommitment(0, 1<<63-1)
	require.NoError(t, err)
	t.Logf("seek to latest_tx=%d", latestTx)

	miss := uint64(0)
	for i, key := range keys {
		if uint64(i+1) >= txs-aggStep {
			continue // finishtx always stores last agg step in db which we deleted, so missing  values which were not aggregated is expected
		}
		stored, _, err := ac.GetLatest(kv.AccountsDomain, key[:length.Addr], nil, newTx)
		require.NoError(t, err)
		if len(stored) == 0 {
			miss++
			//fmt.Printf("%x [%d/%d]", key, miss, i+1) // txnum starts from 1
			continue
		}
		nonce, _, _ := DecodeAccountBytes(stored)

		require.EqualValues(t, i+1, int(nonce))

		storedV, found, err := ac.GetLatest(kv.StorageDomain, key[:length.Addr], key[length.Addr:], newTx)
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

func TestAggregator_ReplaceCommittedKeys(t *testing.T) {
	aggStep := uint64(500)

	db, agg := testDbAndAggregatorv3(t, aggStep)

	tx, err := db.BeginRw(context.Background())
	require.NoError(t, err)
	defer func() {
		if tx != nil {
			tx.Rollback()
		}
	}()
	defer agg.StartUnbufferedWrites().FinishWrites()

	ct := agg.MakeContext()
	defer ct.Close()
	domains := agg.SharedDomains(ct)
	defer agg.CloseSharedDomains()
	domains.SetTx(tx)

	var latestCommitTxNum uint64
	commit := func(txn uint64) error {
		ct.Close()
		err = tx.Commit()
		require.NoError(t, err)

		tx, err = db.BeginRw(context.Background())
		require.NoError(t, err)
		ct = agg.MakeContext()
		domains = agg.SharedDomains(ct)
		atomic.StoreUint64(&latestCommitTxNum, txn)
		domains.SetTx(tx)
		return nil
	}

	txs := (aggStep) * StepsInColdFile
	t.Logf("step=%d tx_count=%d", aggStep, txs)

	rnd := rand.New(rand.NewSource(0))
	keys := make([][]byte, txs/2)

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
		domains.SetTxNum(txNum)

		addr, loc := keys[txNum-1-half][:length.Addr], keys[txNum-1-half][length.Addr:]

		prev, _, err := ct.storage.GetLatest(addr, loc, tx)
		require.NoError(t, err)
		err = domains.WriteAccountStorage(addr, loc, []byte{addr[0], loc[0]}, prev)
		require.NoError(t, err)
	}

	ct.Close()
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
	comp, err := compress.NewCompressor(context.Background(), "cmp", dataPath, tmp, compress.MinPatternScore, 1, log.LvlDebug, logger)
	require.NoError(tb, err)

	collector := etl.NewCollector(BtreeLogPrefix+" genCompress", tb.TempDir(), etl.NewSortableBuffer(datasize.KB*8), logger)

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

	decomp, err := compress.NewDecompressor(dataPath)
	require.NoError(tb, err)

	getter := NewArchiveGetter(decomp.MakeGetter(), compressFlags)
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

func testDbAndAggregatorv3(t *testing.T, aggStep uint64) (kv.RwDB, *AggregatorV3) {
	t.Helper()
	require := require.New(t)
	dirs := datadir.New(t.TempDir())
	logger := log.New()
	db := mdbx.NewMDBX(logger).InMem(dirs.Chaindata).WithTableCfg(func(defaultBuckets kv.TableCfg) kv.TableCfg {
		return kv.ChaindataTablesCfg
	}).MustOpen()
	t.Cleanup(db.Close)

	agg, err := NewAggregatorV3(context.Background(), dirs, aggStep, db, logger)
	require.NoError(err)
	t.Cleanup(agg.Close)
	err = agg.OpenFolder()
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

	mc2 := agg.MakeContext()
	defer mc2.Close()
	domains := agg.SharedDomains(mc2)

	rwTx, err := db.BeginRw(context.Background())
	require.NoError(t, err)
	defer rwTx.Rollback()

	domains.SetTx(rwTx)
	agg.StartWrites()

	//agg.StartUnbufferedWrites()
	defer agg.FinishWrites()
	defer domains.Close()

	keys, vals := generateInputData(t, 20, 16, 10)
	keys = keys[:2]

	var i int
	roots := make([][]byte, 0, 10)
	var pruneFrom uint64 = 5

	mc := agg.MakeContext()
	defer mc.Close()

	for i = 0; i < len(vals); i++ {
		domains.SetTxNum(uint64(i))

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

	ac := agg.MakeContext()
	err = ac.Unwind(context.Background(), pruneFrom, rwTx)
	require.NoError(t, err)
	ac.Close()

	for i = int(pruneFrom); i < len(vals); i++ {
		domains.SetTxNum(uint64(i))

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

	ac.Close()

	ac = agg.MakeContext()
	err = ac.Unwind(context.Background(), pruneFrom, rwTx)
	ac.Close()
	require.NoError(t, err)

	for i = int(pruneFrom); i < len(vals); i++ {
		domains.SetTxNum(uint64(i))

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
