package state

import (
	"context"
	"encoding/binary"
	"fmt"
	"math/rand"
	"os"
	"path/filepath"
	"sync/atomic"
	"testing"

	"github.com/holiman/uint256"
	"github.com/ledgerwatch/log/v3"
	"github.com/stretchr/testify/require"

	"github.com/ledgerwatch/erigon-lib/common/length"
	"github.com/ledgerwatch/erigon-lib/kv"
	"github.com/ledgerwatch/erigon-lib/kv/mdbx"
)

func testDbAndAggregator(t *testing.T, prefixLen int, aggStep uint64) (string, kv.RwDB, *Aggregator) {
	t.Helper()
	path := t.TempDir()
	t.Cleanup(func() { os.RemoveAll(path) })
	logger := log.New()
	db := mdbx.NewMDBX(logger).Path(filepath.Join(path, "db4")).WithTableCfg(func(defaultBuckets kv.TableCfg) kv.TableCfg {
		return kv.ChaindataTablesCfg
	}).MustOpen()
	t.Cleanup(db.Close)
	agg, err := NewAggregator(path, path, aggStep)
	require.NoError(t, err)
	t.Cleanup(agg.Close)
	return path, db, agg
}

func TestAggregator_Merge(t *testing.T) {
	_, db, agg := testDbAndAggregator(t, 0, 100)

	tx, err := db.BeginRw(context.Background())
	require.NoError(t, err)
	defer func() {
		if tx != nil {
			tx.Rollback()
		}
	}()
	agg.SetTx(tx)

	defer agg.StartWrites().FinishWrites()
	txs := uint64(10000)

	// keys are encodings of numbers 1..31
	// each key changes value on every txNum which is multiple of the key
	var maxWrite, otherMaxWrite uint64
	for txNum := uint64(1); txNum <= txs; txNum++ {
		agg.SetTxNum(txNum)
		var v [8]byte
		binary.BigEndian.PutUint64(v[:], txNum)
		var err error
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
	err = agg.Flush()
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

	require.EqualValues(t, otherMaxWrite, binary.BigEndian.Uint64(v[:]))
}

// here we create a bunch of updates for further aggregation.
// FinishTx should merge underlying files several times
// Expected that:
// - we could close first aggregator and open another with previous data still available
// - new aggregator SeekCommitment must return txNum equal to amount of total txns
func TestAggregator_RestartOnDatadir(t *testing.T) {
	aggStep := uint64(50)
	path, db, agg := testDbAndAggregator(t, 0, aggStep)

	tx, err := db.BeginRw(context.Background())
	require.NoError(t, err)
	defer func() {
		if tx != nil {
			tx.Rollback()
		}
		if agg != nil {
			agg.Close()
		}
	}()
	agg.SetTx(tx)
	defer agg.StartWrites().FinishWrites()

	var latestCommitTxNum uint64
	commit := func(txn uint64) error {
		err = agg.Flush()
		require.NoError(t, err)
		err = tx.Commit()
		require.NoError(t, err)
		tx, err = db.BeginRw(context.Background())
		require.NoError(t, err)
		t.Logf("commit to db txn=%d", txn)

		atomic.StoreUint64(&latestCommitTxNum, txn)
		agg.SetTx(tx)
		return nil
	}
	agg.SetCommitFn(commit)

	txs := (aggStep / 2) * 19
	t.Logf("step=%d tx_count=%d", aggStep, txs)
	var aux [8]byte
	// keys are encodings of numbers 1..31
	// each key changes value on every txNum which is multiple of the key
	var maxWrite uint64
	for txNum := uint64(1); txNum <= txs; txNum++ {
		agg.SetTxNum(txNum)
		binary.BigEndian.PutUint64(aux[:], txNum)

		err = agg.UpdateCommitmentData([]byte("key"), aux[:])
		maxWrite = txNum

		require.NoError(t, agg.FinishTx())
	}
	err = agg.Flush()
	require.NoError(t, err)
	err = tx.Commit()
	require.NoError(t, err)
	agg.Close()
	tx, agg = nil, nil

	// Start another aggregator on same datadir
	anotherAgg, err := NewAggregator(path, path, aggStep)
	require.NoError(t, err)
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
	sstartTx, err := anotherAgg.SeekCommitment()
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

	require.EqualValues(t, maxWrite, binary.BigEndian.Uint64(v[:]))
}

func TestAggregator_RestartOnFiles(t *testing.T) {
	aggStep := uint64(1000)

	path, db, agg := testDbAndAggregator(t, 0, aggStep)
	defer db.Close()
	_ = path

	tx, err := db.BeginRw(context.Background())
	require.NoError(t, err)
	defer func() {
		if tx != nil {
			tx.Rollback()
		}
		if agg != nil {
			agg.Close()
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
	agg.SetCommitFn(commit)

	txs := aggStep * 5
	t.Logf("step=%d tx_count=%d", aggStep, txs)

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

	err = tx.Commit()
	tx = nil
	db.Close()
	db = nil
	agg.Close()
	agg = nil

	require.NoError(t, os.RemoveAll(filepath.Join(path, "db4")))

	newDb, err := mdbx.NewMDBX(log.New()).Path(filepath.Join(path, "db4")).WithTableCfg(func(defaultBuckets kv.TableCfg) kv.TableCfg {
		return kv.ChaindataTablesCfg
	}).Open()
	require.NoError(t, err)
	t.Cleanup(newDb.Close)

	newTx, err := newDb.BeginRw(context.Background())
	require.NoError(t, err)
	defer newTx.Rollback()

	newAgg, err := NewAggregator(path, path, aggStep)
	require.NoError(t, err)
	defer newAgg.Close()

	newAgg.SetTx(newTx)

	latestTx, err := newAgg.SeekCommitment()
	require.NoError(t, err)
	t.Logf("seek to latest_tx=%d", latestTx)

	ctx := newAgg.MakeContext()
	miss := uint64(0)
	for i, key := range keys {
		stored, err := ctx.ReadAccountData(key[:length.Addr], newTx)
		require.NoError(t, err)
		if len(stored) == 0 {
			if uint64(i+1) >= txs-aggStep {
				continue // finishtx always stores last agg step in db which we deleted, so miss is expected
			}
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
	require.NoError(t, err)

}

func TestAggregator_ReplaceCommittedKeys(t *testing.T) {
	aggStep := uint64(1000)

	path, db, agg := testDbAndAggregator(t, 0, aggStep)
	defer db.Close()
	_ = path

	tx, err := db.BeginRw(context.Background())
	require.NoError(t, err)
	defer func() {
		if tx != nil {
			tx.Rollback()
		}
		if agg != nil {
			agg.Close()
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
	agg.SetCommitFn(commit)

	txs := aggStep / 2 * 20
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

	ctx := agg.storage.MakeContext()
	for _, key := range keys {
		storedV, err := ctx.Get(key[:length.Addr], key[length.Addr:], tx)
		require.NoError(t, err)
		require.EqualValues(t, key[0], storedV[0])
		require.EqualValues(t, key[length.Addr], storedV[1])
	}
	require.NoError(t, err)

	agg.Close()
	agg = nil
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
