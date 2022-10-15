package state

import (
	"context"
	"encoding/binary"
	"math/rand"
	"sync/atomic"
	"testing"

	"github.com/ledgerwatch/log/v3"
	"github.com/stretchr/testify/require"

	"github.com/ledgerwatch/erigon-lib/kv"
	"github.com/ledgerwatch/erigon-lib/kv/mdbx"
)

func testDbAndAggregator(t *testing.T, prefixLen int, aggStep uint64) (string, kv.RwDB, *Aggregator) {
	t.Helper()
	path := t.TempDir()
	logger := log.New()
	db := mdbx.NewMDBX(logger).Path(path).WithTableCfg(func(defaultBuckets kv.TableCfg) kv.TableCfg {
		return kv.ChaindataTablesCfg
	}).MustOpen()
	agg, err := NewAggregator(path, aggStep)
	require.NoError(t, err)
	return path, db, agg
}

func TestAggregator_Merge(t *testing.T) {
	_, db, agg := testDbAndAggregator(t, 0, 100)
	defer db.Close()
	defer agg.Close()

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
		if txNum%100 == 0 {
			err = tx.Commit()
			require.NoError(t, err)
			tx, err = db.BeginRw(context.Background())
			require.NoError(t, err)
			agg.SetTx(tx)
		}
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
func TestAggregator_RestartOnFiles(t *testing.T) {
	aggStep := uint64(50)
	path, db, agg := testDbAndAggregator(t, 0, aggStep)
	defer db.Close()

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

	txs := (aggStep / 2) * 11
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

	anotherAgg, err := NewAggregator(path, aggStep)
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
	require.EqualValues(t, latestCommitTxNum, sstartTx)
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
