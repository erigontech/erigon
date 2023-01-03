package temporal

import (
	"context"
	"fmt"

	"github.com/ledgerwatch/erigon-lib/kv"
	"github.com/ledgerwatch/erigon-lib/kv/bitmapdb"
	"github.com/ledgerwatch/erigon-lib/kv/kvcfg"
	"github.com/ledgerwatch/erigon-lib/kv/temporal/historyv2"
	"github.com/ledgerwatch/erigon-lib/state"
)

//Variables Naming:
//  ts - TimeStamp
//  tx - Database Transaction
//  txn - Ethereum Transaction (and TxNum - is also number of Etherum Transaction)
//  RoTx - Read-Only Database Transaction
//  RwTx - Read-Write Database Transaction
//  k - key
//  v - value

//Methods Naming:
// Get: exact match of criterias
// Range: [from, to)
// Each: [from, INF)
// Prefix: Has(k, prefix)
// Amount: [from, INF) AND maximum N records

type DB struct {
	kv.RwDB
	agg      *state.AggregatorV3
	hitoryV3 bool
}

func New(kv kv.RwDB, agg *state.AggregatorV3) *DB {
	return &DB{RwDB: kv, agg: agg, hitoryV3: kvcfg.HistoryV3.FromDB(kv)}
}
func (db *DB) BeginTemporalRo(ctx context.Context) (kv.TemporalTx, error) {
	kvTx, err := db.RwDB.BeginRo(ctx)
	if err != nil {
		return nil, err
	}
	tx := &Tx{Tx: kvTx, hitoryV3: db.hitoryV3}
	if db.hitoryV3 {
		tx.agg = db.agg.MakeContext()
		tx.agg.SetTx(kvTx)
	} else {
		tx.accHistoryC, _ = tx.Cursor(kv.AccountsHistory)
		tx.storageHistoryC, _ = tx.Cursor(kv.StorageHistory)
		tx.accChangesC, _ = tx.CursorDupSort(kv.AccountChangeSet)
		tx.storageChangesC, _ = tx.CursorDupSort(kv.StorageChangeSet)
	}
	return tx, nil
}
func (db *DB) ViewTemporal(ctx context.Context, f func(tx kv.TemporalTx) error) error {
	tx, err := db.BeginTemporalRo(ctx)
	if err != nil {
		return err
	}
	defer tx.Rollback()
	return f(tx)
}

// TODO: it's temporary method, allowing inject TemproalTx without changing code. But it's not type-safe.
func (db *DB) BeginRo(ctx context.Context) (kv.Tx, error) {
	return db.BeginTemporalRo(ctx)
}
func (db *DB) View(ctx context.Context, f func(tx kv.Tx) error) error {
	tx, err := db.BeginTemporalRo(ctx)
	if err != nil {
		return err
	}
	defer tx.Rollback()
	return f(tx)
}

type Tx struct {
	kv.Tx
	agg *state.Aggregator22Context

	//HistoryV2 fields
	accHistoryC, storageHistoryC kv.Cursor
	accChangesC, storageChangesC kv.CursorDupSort

	//HistoryV3 fields
	hitoryV3         bool
	resourcesToClose []kv.Closer
}

func (tx *Tx) Rollback() {
	for _, closer := range tx.resourcesToClose {
		closer.Close()
	}
	tx.agg.Close()
	tx.Tx.Rollback()
}

func (tx *Tx) Commit() error {
	for _, closer := range tx.resourcesToClose {
		closer.Close()
	}
	return tx.Tx.Commit()
}

const (
	Accounts kv.History = "accounts"
	Storage  kv.History = "storage"
	Code     kv.History = "code"
)

const (
	LogTopic   kv.InvertedIdx = "LogTopic"
	LogAddr    kv.InvertedIdx = "LogAddr"
	TracesFrom kv.InvertedIdx = "TracesFrom"
	TracesTo   kv.InvertedIdx = "TracesTo"
)

func (tx *Tx) HistoryGet(name kv.History, key []byte, ts uint64) (v []byte, ok bool, err error) {
	if tx.hitoryV3 {
		switch name {
		case Accounts:
			return tx.agg.ReadAccountDataNoStateWithRecent(key, ts)
		case Storage:
			return tx.agg.ReadAccountStorageNoStateWithRecent2(key, ts)
		case Code:
			return tx.agg.ReadAccountCodeNoStateWithRecent(key, ts)
		default:
			panic(fmt.Sprintf("unexpected: %s", name))
		}
	} else {
		switch name {
		case Accounts:
			return historyv2.FindByHistory(tx.accHistoryC, tx.accChangesC, false, key, ts)
		case Storage:
			return historyv2.FindByHistory(tx.storageHistoryC, tx.storageChangesC, true, key, ts)
		case Code:
			panic("not implemented")
		default:
			panic(fmt.Sprintf("unexpected: %s", name))
		}
	}
}

type Cursor struct {
	kv  kv.Cursor
	agg *state.Aggregator22Context

	//HistoryV2 fields
	accHistoryC, storageHistoryC kv.Cursor
	accChangesC, storageChangesC kv.CursorDupSort

	//HistoryV3 fields
	hitoryV3 bool
}

// [fromTs, toTs)
func (tx *Tx) IndexRange(name kv.InvertedIdx, key []byte, fromTs, toTs uint64) (timestamps kv.UnaryStream[uint64], err error) {
	if tx.hitoryV3 {
		switch name {
		case LogTopic:
			t := tx.agg.LogTopicIterator(key, fromTs, toTs, tx)
			tx.resourcesToClose = append(tx.resourcesToClose, t)
			return t, nil
		case LogAddr:
			t := tx.agg.LogAddrIterator(key, fromTs, toTs, tx)
			tx.resourcesToClose = append(tx.resourcesToClose, t)
			return t, nil
		case TracesFrom:
			t := tx.agg.TraceFromIterator(key, fromTs, toTs, tx)
			tx.resourcesToClose = append(tx.resourcesToClose, t)
			return t, nil
		case TracesTo:
			t := tx.agg.TraceToIterator(key, fromTs, toTs, tx)
			tx.resourcesToClose = append(tx.resourcesToClose, t)
			return t, nil
		default:
			panic(fmt.Sprintf("unexpected: %s", name))
		}
	} else {
		var table string
		switch name {
		case LogTopic:
			table = kv.LogTopicIndex
		case LogAddr:
			table = kv.LogAddressIdx
		case TracesFrom:
			table = kv.TracesFromIdx
		case TracesTo:
			table = kv.TracesToIdx
		default:
			panic(fmt.Sprintf("unexpected: %s", name))
		}
		bm, err := bitmapdb.Get64(tx, table, key, fromTs, toTs)
		if err != nil {
			return nil, err
		}
		return kv.StreamArray(bm.ToArray()), nil
	}
}
