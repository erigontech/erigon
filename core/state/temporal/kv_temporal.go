package temporal

import (
	"context"
	"fmt"

	"github.com/RoaringBitmap/roaring/roaring64"
	"github.com/ledgerwatch/erigon-lib/kv"
	"github.com/ledgerwatch/erigon-lib/kv/bitmapdb"
	"github.com/ledgerwatch/erigon-lib/kv/kvcfg"
	"github.com/ledgerwatch/erigon-lib/kv/temporal/historyv2"
	"github.com/ledgerwatch/erigon-lib/state"
)

//Naming:
//  ts - TimeStamp
//  tx - Database Transaction
//  txn - Ethereum Transaction (and TxNum - is also number of Etherum Transaction)
//  RoTx - Read-Only Database Transaction
//  RwTx - Read-Write Database Transaction
//  k - key
//  v - value

type DB struct {
	kv       kv.RoDB
	agg      *state.Aggregator22
	hitoryV3 bool
}

func New(kv kv.RoDB, agg *state.Aggregator22) *DB {
	return &DB{kv: kv, agg: agg, hitoryV3: kvcfg.HistoryV3.FromDB(kv)}
}
func (db *DB) BeginTemporalRo(ctx context.Context) (*Tx, error) {
	kvTx, err := db.kv.BeginRo(ctx)
	if err != nil {
		return nil, err
	}
	tx := &Tx{Tx: kvTx, agg: db.agg.MakeContext()}
	if db.hitoryV3 {

	} else {
		tx.accHistoryC, _ = tx.Cursor(kv.AccountsHistory)
		tx.storageHistoryC, _ = tx.Cursor(kv.StorageHistory)
		tx.accChangesC, _ = tx.CursorDupSort(kv.AccountChangeSet)
		tx.storageChangesC, _ = tx.CursorDupSort(kv.StorageChangeSet)
	}
	return tx, nil
}

type Tx struct {
	kv.Tx
	agg *state.Aggregator22Context

	//HistoryV2 fields
	accHistoryC, storageHistoryC kv.Cursor
	accChangesC, storageChangesC kv.CursorDupSort

	//HistoryV3 fields
	hitoryV3 bool
}

type History string

const (
	Accounts History = "accounts"
	Storage  History = "storage"
	Code     History = "code"
)

type InvertedIdx string

const (
	LogTopic   InvertedIdx = "LogTopic"
	LogAddr    InvertedIdx = "LogAddr"
	TracesFrom InvertedIdx = "TracesFrom"
	TracesTo   InvertedIdx = "TracesTo"
)

func (tx *Tx) GetNoState(name History, key []byte, ts uint64) (v []byte, ok bool, err error) {
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

type It interface {
	Next() uint64
	HasNext() bool
	Close()
}

// [fromTs, toTs)
func (tx *Tx) InvertedIndexRange(name InvertedIdx, key []byte, fromTs, toTs uint64) (bitmap *roaring64.Bitmap, err error) {
	if tx.hitoryV3 {
		switch name {
		case LogTopic:
			t := tx.agg.LogTopicIterator(key, fromTs, toTs, tx)
			return t.ToBitamp(), nil
		case LogAddr:
			t := tx.agg.LogAddrIterator(key, fromTs, toTs, tx)
			return t.ToBitamp(), nil
		case TracesFrom:
			t := tx.agg.TraceFromIterator(key, fromTs, toTs, tx)
			return t.ToBitamp(), nil
		case TracesTo:
			t := tx.agg.TraceToIterator(key, fromTs, toTs, tx)
			return t.ToBitamp(), nil
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
		return bitmapdb.Get64(tx, table, key, fromTs, toTs)
	}
}
