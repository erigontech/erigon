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

type tRestoreCodeHash func(tx kv.Getter, key, v []byte) ([]byte, error)
type tConvertV3toV2 func(v []byte) ([]byte, error)

type DB struct {
	kv.RwDB
	agg      *state.AggregatorV3
	hitoryV3 bool

	convertV3toV2   tConvertV3toV2
	restoreCodeHash tRestoreCodeHash
}

func New(kv kv.RwDB, agg *state.AggregatorV3, cb1 tConvertV3toV2, cb2 tRestoreCodeHash) *DB {
	return &DB{RwDB: kv, agg: agg, hitoryV3: kvcfg.HistoryV3.FromDB(kv), convertV3toV2: cb1, restoreCodeHash: cb2}
}
func (db *DB) BeginTemporalRo(ctx context.Context) (kv.TemporalTx, error) {
	kvTx, err := db.RwDB.BeginRo(ctx)
	if err != nil {
		return nil, err
	}
	tx := &Tx{Tx: kvTx, hitoryV3: db.hitoryV3, db: db}
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
	db  *DB
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
	AccountsDomain kv.Domain = "AccountsDomain"
	StorageDomain  kv.Domain = "StorageDomain"
	CodeDomain     kv.Domain = "CodeDomain"
)

const (
	AccountsHistory kv.History = "AccountsHistory"
	StorageHistory  kv.History = "StorageHistory"
	CodeHistory     kv.History = "CodeHistory"
)

const (
	LogTopicIdx   kv.InvertedIdx = "LogTopicIdx"
	LogAddrIdx    kv.InvertedIdx = "LogAddrIdx"
	TracesFromIdx kv.InvertedIdx = "TracesFromIdx"
	TracesToIdx   kv.InvertedIdx = "TracesToIdx"
)

func (tx *Tx) DomainGet(name kv.Domain, key []byte, ts uint64) (v []byte, ok bool, err error) {
	switch name {
	case AccountsDomain:
		v, ok, err = tx.HistoryGet(AccountsHistory, key, ts)
		if err != nil {
			return nil, false, err
		}
		if ok {
			return v, true, nil
		}
		v, err = tx.GetOne(kv.PlainState, key)
		return v, v != nil, err
	case StorageDomain:
		v, ok, err = tx.HistoryGet(StorageHistory, key, ts)
		if err != nil {
			return nil, false, err
		}
		if ok {
			return v, true, nil
		}
		v, err = tx.GetOne(kv.PlainState, key)
		return v, v != nil, err
	case CodeDomain:
		if tx.hitoryV3 {
			v, ok, err = tx.HistoryGet(CodeHistory, key, ts)
			if err != nil {
				return nil, false, err
			}
			if ok {
				return v, true, nil
			}
			v, err = tx.GetOne(kv.Code, key)
			return v, v != nil, err
		}
		v, err = tx.GetOne(kv.Code, key)
		return v, v != nil, err
	default:
		panic(fmt.Sprintf("unexpected: %s", name))
	}
}

func (tx *Tx) HistoryGet(name kv.History, key []byte, ts uint64) (v []byte, ok bool, err error) {
	if tx.hitoryV3 {
		switch name {
		case AccountsHistory:
			v, ok, err = tx.agg.ReadAccountDataNoStateWithRecent(key, ts)
			if err != nil {
				return nil, false, err
			}
			if !ok {
				return nil, ok, nil
			}
			v, err = tx.db.convertV3toV2(v)
			if err != nil {
				return nil, false, err
			}
			v, err = tx.db.restoreCodeHash(tx.Tx, key, v)
			if err != nil {
				return nil, false, err
			}
			return v, true, nil
		case StorageHistory:
			return tx.agg.ReadAccountStorageNoStateWithRecent2(key, ts)
		case CodeHistory:
			return tx.agg.ReadAccountCodeNoStateWithRecent(key, ts)
		default:
			panic(fmt.Sprintf("unexpected: %s", name))
		}
	} else {
		switch name {
		case AccountsHistory:
			v, ok, err = historyv2.FindByHistory(tx.accHistoryC, tx.accChangesC, false, key, ts)
			if err != nil {
				return nil, false, err
			}
			if !ok {
				return nil, ok, nil
			}
			v, err = tx.db.restoreCodeHash(tx.Tx, key, v)
			if err != nil {
				return nil, false, err
			}
			return v, true, nil
		case StorageHistory:
			return historyv2.FindByHistory(tx.storageHistoryC, tx.storageChangesC, true, key, ts)
		case CodeHistory:
			return nil, false, fmt.Errorf("ErigonV2 doesn't support CodeHistory")
		default:
			return nil, false, fmt.Errorf("unexpected history name: %s", name)
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
		case LogTopicIdx:
			t := tx.agg.LogTopicIterator(key, fromTs, toTs, tx)
			tx.resourcesToClose = append(tx.resourcesToClose, t)
			return t, nil
		case LogAddrIdx:
			t := tx.agg.LogAddrIterator(key, fromTs, toTs, tx)
			tx.resourcesToClose = append(tx.resourcesToClose, t)
			return t, nil
		case TracesFromIdx:
			t := tx.agg.TraceFromIterator(key, fromTs, toTs, tx)
			tx.resourcesToClose = append(tx.resourcesToClose, t)
			return t, nil
		case TracesToIdx:
			t := tx.agg.TraceToIterator(key, fromTs, toTs, tx)
			tx.resourcesToClose = append(tx.resourcesToClose, t)
			return t, nil
		default:
			return nil, fmt.Errorf("unexpected history name: %s", name)
		}
	} else {
		var table string
		switch name {
		case LogTopicIdx:
			table = kv.LogTopicIndex
		case LogAddrIdx:
			table = kv.LogAddressIdx
		case TracesFromIdx:
			table = kv.TracesFromIdx
		case TracesToIdx:
			table = kv.TracesToIdx
		default:
			return nil, fmt.Errorf("unexpected history name: %s", name)
		}
		bm, err := bitmapdb.Get64(tx, table, key, fromTs, toTs)
		if err != nil {
			return nil, err
		}
		return kv.StreamArray(bm.ToArray()), nil
	}
}
