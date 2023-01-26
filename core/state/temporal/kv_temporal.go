package temporal

import (
	"context"
	"encoding/binary"
	"fmt"
	"sort"

	"github.com/ledgerwatch/erigon-lib/common"
	"github.com/ledgerwatch/erigon-lib/common/length"
	"github.com/ledgerwatch/erigon-lib/kv"
	"github.com/ledgerwatch/erigon-lib/kv/iter"
	"github.com/ledgerwatch/erigon-lib/kv/kvcfg"
	"github.com/ledgerwatch/erigon-lib/kv/order"
	"github.com/ledgerwatch/erigon-lib/kv/rawdbv3"
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
// Dual: [from, to)
// Each: [from, INF)
// Prefix: Has(k, prefix)
// Amount: [from, INF) AND maximum N records

type tRestoreCodeHash func(tx kv.Getter, key, v []byte, force *common.Hash) ([]byte, error)
type tConvertV3toV2 func(v []byte) ([]byte, error)
type tParseIncarnation func(v []byte) (uint64, error)

type DB struct {
	kv.RwDB
	agg *state.AggregatorV3

	convertV3toV2        tConvertV3toV2
	restoreCodeHash      tRestoreCodeHash
	parseInc             tParseIncarnation
	systemContractLookup map[common.Address][]common.CodeRecord
}

func New(db kv.RwDB, agg *state.AggregatorV3, cb1 tConvertV3toV2, cb2 tRestoreCodeHash, cb3 tParseIncarnation, systemContractLookup map[common.Address][]common.CodeRecord) (*DB, error) {
	if !kvcfg.HistoryV3.FromDB(db) {
		panic("not supported")
	}
	if systemContractLookup != nil {
		if err := db.View(context.Background(), func(tx kv.Tx) error {
			var err error
			for _, list := range systemContractLookup {
				for i := range list {
					list[i].TxNumber, err = rawdbv3.TxNums.Min(tx, list[i].BlockNumber)
					if err != nil {
						return err
					}
				}
			}
			return nil
		}); err != nil {
			return nil, err
		}
	}

	return &DB{RwDB: db, agg: agg, convertV3toV2: cb1, restoreCodeHash: cb2, parseInc: cb3, systemContractLookup: systemContractLookup}, nil
}
func (db *DB) BeginTemporalRo(ctx context.Context) (kv.TemporalTx, error) {
	kvTx, err := db.RwDB.BeginRo(ctx)
	if err != nil {
		return nil, err
	}
	tx := &Tx{Tx: kvTx, db: db}

	tx.agg = db.agg.MakeContext()
	tx.agg.SetTx(kvTx)
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
	db               *DB
	agg              *state.AggregatorV3Context
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

func (tx *Tx) DomainRangeAscend(name kv.Domain, k1, fromKey []byte, asOfTs uint64, limit int) (pairs iter.KV, err error) {
	switch name {
	case AccountsDomain:
		panic("not implemented yet")
	case StorageDomain:
		toKey, _ := kv.NextSubtree(k1)
		fromKey2 := append(common.Copy(k1), fromKey...)
		it := tx.agg.StorageHistoricalStateRange(asOfTs, fromKey2, toKey, limit, tx)

		accData, err := tx.GetOne(kv.PlainState, k1)
		if err != nil {
			return nil, err
		}
		inc, err := tx.db.parseInc(accData)
		if err != nil {
			return nil, err
		}
		startkey := make([]byte, length.Addr+length.Incarnation+length.Hash)
		copy(startkey, k1)
		binary.BigEndian.PutUint64(startkey[length.Addr:], inc)
		copy(startkey[length.Addr+length.Incarnation:], fromKey)

		toPrefix := make([]byte, length.Addr+length.Incarnation)
		copy(toPrefix, k1)
		binary.BigEndian.PutUint64(toPrefix[length.Addr:], inc+1)

		it2, err := tx.RangeAscend(kv.PlainState, startkey, toPrefix, limit)
		if err != nil {
			return nil, err
		}
		it3 := iter.TransformKV(it2, func(k, v []byte) ([]byte, []byte) {
			return append(append([]byte{}, k[:20]...), k[28:]...), v
		})
		//TODO: seems MergePairs can't handle "amount" request
		return iter.UnionPairs(it, it3), nil
	case CodeDomain:
		panic("not implemented yet")
	default:
		panic(fmt.Sprintf("unexpected: %s", name))
	}
}
func (tx *Tx) DomainGet(name kv.Domain, key, key2 []byte, ts uint64) (v []byte, ok bool, err error) {
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
		v, ok, err = tx.HistoryGet(StorageHistory, append(key[:20], key2...), ts)
		if err != nil {
			return nil, false, err
		}
		if ok {
			return v, true, nil
		}
		v, err = tx.GetOne(kv.PlainState, append(key, key2...))
		return v, v != nil, err
	case CodeDomain:
		v, ok, err = tx.HistoryGet(CodeHistory, key, ts)
		if err != nil {
			return nil, false, err
		}
		if ok {
			return v, true, nil
		}
		v, err = tx.GetOne(kv.Code, key2)
		return v, v != nil, err
	default:
		panic(fmt.Sprintf("unexpected: %s", name))
	}
}

func (tx *Tx) HistoryGet(name kv.History, key []byte, ts uint64) (v []byte, ok bool, err error) {
	switch name {
	case AccountsHistory:
		v, ok, err = tx.agg.ReadAccountDataNoStateWithRecent(key, ts)
		if err != nil {
			return nil, false, err
		}
		if !ok || len(v) == 0 {
			return v, ok, nil
		}
		v, err = tx.db.convertV3toV2(v)
		if err != nil {
			return nil, false, err
		}
		var force *common.Hash
		if tx.db.systemContractLookup != nil {
			if records, ok := tx.db.systemContractLookup[common.BytesToAddress(key)]; ok {
				p := sort.Search(len(records), func(i int) bool {
					return records[i].TxNumber > ts
				})
				hash := records[p-1].CodeHash
				force = &hash
			}
		}
		v, err = tx.db.restoreCodeHash(tx.Tx, key, v, force)
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
}

type Cursor struct {
	kv  kv.Cursor
	agg *state.AggregatorV3Context

	//HistoryV2 fields
	accHistoryC, storageHistoryC kv.Cursor
	accChangesC, storageChangesC kv.CursorDupSort

	//HistoryV3 fields
	hitoryV3 bool
}

func (tx *Tx) IndexRange(name kv.InvertedIdx, key []byte, fromTs, toTs uint64, asc order.By, limit int) (timestamps iter.U64, err error) {
	return tx.IndexStream(name, key, fromTs, toTs, asc, limit)
}

// [fromTs, toTs)
func (tx *Tx) IndexStream(name kv.InvertedIdx, key []byte, fromTs, toTs uint64, asc order.By, limit int) (timestamps iter.U64, err error) {
	switch name {
	case LogTopicIdx:
		t, err := tx.agg.LogTopicIterator(key, fromTs, toTs, asc, limit, tx)
		if err != nil {
			return nil, err
		}
		tx.resourcesToClose = append(tx.resourcesToClose, t)
		return t, nil
	case LogAddrIdx:
		t, err := tx.agg.LogAddrIterator(key, fromTs, toTs, asc, limit, tx)
		if err != nil {
			return nil, err
		}
		tx.resourcesToClose = append(tx.resourcesToClose, t)
		return t, nil
	case TracesFromIdx:
		t, err := tx.agg.TraceFromIterator(key, fromTs, toTs, asc, limit, tx)
		if err != nil {
			return nil, err
		}
		tx.resourcesToClose = append(tx.resourcesToClose, t)
		return t, nil
	case TracesToIdx:
		t, err := tx.agg.TraceToIterator(key, fromTs, toTs, asc, limit, tx)
		if err != nil {
			return nil, err
		}
		tx.resourcesToClose = append(tx.resourcesToClose, t)
		return t, nil
	default:
		return nil, fmt.Errorf("unexpected history name: %s", name)
	}
}
