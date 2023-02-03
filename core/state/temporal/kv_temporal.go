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
// Range: [from, to)
// Each: [from, INF)
// Prefix: Has(k, prefix)
// Limit: [from, INF) AND maximum N records

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
	if tx.agg != nil {
		tx.agg.Close()
	}
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
	AccountsHistoryIdx kv.InvertedIdx = "AccountsHistoryIdx"
	StorageHistoryIdx  kv.InvertedIdx = "StorageHistoryIdx"
	CodeHistoryIdx     kv.InvertedIdx = "CodeHistoryIdx"

	LogTopicIdx   kv.InvertedIdx = "LogTopicIdx"
	LogAddrIdx    kv.InvertedIdx = "LogAddrIdx"
	TracesFromIdx kv.InvertedIdx = "TracesFromIdx"
	TracesToIdx   kv.InvertedIdx = "TracesToIdx"
)

func (tx *Tx) DomainRange(name kv.Domain, k1, k2 []byte, asOfTs uint64, asc order.By, limit int) (it iter.KV, err error) {
	if asc == order.Desc {
		panic("not supported yet")
	}
	switch name {
	case AccountsDomain:
		histStateIt := tx.agg.AccountHistoricalStateRange(asOfTs, k1, nil, -1, tx)
		// TODO: somehow avoid common.Copy(k) - WalkAsOfIter is not zero-copy
		// Is histStateIt possible to increase keys lifetime to: 2 .Next() calls??
		histStateIt2 := iter.TransformKV(histStateIt, func(k, v []byte) ([]byte, []byte, error) {
			if len(v) == 0 {
				return k[:20], v, nil
			}
			v, err = tx.db.convertV3toV2(v)
			if err != nil {
				return nil, nil, err
			}
			var force *common.Hash
			if tx.db.systemContractLookup != nil {
				if records, ok := tx.db.systemContractLookup[common.BytesToAddress(k)]; ok {
					p := sort.Search(len(records), func(i int) bool {
						return records[i].TxNumber > asOfTs
					})
					hash := records[p-1].CodeHash
					force = &hash
				}
			}
			v, err = tx.db.restoreCodeHash(tx.Tx, k, v, force)
			if err != nil {
				return nil, nil, err
			}
			return k[:20], v, nil
		})
		lastestStateIt, err := tx.RangeAscend(kv.PlainState, k1, nil, -1)
		if err != nil {
			return nil, err
		}
		// TODO: instead of iterate over whole storage, need implement iterator which does cursor.Seek(nextAccount)
		latestStateIt2 := iter.FilterKV(lastestStateIt, func(k, v []byte) bool {
			return len(k) == 20
		})
		//TODO: seems UnionKV can't handle "amount" request
		return iter.UnionKV(histStateIt2, latestStateIt2), nil
	case StorageDomain:
		toKey, _ := kv.NextSubtree(k1)
		fromKey2 := append(common.Copy(k1), k2...)
		it := tx.agg.StorageHistoricalStateRange(asOfTs, fromKey2, toKey, limit, tx)
		it11 := iter.TransformKV(it, func(k, v []byte) ([]byte, []byte, error) {
			return k, v, nil
		})

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
		copy(startkey[length.Addr+length.Incarnation:], k2)

		toPrefix := make([]byte, length.Addr+length.Incarnation)
		copy(toPrefix, k1)
		binary.BigEndian.PutUint64(toPrefix[length.Addr:], inc+1)

		it2, err := tx.RangeAscend(kv.PlainState, startkey, toPrefix, limit)
		if err != nil {
			return nil, err
		}
		it3 := iter.TransformKV(it2, func(k, v []byte) ([]byte, []byte, error) {
			return append(append([]byte{}, k[:20]...), k[28:]...), v, nil
		})
		//TODO: seems MergePairs can't handle "amount" request
		return iter.UnionKV(it11, it3), nil
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
		v, ok, err = tx.agg.ReadAccountDataNoStateWithRecent(key, ts, tx.Tx)
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
		return tx.agg.ReadAccountStorageNoStateWithRecent2(key, ts, tx.Tx)
	case CodeHistory:
		return tx.agg.ReadAccountCodeNoStateWithRecent(key, ts, tx.Tx)
	default:
		panic(fmt.Sprintf("unexpected: %s", name))
	}
}

func (tx *Tx) IndexRange(name kv.InvertedIdx, k []byte, fromTs, toTs int, asc order.By, limit int) (timestamps iter.U64, err error) {
	switch name {
	case AccountsHistoryIdx:
		timestamps, err = tx.agg.AccountHistoyIdxIterator(k, fromTs, toTs, asc, limit, tx)
	case StorageHistoryIdx:
		timestamps, err = tx.agg.StorageHistoyIdxIterator(k, fromTs, toTs, asc, limit, tx)
	case CodeHistoryIdx:
		timestamps, err = tx.agg.CodeHistoyIdxIterator(k, fromTs, toTs, asc, limit, tx)
	case LogTopicIdx:
		timestamps, err = tx.agg.LogTopicIterator(k, fromTs, toTs, asc, limit, tx)
	case LogAddrIdx:
		timestamps, err = tx.agg.LogAddrIterator(k, fromTs, toTs, asc, limit, tx)
	case TracesFromIdx:
		timestamps, err = tx.agg.TraceFromIterator(k, fromTs, toTs, asc, limit, tx)
	case TracesToIdx:
		timestamps, err = tx.agg.TraceToIterator(k, fromTs, toTs, asc, limit, tx)
	default:
		return nil, fmt.Errorf("unexpected history name: %s", name)
	}
	if err != nil {
		return nil, err
	}
	if closer, ok := timestamps.(kv.Closer); ok {
		tx.resourcesToClose = append(tx.resourcesToClose, closer)
	}
	return timestamps, nil
}

func (tx *Tx) HistoryRange(name kv.History, fromTs, toTs int, asc order.By, limit int) (it iter.KV, err error) {
	if asc == order.Desc {
		panic("not implemented yet")
	}
	if limit >= 0 {
		panic("not implemented yet")
	}
	switch name {
	case AccountsHistory:
		it = tx.agg.AccountHistoryIterateChanged(fromTs, toTs, asc, limit, tx)
		return it, nil
	case StorageHistory:
		it = tx.agg.StorageHistoryIterateChanged(fromTs, toTs, asc, limit, tx)
		return it, nil
	case CodeHistory:
		it = tx.agg.CodeHistoryIterateChanged(fromTs, toTs, asc, limit, tx)
		return it, nil
	default:
		return nil, fmt.Errorf("unexpected history name: %s", name)
	}
}
