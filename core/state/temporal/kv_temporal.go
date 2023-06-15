package temporal

import (
	"context"
	"encoding/binary"
	"fmt"
	"testing"

	"github.com/ledgerwatch/erigon-lib/common"
	"github.com/ledgerwatch/erigon-lib/common/datadir"
	"github.com/ledgerwatch/erigon-lib/common/dir"
	"github.com/ledgerwatch/erigon-lib/common/length"
	"github.com/ledgerwatch/erigon-lib/kv"
	"github.com/ledgerwatch/erigon-lib/kv/iter"
	"github.com/ledgerwatch/erigon-lib/kv/kvcfg"
	"github.com/ledgerwatch/erigon-lib/kv/mdbx"
	"github.com/ledgerwatch/erigon-lib/kv/memdb"
	"github.com/ledgerwatch/erigon-lib/kv/order"
	"github.com/ledgerwatch/erigon-lib/kv/rawdbv3"
	"github.com/ledgerwatch/erigon-lib/state"
	"github.com/ledgerwatch/erigon/core/state/historyv2read"
	"github.com/ledgerwatch/erigon/core/systemcontracts"
	"github.com/ledgerwatch/erigon/core/types"
	"github.com/ledgerwatch/erigon/core/types/accounts"
	"github.com/ledgerwatch/erigon/eth/ethconfig"
	"github.com/ledgerwatch/log/v3"
)

//Variables Naming:
//  tx - Database Transaction
//  txn - Ethereum Transaction (and TxNum - is also number of Etherum Transaction)
//  RoTx - Read-Only Database Transaction. RwTx - read-write
//  k, v - key, value
//  ts - TimeStamp. Usually it's Etherum's TransactionNumber (auto-increment ID). Or BlockNumber.
//  Cursor - low-level mdbx-tide api to navigate over Table
//  Iter - high-level iterator-like api over Table/InvertedIndex/History/Domain. Has less features than Cursor. See package `iter`

//Methods Naming:
//  Get: exact match of criterias
//  Range: [from, to). from=nil means StartOfTable, to=nil means EndOfTable, rangeLimit=-1 means Unlimited
//  Prefix: `Range(Table, prefix, kv.NextSubtree(prefix))`

//Abstraction Layers:
// LowLevel:
//      1. DB/Tx - low-level key-value database
//      2. Snapshots/Freeze - immutable files with historical data. May be downloaded at first App
//              start or auto-generate by moving old data from DB to Snapshots.
// MediumLevel:
//      1. TemporalDB - abstracting DB+Snapshots. Target is:
//              - provide 'time-travel' API for data: consistan snapshot of data as of given Timestamp.
//              - to keep DB small - only for Hot/Recent data (can be update/delete by re-org).
//              - using next entities:
//                      - InvertedIndex: supports range-scans
//                      - History: can return value of key K as of given TimeStamp. Doesn't know about latest/current
//                          value of key K. Returns NIL if K not changed after TimeStamp.
//                      - Domain: as History but also aware about latest/current value of key K. Can move
//                          cold (updated long time ago) parts of state from db to snapshots.

// HighLevel:
//      1. Application - rely on TemporalDB (Ex: ExecutionLayer) or just DB (Ex: TxPool, Sentry, Downloader).

type tRestoreCodeHash func(tx kv.Getter, key, v []byte, force *common.Hash) ([]byte, error)
type tConvertAccount func(v []byte) ([]byte, error)
type tParseIncarnation func(v []byte) (uint64, error)

type DB struct {
	kv.RwDB
	agg *state.AggregatorV3

	convertV3toV2        tConvertAccount
	convertV2toV3        tConvertAccount
	restoreCodeHash      tRestoreCodeHash
	parseInc             tParseIncarnation
	systemContractLookup map[common.Address][]common.CodeRecord
}

func New(db kv.RwDB, agg *state.AggregatorV3, systemContractLookup map[common.Address][]common.CodeRecord) (*DB, error) {
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

	return &DB{RwDB: db, agg: agg,
		convertV3toV2: accounts.ConvertV3toV2, convertV2toV3: accounts.ConvertV2toV3,
		restoreCodeHash: historyv2read.RestoreCodeHash, parseInc: accounts.DecodeIncarnationFromStorage,
		systemContractLookup: systemContractLookup,
	}, nil
}
func (db *DB) Agg() *state.AggregatorV3 { return db.agg }
func (db *DB) InternalDB() kv.RwDB      { return db.RwDB }

func (db *DB) BeginTemporalRo(ctx context.Context) (kv.TemporalTx, error) {
	kvTx, err := db.RwDB.BeginRo(ctx)
	if err != nil {
		return nil, err
	}
	tx := &Tx{MdbxTx: kvTx.(*mdbx.MdbxTx), db: db}

	tx.aggCtx = db.agg.MakeContext()
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

func (db *DB) BeginTemporalRw(ctx context.Context) (kv.RwTx, error) {
	kvTx, err := db.RwDB.BeginRw(ctx) //nolint:gocritic
	if err != nil {
		return nil, err
	}
	tx := &Tx{MdbxTx: kvTx.(*mdbx.MdbxTx), db: db}

	tx.aggCtx = db.agg.MakeContext()
	return tx, nil
}
func (db *DB) BeginRw(ctx context.Context) (kv.RwTx, error) {
	return db.BeginTemporalRw(ctx)
}
func (db *DB) Update(ctx context.Context, f func(tx kv.RwTx) error) error {
	tx, err := db.BeginTemporalRw(ctx)
	if err != nil {
		return err
	}
	defer tx.Rollback()
	if err = f(tx); err != nil {
		return err
	}
	return tx.Commit()
}

func (db *DB) BeginTemporalRwNosync(ctx context.Context) (kv.RwTx, error) {
	kvTx, err := db.RwDB.BeginRwNosync(ctx) //nolint:gocritic
	if err != nil {
		return nil, err
	}
	tx := &Tx{MdbxTx: kvTx.(*mdbx.MdbxTx), db: db}

	tx.aggCtx = db.agg.MakeContext()
	return tx, nil
}
func (db *DB) BeginRwNosync(ctx context.Context) (kv.RwTx, error) {
	return db.BeginTemporalRwNosync(ctx) //nolint:gocritic
}
func (db *DB) UpdateNosync(ctx context.Context, f func(tx kv.RwTx) error) error {
	tx, err := db.BeginTemporalRwNosync(ctx)
	if err != nil {
		return err
	}
	defer tx.Rollback()
	if err = f(tx); err != nil {
		return err
	}
	return tx.Commit()
}

type Tx struct {
	*mdbx.MdbxTx
	db               *DB
	aggCtx           *state.AggregatorV3Context
	resourcesToClose []kv.Closer
}

func (tx *Tx) AggCtx() *state.AggregatorV3Context { return tx.aggCtx }
func (tx *Tx) Agg() *state.AggregatorV3           { return tx.db.agg }
func (tx *Tx) Rollback() {
	tx.autoClose()
	tx.MdbxTx.Rollback()
}
func (tx *Tx) autoClose() {
	for _, closer := range tx.resourcesToClose {
		closer.Close()
	}
	if tx.aggCtx != nil {
		tx.aggCtx.Close()
	}
}
func (tx *Tx) Commit() error {
	tx.autoClose()
	return tx.MdbxTx.Commit()
}

func (tx *Tx) DomainRange(name kv.Domain, fromKey, toKey []byte, asOfTs uint64, asc order.By, limit int) (it iter.KV, err error) {
	if asc == order.Desc {
		panic("not supported yet")
	}
	switch name {
	case kv.AccountsDomain:
		histStateIt := tx.aggCtx.AccountHistoricalStateRange(asOfTs, fromKey, toKey, limit, tx)
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
			/*
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
				v, err = tx.db.restoreCodeHash(tx.MdbxTx, k, v, force)
				if err != nil {
					return nil, nil, err
				}
			*/
			return k[:20], common.Copy(v), nil
		})
		lastestStateIt, err := tx.RangeAscend(kv.PlainState, fromKey, toKey, -1) // don't apply limit, because need filter
		if err != nil {
			return nil, err
		}
		// TODO: instead of iterate over whole storage, need implement iterator which does cursor.Seek(nextAccount)
		latestStateIt2 := iter.FilterKV(lastestStateIt, func(k, v []byte) bool {
			return len(k) == 20
		})
		it = iter.UnionKV(histStateIt2, latestStateIt2, limit)
	case kv.StorageDomain:
		storageIt := tx.aggCtx.StorageHistoricalStateRange(asOfTs, fromKey, toKey, limit, tx)
		storageIt1 := iter.TransformKV(storageIt, func(k, v []byte) ([]byte, []byte, error) {
			return k, v, nil
		})

		accData, err := tx.GetOne(kv.PlainState, fromKey[:20])
		if err != nil {
			return nil, err
		}
		inc, err := tx.db.parseInc(accData)
		if err != nil {
			return nil, err
		}
		startkey := make([]byte, length.Addr+length.Incarnation+length.Hash)
		copy(startkey, fromKey[:20])
		binary.BigEndian.PutUint64(startkey[length.Addr:], inc)
		copy(startkey[length.Addr+length.Incarnation:], fromKey[20:])

		toPrefix := make([]byte, length.Addr+length.Incarnation)
		copy(toPrefix, fromKey[:20])
		binary.BigEndian.PutUint64(toPrefix[length.Addr:], inc+1)

		it2, err := tx.RangeAscend(kv.PlainState, startkey, toPrefix, limit)
		if err != nil {
			return nil, err
		}
		it3 := iter.TransformKV(it2, func(k, v []byte) ([]byte, []byte, error) {
			return append(append([]byte{}, k[:20]...), k[28:]...), v, nil
		})
		it = iter.UnionKV(storageIt1, it3, limit)
	case kv.CodeDomain:
		panic("not implemented yet")
	default:
		panic(fmt.Sprintf("unexpected: %s", name))
	}

	if closer, ok := it.(kv.Closer); ok {
		tx.resourcesToClose = append(tx.resourcesToClose, closer)
	}

	return it, nil
}
func (tx *Tx) DomainGet(name kv.Domain, key, key2 []byte) (v []byte, ok bool, err error) {
	if ethconfig.EnableHistoryV4InTest {
		panic("implement me")
	}
	switch name {
	case kv.AccountsDomain:
		v, err = tx.GetOne(kv.PlainState, key)
		return v, v != nil, err
	case kv.StorageDomain:
		v, err = tx.GetOne(kv.PlainState, append(common.Copy(key), key2...))
		return v, v != nil, err
	case kv.CodeDomain:
		v, err = tx.GetOne(kv.Code, key2)
		return v, v != nil, err
	default:
		panic(fmt.Sprintf("unexpected: %s", name))
	}
}
func (tx *Tx) DomainGetAsOf(name kv.Domain, key, key2 []byte, ts uint64) (v []byte, ok bool, err error) {
	if ethconfig.EnableHistoryV4InTest {
		panic("implement me")
	}
	switch name {
	case kv.AccountsDomain:
		v, ok, err = tx.HistoryGet(kv.AccountsHistory, key, ts)
		if err != nil {
			return nil, false, err
		}
		if ok {
			return v, true, nil
		}
		v, err = tx.GetOne(kv.PlainState, key)
		if len(v) > 0 {
			v, err = accounts.ConvertV2toV3(v)
			if err != nil {
				return nil, false, err
			}
		}
		return v, v != nil, err
	case kv.StorageDomain:
		v, ok, err = tx.HistoryGet(kv.StorageHistory, append(key[:20], key2...), ts)
		if err != nil {
			return nil, false, err
		}
		if ok {
			return v, true, nil
		}
		v, err = tx.GetOne(kv.PlainState, append(key, key2...))
		return v, v != nil, err
	case kv.CodeDomain:
		v, ok, err = tx.HistoryGet(kv.CodeHistory, key, ts)
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
	case kv.AccountsHistory:
		v, ok, err = tx.aggCtx.ReadAccountDataNoStateWithRecent(key, ts, tx.MdbxTx)
		if err != nil {
			return nil, false, err
		}
		if !ok || len(v) == 0 {
			return v, ok, nil
		}
		/*
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
			v, err = tx.db.restoreCodeHash(tx.MdbxTx, key, v, force)
			if err != nil {
				return nil, false, err
			}
			if len(v) > 0 {
				v, err = tx.db.convertV2toV3(v)
				if err != nil {
					return nil, false, err
				}
			}
		*/
		return v, true, nil
	case kv.StorageHistory:
		return tx.aggCtx.ReadAccountStorageNoStateWithRecent2(key, ts, tx.MdbxTx)
	case kv.CodeHistory:
		return tx.aggCtx.ReadAccountCodeNoStateWithRecent(key, ts, tx.MdbxTx)
	default:
		panic(fmt.Sprintf("unexpected: %s", name))
	}
}

func (tx *Tx) IndexRange(name kv.InvertedIdx, k []byte, fromTs, toTs int, asc order.By, limit int) (timestamps iter.U64, err error) {
	switch name {
	case kv.AccountsHistoryIdx:
		timestamps, err = tx.aggCtx.AccountHistoyIdxRange(k, fromTs, toTs, asc, limit, tx)
	case kv.StorageHistoryIdx:
		timestamps, err = tx.aggCtx.StorageHistoyIdxRange(k, fromTs, toTs, asc, limit, tx)
	case kv.CodeHistoryIdx:
		timestamps, err = tx.aggCtx.CodeHistoyIdxRange(k, fromTs, toTs, asc, limit, tx)
	case kv.LogTopicIdx:
		timestamps, err = tx.aggCtx.LogTopicRange(k, fromTs, toTs, asc, limit, tx)
	case kv.LogAddrIdx:
		timestamps, err = tx.aggCtx.LogAddrRange(k, fromTs, toTs, asc, limit, tx)
	case kv.TracesFromIdx:
		timestamps, err = tx.aggCtx.TraceFromRange(k, fromTs, toTs, asc, limit, tx)
	case kv.TracesToIdx:
		timestamps, err = tx.aggCtx.TraceToRange(k, fromTs, toTs, asc, limit, tx)
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
	case kv.AccountsHistory:
		it, err = tx.aggCtx.AccountHistoryRange(fromTs, toTs, asc, limit, tx)
	case kv.StorageHistory:
		it, err = tx.aggCtx.StorageHistoryRange(fromTs, toTs, asc, limit, tx)
	case kv.CodeHistory:
		it, err = tx.aggCtx.CodeHistoryRange(fromTs, toTs, asc, limit, tx)
	default:
		return nil, fmt.Errorf("unexpected history name: %s", name)
	}
	if err != nil {
		return nil, err
	}
	if closer, ok := it.(kv.Closer); ok {
		tx.resourcesToClose = append(tx.resourcesToClose, closer)
	}
	return it, err
}

// TODO: need remove `gspec` param (move SystemContractCodeLookup feature somewhere)
func NewTestDB(tb testing.TB, ctx context.Context, dirs datadir.Dirs, gspec *types.Genesis, logger log.Logger) (histV3 bool, db kv.RwDB, agg *state.AggregatorV3) {
	historyV3 := ethconfig.EnableHistoryV3InTest

	if tb != nil {
		db = memdb.NewTestDB(tb)
	} else {
		db = memdb.New(dirs.DataDir)
	}
	_ = db.UpdateNosync(context.Background(), func(tx kv.RwTx) error {
		_, _ = kvcfg.HistoryV3.WriteOnce(tx, historyV3)
		return nil
	})

	if historyV3 {
		var err error
		dir.MustExist(dirs.SnapHistory)
		agg, err = state.NewAggregatorV3(ctx, dirs.SnapHistory, dirs.Tmp, ethconfig.HistoryV3AggregationStep, db, logger)
		if err != nil {
			panic(err)
		}
		if err := agg.OpenFolder(); err != nil {
			panic(err)
		}

		var sc map[common.Address][]common.CodeRecord
		if gspec != nil {
			sc = systemcontracts.SystemContractCodeLookup[gspec.Config.ChainName]
		}

		db, err = New(db, agg, sc)
		if err != nil {
			panic(err)
		}
	}
	return historyV3, db, agg
}
