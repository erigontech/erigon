package temporal

import (
	"context"
	"fmt"
	"testing"

	"github.com/ledgerwatch/log/v3"

	"github.com/ledgerwatch/erigon-lib/common"
	"github.com/ledgerwatch/erigon-lib/common/datadir"
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
)

//Variables Naming:
//  tx - Database Transaction
//  txn - Ethereum Transaction (and TxNum - is also number of Ethereum Transaction)
//  RoTx - Read-Only Database Transaction. RwTx - read-write
//  k, v - key, value
//  ts - TimeStamp. Usually it's Ethereum's TransactionNumber (auto-increment ID). Or BlockNumber.
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
//              - provide 'time-travel' API for data: consistent snapshot of data as of given Timestamp.
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
	kvTx, err := db.RwDB.BeginRo(ctx) //nolint:gocritic
	if err != nil {
		return nil, err
	}
	tx := &Tx{MdbxTx: kvTx.(*mdbx.MdbxTx), db: db}

	tx.aggCtx = db.agg.BeginFilesRo()
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

	tx.aggCtx = db.agg.BeginFilesRo()
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

	tx.aggCtx = db.agg.BeginFilesRo()
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
	aggCtx           *state.AggregatorRoTx
	resourcesToClose []kv.Closer
}

func (tx *Tx) ForceReopenAggCtx() {
	tx.aggCtx.Close()
	tx.aggCtx = tx.Agg().BeginFilesRo()
}

func (tx *Tx) WarmupDB(force bool) error { return tx.MdbxTx.WarmupDB(force) }
func (tx *Tx) LockDBInRam() error        { return tx.MdbxTx.LockDBInRam() }
func (tx *Tx) AggCtx() interface{}       { return tx.aggCtx }
func (tx *Tx) Agg() *state.AggregatorV3  { return tx.db.agg }
func (tx *Tx) Rollback() {
	tx.autoClose()
	if tx.MdbxTx == nil { // invariant: it's safe to call Commit/Rollback multiple times
		return
	}
	mdbxTx := tx.MdbxTx
	tx.MdbxTx = nil
	mdbxTx.Rollback()
}
func (tx *Tx) autoClose() {
	for _, closer := range tx.resourcesToClose {
		closer.Close()
	}
	tx.aggCtx.Close()
}
func (tx *Tx) Commit() error {
	tx.autoClose()
	if tx.MdbxTx == nil { // invariant: it's safe to call Commit/Rollback multiple times
		return nil
	}
	mdbxTx := tx.MdbxTx
	tx.MdbxTx = nil
	return mdbxTx.Commit()
}

func (tx *Tx) DomainRange(name kv.Domain, fromKey, toKey []byte, asOfTs uint64, asc order.By, limit int) (it iter.KV, err error) {
	it, err = tx.aggCtx.DomainRange(tx.MdbxTx, name, fromKey, toKey, asOfTs, asc, limit)
	if err != nil {
		return nil, err
	}
	if closer, ok := it.(kv.Closer); ok {
		tx.resourcesToClose = append(tx.resourcesToClose, closer)
	}
	return it, nil
}

func (tx *Tx) DomainGet(name kv.Domain, k, k2 []byte) (v []byte, step uint64, err error) {
	v, step, ok, err := tx.aggCtx.GetLatest(name, k, k2, tx.MdbxTx)
	if err != nil {
		return nil, step, err
	}
	if !ok {
		return nil, step, nil
	}
	return v, step, nil
}
func (tx *Tx) DomainGetAsOf(name kv.Domain, key, key2 []byte, ts uint64) (v []byte, ok bool, err error) {
	if key2 != nil {
		key = append(common.Copy(key), key2...)
	}
	return tx.aggCtx.DomainGetAsOf(tx.MdbxTx, name, key, ts)
}

func (tx *Tx) HistoryGet(name kv.History, key []byte, ts uint64) (v []byte, ok bool, err error) {
	return tx.aggCtx.HistoryGet(name, key, ts, tx.MdbxTx)
}

func (tx *Tx) IndexRange(name kv.InvertedIdx, k []byte, fromTs, toTs int, asc order.By, limit int) (timestamps iter.U64, err error) {
	timestamps, err = tx.aggCtx.IndexRange(name, k, fromTs, toTs, asc, limit, tx.MdbxTx)
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
func NewTestDB(tb testing.TB, dirs datadir.Dirs, gspec *types.Genesis) (histV3 bool, db kv.RwDB, agg *state.AggregatorV3) {
	historyV3 := true
	logger := log.New()

	if tb != nil {
		db = memdb.NewTestDB(tb)
	} else {
		db = memdb.New(dirs.DataDir)
	}
	_ = db.UpdateNosync(context.Background(), func(tx kv.RwTx) error {
		_, _ = kvcfg.HistoryV3.WriteOnce(tx, historyV3)
		return nil
	})

	var err error
	agg, err = state.NewAggregatorV3(context.Background(), dirs, ethconfig.HistoryV3AggregationStep, db, logger)
	if err != nil {
		panic(err)
	}
	if err := agg.OpenFolder(false); err != nil {
		panic(err)
	}

	var sc map[common.Address][]common.CodeRecord
	if gspec != nil {
		sc = systemcontracts.SystemContractCodeLookup[gspec.Config.ChainName]
	}

	if historyV3 {
		db, err = New(db, agg, sc)
		if err != nil {
			panic(err)
		}
	}
	return historyV3, db, agg
}
