package snapshotdb

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"sync"

	"github.com/ledgerwatch/erigon-lib/kv"
	"github.com/ledgerwatch/erigon/common"
	"github.com/ledgerwatch/erigon/ethdb"
	"github.com/ledgerwatch/log/v3"
)

var (
	_ kv.RwDB           = &SnapshotKV{}
	_ kv.RoDB           = &SnapshotKV{}
	_ kv.Tx             = &snTX{}
	_ kv.BucketMigrator = &snTX{}
	_ kv.RwCursor       = &snCursor{}
	_ kv.Cursor         = &snCursor{}
)

type SnapshotUpdater interface {
	UpdateSnapshots(tp string, snapshotKV kv.RoDB, done chan struct{})
	HeadersSnapshot() kv.RoDB
	BodiesSnapshot() kv.RoDB
	StateSnapshot() kv.RoDB
}

type WriteDB interface {
	WriteDB() kv.RwDB
}

func NewSnapshotKV() snapshotOpts {

	return snapshotOpts{}
}

type snapshotOpts struct {
	db              kv.RwDB
	headersSnapshot kv.RoDB
	bodiesSnapshot  kv.RoDB
	stateSnapshot   kv.RoDB
}

func (opts snapshotOpts) HeadersSnapshot(kv kv.RoDB) snapshotOpts {
	opts.headersSnapshot = kv
	return opts
}
func (opts snapshotOpts) BodiesSnapshot(kv kv.RoDB) snapshotOpts {
	opts.bodiesSnapshot = kv
	return opts
}
func (opts snapshotOpts) StateSnapshot(kv kv.RoDB) snapshotOpts {
	opts.stateSnapshot = kv
	return opts
}

func (opts snapshotOpts) DB(db kv.RwDB) snapshotOpts {
	opts.db = db
	return opts
}

func (opts snapshotOpts) Open() *SnapshotKV {
	return &SnapshotKV{
		headersSnapshot: opts.headersSnapshot,
		bodiesSnapshot:  opts.bodiesSnapshot,
		stateSnapshot:   opts.stateSnapshot,
		db:              opts.db,
	}
}

type SnapshotKV struct {
	db              kv.RwDB
	headersSnapshot kv.RoDB
	bodiesSnapshot  kv.RoDB
	stateSnapshot   kv.RoDB
	mtx             sync.RWMutex

	tmpDB        kv.RwDB
	tmpDBBuckets map[string]struct{}
}

func (s *SnapshotKV) View(ctx context.Context, f func(tx kv.Tx) error) error {
	snTX, err := s.BeginRo(ctx)
	if err != nil {
		return err
	}
	defer snTX.Rollback()
	return f(snTX)
}

func (s *SnapshotKV) Update(ctx context.Context, f func(tx kv.RwTx) error) error {
	tx, err := s.BeginRw(ctx)
	if err != nil {
		return err
	}
	defer tx.Rollback()

	err = f(tx)
	if err == nil {
		return tx.Commit()
	}
	return err
}

func (s *SnapshotKV) Close() {
	defer s.db.Close()
	s.mtx.Lock()
	defer s.mtx.Unlock()
	if s.headersSnapshot != nil {
		defer s.headersSnapshot.Close()
	}
	if s.bodiesSnapshot != nil {
		defer s.bodiesSnapshot.Close()
	}
	if s.stateSnapshot != nil {
		defer s.stateSnapshot.Close()
	}
}

func (s *SnapshotKV) UpdateSnapshots(tp string, snapshotKV kv.RoDB, done chan struct{}) {
	var toClose kv.RoDB
	s.mtx.Lock()
	defer s.mtx.Unlock()
	switch {
	case tp == "headers":
		toClose = s.headersSnapshot
		s.headersSnapshot = snapshotKV
	case tp == "bodies":
		toClose = s.bodiesSnapshot
		s.bodiesSnapshot = snapshotKV
	case tp == "state":
		toClose = s.stateSnapshot
		s.stateSnapshot = snapshotKV
	default:
		log.Error("incorrect type", "tp", tp)
	}

	go func() {
		if toClose != nil {
			toClose.Close()
		}
		done <- struct{}{}
		log.Info("old snapshot closed", "tp", tp)
	}()
}

func (s *SnapshotKV) WriteDB() kv.RwDB {
	return s.db
}

func (s *SnapshotKV) TempDB() kv.RwDB {
	return s.tmpDB
}

func (s *SnapshotKV) SetTempDB(kv kv.RwDB, buckets []string) {
	bucketsMap := make(map[string]struct{}, len(buckets))
	for _, bucket := range buckets {
		bucketsMap[bucket] = struct{}{}
	}
	s.tmpDB = kv
	s.tmpDBBuckets = bucketsMap
}

//todo
func (s *SnapshotKV) HeadersSnapshot() kv.RoDB {
	return s.headersSnapshot
}
func (s *SnapshotKV) BodiesSnapshot() kv.RoDB {
	return s.bodiesSnapshot
}
func (s *SnapshotKV) StateSnapshot() kv.RoDB {
	return s.stateSnapshot
}

func (s *SnapshotKV) snapsthotsTx(ctx context.Context) (kv.Tx, kv.Tx, kv.Tx, error) {
	var headersTX, bodiesTX, stateTX kv.Tx
	var err error
	defer func() {
		if err != nil {
			if headersTX != nil {
				headersTX.Rollback()
			}
			if bodiesTX != nil {
				bodiesTX.Rollback()
			}
			if stateTX != nil {
				stateTX.Rollback()
			}
		}
	}()
	if s.headersSnapshot != nil {
		headersTX, err = s.headersSnapshot.BeginRo(ctx)
		if err != nil {
			return nil, nil, nil, err
		}
	}
	if s.bodiesSnapshot != nil {
		bodiesTX, err = s.bodiesSnapshot.BeginRo(ctx)
		if err != nil {
			return nil, nil, nil, err
		}
	}
	if s.stateSnapshot != nil {
		stateTX, err = s.stateSnapshot.BeginRo(ctx)
		if err != nil {
			return nil, nil, nil, err
		}
	}
	return headersTX, bodiesTX, stateTX, nil
}
func (s *SnapshotKV) BeginRo(ctx context.Context) (kv.Tx, error) {
	dbTx, err := s.db.BeginRo(ctx)
	if err != nil {
		return nil, err
	}
	var tmpTX kv.Tx
	if s.tmpDB != nil {
		tmpTX, err = s.tmpDB.BeginRo(context.Background())
		if err != nil {
			return nil, err
		}
	}
	headersTX, bodiesTX, stateTX, err := s.snapsthotsTx(ctx)
	if err != nil {
		return nil, err
	}
	return &snTX{
		dbTX:      dbTx,
		headersTX: headersTX,
		bodiesTX:  bodiesTX,
		stateTX:   stateTX,
		tmpTX:     tmpTX,
		buckets:   s.tmpDBBuckets,
	}, nil
}

func (s *SnapshotKV) BeginRw(ctx context.Context) (kv.RwTx, error) {
	dbTx, err := s.db.BeginRw(ctx) //nolint
	if err != nil {
		return nil, err
	}

	var tmpTX kv.Tx
	if s.tmpDB != nil {
		tmpTX, err = s.tmpDB.BeginRw(context.Background())
		if err != nil {
			return nil, err
		}
	}

	headersTX, bodiesTX, stateTX, err := s.snapsthotsTx(ctx)
	if err != nil {
		return nil, err
	}

	return &snTX{
		dbTX:      dbTx,
		headersTX: headersTX,
		bodiesTX:  bodiesTX,
		stateTX:   stateTX,
		tmpTX:     tmpTX,
		buckets:   s.tmpDBBuckets,
	}, nil
}

func (s *SnapshotKV) AllBuckets() kv.TableCfg {
	return s.db.AllBuckets()
}

var ErrUnavailableSnapshot = errors.New("unavailable snapshot")

type snTX struct {
	dbTX      kv.Tx
	headersTX kv.Tx
	bodiesTX  kv.Tx
	stateTX   kv.Tx

	//just an experiment with temp db for state snapshot migration.
	tmpTX   kv.Tx
	buckets map[string]struct{}
}

type DBTX interface {
	DBTX() kv.RwTx
}

func (s *snTX) DBTX() kv.RwTx  { return s.dbTX.(kv.RwTx) }
func (s *snTX) ViewID() uint64 { return s.dbTX.ViewID() }

func (s *snTX) RwCursor(bucket string) (kv.RwCursor, error) {
	if !IsSnapshotBucket(bucket) {
		return s.dbTX.(kv.RwTx).RwCursor(bucket)
	}
	tx, err := s.getSnapshotTX(bucket)
	if err != nil && !errors.Is(err, ErrUnavailableSnapshot) {
		panic(err.Error())
	}
	//process only db buckets
	if errors.Is(err, ErrUnavailableSnapshot) {
		return s.dbTX.(kv.RwTx).RwCursor(bucket)
	}

	snCursor2, err := tx.Cursor(bucket)
	if err != nil {
		return nil, err
	}

	if IsStateSnapshotSnapshotBucket(bucket) && s.tmpTX != nil {
		mainDBCursor, err := s.dbTX.Cursor(bucket)
		if err != nil {
			return nil, err
		}
		tmpDBCursor, err := s.tmpTX.(kv.RwTx).RwCursor(bucket)
		if err != nil {
			return nil, err
		}

		return &snCursor{
			dbCursor: &snCursor{
				dbCursor: tmpDBCursor,
				snCursor: mainDBCursor,
			},
			snCursor: snCursor2,
		}, nil
	}
	dbCursor, err := s.dbTX.(kv.RwTx).RwCursor(bucket)
	if err != nil {
		return nil, err
	}

	return &snCursor{
		dbCursor: dbCursor,
		snCursor: snCursor2,
	}, nil

}

func (s *snTX) DropBucket(bucket string) error {
	return s.dbTX.(kv.BucketMigrator).DropBucket(bucket)
}

func (s *snTX) CreateBucket(bucket string) error {
	return s.dbTX.(kv.BucketMigrator).CreateBucket(bucket)
}

func (s *snTX) ExistsBucket(bucket string) (bool, error) {
	return s.dbTX.(kv.BucketMigrator).ExistsBucket(bucket)
}

func (s *snTX) ClearBucket(bucket string) error {
	return s.dbTX.(kv.BucketMigrator).ClearBucket(bucket)
}

func (s *snTX) ListBuckets() ([]string, error) {
	return s.dbTX.(kv.BucketMigrator).ListBuckets()
}

func (s *snTX) Cursor(bucket string) (kv.Cursor, error) {
	if !IsSnapshotBucket(bucket) {
		return s.dbTX.Cursor(bucket)
	}

	tx, err := s.getSnapshotTX(bucket)
	if err != nil && !errors.Is(err, ErrUnavailableSnapshot) {
		panic(err.Error())
	}
	//process only db buckets
	if errors.Is(err, ErrUnavailableSnapshot) {
		return s.dbTX.Cursor(bucket)
	}
	dbCursor, err := s.dbTX.Cursor(bucket)
	if err != nil {
		return nil, err
	}
	snCursor2, err := tx.Cursor(bucket)
	if err != nil {
		return nil, err
	}
	if IsStateSnapshotSnapshotBucket(bucket) && s.tmpTX != nil {
		tmpDBCursor, err := s.tmpTX.Cursor(bucket)
		if err != nil {
			return nil, err
		}

		return &snCursor{
			dbCursor: &snCursor{
				dbCursor: tmpDBCursor,
				snCursor: dbCursor,
			},
			snCursor: snCursor2,
		}, nil
	}
	return &snCursor{
		dbCursor: dbCursor,
		snCursor: snCursor2,
	}, nil
}

func (s *snTX) CursorDupSort(bucket string) (kv.CursorDupSort, error) {
	tx, err := s.getSnapshotTX(bucket)
	if err != nil && !errors.Is(err, ErrUnavailableSnapshot) {
		panic(err.Error())
	}
	//process only db buckets
	if errors.Is(err, ErrUnavailableSnapshot) {
		return s.dbTX.CursorDupSort(bucket)
	}
	dbc, err := s.dbTX.CursorDupSort(bucket)
	if err != nil {
		return nil, err
	}
	sncbc, err := tx.CursorDupSort(bucket)
	if err != nil {
		return nil, err
	}
	return &snCursorDup{
		dbc,
		sncbc,
		snCursor{
			dbCursor: dbc,
			snCursor: sncbc,
		},
	}, nil
}

func (s *snTX) RwCursorDupSort(bucket string) (kv.RwCursorDupSort, error) {
	c, err := s.CursorDupSort(bucket)
	if err != nil {
		return nil, err
	}
	return c.(kv.RwCursorDupSort), nil
}
func (s *snTX) GetOne(bucket string, key []byte) (val []byte, err error) {
	v, err := s.dbTX.GetOne(bucket, key)
	if err != nil {
		return nil, err
	}
	if len(v) == 0 {
		snTx, innerErr := s.getSnapshotTX(bucket)
		if innerErr != nil && !errors.Is(innerErr, ErrUnavailableSnapshot) {
			return nil, innerErr
		}
		//process only db buckets
		if errors.Is(innerErr, ErrUnavailableSnapshot) {
			return v, nil
		}
		v, err = snTx.GetOne(bucket, key)
		if err != nil {
			return nil, err
		}
		if bytes.Equal(v, DeletedValue) {
			return nil, nil
		}
		return v, nil
	}
	return v, nil
}

func (s *snTX) Put(bucket string, k, v []byte) error {
	if s.tmpTX != nil && IsStateSnapshotSnapshotBucket(bucket) {
		return s.tmpTX.(kv.RwTx).Put(bucket, k, v)
	}
	return s.dbTX.(kv.RwTx).Put(bucket, k, v)
}
func (s *snTX) Append(bucket string, k, v []byte) error {
	if s.tmpTX != nil && IsStateSnapshotSnapshotBucket(bucket) {
		return s.tmpTX.(kv.RwTx).Put(bucket, k, v)
	}
	return s.dbTX.(kv.RwTx).Append(bucket, k, v)
}
func (s *snTX) AppendDup(bucket string, k, v []byte) error {
	if s.tmpTX != nil && IsStateSnapshotSnapshotBucket(bucket) {
		return s.tmpTX.(kv.RwTx).Put(bucket, k, v)
	}
	return s.dbTX.(kv.RwTx).AppendDup(bucket, k, v)
}
func (s *snTX) Delete(bucket string, k, v []byte) error {
	//note we can't use Delete here, because we can't change snapshots
	//if we delete in main database we can find the value in snapshot
	//so we are just marking that this value is deleted.
	//this value will be removed on snapshot merging
	if s.tmpTX != nil && IsStateSnapshotSnapshotBucket(bucket) {
		return s.tmpTX.(kv.RwTx).Put(bucket, k, DeletedValue)
	}

	return s.dbTX.(kv.RwTx).Put(bucket, k, DeletedValue)
}

func (s *snTX) CollectMetrics() {
	if rw, ok := s.dbTX.(kv.RwTx); ok {
		rw.CollectMetrics()
	}
}

func (s *snTX) getSnapshotTX(bucket string) (kv.Tx, error) {
	var tx kv.Tx
	switch bucket {
	case kv.Headers:
		tx = s.headersTX
	case kv.BlockBody, kv.EthTx:
		tx = s.bodiesTX
	case kv.PlainState, kv.PlainContractCode, kv.Code:
		tx = s.stateTX
	}
	if tx == nil {
		return nil, fmt.Errorf("%s  %w", bucket, ErrUnavailableSnapshot)
	}
	return tx, nil
}

func (s *snTX) Has(bucket string, key []byte) (bool, error) {
	v, err := s.dbTX.Has(bucket, key)
	if err != nil {
		return false, err
	}
	if !v {
		snTx, err := s.getSnapshotTX(bucket)
		if err != nil && !errors.Is(err, ErrUnavailableSnapshot) {
			return false, err
		}
		//process only db buckets
		if errors.Is(err, ErrUnavailableSnapshot) {
			return v, nil
		}

		v, err := snTx.GetOne(bucket, key)
		if err != nil {
			return false, err
		}
		if bytes.Equal(v, DeletedValue) {
			return false, nil
		}

		return true, nil
	}
	return v, nil
}

func (s *snTX) ForEach(bucket string, fromPrefix []byte, walker func(k, v []byte) error) error {
	c, err := s.Cursor(bucket)
	if err != nil {
		return err
	}
	defer c.Close()

	for k, v, err := c.Seek(fromPrefix); k != nil; k, v, err = c.Next() {
		if err != nil {
			return err
		}
		if err := walker(k, v); err != nil {
			return err
		}
	}
	return nil
}

func (s *snTX) ForPrefix(bucket string, prefix []byte, walker func(k, v []byte) error) error {
	c, err := s.Cursor(bucket)
	if err != nil {
		return err
	}
	defer c.Close()

	for k, v, err := c.Seek(prefix); k != nil; k, v, err = c.Next() {
		if err != nil {
			return err
		}
		if !bytes.HasPrefix(k, prefix) {
			break
		}
		if err := walker(k, v); err != nil {
			return err
		}
	}
	return nil
}
func (s *snTX) ForAmount(bucket string, fromPrefix []byte, amount uint32, walker func(k, v []byte) error) error {
	c, err := s.Cursor(bucket)
	if err != nil {
		return err
	}
	defer c.Close()

	for k, v, err := c.Seek(fromPrefix); k != nil && amount > 0; k, v, err = c.Next() {
		if err != nil {
			return err
		}
		if err := walker(k, v); err != nil {
			return err
		}
		amount--
	}
	return nil
}

func (s *snTX) Commit() error {
	defer s.snapshotsRollback()
	if s.tmpTX != nil {
		err := s.tmpTX.Commit()
		if err != nil {
			s.dbTX.Rollback()
			return err
		}
	}
	return s.dbTX.Commit()
}
func (s *snTX) snapshotsRollback() {
	if s.headersTX != nil {
		defer s.headersTX.Rollback()
	}
	if s.bodiesTX != nil {
		defer s.bodiesTX.Rollback()
	}
	if s.stateTX != nil {
		defer s.stateTX.Rollback()
	}
}
func (s *snTX) Rollback() {
	defer s.snapshotsRollback()
	defer func() {
		if s.tmpTX != nil {
			s.tmpTX.Rollback()
		}
	}()
	s.dbTX.Rollback()
}

func (s *snTX) BucketSize(bucket string) (uint64, error) {
	return s.dbTX.BucketSize(bucket)
}

func (s *snTX) IncrementSequence(bucket string, amount uint64) (uint64, error) {
	return s.dbTX.(kv.RwTx).IncrementSequence(bucket, amount)
}

func (s *snTX) ReadSequence(bucket string) (uint64, error) {
	return s.dbTX.ReadSequence(bucket)
}

func (s *snTX) BucketExists(bucket string) (bool, error) {
	return s.dbTX.(ethdb.BucketsMigrator).BucketExists(bucket)
}

func (s *snTX) ClearBuckets(buckets ...string) error {
	return s.dbTX.(ethdb.BucketsMigrator).ClearBuckets(buckets...)
}

func (s *snTX) DropBuckets(buckets ...string) error {
	return s.dbTX.(ethdb.BucketsMigrator).DropBuckets(buckets...)
}

var DeletedValue = []byte{0}

type snCursor struct {
	dbCursor kv.Cursor
	snCursor kv.Cursor

	currentKey []byte
}

func (s *snCursor) First() ([]byte, []byte, error) {
	var err error
	lastDBKey, lastDBVal, err := s.dbCursor.First()
	if err != nil {
		return nil, nil, err
	}

	for bytes.Equal(lastDBVal, DeletedValue) {
		lastDBKey, lastDBVal, err = s.dbCursor.Next()
		if err != nil {
			return nil, nil, err
		}

	}

	lastSNDBKey, lastSNDBVal, err := s.snCursor.First()
	if err != nil {
		return nil, nil, err
	}
	cmp, br := common.KeyCmp(lastDBKey, lastSNDBKey)
	if br {
		return nil, nil, nil
	}

	if cmp <= 0 {
		s.saveCurrent(lastDBKey)
		return lastDBKey, lastDBVal, nil
	}
	s.saveCurrent(lastSNDBKey)
	return lastSNDBKey, lastSNDBVal, nil
}

func (s *snCursor) Seek(seek []byte) ([]byte, []byte, error) {
	dbKey, dbVal, err := s.dbCursor.Seek(seek)
	if err != nil && !errors.Is(err, ethdb.ErrKeyNotFound) {
		return nil, nil, err
	}

	for bytes.Equal(dbVal, DeletedValue) {
		dbKey, dbVal, err = s.dbCursor.Next()
		if err != nil {
			return nil, nil, err
		}
	}

	sndbKey, sndbVal, err := s.snCursor.Seek(seek)
	if err != nil && !errors.Is(err, ethdb.ErrKeyNotFound) {
		return nil, nil, err
	}

	if bytes.Equal(dbKey, seek) && dbVal != nil {
		return dbKey, dbVal, err
	}
	if bytes.Equal(sndbKey, seek) && sndbVal != nil {
		return sndbKey, sndbVal, err
	}
	cmp, _ := common.KeyCmp(dbKey, sndbKey)
	if cmp <= 0 {
		s.saveCurrent(dbKey)
		return dbKey, dbVal, nil
	}
	s.saveCurrent(sndbKey)
	return sndbKey, sndbVal, nil
}

func (s *snCursor) SeekExact(key []byte) ([]byte, []byte, error) {
	k, v, err := s.dbCursor.SeekExact(key)
	if err != nil {
		return nil, nil, err
	}
	if bytes.Equal(v, DeletedValue) {
		return nil, nil, nil
	}
	if v == nil {
		k, v, err = s.snCursor.SeekExact(key)
		s.saveCurrent(k)
		return k, v, err
	}
	s.saveCurrent(k)
	return k, v, err
}

func (s *snCursor) iteration(dbNextElement func() ([]byte, []byte, error), sndbNextElement func() ([]byte, []byte, error), cmpFunc func(kdb, ksndb []byte) (int, bool)) ([]byte, []byte, error) {
	var err error
	var noDBNext, noSnDBNext bool
	//current returns error on empty bucket
	lastDBKey, lastDBVal, err := s.dbCursor.Current()
	if err != nil {
		var innerErr error
		lastDBKey, lastDBVal, innerErr = dbNextElement()
		if innerErr != nil {
			return nil, nil, fmt.Errorf("get current from db %w inner %v", err, innerErr)
		}
		noDBNext = true
	}

	lastSNDBKey, lastSNDBVal, err := s.snCursor.Current()
	if err != nil {
		var innerErr error
		lastSNDBKey, lastSNDBVal, innerErr = sndbNextElement()
		if innerErr != nil {
			return nil, nil, fmt.Errorf("get current from snapshot %w inner %v", err, innerErr)
		}
		noSnDBNext = true
	}

	cmp, br := cmpFunc(lastDBKey, lastSNDBKey)
	if br {
		return nil, nil, nil
	}

	//todo Seek fastpath
	if cmp > 0 {
		if !noSnDBNext {
			lastSNDBKey, lastSNDBVal, err = sndbNextElement()
			if err != nil {
				return nil, nil, err
			}

			if currentKeyCmp, _ := common.KeyCmp(s.currentKey, lastDBKey); len(lastSNDBKey) == 0 && currentKeyCmp >= 0 && len(s.currentKey) > 0 {
				lastDBKey, lastDBVal, err = dbNextElement()
			}
			if err != nil {
				return nil, nil, err
			}
		}
	}

	//current receives last acceptable key. If it is empty
	if cmp < 0 {
		if !noDBNext {
			lastDBKey, lastDBVal, err = dbNextElement()
			if err != nil {
				return nil, nil, err
			}
			if currentKeyCmp, _ := common.KeyCmp(s.currentKey, lastSNDBKey); len(lastDBKey) == 0 && currentKeyCmp >= 0 && len(s.currentKey) > 0 {
				lastSNDBKey, lastSNDBVal, err = sndbNextElement()
			}
			if err != nil {
				return nil, nil, err
			}
		}
	}
	if cmp == 0 {
		if !noDBNext {
			lastDBKey, lastDBVal, err = dbNextElement()
			if err != nil {
				return nil, nil, err
			}
		}
		if !noSnDBNext {
			lastSNDBKey, lastSNDBVal, err = sndbNextElement()
			if err != nil {
				return nil, nil, err
			}
		}
	}

	cmp, br = cmpFunc(lastDBKey, lastSNDBKey)
	if br {
		return nil, nil, nil
	}
	if cmp <= 0 {
		return lastDBKey, lastDBVal, nil
	}

	return lastSNDBKey, lastSNDBVal, nil
}

func (s *snCursor) Next() ([]byte, []byte, error) {
	k, v, err := s.iteration(s.dbCursor.Next, s.snCursor.Next, common.KeyCmp) //f(s.dbCursor.Next, s.snCursor.Next)
	if err != nil {
		return nil, nil, err
	}
	for bytes.Equal(v, DeletedValue) {
		k, v, err = s.iteration(s.dbCursor.Next, s.snCursor.Next, common.KeyCmp) // f(s.dbCursor.Next, s.snCursor.Next)
		if err != nil {
			return nil, nil, err
		}

	}
	s.saveCurrent(k)
	return k, v, nil
}

func (s *snCursor) Prev() ([]byte, []byte, error) {
	k, v, err := s.iteration(s.dbCursor.Prev, s.snCursor.Prev, func(kdb, ksndb []byte) (int, bool) {
		cmp, br := KeyCmpBackward(kdb, ksndb)
		return -1 * cmp, br
	})
	if err != nil {
		return nil, nil, err
	}
	for cmp, _ := KeyCmpBackward(k, s.currentKey); bytes.Equal(v, DeletedValue) || cmp >= 0; cmp, _ = KeyCmpBackward(k, s.currentKey) {
		k, v, err = s.iteration(s.dbCursor.Prev, s.snCursor.Prev, func(kdb, ksndb []byte) (int, bool) {
			cmp, br := KeyCmpBackward(kdb, ksndb)
			return -1 * cmp, br
		})
		if err != nil {
			return nil, nil, err
		}
	}
	s.saveCurrent(k)
	return k, v, nil
}

func (s *snCursor) Last() ([]byte, []byte, error) {
	var err error
	lastSNDBKey, lastSNDBVal, err := s.snCursor.Last()
	if err != nil {
		return nil, nil, err
	}
	lastDBKey, lastDBVal, err := s.dbCursor.Last()
	if err != nil {
		return nil, nil, err
	}

	for bytes.Equal(lastDBVal, DeletedValue) {
		lastDBKey, lastDBVal, err = s.dbCursor.Prev()
		if err != nil {
			return nil, nil, err
		}
	}

	cmp, br := KeyCmpBackward(lastDBKey, lastSNDBKey)
	if br {
		return nil, nil, nil
	}

	if cmp >= 0 {
		s.saveCurrent(lastDBKey)
		return lastDBKey, lastDBVal, nil
	}
	s.saveCurrent(lastSNDBKey)
	return lastSNDBKey, lastSNDBVal, nil
}

func (s *snCursor) Current() ([]byte, []byte, error) {
	k, v, err := s.dbCursor.Current()
	if bytes.Equal(k, s.currentKey) {
		return k, v, err
	}
	return s.snCursor.Current()
}

func (s *snCursor) Put(k, v []byte) error {
	return s.dbCursor.(kv.RwCursor).Put(k, v)
}

func (s *snCursor) Append(k []byte, v []byte) error {
	return s.dbCursor.(kv.RwCursor).Append(k, v)
}

func (s *snCursor) Delete(k, v []byte) error {
	return s.dbCursor.(kv.RwCursor).Put(k, DeletedValue)
}

func (s *snCursor) DeleteCurrent() error {
	panic("implement me")
}

func (s *snCursor) Count() (uint64, error) {
	panic("implement me")
}

func (s *snCursor) Close() {
	s.dbCursor.Close()
	s.snCursor.Close()
}

type snCursorDup struct {
	dbCursorDup   kv.CursorDupSort
	sndbCursorDup kv.CursorDupSort
	snCursor
}

func (c *snCursorDup) SeekBothExact(key, value []byte) ([]byte, []byte, error) {
	k, v, err := c.dbCursorDup.SeekBothExact(key, value)
	if err != nil {
		return nil, nil, err
	}
	if v == nil {
		k, v, err = c.sndbCursorDup.SeekBothExact(key, value)
		c.saveCurrent(k)
		return k, v, err
	}
	c.saveCurrent(k)
	return k, v, err

}

func (c *snCursorDup) SeekBothRange(key, value []byte) ([]byte, error) {
	dbVal, err := c.dbCursorDup.SeekBothRange(key, value)
	if err != nil {
		return nil, err
	}
	snDBVal, err := c.sndbCursorDup.SeekBothRange(key, value)
	if err != nil {
		return nil, err
	}

	if dbVal == nil {
		c.saveCurrent(key)
		return dbVal, nil
	}

	return snDBVal, nil
}

func (c *snCursorDup) FirstDup() ([]byte, error) {
	panic("implement me")
}

func (c *snCursorDup) NextDup() ([]byte, []byte, error) {
	panic("implement me")
}

func (c *snCursorDup) NextNoDup() ([]byte, []byte, error) {
	panic("implement me")
}

func (c *snCursorDup) LastDup() ([]byte, error) {
	panic("implement me")
}

func (c *snCursorDup) CountDuplicates() (uint64, error) {
	panic("implement me")
}

func (c *snCursorDup) DeleteCurrentDuplicates() error {
	panic("implement me")
}

func (c *snCursorDup) AppendDup(key, value []byte) error {
	panic("implement me")
}

func (s *snCursor) saveCurrent(k []byte) {
	if k != nil {
		s.currentKey = common.CopyBytes(k)
	}
}

func KeyCmpBackward(key1, key2 []byte) (int, bool) {
	switch {
	case len(key1) == 0 && len(key2) == 0:
		return 0, true
	case len(key1) == 0 && len(key2) != 0:
		return -1, false
	case len(key1) != 0 && len(key2) == 0:
		return 1, false
	default:
		return bytes.Compare(key1, key2), false
	}
}

func IsSnapshotBucket(bucket string) bool {
	return IsStateSnapshotSnapshotBucket(bucket) || IsHeaderSnapshotSnapshotBucket(bucket) || IsBodiesSnapshotSnapshotBucket(bucket)
}
func IsHeaderSnapshotSnapshotBucket(bucket string) bool {
	return bucket == kv.Headers
}
func IsBodiesSnapshotSnapshotBucket(bucket string) bool {
	return bucket == kv.BlockBody || bucket == kv.EthTx
}
func IsStateSnapshotSnapshotBucket(bucket string) bool {
	return bucket == kv.PlainState || bucket == kv.PlainContractCode || bucket == kv.Code
}
