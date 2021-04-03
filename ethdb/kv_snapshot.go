package ethdb

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"unsafe"

	"github.com/ledgerwatch/turbo-geth/common"
	"github.com/ledgerwatch/turbo-geth/common/dbutils"
)

var (
	_ RwKV           = &SnapshotKV2{}
	_ Tx             = &sn2TX{}
	_ BucketMigrator = &sn2TX{}
	_ Cursor         = &snCursor2{}
)

func NewSnapshot2KV() snapshotOpts2 {
	return snapshotOpts2{}
}

type snapshotData struct {
	buckets  []string
	snapshot RwKV
}
type snapshotOpts2 struct {
	db        RwKV
	snapshots []snapshotData
}

func (opts snapshotOpts2) SnapshotDB(buckets []string, db RwKV) snapshotOpts2 {
	opts.snapshots = append(opts.snapshots, snapshotData{
		buckets:  buckets,
		snapshot: db,
	})
	return opts
}

func (opts snapshotOpts2) DB(db RwKV) snapshotOpts2 {
	opts.db = db
	return opts
}

func (opts snapshotOpts2) MustOpen() RwKV {
	snapshots := make(map[string]snapshotData)
	for i, v := range opts.snapshots {
		for _, bucket := range v.buckets {
			snapshots[bucket] = opts.snapshots[i]
		}
	}
	return &SnapshotKV2{
		snapshots: snapshots,
		db:        opts.db,
	}
}

type SnapshotKV2 struct {
	db        RwKV
	snapshots map[string]snapshotData
}

func (s *SnapshotKV2) View(ctx context.Context, f func(tx Tx) error) error {
	snTX, err := s.BeginRo(ctx)
	if err != nil {
		return err
	}
	defer snTX.Rollback()
	return f(snTX)
}

func (s *SnapshotKV2) Update(ctx context.Context, f func(tx RwTx) error) error {
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

func (s *SnapshotKV2) Close() {
	s.db.Close()
	for i := range s.snapshots {
		s.snapshots[i].snapshot.Close()
	}
}

func (s *SnapshotKV2) CollectMetrics() {
	s.db.CollectMetrics()
}

func (s *SnapshotKV2) BeginRo(ctx context.Context) (Tx, error) {
	dbTx, err := s.db.BeginRo(ctx)
	if err != nil {
		return nil, err
	}
	return &sn2TX{
		dbTX:      dbTx,
		snapshots: s.snapshots,
		snTX:      map[string]Tx{},
	}, nil
}

func (s *SnapshotKV2) BeginRw(ctx context.Context) (RwTx, error) {
	dbTx, err := s.db.BeginRw(ctx)
	if err != nil {
		return nil, err
	}
	return &sn2TX{
		dbTX:      dbTx,
		snapshots: s.snapshots,
		snTX:      map[string]Tx{},
	}, nil
}

func (s *SnapshotKV2) AllBuckets() dbutils.BucketsCfg {
	return s.db.AllBuckets()
}

var ErrUnavailableSnapshot = errors.New("unavailable snapshot")

type sn2TX struct {
	dbTX      Tx
	snapshots map[string]snapshotData
	snTX      map[string]Tx
}

func (s *sn2TX) DropBucket(bucket string) error {
	return s.dbTX.(BucketMigrator).DropBucket(bucket)
}

func (s *sn2TX) CreateBucket(bucket string) error {
	return s.dbTX.(BucketMigrator).CreateBucket(bucket)
}

func (s *sn2TX) ExistsBucket(bucket string) bool {
	//todo snapshot check?
	return s.dbTX.(BucketMigrator).ExistsBucket(bucket)
}

func (s *sn2TX) ClearBucket(bucket string) error {
	return s.dbTX.(BucketMigrator).ClearBucket(bucket)
}

func (s *sn2TX) ExistingBuckets() ([]string, error) {
	panic("implement me")
}

func (s *sn2TX) Cursor(bucket string) (Cursor, error) {
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
	snCursor, err := tx.Cursor(bucket)
	if err != nil {
		return nil, err
	}
	return &snCursor2{
		dbCursor: dbCursor,
		snCursor: snCursor,
	}, nil
}

func (s *sn2TX) RwCursor(bucket string) (RwCursor, error) {
	c, err := s.Cursor(bucket)
	if err != nil {
		return nil, err
	}
	return c.(RwCursor), nil
}

func (s *sn2TX) CursorDupSort(bucket string) (CursorDupSort, error) {
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
	return &snCursor2Dup{
		snCursor2{
			dbCursor: dbc,
			snCursor: sncbc,
		},
		dbc,
		sncbc,
	}, nil
}

func (s *sn2TX) RwCursorDupSort(bucket string) (RwCursorDupSort, error) {
	c, err := s.CursorDupSort(bucket)
	if err != nil {
		return nil, err
	}
	return c.(RwCursorDupSort), nil
}

func (s *sn2TX) GetOne(bucket string, key []byte) (val []byte, err error) {
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

func (s *sn2TX) Put(bucket string, k, v []byte) error {
	return s.dbTX.(RwTx).Put(bucket, k, v)
}

func (s *sn2TX) Delete(bucket string, k, v []byte) error {
	return s.dbTX.(RwTx).Delete(bucket, k, v)
}

func (s *sn2TX) getSnapshotTX(bucket string) (Tx, error) {
	tx, ok := s.snTX[bucket]
	if ok {
		return tx, nil
	}
	sn, ok := s.snapshots[bucket]
	if !ok {
		return nil, fmt.Errorf("%s  %w", bucket, ErrUnavailableSnapshot)
	}
	var err error
	tx, err = sn.snapshot.BeginRo(context.TODO())
	if err != nil {
		return nil, err
	}

	s.snTX[bucket] = tx
	return tx, nil
}

func (s *sn2TX) HasOne(bucket string, key []byte) (bool, error) {
	v, err := s.dbTX.HasOne(bucket, key)
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

func (s *sn2TX) Commit() error {
	for i := range s.snTX {
		defer s.snTX[i].Rollback()
	}
	return s.dbTX.Commit()
}

func (s *sn2TX) Rollback() {
	for i := range s.snTX {
		defer s.snTX[i].Rollback()
	}
	s.dbTX.Rollback()

}

func (s *sn2TX) BucketSize(name string) (uint64, error) {
	panic("implement me")
}

func (s *sn2TX) Comparator(bucket string) dbutils.CmpFunc {
	return s.dbTX.Comparator(bucket)
}

func (s *sn2TX) IncrementSequence(bucket string, amount uint64) (uint64, error) {
	panic("implement me")
}

func (s *sn2TX) ReadSequence(bucket string) (uint64, error) {
	panic("implement me")
}

func (s *sn2TX) CHandle() unsafe.Pointer {
	return s.dbTX.CHandle()
}

var DeletedValue = []byte("it is deleted value")

type snCursor2 struct {
	dbCursor Cursor
	snCursor Cursor

	currentKey []byte
}

func (s *snCursor2) Prefetch(v uint) Cursor {
	panic("implement me")
}

func (s *snCursor2) First() ([]byte, []byte, error) {
	var err error
	lastDBKey, lastDBVal, err := s.dbCursor.First()
	if err != nil {
		return nil, nil, err
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

func (s *snCursor2) Seek(seek []byte) ([]byte, []byte, error) {
	dbKey, dbVal, err := s.dbCursor.Seek(seek)
	if err != nil && !errors.Is(err, ErrKeyNotFound) {
		return nil, nil, err
	}
	sndbKey, sndbVal, err := s.snCursor.Seek(seek)
	if err != nil && !errors.Is(err, ErrKeyNotFound) {
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

func (s *snCursor2) SeekExact(key []byte) ([]byte, []byte, error) {
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

func (s *snCursor2) iteration(dbNextElement func() ([]byte, []byte, error), sndbNextElement func() ([]byte, []byte, error), cmpFunc func(kdb, ksndb []byte) (int, bool)) ([]byte, []byte, error) {
	var err error
	//current returns error on empty bucket
	lastDBKey, lastDBVal, err := s.dbCursor.Current()
	if err != nil {
		var innerErr error
		lastDBKey, lastDBVal, innerErr = dbNextElement()
		if innerErr != nil {
			return nil, nil, fmt.Errorf("get current from db %w inner %v", err, innerErr)
		}
	}

	lastSNDBKey, lastSNDBVal, err := s.snCursor.Current()
	if err != nil {
		return nil, nil, err
	}

	cmp, br := cmpFunc(lastDBKey, lastSNDBKey)
	if br {
		return nil, nil, nil
	}

	//todo Seek fastpath
	if cmp > 0 {
		lastSNDBKey, lastSNDBVal, err = sndbNextElement()
		if err != nil {
			return nil, nil, err
		}
		//todo
		if currentKeyCmp, _ := common.KeyCmp(s.currentKey, lastDBKey); len(lastSNDBKey) == 0 && currentKeyCmp >= 0 && len(s.currentKey) > 0 {
			lastDBKey, lastDBVal, err = dbNextElement()
		}
		if err != nil {
			return nil, nil, err
		}
	}

	//current receives last acceptable key. If it is empty
	if cmp < 0 {
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
	if cmp == 0 {
		lastDBKey, lastDBVal, err = dbNextElement()
		if err != nil {
			return nil, nil, err
		}
		lastSNDBKey, lastSNDBVal, err = sndbNextElement()
		if err != nil {
			return nil, nil, err
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

func (s *snCursor2) Next() ([]byte, []byte, error) {
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

func (s *snCursor2) Prev() ([]byte, []byte, error) {
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

func (s *snCursor2) Last() ([]byte, []byte, error) {
	var err error
	lastSNDBKey, lastSNDBVal, err := s.snCursor.Last()
	if err != nil {
		return nil, nil, err
	}
	lastDBKey, lastDBVal, err := s.dbCursor.Last()
	if err != nil {
		return nil, nil, err
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

func (s *snCursor2) Current() ([]byte, []byte, error) {
	k, v, err := s.dbCursor.Current()
	if bytes.Equal(k, s.currentKey) {
		return k, v, err
	}
	return s.snCursor.Current()
}

func (s *snCursor2) Put(k, v []byte) error {
	return s.dbCursor.(RwCursor).Put(k, v)
}

func (s *snCursor2) Append(k []byte, v []byte) error {
	return s.dbCursor.(RwCursor).Append(k, v)
}

func (s *snCursor2) Delete(k, v []byte) error {
	return s.dbCursor.(RwCursor).Put(k, DeletedValue)
}

func (s *snCursor2) DeleteCurrent() error {
	panic("implement me")
}

func (s *snCursor2) Count() (uint64, error) {
	panic("implement me")
}

func (s *snCursor2) Close() {
	s.dbCursor.Close()
	s.snCursor.Close()
}

type snCursor2Dup struct {
	snCursor2
	dbCursorDup   CursorDupSort
	sndbCursorDup CursorDupSort
}

func (c *snCursor2Dup) SeekBothExact(key, value []byte) ([]byte, []byte, error) {
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

func (c *snCursor2Dup) SeekBothRange(key, value []byte) ([]byte, error) {
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

func (c *snCursor2Dup) FirstDup() ([]byte, error) {
	panic("implement me")
}

func (c *snCursor2Dup) NextDup() ([]byte, []byte, error) {
	panic("implement me")
}

func (c *snCursor2Dup) NextNoDup() ([]byte, []byte, error) {
	panic("implement me")
}

func (c *snCursor2Dup) LastDup() ([]byte, error) {
	panic("implement me")
}

func (c *snCursor2Dup) CountDuplicates() (uint64, error) {
	panic("implement me")
}

func (c *snCursor2Dup) DeleteCurrentDuplicates() error {
	panic("implement me")
}

func (c *snCursor2Dup) AppendDup(key, value []byte) error {
	panic("implement me")
}

func (s *snCursor2) saveCurrent(k []byte) {
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

type KvData struct {
	K []byte
	V []byte
}

func GenStateData(data []KvData) (RwKV, error) {
	snapshot := NewLMDB().WithBucketsConfig(func(defaultBuckets dbutils.BucketsCfg) dbutils.BucketsCfg {
		return dbutils.BucketsCfg{
			dbutils.PlainStateBucket: dbutils.BucketConfigItem{},
		}
	}).InMem().MustOpen()

	err := snapshot.Update(context.Background(), func(tx RwTx) error {
		c, err := tx.RwCursor(dbutils.PlainStateBucket)
		if err != nil {
			return err
		}
		for i := range data {
			innerErr := c.Put(data[i].K, data[i].V)
			if innerErr != nil {
				return innerErr
			}
		}
		return nil
	})
	if err != nil {
		return nil, err
	}
	return snapshot, nil
}

//type cursorSnapshotDupsort struct {
//
//}
//
//func (c *cursorSnapshotDupsort) First() ([]byte, []byte, error) {
//	panic("implement me")
//}
//
//func (c *cursorSnapshotDupsort) Seek(seek []byte) ([]byte, []byte, error) {
//	panic("implement me")
//}
//
//func (c *cursorSnapshotDupsort) SeekExact(key []byte) ([]byte, []byte, error) {
//	panic("implement me")
//}
//
//func (c *cursorSnapshotDupsort) Next() ([]byte, []byte, error) {
//	panic("implement me")
//}
//
//func (c *cursorSnapshotDupsort) Prev() ([]byte, []byte, error) {
//	panic("implement me")
//}
//
//func (c *cursorSnapshotDupsort) Last() ([]byte, []byte, error) {
//	panic("implement me")
//}
//
//func (c *cursorSnapshotDupsort) Current() ([]byte, []byte, error) {
//	panic("implement me")
//}
//
//func (c *cursorSnapshotDupsort) Put(k, v []byte) error {
//	panic("implement me")
//}
//
//func (c *cursorSnapshotDupsort) Append(k []byte, v []byte) error {
//	panic("implement me")
//}
//
//func (c *cursorSnapshotDupsort) Delete(k, v []byte) error {
//	panic("implement me")
//}
//
//func (c *cursorSnapshotDupsort) DeleteCurrent() error {
//	panic("implement me")
//}
//
//func (c *cursorSnapshotDupsort) Count() (uint64, error) {
//	panic("implement me")
//}
//
//func (c *cursorSnapshotDupsort) Close() {
//	panic("implement me")
//}
//
//
////dupsort
//func (c *cursorSnapshotDupsort) SeekBothExact(key, value []byte) ([]byte, []byte, error) {
//	panic("implement me")
//}
//
//func (c *cursorSnapshotDupsort) SeekBothRange(key, value []byte) ([]byte, []byte, error) {
//	panic("implement me")
//}
//
//func (c *cursorSnapshotDupsort) FirstDup() ([]byte, error) {
//	panic("implement me")
//}
//
//func (c *cursorSnapshotDupsort) NextDup() ([]byte, []byte, error) {
//	panic("implement me")
//}
//
//func (c *cursorSnapshotDupsort) NextNoDup() ([]byte, []byte, error) {
//	panic("implement me")
//}
//
//func (c *cursorSnapshotDupsort) LastDup(k []byte) ([]byte, error) {
//	panic("implement me")
//}
//
//func (c *cursorSnapshotDupsort) CountDuplicates() (uint64, error) {
//	panic("implement me")
//}
//
//func (c *cursorSnapshotDupsort) DeleteCurrentDuplicates() error {
//	panic("implement me")
//}
//
//func (c *cursorSnapshotDupsort) AppendDup(key, value []byte) error {
//	panic("implement me")
//}
