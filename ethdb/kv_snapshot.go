package ethdb

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"sync"
	"unsafe"

	"github.com/ledgerwatch/turbo-geth/common"
	"github.com/ledgerwatch/turbo-geth/common/dbutils"
)

var (
	_ KV             = &SnapshotKV{}
	_ Tx             = &snTX{}
	_ BucketMigrator = &snTX{}
	_ Cursor         = &snCursor{}
)

type SnapshotUpdater interface {
	UpdateSnapshots(buckets []string, snapshotKV KV, done chan struct{})
	WriteDB() KV
	SnapshotKV(bucket string) KV
}

func NewSnapshotKV() snapshotOpts {
	return snapshotOpts{}
}

type snapshotData struct {
	buckets  []string
	snapshot KV
}
type snapshotOpts struct {
	db        KV
	snapshots []snapshotData
}

func (opts snapshotOpts) SnapshotDB(buckets []string, db KV) snapshotOpts {
	opts.snapshots = append(opts.snapshots, snapshotData{
		buckets:  buckets,
		snapshot: db,
	})
	return opts
}

func (opts snapshotOpts) DB(db KV) snapshotOpts {
	opts.db = db
	return opts
}

func (opts snapshotOpts) Open() KV {
	snapshots := make(map[string]snapshotData)
	for i, v := range opts.snapshots {
		for _, bucket := range v.buckets {
			snapshots[bucket] = opts.snapshots[i]
		}
	}
	return &SnapshotKV{
		snapshots: snapshots,
		db:        opts.db,
	}
}

type SnapshotKV struct {
	db        KV
	mtx		  sync.RWMutex
	snapshots map[string]snapshotData
}

func (s *SnapshotKV) View(ctx context.Context, f func(tx Tx) error) error {
	snTX, err := s.Begin(ctx)
	if err != nil {
		return err
	}
	defer snTX.Rollback()
	return f(snTX)
}

func (s *SnapshotKV) Update(ctx context.Context, f func(tx RwTx) error) error {
	tx, err := s.BeginRw(ctx)
	if err != nil {
		return err
	}
	defer tx.Rollback()

	err = f(tx)
	if err == nil {
		return tx.Commit(ctx)
	}
	return err
}

func (s *SnapshotKV) Close() {
	s.db.Close()
	for i := range s.snapshots {
		s.snapshots[i].snapshot.Close()
	}
}


func (s *SnapshotKV) UpdateSnapshots(buckets []string, snapshotKV KV, done chan struct{}) {
	sd:=snapshotData{
		buckets: buckets,
		snapshot: snapshotKV,
	}

	toClose:=[]KV{}
	var (
		snData snapshotData
		ok bool
	)
	
	for _,bucket:=range buckets {
		snData,ok = s.snapshots[bucket]
		if ok {
			toClose = append(toClose, snData.snapshot)
		}
		s.snapshots[bucket] = sd
	}
	go func() {
		wg:=sync.WaitGroup{}
		wg.Add(len(toClose))

		for i:=range toClose {
			i:=i
			go func() {
				defer wg.Done()
				toClose[i].Close()
			}()
		}
		wg.Wait()
		close(done)
	}()
}

func (s *SnapshotKV) WriteDB() KV {
	return s.db
}
func (s *SnapshotKV) SnapshotKV(bucket string) KV {
	return s.snapshots[bucket].snapshot
}

func (s *SnapshotKV) CollectMetrics() {
	s.db.CollectMetrics()
}

func (s *SnapshotKV) Begin(ctx context.Context) (Tx, error) {
	dbTx, err := s.db.Begin(ctx)
	if err != nil {
		return nil, err
	}
	return &snTX{
		dbTX:      dbTx,
		snapshots: s.copySnapshots(),
		snTX:      map[string]Tx{},
	}, nil
}
func(s *SnapshotKV) copySnapshots() map[string]snapshotData  {
	s.mtx.RLock()
	defer s.mtx.RUnlock()
	mp:=make(map[string]snapshotData, len(s.snapshots))
	for i:=range s.snapshots {
		mp[i]=s.snapshots[i]
	}
	return mp
}

func (s *SnapshotKV) BeginRw(ctx context.Context) (RwTx, error) {
	dbTx, err := s.db.BeginRw(ctx)
	if err != nil {
		return nil, err
	}
	return &snTX{
		dbTX:      dbTx,
		snapshots: s.copySnapshots(),
		snTX:      map[string]Tx{},
	}, nil
}

func (s *SnapshotKV) AllBuckets() dbutils.BucketsCfg {
	return s.db.AllBuckets()
}

var ErrUnavailableSnapshot = errors.New("unavailable snapshot")

type snTX struct {
	dbTX      Tx
	snapshots map[string]snapshotData
	snTX      map[string]Tx
}

func (s *snTX) DropBucket(bucket string) error {
	return s.dbTX.(BucketMigrator).DropBucket(bucket)
}

func (s *snTX) CreateBucket(bucket string) error {
	return s.dbTX.(BucketMigrator).CreateBucket(bucket)
}

func (s *snTX) ExistsBucket(bucket string) bool {
	//todo snapshot check?
	return s.dbTX.(BucketMigrator).ExistsBucket(bucket)
}

func (s *snTX) ClearBucket(bucket string) error {
	return s.dbTX.(BucketMigrator).ClearBucket(bucket)
}

func (s *snTX) ExistingBuckets() ([]string, error) {
	return s.dbTX.(BucketMigrator).ExistingBuckets()
}

func (s *snTX) Cursor(bucket string) Cursor {
	tx, err := s.getSnapshotTX(bucket)
	if err != nil && !errors.Is(err, ErrUnavailableSnapshot) {
		panic(err.Error())
	}
	//process only db buckets
	if errors.Is(err, ErrUnavailableSnapshot) {
		return s.dbTX.Cursor(bucket)
	}
	return &snCursor{
		dbCursor: s.dbTX.Cursor(bucket),
		snCursor: tx.Cursor(bucket),
	}
}

func (s *snTX) RwCursor(bucket string) RwCursor {
	return s.Cursor(bucket).(RwCursor)
}

func (s *snTX) CursorDupSort(bucket string) CursorDupSort {
	tx, err := s.getSnapshotTX(bucket)
	if err != nil && !errors.Is(err, ErrUnavailableSnapshot) {
		panic(err.Error())
	}
	//process only db buckets
	if errors.Is(err, ErrUnavailableSnapshot) {
		return s.dbTX.CursorDupSort(bucket)
	}
	dbc := s.dbTX.CursorDupSort(bucket)
	sncbc := tx.CursorDupSort(bucket)
	return &snCursorDup{
		snCursor{
			dbCursor: dbc,
			snCursor: sncbc,
		},
		dbc,
		sncbc,
	}
}

func (s *snTX) RwCursorDupSort(bucket string) RwCursorDupSort {
	return s.CursorDupSort(bucket).(RwCursorDupSort)
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
func (s *snTX) getSnapshotTX(bucket string) (Tx, error) {
	tx, ok := s.snTX[bucket]
	if ok {
		return tx, nil
	}
	sn, ok := s.snapshots[bucket]
	if !ok {
		return nil, fmt.Errorf("%s  %w", bucket, ErrUnavailableSnapshot)
	}
	var err error
	tx, err = sn.snapshot.Begin(context.TODO())
	if err != nil {
		return nil, err
	}

	s.snTX[bucket] = tx
	return tx, nil
}

func (s *snTX) HasOne(bucket string, key []byte) (bool, error) {
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

func (s *snTX) Commit(ctx context.Context) error {
	for i := range s.snTX {
		defer s.snTX[i].Rollback()
	}
	return s.dbTX.Commit(ctx)
}

func (s *snTX) Rollback() {
	for i := range s.snTX {
		defer s.snTX[i].Rollback()
	}
	s.dbTX.Rollback()

}

func (s *snTX) BucketSize(name string) (uint64, error) {
	panic("implement me")
}

func (s *snTX) Comparator(bucket string) dbutils.CmpFunc {
	return s.dbTX.Comparator(bucket)
}

func (s *snTX) IncrementSequence(bucket string, amount uint64) (uint64, error) {
	panic("implement me")
}

func (s *snTX) ReadSequence(bucket string) (uint64, error) {
	panic("implement me")
}

func (s *snTX) CHandle() unsafe.Pointer {
	return s.dbTX.CHandle()
}

var DeletedValue = []byte("it is deleted value")

type snCursor struct {
	dbCursor Cursor
	snCursor Cursor

	currentKey []byte
}


func (s *snCursor) First() ([]byte, []byte, error) {
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

func (s *snCursor) Seek(seek []byte) ([]byte, []byte, error) {
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
	return s.dbCursor.(RwCursor).Put(k, v)
}

func (s *snCursor) Append(k []byte, v []byte) error {
	return s.dbCursor.(RwCursor).Append(k, v)
}

func (s *snCursor) Delete(k, v []byte) error {
	return s.dbCursor.(RwCursor).Put(k, DeletedValue)
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
	snCursor
	dbCursorDup   CursorDupSort
	sndbCursorDup CursorDupSort
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

type KvData struct {
	K []byte
	V []byte
}

func GenStateData(data []KvData) (KV, error) {
	snapshot := NewLMDB().WithBucketsConfig(func(defaultBuckets dbutils.BucketsCfg) dbutils.BucketsCfg {
		return dbutils.BucketsCfg{
			dbutils.PlainStateBucket: dbutils.BucketConfigItem{},
		}
	}).InMem().MustOpen()

	err := snapshot.Update(context.Background(), func(tx RwTx) error {
		c := tx.RwCursor(dbutils.PlainStateBucket)
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
