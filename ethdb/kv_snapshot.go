package ethdb

import (
	"context"
	"fmt"
	"github.com/ledgerwatch/turbo-geth/common"
	"github.com/ledgerwatch/turbo-geth/log"
)

var (
	_ KV = &SnapshotKV{}
	_ BucketMigrator = &snapshotTX{}
	_ Tx = &snapshotTX{}
	_ Cursor = &snapshotCursor{}
)

func (s *snapshotTX) DropBucket(bucket string) error {
	db,ok:=s.dbTX.(BucketMigrator)
	if !ok {
		return fmt.Errorf("%T doesn't implement ethdb.TxMigrator interface", s.dbTX)
	}
	return db.DropBucket(bucket)
}

func (s *snapshotTX) CreateBucket(bucket string) error {
	db,ok:=s.dbTX.(BucketMigrator)
	if !ok {
		return fmt.Errorf("%T doesn't implement ethdb.TxMigrator interface", s.dbTX)
	}
	return db.CreateBucket(bucket)
}

func (s *snapshotTX) ExistsBucket(bucket string) bool {
	db,ok:=s.dbTX.(BucketMigrator)
	if !ok {
		return false
	}
	return db.ExistsBucket(bucket)
}

func (s *snapshotTX) ClearBucket(bucket string) error {
	db,ok:=s.dbTX.(BucketMigrator)
	if !ok {
		return fmt.Errorf("%T doesn't implement ethdb.TxMigrator interface", s.dbTX)
	}
	return db.ClearBucket(bucket)
}

func (s *snapshotTX) ExistingBuckets() ([]string, error) {
	db,ok:=s.dbTX.(BucketMigrator)
	if !ok {
		return []string{},fmt.Errorf("%T doesn't implement ethdb.TxMigrator interface", s.dbTX)
	}
	return db.ExistingBuckets()
}

type SnapshotUsageOpts struct{
	Path string
	ForBuckets [][]byte
}

func NewSnapshotKV() snapshotOpts {
	return snapshotOpts{}
}


type SnapshotKV struct {
	db KV
	snapshotDB KV
	snapshotPath string
	forBuckets [][]byte
}

type snapshotOpts struct {
	path     string
	db     KV
	forBuckets [][]byte
}

func (opts snapshotOpts) Path(path string) snapshotOpts {
	opts.path = path
	return opts
}

func (opts snapshotOpts) DB(db KV) snapshotOpts {
	opts.db = db
	return opts
}

func (opts snapshotOpts) For(bucket []byte) snapshotOpts {
	opts.forBuckets = append(opts.forBuckets, bucket)
	return opts
}


func (opts snapshotOpts) Open() KV {
	snapshotDB,err:=NewLMDB().Path(opts.path).ReadOnly().Open()
	if err!=nil {
		log.Warn("Snapshot db has not opened", "err", err)
	}
	return &SnapshotKV{
		snapshotDB: snapshotDB,
		db: opts.db,
		snapshotPath: opts.path,
		forBuckets: opts.forBuckets,
	}
}

func (s SnapshotKV) View(ctx context.Context, f func(tx Tx) error) error {
	if s.snapshotDB==nil {
		snapshotDB,err:=NewLMDB().Path(s.snapshotPath).ReadOnly().Open()
		if err!=nil {
			log.Warn("Snapshot db has not opened", "err", err)
			return s.db.View(ctx, f)
		}
		s.snapshotDB=snapshotDB
	}

	snTx, err:=s.snapshotDB.Begin(ctx, nil,false)
	if err!=nil {
		return err
	}
	dbTx,err:=s.db.Begin(ctx, nil, false)
	if err!=nil {
		return err
	}
	defer dbTx.Rollback()
	defer snTx.Rollback()

	t:=&snapshotTX{
		dbTX: dbTx,
		snTX: snTx,
	}
	return f(t)
}


func (s SnapshotKV) Update(ctx context.Context, f func(tx Tx) error) error {
	return s.db.Update(ctx, f)
}

func (s SnapshotKV) Close() {
	defer s.db.Close()
	if s.snapshotDB!=nil {
		defer s.snapshotDB.Close()
	}
}

func (s SnapshotKV) Begin(ctx context.Context, parentTx Tx, writable bool) (Tx, error) {
	snTx,err:=s.snapshotDB.Begin(ctx, parentTx, false)
	if err!=nil {
		return nil, err
	}
	dbTx,err:= s.db.Begin(ctx, parentTx, writable)
	if err!=nil {
		return nil, err
	}

	t:=&snapshotTX{
		dbTX: dbTx,
		snTX: snTx,
	}
	return t, nil
}

func (s SnapshotKV) IdealBatchSize() int {
	return s.db.IdealBatchSize()
}

type snapshotTX struct {
	dbTX Tx
	snTX Tx
}



func (s *snapshotTX) Commit(ctx context.Context) error {
	defer s.snTX.Rollback()
	return s.dbTX.Commit(ctx)
}

func (s *snapshotTX) Rollback() {
	defer s.snTX.Rollback()
	defer s.dbTX.Rollback()
}

func (s *snapshotTX) Cursor(bucket string) Cursor {
	return &snapshotCursor{
				snCursor: s.snTX.Cursor(bucket),
				dbCursor: s.dbTX.Cursor(bucket),
			}
}

func (s *snapshotTX) Get(bucket string, key []byte) (val []byte, err error) {
	v,err:=s.snTX.Get(bucket, key)
	if err==nil && v!=nil {
		return v, nil
	} else if err!=nil && err!=ErrKeyNotFound {
		return nil, err
	}
	return s.dbTX.Get(bucket, key)

}

func (s *snapshotTX) BucketSize(name string) (uint64, error) {
	dbSize,err:=s.snTX.BucketSize(name)
	if err!=nil {
		return 0, fmt.Errorf("db err %w", err)
	}
	snSize,err:=s.dbTX.BucketSize(name)
	if err!=nil {
		return 0, fmt.Errorf("Snapshot db err %w", err)
	}
	return dbSize+snSize, nil

}







//func (s *snapshotTX) Bucket(name []byte) Bucket {
//	if bytes.Equal(name, dbutils.HeadHeaderKey) ||
//		bytes.Equal(name, dbutils.HeaderPrefix) ||
//		bytes.Equal(name, dbutils.HeaderNumberPrefix) {
//		return &snapshotBucket{
//			dbBucket: s.dbTX.Bucket(name),
//			snBucket: s.snTX.Bucket(name),
//		}
//	}
//	return s.dbTX.Bucket(name)
//}


//func (s *snapshotBucket) Cursor() Cursor {
//	return &snapshotCursor{
//		snCursor: s.snBucket.Cursor(),
//		dbCursor: s.dbBucket.Cursor(),
//	}
//}

//func (s *snapshotBucket) Clear() error {
//	return s.dbBucket.Clear()
//}

type snapshotCursor struct {
	snCursor Cursor
	dbCursor Cursor

	lastDBKey []byte
	lastSNDBKey []byte
	lastDBVal []byte
	lastSNDBVal []byte
	keyCmp int

}

func (s *snapshotCursor) Prefix(v []byte) Cursor {
	panic("implement me")
}

func (s *snapshotCursor) MatchBits(u uint) Cursor {
	panic("implement me")
}

func (s *snapshotCursor) Prefetch(v uint) Cursor {
	panic("implement me")
}

func (s *snapshotCursor) NoValues() NoValuesCursor {
	panic("implement me")
}

func (s *snapshotCursor) First() ([]byte, []byte, error) {
	panic("implement me")
}

func (s *snapshotCursor) Seek(seek []byte) ([]byte, []byte, error) {
	var err error
	s.lastSNDBKey, s.lastSNDBVal, err = s.snCursor.Seek(seek)
	if err!=nil {
		return nil, nil, err
	}
	s.lastDBKey, s.lastDBVal, err = s.dbCursor.Seek(seek)
	if err!=nil {
		return nil, nil, err
	}
	cmp, br:=common.KeyCmp(s.lastDBKey, s.lastSNDBKey)
	if br {
		return nil,nil,nil
	}

	s.keyCmp=cmp
	if cmp <=0 {
		return s.lastDBKey, s.lastDBVal, nil
	} else {
		return s.lastSNDBKey, s.lastSNDBVal, nil
	}
}

func (s *snapshotCursor) SeekTo(seek []byte) ([]byte, []byte, error) {
	panic("implement me")
}

func (s *snapshotCursor) Next() ([]byte, []byte, error) {
	var err error
	if s.keyCmp>=0 {
		s.lastSNDBKey, s.lastSNDBVal, err = s.snCursor.Next()
	}
	if err!=nil {
		return nil, nil, err
	}
	if s.keyCmp<=0 {
		s.lastDBKey, s.lastDBVal, err = s.dbCursor.Next()
	}
	if err!=nil {
		return nil, nil, err
	}

	cmp, br:=common.KeyCmp(s.lastDBKey, s.lastSNDBKey)
	if br {
		return nil,nil,nil
	}

	s.keyCmp=cmp
	if cmp <=0 {
		return s.lastDBKey, s.lastDBVal, nil
	} else {
		return s.lastSNDBKey, s.lastSNDBVal, nil
	}
}

func (s *snapshotCursor) Walk(walker func(k []byte, v []byte) (bool, error)) error {
	panic("implement me")
}

func (s *snapshotCursor) Put(key []byte, value []byte) error {
	return s.dbCursor.Put(key, value)
}

func (s *snapshotCursor) Delete(key []byte) error {
	return s.dbCursor.Delete(key)
}

func (s *snapshotCursor) Append(key []byte, value []byte) error {
	return s.dbCursor.Append(key, value)
}

func (s *snapshotCursor) SeekExact(key []byte) ([]byte, error) {
	panic("implement me")
}

func (s *snapshotCursor) Last() ([]byte, []byte, error) {
	panic("implement me")
}


/*
number=10487424

WriteHeadHeaderHash - db.Put(dbutils.HeadHeaderKey, dbutils.HeadHeaderKey, hash.Bytes())
DeleteCanonicalHash - db.Delete(dbutils.HeaderPrefix, dbutils.HeaderHashKey(number))
WriteCanonicalHash - db.Put(dbutils.HeaderPrefix, dbutils.HeaderHashKey(number), hash.Bytes())
WriteHeader - db.Put(dbutils.HeaderNumberPrefix, hash[:], encoded),  db.Put(dbutils.HeaderPrefix, dbutils.HeaderKey(number, hash), data)
WriteTd - db.Put(dbutils.HeaderPrefix, dbutils.HeaderTDKey(number, hash), data)

ReadHeader
ReadCanonicalHash
ReadHeaderNumber
ReadHeadHeaderHash
ReadTd
*/
