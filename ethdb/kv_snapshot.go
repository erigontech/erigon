package ethdb

import (
	"bytes"
	"context"
	"fmt"
	"github.com/ledgerwatch/turbo-geth/common/dbutils"
	"github.com/ledgerwatch/turbo-geth/log"
)


func NewSnapshotKV() snapshotOpts {
	return snapshotOpts{}
}


type SnapshotKV struct {
	db KV
	snapshotDB KV
	snapshotPath string
}

type snapshotOpts struct {
	path     string
	db     KV

}

func (opts snapshotOpts) Path(path string) snapshotOpts {
	opts.path = path
	return opts
}

func (opts snapshotOpts) DB(db KV) snapshotOpts {
	opts.db = db
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
	snTx, err:=s.snapshotDB.Begin(ctx, false)
	if err!=nil {
		return err
	}
	dbTx,err:=s.db.Begin(ctx, false)
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

func (s SnapshotKV) Begin(ctx context.Context, writable bool) (Tx, error) {
	if !writable {

	}
	return s.db.Begin(ctx,writable)
}

func (s SnapshotKV) IdealBatchSize() int {
	return s.db.IdealBatchSize()
}

type snapshotTX struct {
	dbTX Tx
	snTX Tx
}


func (s *snapshotTX) Bucket(name []byte) Bucket {
	if bytes.Equal(name, dbutils.HeadHeaderKey) ||
		bytes.Equal(name, dbutils.HeaderPrefix) ||
		bytes.Equal(name, dbutils.HeaderNumberPrefix) {
		return &snapshotBucket{
			dbBucket: s.dbTX.Bucket(name),
			snBucket: s.snTX.Bucket(name),
		}
	}
	return s.dbTX.Bucket(name)
}

func (s *snapshotTX) Commit(ctx context.Context) error {
	defer s.snTX.Rollback()
	return s.dbTX.Commit(ctx)
}

func (s *snapshotTX) Rollback() {
	defer s.snTX.Rollback()
	defer s.dbTX.Rollback()
}

type snapshotBucket struct {
	dbBucket Bucket
	snBucket Bucket
}

func (s *snapshotBucket) Get(key []byte) (val []byte, err error) {
	v,err:=s.dbBucket.Get(key)
	if err==nil && v!=nil {
		return v, nil
	} else if err!=ErrKeyNotFound {
		return nil, err
	}
	return s.snBucket.Get(key)
}

func (s *snapshotBucket) Put(key []byte, value []byte) error {
	return s.dbBucket.Put(key, value)
}

func (s *snapshotBucket) Delete(key []byte) error {
	return s.dbBucket.Delete(key)
}

func (s *snapshotBucket) Cursor() Cursor {
	return &snapshotCursor{
		snCursor: s.snBucket.Cursor(),
		dbCursor: s.dbBucket.Cursor(),
	}
}

func (s *snapshotBucket) Size() (uint64, error) {
	dbSize,err:=s.dbBucket.Size()
	if err!=nil {
		return 0, fmt.Errorf("db err %w", err)
	}
	snSize,err:=s.snBucket.Size()
	if err!=nil {
		return 0, fmt.Errorf("Snapshot db err %w", err)
	}
	return dbSize+snSize, nil
}

func (s *snapshotBucket) Clear() error {
	return s.dbBucket.Clear()
}

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
	s.lastSNDBKey, s.lastDBVal, err = s.snCursor.Seek(seek)
	if err!=nil {
		return nil, nil, err
	}
	s.lastDBKey, s.lastDBVal, err = s.dbCursor.Seek(seek)
	if err!=nil {
		return nil, nil, err
	}
	cmp, br:=keyCmp(s.lastDBKey, s.lastSNDBKey)
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
	panic("implement me")
}

func (s *snapshotCursor) Walk(walker func(k []byte, v []byte) (bool, error)) error {
	panic("implement me")
}

func (s *snapshotCursor) Put(key []byte, value []byte) error {
	panic("implement me")
}

func (s *snapshotCursor) Delete(key []byte) error {
	panic("implement me")
}

func (s *snapshotCursor) Append(key []byte, value []byte) error {
	panic("implement me")
}


func keyCmp(key1, key2 []byte) (int, bool) {
	switch {
	case key1 == nil && key2 == nil:
		return 0, true
	case key1 == nil && key2 != nil:
		return 1, false
	case key1 != nil && key2 == nil:
		return -1, false
	default:
		return bytes.Compare(key1, key2), false
	}
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
