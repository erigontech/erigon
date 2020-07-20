package ethdb

import (
	"bytes"
	"context"
	"fmt"
	"io/ioutil"
	"os"
	"path"
	"sync"
	"time"

	"github.com/ledgerwatch/lmdb-go/exp/lmdbpool"
	"github.com/ledgerwatch/lmdb-go/lmdb"
	"github.com/ledgerwatch/turbo-geth/common"
	"github.com/ledgerwatch/turbo-geth/common/dbutils"
	"github.com/ledgerwatch/turbo-geth/log"
)

type lmdbOpts struct {
	path     string
	inMem    bool
	readOnly bool
}

func (opts lmdbOpts) Path(path string) lmdbOpts {
	opts.path = path
	return opts
}

func (opts lmdbOpts) InMem() lmdbOpts {
	opts.inMem = true
	return opts
}

func (opts lmdbOpts) ReadOnly() lmdbOpts {
	opts.readOnly = true
	return opts
}

func (opts lmdbOpts) Open() (KV, error) {
	env, err := lmdb.NewEnv()
	if err != nil {
		return nil, err
	}
	err = env.SetMaxDBs(100)
	if err != nil {
		return nil, err
	}

	var logger log.Logger

	if opts.inMem {
		err = env.SetMapSize(32 << 20) // 32MB
		logger = log.New("lmdb", "inMem")
		if err != nil {
			return nil, err
		}
		opts.path, _ = ioutil.TempDir(os.TempDir(), "lmdb")
	} else {
		err = env.SetMapSize(32 << 40) // 32TB
		logger = log.New("lmdb", path.Base(opts.path))
		if err != nil {
			return nil, err
		}
	}
	if err = os.MkdirAll(opts.path, 0744); err != nil {
		return nil, fmt.Errorf("could not create dir: %s, %w", opts.path, err)
	}

	var flags uint = lmdb.NoReadahead
	if opts.readOnly {
		flags |= lmdb.Readonly
	}
	if opts.inMem {
		flags |= lmdb.NoSync | lmdb.NoMetaSync
	}
	err = env.Open(opts.path, flags, 0664)
	if err != nil {
		return nil, err
	}

	db := &LmdbKV{
		opts:       opts,
		env:        env,
		log:        logger,
		lmdbTxPool: lmdbpool.NewTxnPool(env),
		wg:         &sync.WaitGroup{},
	}

	db.buckets = make([]lmdb.DBI, len(dbutils.Buckets))
	if opts.readOnly {
		if err := env.View(func(tx *lmdb.Txn) error {
			for _, name := range dbutils.Buckets {
				dbi, createErr := tx.OpenDBI(string(name), 0)
				if createErr != nil {
					return createErr
				}
				db.buckets[dbutils.BucketsIndex[string(name)]] = dbi
			}
			return nil
		}); err != nil {
			return nil, err
		}
	} else {
		if err := env.Update(func(tx *lmdb.Txn) error {
			for id := range dbutils.Buckets {
				if err := createBucket(tx, db, id); err != nil {
					return err
				}
			}
			return nil
		}); err != nil {
			return nil, err
		}
	}

	if !opts.inMem {
		ctx, ctxCancel := context.WithCancel(context.Background())
		db.stopStaleReadsCheck = ctxCancel
		db.wg.Add(1)
		go func() {
			defer db.wg.Done()
			ticker := time.NewTicker(time.Minute)
			defer ticker.Stop()
			db.staleReadsCheckLoop(ctx, ticker)
		}()
	}

	return db, nil
}

func createBucket(tx *lmdb.Txn, db *LmdbKV, id int) error {
	var flags uint = lmdb.Create
	for _, c := range dbutils.DupSortConfig {
		if c.ID != id {
			continue
		}
		flags |= lmdb.DupSort
		break
	}

	dbi, err := tx.OpenDBI(string(dbutils.Buckets[id]), flags)
	if err != nil {
		return err
	}
	db.buckets[id] = dbi
	return nil
}

func (opts lmdbOpts) MustOpen() KV {
	db, err := opts.Open()
	if err != nil {
		panic(fmt.Errorf("fail to open lmdb: %w", err))
	}
	return db
}

type LmdbKV struct {
	opts                lmdbOpts
	env                 *lmdb.Env
	log                 log.Logger
	buckets             []lmdb.DBI
	lmdbTxPool          *lmdbpool.TxnPool // pool of lmdb.Txn objects
	stopStaleReadsCheck context.CancelFunc
	wg                  *sync.WaitGroup
}

func NewLMDB() lmdbOpts {
	return lmdbOpts{}
}

// staleReadsCheckLoop - In any real application it is important to check for readers that were
// never closed by their owning process, and for which the owning process
// has exited.  See the documentation on transactions for more information.
func (db *LmdbKV) staleReadsCheckLoop(ctx context.Context, ticker *time.Ticker) {
	for {
		// check once on app start
		staleReaders, err2 := db.env.ReaderCheck()
		if err2 != nil {
			db.log.Error("failed ReaderCheck", "err", err2)
		}
		if staleReaders > 0 {
			db.log.Info("cleared reader slots from dead processes", "amount", staleReaders)
		}

		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
		}
	}
}

// Close closes db
// All transactions must be closed before closing the database.
func (db *LmdbKV) Close() {
	if db.lmdbTxPool != nil {
		db.lmdbTxPool.Close()
	}
	if db.stopStaleReadsCheck != nil {
		db.stopStaleReadsCheck()
	}
	db.wg.Wait()

	if db.env != nil {
		if err := db.env.Close(); err != nil {
			db.log.Warn("failed to close DB", "err", err)
		} else {
			db.log.Info("database closed (LMDB)")
		}
		db.env = nil
	}

	if db.opts.inMem {
		if err := os.RemoveAll(db.opts.path); err != nil {
			db.log.Warn("failed to remove in-mem db file", "err", err)
		}
	}

}

func (db *LmdbKV) DiskSize(_ context.Context) (common.StorageSize, error) {
	return common.StorageSize(0), nil
}

func (db *LmdbKV) BucketsStat(_ context.Context) (map[string]common.StorageBucketWriteStats, error) {
	return map[string]common.StorageBucketWriteStats{}, nil
}

func (db *LmdbKV) dbi(bucket []byte) lmdb.DBI {
	if id, ok := dbutils.BucketsIndex[string(bucket)]; ok {
		return db.buckets[id]
	}
	panic(fmt.Errorf("unknown bucket: %s. add it to dbutils.Buckets", string(bucket)))
}

func (db *LmdbKV) IdealBatchSize() int {
	return 50 * 1024 * 1024 // 50 Mb
}

func (db *LmdbKV) Begin(ctx context.Context, writable bool) (Tx, error) {
	flags := uint(0)
	if !writable {
		flags |= lmdb.Readonly
	}
	tx, err := db.env.BeginTxn(nil, flags)
	if err != nil {
		return nil, err
	}

	tx.RawRead = true
	return &lmdbTx{
		db:  db,
		ctx: ctx,
		tx:  tx,
	}, nil
}

type lmdbTx struct {
	tx      *lmdb.Txn
	ctx     context.Context
	db      *LmdbKV
	cursors []*lmdb.Cursor
}

type lmdbBucket struct {
	id        int
	isDupsort bool
	dupFrom   int
	dupTo     int
	tx        *lmdbTx
	dbi       lmdb.DBI
}

type LmdbCursor struct {
	ctx    context.Context
	bucket *lmdbBucket
	prefix []byte

	cursor *lmdb.Cursor
}

func (db *LmdbKV) View(ctx context.Context, f func(tx Tx) error) (err error) {
	if db.env == nil {
		return fmt.Errorf("db closed")
	}
	t := &lmdbTx{db: db, ctx: ctx}
	return db.lmdbTxPool.View(func(tx *lmdb.Txn) error {
		defer t.closeCursors()
		tx.Pooled, tx.RawRead = true, true
		t.tx = tx
		return f(t)
	})
}

func (db *LmdbKV) Update(ctx context.Context, f func(tx Tx) error) (err error) {
	t := &lmdbTx{db: db, ctx: ctx}
	return db.env.Update(func(tx *lmdb.Txn) error {
		defer t.closeCursors()
		tx.RawRead = true
		t.tx = tx
		return f(t)
	})
}

func (tx *lmdbTx) Bucket(name []byte) Bucket {
	id, ok := dbutils.BucketsIndex[string(name)]
	if !ok {
		panic(fmt.Errorf("unknown bucket: %s. add it to dbutils.Buckets", string(name)))
	}

	b := &lmdbBucket{tx: tx, id: id, dbi: tx.db.buckets[id]}
	for _, c := range dbutils.DupSortConfig {
		if c.ID != id {
			continue
		}
		b.isDupsort = true
		b.dupTo = c.ToLen
		b.dupFrom = c.FromLen
		break
	}

	return b
}

func (tx *lmdbTx) Commit(ctx context.Context) error {
	tx.closeCursors()
	return tx.tx.Commit()
}

func (tx *lmdbTx) Rollback() {
	tx.closeCursors()
	tx.tx.Reset()
}

func (tx *lmdbTx) closeCursors() {
	for _, c := range tx.cursors {
		if c != nil {
			c.Close()
		}
	}
	tx.cursors = tx.cursors[:0]
}

func (c *LmdbCursor) Prefix(v []byte) Cursor {
	c.prefix = v
	return c
}

func (c *LmdbCursor) MatchBits(n uint) Cursor {
	panic("not implemented yet")
}

func (c *LmdbCursor) Prefetch(v uint) Cursor {
	//c.cursorOpts.PrefetchSize = int(v)
	return c
}

func (c *LmdbCursor) NoValues() NoValuesCursor {
	//c.cursorOpts.PrefetchValues = false
	return &lmdbNoValuesCursor{LmdbCursor: c}
}

func (b lmdbBucket) Get(key []byte) ([]byte, error) {
	if b.isDupsort {
		return b.getDupSort(key)
	}

	val, err := b.tx.tx.Get(b.dbi, key)
	if err != nil {
		if lmdb.IsNotFound(err) {
			return nil, nil
		}
		return nil, err
	}
	return val, nil
}

func (b lmdbBucket) getDupSort(key []byte) ([]byte, error) {
	c := b.Cursor().(*LmdbCursor)
	if err := c.initCursor(); err != nil {
		return nil, err
	}
	if len(key) == b.dupFrom {
		_, v, err := c.cursor.Get(key[:b.dupTo], key[b.dupTo:], lmdb.GetBothRange)
		if err != nil {
			if lmdb.IsNotFound(err) {
				return nil, nil
			}
			return nil, err
		}
		if !bytes.Equal(key[b.dupTo:], v[:b.dupFrom-b.dupTo]) {
			return nil, nil
		}
		return v[b.dupFrom-b.dupTo:], nil
	}

	_, v, err := c.cursor.Get(key, nil, lmdb.Set)
	if err != nil {
		if lmdb.IsNotFound(err) {
			return nil, nil
		}
		return nil, err
	}
	return v, nil
}
func (b *lmdbBucket) Put(key []byte, value []byte) error {
	return b.Cursor().Put(key, value)
}

func (b *lmdbBucket) Delete(key []byte) error {
	return b.Cursor().Delete(key)
}

func (b *lmdbBucket) Size() (uint64, error) {
	st, err := b.tx.tx.Stat(b.dbi)
	if err != nil {
		return 0, err
	}
	return (st.LeafPages + st.BranchPages + st.OverflowPages) * uint64(os.Getpagesize()), nil
}

func (b *lmdbBucket) Clear() error {
	if err := b.tx.tx.Drop(b.dbi, true); err != nil {
		return err
	}
	if err := createBucket(b.tx.tx, b.tx.db, b.id); err != nil {
		return err
	}
	return nil
}

func (b *lmdbBucket) Cursor() Cursor {
	return &LmdbCursor{bucket: b, ctx: b.tx.ctx}
}

func (c *LmdbCursor) initCursor() error {
	if c.cursor != nil {
		return nil
	}
	tx := c.bucket.tx

	var err error
	c.cursor, err = tx.tx.OpenCursor(c.bucket.dbi)
	if err != nil {
		return err
	}

	// add to auto-cleanup on end of transactions
	if tx.cursors == nil {
		tx.cursors = make([]*lmdb.Cursor, 0, 1)
	}
	tx.cursors = append(tx.cursors, c.cursor)
	return nil
}

func (c *LmdbCursor) First() ([]byte, []byte, error) {
	if c.cursor == nil {
		if err := c.initCursor(); err != nil {
			return []byte{}, nil, err
		}
	}

	return c.Seek(c.prefix)
}

func (c *LmdbCursor) Seek(seek []byte) (k, v []byte, err error) {
	if c.cursor == nil {
		if err := c.initCursor(); err != nil {
			return []byte{}, nil, err
		}
	}

	if c.bucket.isDupsort {
		return c.seekDupSort(seek)
	}

	if len(seek) == 0 {
		k, v, err = c.cursor.Get(nil, nil, lmdb.First)
	} else {
		k, v, err = c.cursor.Get(seek, nil, lmdb.SetRange)
	}
	if err != nil {
		if lmdb.IsNotFound(err) {
			return nil, nil, nil
		}
		err = fmt.Errorf("failed LmdbKV cursor.Seek(): %w, bucket: %d %s, isDupsort: %t, key: %x, dupsortConfig: %+v", err, c.bucket.id, dbutils.Buckets[c.bucket.id], c.bucket.isDupsort, seek, dbutils.DupSortConfig)
		return []byte{}, nil, err
	}
	if c.prefix != nil && !bytes.HasPrefix(k, c.prefix) {
		k, v = nil, nil
	}

	return k, v, nil
}

func (c *LmdbCursor) seekDupSort(seek []byte) (k, v []byte, err error) {
	b := c.bucket
	if len(seek) == 0 {
		k, v, err = c.cursor.Get(nil, nil, lmdb.First)
		if err != nil {
			if lmdb.IsNotFound(err) {
				return nil, nil, nil
			}
			return []byte{}, nil, err
		}
		if c.prefix != nil && !bytes.HasPrefix(k, c.prefix) {
			k, v = nil, nil
		}
		return k, v, nil
	}

	var seek1, seek2 []byte
	if len(seek) > b.dupTo {
		seek1, seek2 = seek[:b.dupTo], seek[b.dupTo:]
	} else {
		seek1 = seek
	}
	k, v, err = c.cursor.Get(seek1, nil, lmdb.SetRange)
	if err != nil {
		if lmdb.IsNotFound(err) {
			return nil, nil, nil
		}

		return []byte{}, nil, err
	}

	if seek2 != nil && bytes.Equal(seek1, k) {
		k, v, err = c.cursor.Get(seek1, seek2, lmdb.GetBothRange)
		if err != nil && lmdb.IsNotFound(err) {
			k, v, err = c.cursor.Get(nil, nil, lmdb.Next)
			if err != nil {
				if lmdb.IsNotFound(err) {
					return nil, nil, nil
				}
				return []byte{}, nil, err
			}
		} else if err != nil {
			return []byte{}, nil, err
		}
	}

	if len(k) == b.dupTo {
		k2 := make([]byte, 0, len(k)+b.dupFrom-b.dupTo)
		k2 = append(append(k2, k...), v[:b.dupFrom-b.dupTo]...)
		v = v[b.dupFrom-b.dupTo:]
		k = k2
	}

	if c.prefix != nil && !bytes.HasPrefix(k, c.prefix) {
		k, v = nil, nil
	}
	return k, v, nil
}

func (c *LmdbCursor) SeekTo(seek []byte) ([]byte, []byte, error) {
	return c.Seek(seek)
}

func (c *LmdbCursor) Next() (k, v []byte, err error) {
	select {
	case <-c.ctx.Done():
		return []byte{}, nil, c.ctx.Err()
	default:
	}

	if c.bucket.isDupsort {
		return c.nextDupSort()
	}

	k, v, err = c.cursor.Get(nil, nil, lmdb.Next)
	if err != nil {
		if lmdb.IsNotFound(err) {
			return nil, nil, nil
		}
		return []byte{}, nil, fmt.Errorf("failed LmdbKV cursor.Next(): %w", err)
	}
	if c.prefix != nil && !bytes.HasPrefix(k, c.prefix) {
		k, v = nil, nil
	}

	return k, v, nil
}

func (c *LmdbCursor) nextDupSort() (k, v []byte, err error) {
	b := c.bucket
	k, v, err = c.cursor.Get(nil, nil, lmdb.NextDup)
	if err != nil && lmdb.IsNotFound(err) {
		k, v, err = c.cursor.Get(nil, nil, lmdb.Next)
		if err != nil {
			if lmdb.IsNotFound(err) {
				return nil, nil, nil
			}
			return []byte{}, nil, fmt.Errorf("failed LmdbKV cursor.Next(): %w", err)
		}
	} else if err != nil {
		return nil, nil, err
	}
	if len(k) == b.dupTo {
		k = append(k, v[:b.dupFrom-b.dupTo]...)
		v = v[b.dupFrom-b.dupTo:]
	}

	if c.prefix != nil && !bytes.HasPrefix(k, c.prefix) {
		k, v = nil, nil
	}
	return k, v, nil
}

func (c *LmdbCursor) Delete(key []byte) error {
	select {
	case <-c.ctx.Done():
		return c.ctx.Err()
	default:
	}

	if c.cursor == nil {
		if err := c.initCursor(); err != nil {
			return err
		}
	}

	if c.bucket.isDupsort {
		return c.deleteDupSort(key)
	}

	_, _, err := c.cursor.Get(key, nil, lmdb.Set)
	if err != nil {
		if lmdb.IsNotFound(err) {
			return nil
		}
		return err
	}

	return c.cursor.Del(0)
}

func (c *LmdbCursor) deleteDupSort(key []byte) error {
	b := c.bucket
	if len(key) != b.dupFrom && len(key) >= b.dupTo {
		return fmt.Errorf("dupsort bucket: %s, can have keys of len==%d and len<%d. key: %x", dbutils.Buckets[b.id], b.dupFrom, b.dupTo, key)
	}

	if len(key) == b.dupFrom {
		b := c.bucket
		_, v, err := c.cursor.Get(key[:b.dupTo], key[b.dupTo:], lmdb.GetBothRange)
		if err != nil { // if key not found, or found another one - then nothing to delete
			if lmdb.IsNotFound(err) {
				return nil
			}
			return err
		}
		if !bytes.Equal(v[:b.dupFrom-b.dupTo], key[b.dupTo:]) {
			return nil
		}
		return c.cursor.Del(0)
	}

	_, _, err := c.cursor.Get(key, nil, lmdb.Set)
	if err != nil {
		if lmdb.IsNotFound(err) {
			return nil
		}
		return err
	}

	return c.cursor.Del(0)
}

func (c *LmdbCursor) Put(key []byte, value []byte) error {
	select {
	case <-c.ctx.Done():
		return c.ctx.Err()
	default:
	}

	if len(key) == 0 {
		return fmt.Errorf("lmdb doesn't support empty keys. bucket: %s", dbutils.Buckets[c.bucket.id])
	}
	if c.cursor == nil {
		if err := c.initCursor(); err != nil {
			return err
		}
	}

	if c.bucket.isDupsort {
		return c.putDupSort(key, value)
	}

	return c.cursor.Put(key, value, 0)
}

func (c *LmdbCursor) putDupSort(key []byte, value []byte) error {
	b := c.bucket
	if len(key) != b.dupFrom && len(key) >= b.dupTo {
		return fmt.Errorf("dupsort bucket: %s, can have keys of len==%d and len<%d. key: %x", dbutils.Buckets[b.id], b.dupFrom, b.dupTo, key)
	}
	if len(key) != b.dupFrom {
		_, _, err := c.cursor.Get(key, nil, lmdb.Set)
		if err != nil {
			if lmdb.IsNotFound(err) {
				return c.cursor.Put(key, value, 0)
			}
			return err
		}

		// From docs of .Put(MDB_CURRENT) flag:
		// If using sorted duplicates (#MDB_DUPSORT) the data item must still
		// sort into the same place. This is intended to be used when the
		// new data is the same size as the old.
		//
		// It's not achivable, then just delete and insert
		// maybe can be optimized in future
		err = c.cursor.Del(0)
		if err != nil {
			return err
		}
		return c.cursor.Put(key, value, 0)
	}

	newValue := make([]byte, 0, b.dupFrom-b.dupTo+len(value))
	newValue = append(append(newValue, key[b.dupTo:]...), value...)
	key = key[:b.dupTo]

	_, v, err := c.cursor.Get(key, newValue[:b.dupFrom-b.dupTo], lmdb.GetBothRange)
	if err != nil { // if key not found, or found another one - then just insert
		if lmdb.IsNotFound(err) {
			return c.cursor.Put(key, newValue, 0)
		}
		return err
	}

	if bytes.Equal(v[:b.dupFrom-b.dupTo], newValue[:b.dupFrom-b.dupTo]) {
		err = c.cursor.Del(0)
		if err != nil {
			return err
		}
	}

	return c.cursor.Put(key, newValue, 0)
}

// Append - speedy feature of lmdb which is not part of KV interface.
// Cast your cursor to *LmdbCursor to use this method.
// Danger: if provided data will not sorted (or bucket have old records which mess with new in sorting manner) - db will corrupt.
func (c *LmdbCursor) Append(key []byte, value []byte) error {
	if len(key) == 0 {
		return fmt.Errorf("lmdb doesn't support empty keys. bucket: %s", dbutils.Buckets[c.bucket.id])
	}

	if c.cursor == nil {
		if err := c.initCursor(); err != nil {
			return err
		}
	}
	b := c.bucket
	if b.isDupsort {
		if len(key) != b.dupFrom && len(key) >= b.dupTo {
			return fmt.Errorf("dupsort bucket: %s, can have keys of len==%d and len<%d. key: %x", dbutils.Buckets[b.id], b.dupFrom, b.dupTo, key)
		}

		if len(key) == b.dupFrom {
			newValue := make([]byte, 0, b.dupFrom-b.dupTo+len(value))
			newValue = append(append(newValue, key[b.dupTo:]...), value...)
			key = key[:b.dupTo]
			return c.cursor.Put(key, newValue, lmdb.AppendDup)
		}
		return c.cursor.Put(key, value, lmdb.Append)
	}
	return c.cursor.Put(key, value, lmdb.Append)
}

func (c *LmdbCursor) Walk(walker func(k, v []byte) (bool, error)) error {
	for k, v, err := c.First(); k != nil; k, v, err = c.Next() {
		if err != nil {
			return err
		}
		ok, err := walker(k, v)
		if err != nil {
			return err
		}
		if !ok {
			return nil
		}
	}
	return nil
}

type lmdbNoValuesCursor struct {
	*LmdbCursor
}

func (c *lmdbNoValuesCursor) Walk(walker func(k []byte, vSize uint32) (bool, error)) error {
	for k, vSize, err := c.First(); k != nil; k, vSize, err = c.Next() {
		if err != nil {
			return err
		}
		ok, err := walker(k, vSize)
		if err != nil {
			return err
		}
		if !ok {
			return nil
		}
	}
	return nil
}

func (c *lmdbNoValuesCursor) First() (k []byte, v uint32, err error) {
	return c.Seek(c.prefix)
}

func (c *lmdbNoValuesCursor) Seek(seek []byte) (k []byte, vSize uint32, err error) {
	k, v, err := c.LmdbCursor.Seek(seek)
	if err != nil {
		return []byte{}, 0, err
	}
	return k, uint32(len(v)), err
}

func (c *lmdbNoValuesCursor) SeekTo(seek []byte) ([]byte, uint32, error) {
	return c.Seek(seek)
}

func (c *lmdbNoValuesCursor) Next() (k []byte, vSize uint32, err error) {
	k, v, err := c.LmdbCursor.Next()
	if err != nil {
		return []byte{}, 0, err
	}
	return k, uint32(len(v)), err
}
