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

	"github.com/ledgerwatch/lmdb-go/lmdb"
	"github.com/ledgerwatch/turbo-geth/common/dbutils"
	"github.com/ledgerwatch/turbo-geth/log"
)

const LMDBMapSize = 2 * 1024 * 1024 * 1024 * 1024 // 2TB

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
		err = env.SetMapSize(LMDBMapSize)
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
		return nil, fmt.Errorf("%w, path: %s", err, opts.path)
	}

	db := &LmdbKV{
		opts: opts,
		env:  env,
		log:  logger,
		wg:   &sync.WaitGroup{},
	}

	db.buckets = make([]lmdb.DBI, len(dbutils.Buckets)+len(dbutils.DeprecatedBuckets))
	if opts.readOnly {
		if err := env.View(func(tx *lmdb.Txn) error {
			for _, name := range dbutils.Buckets {
				dbi, createErr := tx.OpenDBI(string(name), 0)
				if createErr != nil {
					return createErr
				}
				db.buckets[dbutils.BucketsCfg[string(name)].ID] = dbi
			}
			return nil
		}); err != nil {
			return nil, err
		}
	} else {
		if err := db.CreateBuckets(dbutils.Buckets...); err != nil {
			return nil, err
		}
		// don't create deprecated buckets
	}

	if err := env.View(func(tx *lmdb.Txn) error {
		for _, name := range dbutils.DeprecatedBuckets {
			dbi, createErr := tx.OpenDBI(string(name), 0)
			if createErr == nil {
				continue // if deprecated bucket couldn't be open - then it's deleted and it's fine
			}
			db.buckets[dbutils.BucketsCfg[string(name)].ID] = dbi
		}
		return nil
	}); err != nil {
		return nil, err
	}

	if !opts.inMem {
		if staleReaders, err := db.env.ReaderCheck(); err != nil {
			db.log.Error("failed ReaderCheck", "err", err)
		} else if staleReaders > 0 {
			db.log.Debug("cleared reader slots from dead processes", "amount", staleReaders)
		}
	}

	return db, nil
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
	stopStaleReadsCheck context.CancelFunc
	wg                  *sync.WaitGroup
}

func NewLMDB() lmdbOpts {
	return lmdbOpts{}
}

func (db *LmdbKV) CreateBuckets(buckets ...[]byte) error {
	for _, name := range buckets {
		name := name
		cfg, ok := dbutils.BucketsCfg[string(name)]
		if !ok {
			continue
		}

		var flags uint = lmdb.Create
		if cfg.IsDupsort {
			flags |= lmdb.DupSort
		}
		if err := db.Update(context.Background(), func(tx Tx) error {
			dbi, err := tx.(*lmdbTx).tx.OpenDBI(string(name), flags)
			if err != nil {
				return err
			}
			db.buckets[cfg.ID] = dbi
			return nil
		}); err != nil {
			return err
		}
	}
	return nil
}

func (db *LmdbKV) DropBuckets(buckets ...[]byte) error {
	if db.env == nil {
		return fmt.Errorf("db closed")
	}

	for _, name := range buckets {
		name := name
		cfg, ok := dbutils.BucketsCfg[string(name)]
		if !ok {
			panic(fmt.Errorf("unknown bucket: %s. add it to dbutils.Buckets", string(name)))
		}

		if cfg.ID < len(dbutils.Buckets) {
			return fmt.Errorf("only buckets from dbutils.DeprecatedBuckets can be deleted, bucket: %s", name)
		}

		if err := db.env.Update(func(txn *lmdb.Txn) error {
			dbi := db.buckets[cfg.ID]
			if dbi == 0 { // if bucket was not open on db start, then try to open it now, and if fail then nothing to drop
				var openErr error
				dbi, openErr = txn.OpenDBI(string(name), 0)
				if openErr != nil {
					return nil // DBI doesn't exists means no drop needed
				}
			}
			return txn.Drop(dbi, true)
		}); err != nil {
			return err
		}
	}

	return nil
}

// Close closes db
// All transactions must be closed before closing the database.
func (db *LmdbKV) Close() {
	if db.env != nil {
		db.wg.Wait()
	}

	if db.env != nil {
		env := db.env
		db.env = nil
		time.Sleep(10 * time.Millisecond) // TODO: remove after consensus/ethash/consensus.go:VerifyHeaders will spawn controllable goroutines
		if err := env.Close(); err != nil {
			db.log.Warn("failed to close DB", "err", err)
		} else {
			db.log.Info("database closed (LMDB)")
		}
	}

	if db.opts.inMem {
		if err := os.RemoveAll(db.opts.path); err != nil {
			db.log.Warn("failed to remove in-mem db file", "err", err)
		}
	}

}

func (db *LmdbKV) DiskSize(_ context.Context) (uint64, error) {
	stats, err := db.env.Stat()
	if err != nil {
		return 0, fmt.Errorf("could not read database size: %w", err)
	}
	return uint64(stats.PSize) * (stats.LeafPages + stats.BranchPages + stats.OverflowPages), nil
}

func (db *LmdbKV) dbi(bucket []byte) lmdb.DBI {
	if cfg, ok := dbutils.BucketsCfg[string(bucket)]; ok {
		return db.buckets[cfg.ID]
	}
	panic(fmt.Errorf("unknown bucket: %s. add it to dbutils.Buckets", string(bucket)))
}

func (db *LmdbKV) IdealBatchSize() int {
	return 50 * 1024 * 1024 // 50 Mb
}

func (db *LmdbKV) Begin(ctx context.Context, writable bool) (Tx, error) {
	if db.env == nil {
		return nil, fmt.Errorf("db closed")
	}
	db.wg.Add(1)
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
	name      []byte
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
	db.wg.Add(1)
	defer db.wg.Done()
	t := &lmdbTx{db: db, ctx: ctx}
	return db.env.View(func(tx *lmdb.Txn) error {
		defer t.closeCursors()
		tx.RawRead = true
		t.tx = tx
		return f(t)
	})
}

func (db *LmdbKV) Update(ctx context.Context, f func(tx Tx) error) (err error) {
	if db.env == nil {
		return fmt.Errorf("db closed")
	}
	db.wg.Add(1)
	defer db.wg.Done()
	t := &lmdbTx{db: db, ctx: ctx}
	return db.env.Update(func(tx *lmdb.Txn) error {
		defer t.closeCursors()
		tx.RawRead = true
		t.tx = tx
		return f(t)
	})
}

func (tx *lmdbTx) Bucket(name []byte) Bucket {
	cfg, ok := dbutils.BucketsCfg[string(name)]
	if !ok {
		panic(fmt.Errorf("unknown bucket: %s. add it to dbutils.Buckets", string(name)))
	}

	return &lmdbBucket{tx: tx, id: cfg.ID, dbi: tx.db.buckets[cfg.ID], isDupsort: cfg.IsDupsort, dupFrom: cfg.DupFromLen, dupTo: cfg.DupToLen, name: name}
}

func (tx *lmdbTx) Commit(ctx context.Context) error {
	if tx.db.env == nil {
		return fmt.Errorf("db closed")
	}
	defer tx.db.wg.Done()
	tx.closeCursors()
	return tx.tx.Commit()
}

func (tx *lmdbTx) Rollback() {
	if tx.db.env == nil {
		return
	}
	defer tx.db.wg.Done()
	tx.closeCursors()
	tx.tx.Abort()
}

func (tx *lmdbTx) get(dbi lmdb.DBI, key []byte) ([]byte, error) {
	return tx.tx.Get(dbi, key)
}

func (tx *lmdbTx) closeCursors() {
	for _, c := range tx.cursors {
		if c != nil {
			c.Close()
		}
	}
	tx.cursors = []*lmdb.Cursor{}
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

	val, err := b.tx.get(b.dbi, key)
	if err != nil {
		if lmdb.IsNotFound(err) {
			return nil, nil
		}
		return nil, err
	}
	return val, nil
}

func (b lmdbBucket) getDupSort(key []byte) ([]byte, error) {
	if len(key) == b.dupFrom {
		c := b.Cursor().(*LmdbCursor)
		if err := c.initCursor(); err != nil {
			return nil, err
		}
		_, v, err := c.dupBothRange(key[:b.dupTo], key[b.dupTo:])
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

	val, err := b.tx.get(b.dbi, key)
	if err != nil {
		if lmdb.IsNotFound(err) {
			return nil, nil
		}
		return nil, err
	}
	return val, nil
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
		k, v, err = c.setRange(seek)
	}
	if err != nil {
		if lmdb.IsNotFound(err) {
			return nil, nil, nil
		}
		err = fmt.Errorf("failed LmdbKV cursor.Seek(): %w, bucket: %s, isDupsort: %t, key: %x", err, c.bucket.name, c.bucket.isDupsort, seek)
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
	k, v, err = c.setRange(seek1)
	if err != nil {
		if lmdb.IsNotFound(err) {
			return nil, nil, nil
		}

		return []byte{}, nil, err
	}

	if seek2 != nil && bytes.Equal(seek1, k) {
		k, v, err = c.dupBothRange(seek1, seek2)
		if err != nil && lmdb.IsNotFound(err) {
			k, v, err = c.next()
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
		return fmt.Errorf("lmdb doesn't support empty keys. bucket: %s", c.bucket.name)
	}
	if c.cursor == nil {
		if err := c.initCursor(); err != nil {
			return err
		}
	}

	if c.bucket.isDupsort {
		return c.putDupSort(key, value)
	}

	return c.put(key, value)
}

func (c *LmdbCursor) putDupSort(key []byte, value []byte) error {
	b := c.bucket
	if len(key) != b.dupFrom && len(key) >= b.dupTo {
		return fmt.Errorf("dupsort bucket: %s, can have keys of len==%d and len<%d. key: %x", dbutils.Buckets[b.id], b.dupFrom, b.dupTo, key)
	}

	if len(key) != b.dupFrom {
		_, _, err := c.setExact(key)
		if err != nil {
			if lmdb.IsNotFound(err) {
				return c.put(key, value)
			}
			return err
		}

		return c.putCurrent(key, value)
	}

	newValue := make([]byte, 0, b.dupFrom-b.dupTo+len(value))
	newValue = append(append(newValue, key[b.dupTo:]...), value...)

	key = key[:b.dupTo]
	_, v, err := c.dupBothRange(key, newValue[:b.dupFrom-b.dupTo])
	if err != nil { // if key not found, or found another one - then just insert
		if lmdb.IsNotFound(err) {
			return c.put(key, newValue)
		}
		return err
	}

	if bytes.Equal(v[:b.dupFrom-b.dupTo], newValue[:b.dupFrom-b.dupTo]) {
		if len(v) == len(newValue) { // in DupSort case lmdb.Current works only with values of same length
			return c.putCurrent(key, newValue)
		}
		err = c.delCurrent()
		if err != nil {
			return err
		}
		return c.put(key, newValue)
	}

	return c.put(key, newValue)
}

func (c *LmdbCursor) setExact(key []byte) ([]byte, []byte, error) {
	return c.cursor.Get(key, nil, lmdb.Set)
}
func (c *LmdbCursor) setRange(key []byte) ([]byte, []byte, error) {
	return c.cursor.Get(key, nil, lmdb.SetRange)
}
func (c *LmdbCursor) next() ([]byte, []byte, error) {
	return c.cursor.Get(nil, nil, lmdb.Next)
}

func (c *LmdbCursor) dupBothRange(key []byte, value []byte) ([]byte, []byte, error) {
	k, v, err := c.cursor.Get(key, value, lmdb.GetBothRange)
	if err != nil {
		return []byte{}, nil, err
	}
	return k, v, nil
}

func (c *LmdbCursor) delCurrent() error {
	return c.cursor.Del(0)
}

func (c *LmdbCursor) put(key []byte, value []byte) error {
	return c.cursor.Put(key, value, 0)
}

func (c *LmdbCursor) putCurrent(key []byte, value []byte) error {
	return c.cursor.Put(key, value, lmdb.Current)
}

// Append - speedy feature of lmdb which is not part of KV interface.
// Cast your cursor to *LmdbCursor to use this method.
// Danger: if provided data will not sorted (or bucket have old records which mess with new in sorting manner) - db will corrupt.
func (c *LmdbCursor) Append(key []byte, value []byte) error {
	if len(key) == 0 {
		return fmt.Errorf("lmdb doesn't support empty keys. bucket: %s", c.bucket.name)
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

func (c *LmdbCursor) Close() error {
	if c.cursor != nil {
		c.cursor.Close()
		c.cursor = nil
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
