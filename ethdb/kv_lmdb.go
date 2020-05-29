package ethdb

import (
	"bytes"
	"context"
	"fmt"
	"io/ioutil"
	"os"
	"path"
	"time"

	"github.com/bmatsuo/lmdb-go/lmdb"
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
	opts.path, _ = ioutil.TempDir(os.TempDir(), "lmdb")
	opts.inMem = true
	return opts
}

func (opts lmdbOpts) ReadOnly() lmdbOpts {
	opts.readOnly = true
	return opts
}

func (opts lmdbOpts) Open(ctx context.Context) (KV, error) {
	env, err := lmdb.NewEnv()
	if err != nil {
		return nil, err
	}
	//defer env.Close()

	err = env.SetMaxDBs(100)
	if err != nil {
		return nil, err
	}

	// set the memory map size (maximum database size) to 1GB.
	err = env.SetMapSize(1 << 30)
	if err != nil {
		// ..
	}

	if !opts.inMem {
		if err := os.MkdirAll(opts.path, 0744); err != nil {
			return nil, err
		}
	}

	logger := log.New("lmdb", path.Base(opts.path))

	go func() {
		for {
			tick := time.NewTicker(time.Minute)
			select {
			case <-ctx.Done():
				tick.Stop()
				return
			case <-tick.C:
				// In any real application it is important to check for readers that were
				// never closed by their owning process, and for which the owning process
				// has exited.  See the documentation on transactions for more information.
				staleReaders, err := env.ReaderCheck()
				if err != nil {
					logger.Error("failed ReaderCheck", "err", err)
				}
				if staleReaders > 0 {
					logger.Info("cleared reader slots from dead processes", "amount", staleReaders)
				}
			}
		}
	}()

	flags := uint(0)
	if opts.readOnly {
		flags |= lmdb.Readonly
	}
	if opts.inMem {
		flags |= lmdb.NoSync | lmdb.NoMetaSync | lmdb.WriteMap
	}
	err = env.Open(opts.path, flags, 0644)
	if err != nil {
		return nil, err
	}

	buckets := map[string]lmdb.DBI{}
	if !opts.readOnly {
		if err := env.Update(func(tx *lmdb.Txn) error {
			for _, name := range dbutils.Buckets {
				dbi, createErr := tx.OpenDBI(string(name), lmdb.Create)
				if createErr != nil {
					return createErr
				}
				buckets[string(name)] = dbi
			}
			return nil
		}); err != nil {
			return nil, err
		}
	}

	return &lmdbKV{
		opts:    opts,
		env:     env,
		log:     logger,
		buckets: buckets,
	}, nil
}

func (opts lmdbOpts) MustOpen(ctx context.Context) KV {
	db, err := opts.Open(ctx)
	if err != nil {
		panic(err)
	}
	return db
}

type lmdbKV struct {
	opts    lmdbOpts
	env     *lmdb.Env
	log     log.Logger
	buckets map[string]lmdb.DBI
}

func NewLMDB() lmdbOpts {
	return lmdbOpts{}
}

// Close closes db
// All transactions must be closed before closing the database.
func (db *lmdbKV) Close() {
	if err := db.env.Close(); err != nil {
		db.log.Warn("failed to close DB", "err", err)
	} else {
		db.log.Info("database closed")
	}
}
func (db *lmdbKV) Size() uint64 {
	stats, err := db.env.Stat()
	if err != nil {
		log.Error("could not read database size", "err", err)
		return 0
	}
	return uint64(stats.PSize) * (stats.LeafPages + stats.BranchPages + stats.OverflowPages)
}

func (db *lmdbKV) Begin(ctx context.Context, writable bool) (Tx, error) {
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
	ctx context.Context
	db  *lmdbKV

	tx      *lmdb.Txn
	cursors []*lmdb.Cursor
}

type lmdbBucket struct {
	tx  *lmdbTx
	dbi lmdb.DBI

	nameLen uint
}

type lmdbCursor struct {
	ctx    context.Context
	bucket lmdbBucket
	prefix []byte

	cursor *lmdb.Cursor

	k   []byte
	v   []byte
	err error
}

func (db *lmdbKV) View(ctx context.Context, f func(tx Tx) error) (err error) {
	t := &lmdbTx{db: db, ctx: ctx}
	return db.env.View(func(tx *lmdb.Txn) error {
		defer t.cleanup()
		t.tx = tx
		return f(t)
	})
}

func (db *lmdbKV) Update(ctx context.Context, f func(tx Tx) error) (err error) {
	t := &lmdbTx{db: db, ctx: ctx}
	return db.env.Update(func(tx *lmdb.Txn) error {
		defer t.cleanup()
		t.tx = tx
		return f(t)
	})
}

func (tx *lmdbTx) Bucket(name []byte) Bucket {
	b, ok := tx.db.buckets[string(name)]
	if !ok {
		panic(fmt.Errorf("unknown bucket: %s", string(name)))
	}
	return lmdbBucket{tx: tx, dbi: b}
}

func (tx *lmdbTx) Commit(ctx context.Context) error {
	tx.cleanup()
	return tx.tx.Commit()
}

func (tx *lmdbTx) Rollback() error {
	tx.cleanup()
	tx.tx.Reset()
	return nil
}

func (tx *lmdbTx) cleanup() {
	for _, c := range tx.cursors {
		c.Close()
	}
}

func (c *lmdbCursor) Prefix(v []byte) Cursor {
	c.prefix = v
	return c
}

func (c *lmdbCursor) MatchBits(n uint) Cursor {
	panic("not implemented yet")
}

func (c *lmdbCursor) Prefetch(v uint) Cursor {
	//c.cursorOpts.PrefetchSize = int(v)
	return c
}

func (c *lmdbCursor) NoValues() NoValuesCursor {
	//c.cursorOpts.PrefetchValues = false
	return &lmdbNoValuesCursor{lmdbCursor: *c}
}

func (b lmdbBucket) Get(key []byte) (val []byte, err error) {
	select {
	case <-b.tx.ctx.Done():
		return nil, b.tx.ctx.Err()
	default:
	}

	val, err = b.tx.tx.Get(b.dbi, key)
	fmt.Printf("Res: %x -> %x %s\n", key, val, err)
	if lmdb.IsNotFound(err) {
		return nil, nil
	}
	return val, err
}

func (b lmdbBucket) Put(key []byte, value []byte) error {
	select {
	case <-b.tx.ctx.Done():
		return b.tx.ctx.Err()
	default:
	}

	if err := b.tx.tx.Put(b.dbi, key, value, 0); err != nil {
		return fmt.Errorf("failed lmdb put: %w, key=%x, val=%x\n", err, key, value)
	}
	return nil
}

func (b lmdbBucket) Delete(key []byte) error {
	select {
	case <-b.tx.ctx.Done():
		return b.tx.ctx.Err()
	default:
	}

	err := b.tx.tx.Del(b.dbi, key, nil)
	if lmdb.IsNotFound(err) {
		return nil
	}
	return err
}

func (b lmdbBucket) Cursor() Cursor {
	c := &lmdbCursor{bucket: b, ctx: b.tx.ctx}
	return c
}

func (c *lmdbCursor) initCursor() error {
	if c.cursor != nil {
		return nil
	}

	var err error
	c.cursor, err = c.bucket.tx.tx.OpenCursor(c.bucket.dbi)
	if err != nil {
		return err
	}

	// add to auto-cleanup on end of transactions
	if c.bucket.tx.cursors == nil {
		c.bucket.tx.cursors = make([]*lmdb.Cursor, 0, 1)
	}
	c.bucket.tx.cursors = append(c.bucket.tx.cursors, c.cursor)
	return nil
}

func (c *lmdbCursor) First() ([]byte, []byte, error) {
	if err := c.initCursor(); err != nil {
		return []byte{}, nil, err
	}

	return c.Seek(c.prefix)
}

func (c *lmdbCursor) Seek(seek []byte) ([]byte, []byte, error) {
	select {
	case <-c.ctx.Done():
		return []byte{}, nil, c.ctx.Err()
	default:
	}

	if err := c.initCursor(); err != nil {
		return []byte{}, nil, err
	}

	if seek == nil {
		c.k, c.v, c.err = c.cursor.Get(nil, nil, lmdb.First)
	} else {
		c.k, c.v, c.err = c.cursor.Get(seek, nil, lmdb.SetRange)

	}
	if lmdb.IsNotFound(c.err) {
		return nil, c.v, nil
	}
	if c.err != nil {
		return []byte{}, nil, fmt.Errorf("failed lmdlb cursor.Seek(): %w, key: %x", c.err, seek)
	}
	if !bytes.HasPrefix(c.k, c.prefix) {
		c.k, c.v = nil, nil
	}

	return c.k, c.v, nil
}

func (c *lmdbCursor) SeekTo(seek []byte) ([]byte, []byte, error) {
	return c.Seek(seek)
}

func (c *lmdbCursor) Next() ([]byte, []byte, error) {
	select {
	case <-c.ctx.Done():
		return []byte{}, nil, c.ctx.Err()
	default:
	}

	c.k, c.v, c.err = c.cursor.Get(nil, nil, lmdb.Next)
	if lmdb.IsNotFound(c.err) {
		return nil, c.v, nil
	}
	if c.err != nil {
		return []byte{}, nil, fmt.Errorf("failed lmdlb cursor.Next(): %w", c.err)
	}
	if !bytes.HasPrefix(c.k, c.prefix) {
		c.k, c.v = nil, nil
	}

	return c.k, c.v, nil
}

func (c *lmdbCursor) Walk(walker func(k, v []byte) (bool, error)) error {
	for k, v, err := c.First(); k != nil || err != nil; k, v, err = c.Next() {
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
	lmdbCursor
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

func (c *lmdbNoValuesCursor) First() ([]byte, uint32, error) {
	if err := c.initCursor(); err != nil {
		return []byte{}, 0, err
	}
	if c.prefix == nil {
		c.k, c.v, c.err = c.cursor.Get(nil, nil, lmdb.First)
	} else {
		c.k, c.v, c.err = c.cursor.Get(c.prefix, nil, lmdb.SetKey)
	}
	if lmdb.IsNotFound(c.err) {
		return []byte{}, uint32(len(c.v)), nil
	}
	if c.err != nil {
		return []byte{}, 0, c.err
	}
	if !bytes.HasPrefix(c.k, c.prefix) {
		c.k, c.v = nil, nil
	}
	return c.k, uint32(len(c.v)), c.err
}

func (c *lmdbNoValuesCursor) Seek(seek []byte) ([]byte, uint32, error) {
	select {
	case <-c.ctx.Done():
		return []byte{}, 0, c.ctx.Err()
	default:
	}

	if err := c.initCursor(); err != nil {
		return []byte{}, 0, err
	}

	fmt.Printf("!!!!!!!!!!!!!seek: %x %x %x\n", seek, c.k, c.v)
	c.k, c.v, c.err = c.cursor.Get(seek, nil, lmdb.SetKey)
	fmt.Printf("!!!!!!!!!!!!!seek: %x %x %x\n", seek, c.k, c.v)
	if lmdb.IsNotFound(c.err) {
		return nil, uint32(len(c.v)), nil
	}
	if c.err != nil {
		return []byte{}, 0, c.err
	}
	if !bytes.HasPrefix(c.k, c.prefix) {
		c.k, c.v = nil, nil
	}

	return c.k, uint32(len(c.v)), c.err
}

func (c *lmdbNoValuesCursor) SeekTo(seek []byte) ([]byte, uint32, error) {
	return c.Seek(seek)
}

func (c *lmdbNoValuesCursor) Next() ([]byte, uint32, error) {
	select {
	case <-c.ctx.Done():
		return []byte{}, 0, c.ctx.Err()
	default:
	}

	c.k, c.v, c.err = c.cursor.Get(nil, nil, lmdb.Next)
	if lmdb.IsNotFound(c.err) {
		return nil, uint32(len(c.v)), nil
	}
	if c.err != nil {
		return []byte{}, 0, c.err
	}
	if !bytes.HasPrefix(c.k, c.prefix) {
		c.k, c.v = nil, nil
	}

	return c.k, uint32(len(c.v)), c.err
}
