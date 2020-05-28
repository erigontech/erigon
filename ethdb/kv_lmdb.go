package ethdb

import (
	"context"
	"time"

	"github.com/bmatsuo/lmdb-go/lmdb"
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

func (opts lmdbOpts) Open(ctx context.Context) (KV, error) {
	env, err := lmdb.NewEnv()
	if err != nil {
		return nil, err
	}
	//defer env.Close()

	err = env.SetMaxDBs(1)
	if err != nil {
		return nil, err
	}
	//err = env.SetMapSize(1 << 30)
	//if err != nil {
	//	// ..
	//}

	logger := log.New("lmdb", opts.path)

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
	err = env.Open(opts.path, 0, 0644)
	if err != nil {
		return nil, err
	}

	// Open a database handle that will be used for the entire lifetime of this
	// application.  Because the database may not have existed before, and the
	// database may need to be created, we need to get the database handle in
	// an update transacation.
	var dbi lmdb.DBI
	err = env.Update(func(txn *lmdb.Txn) (err error) {
		dbi, err = txn.OpenDBI("turbo-geth", lmdb.Create)
		return err
	})
	if err != nil {
		return nil, err
	}

	return &lmdbKV{
		opts: opts,
		env:  env,
		db:   dbi,
		log:  logger,
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
	opts lmdbOpts
	db   lmdb.DBI
	env  *lmdb.Env
	log  log.Logger
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
	tx *lmdbTx

	prefix  []byte
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
	b := lmdbBucket{tx: tx, nameLen: uint(len())}

	b.prefix = name
	return b
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
	c.prefix = append(c.bucket.prefix[:c.bucket.nameLen], v...)
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

	b.prefix = append(b.prefix[:b.nameLen], key...)

	val, err = b.tx.tx.Get(b.tx.db.db, b.prefix)
	if lmdb.IsNotFound(err) {
		return nil, err
	}
	return val, err
}

func (b lmdbBucket) Put(key []byte, value []byte) error {
	select {
	case <-b.tx.ctx.Done():
		return b.tx.ctx.Err()
	default:
	}

	b.prefix = append(b.prefix[:b.nameLen], key...)
	return b.tx.tx.Put(b.tx.db.db, b.prefix, value, 0)
}

func (b lmdbBucket) Delete(key []byte) error {
	select {
	case <-b.tx.ctx.Done():
		return b.tx.ctx.Err()
	default:
	}

	b.prefix = append(b.prefix[:b.nameLen], key...)
	err := b.tx.tx.Del(b.tx.db.db, b.prefix, nil)
	if lmdb.IsNotFound(err) {
		return nil
	}
	return err
}

func (b lmdbBucket) Cursor() Cursor {
	c := &lmdbCursor{bucket: b, ctx: b.tx.ctx}
	c.prefix = append(c.prefix, b.prefix[:b.nameLen]...) // set bucket
	return c
}

func (c *lmdbCursor) initCursor() error {
	if c.cursor != nil {
		return nil
	}

	var err error
	c.cursor, err = c.bucket.tx.tx.OpenCursor(c.bucket.tx.db.db)
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
		return nil, nil, err
	}

	c.k, c.v, c.err = c.cursor.Get(nil, nil, lmdb.First)
	if lmdb.IsNotFound(c.err) {
		return nil, c.v, nil
	}
	if c.err != nil {
		return []byte{}, nil, c.err
	}

	return c.k, c.v, nil
}

func (c *lmdbCursor) Seek(seek []byte) ([]byte, []byte, error) {
	select {
	case <-c.ctx.Done():
		return nil, nil, c.ctx.Err()
	default:
	}

	if err := c.initCursor(); err != nil {
		return []byte{}, nil, err
	}

	c.k, c.v, c.err = c.cursor.Get(append(c.bucket.prefix[:c.bucket.nameLen], seek...), nil, lmdb.SetKey)
	if lmdb.IsNotFound(c.err) {
		return nil, c.v, nil
	}
	if c.err != nil {
		return []byte{}, nil, c.err
	}

	return c.k, c.v, nil
}

func (c *lmdbCursor) SeekTo(seek []byte) ([]byte, []byte, error) {
	return c.Seek(seek)
}

func (c *lmdbCursor) Next() ([]byte, []byte, error) {
	select {
	case <-c.ctx.Done():
		return nil, nil, c.ctx.Err()
	default:
	}

	c.cursor.Next()
	if !c.cursor.Valid() {
		c.k = nil
		return c.k, c.v, c.err
	}
	item := c.cursor.Item()
	c.k = item.Key()[c.bucket.nameLen:]
	if c.cursorOpts.PrefetchValues {
		c.v, c.err = item.ValueCopy(c.v)
	}

	return c.k, c.v, c.err
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

type badgerNoValuesCursor struct {
	lmdbCursor
}

func (c *badgerNoValuesCursor) Walk(walker func(k []byte, vSize uint32) (bool, error)) error {
	for k, vSize, err := c.First(); k != nil || err != nil; k, vSize, err = c.Next() {
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

func (c *badgerNoValuesCursor) First() ([]byte, uint32, error) {
	c.initCursor()
	c.cursor.Rewind()
	if !c.cursor.Valid() {
		c.k = nil
		return c.k, 0, c.err
	}
	item := c.cursor.Item()
	c.k = item.Key()[c.bucket.nameLen:]
	return c.k, uint32(item.ValueSize()), c.err
}

func (c *badgerNoValuesCursor) Seek(seek []byte) ([]byte, uint32, error) {
	select {
	case <-c.ctx.Done():
		return nil, 0, c.ctx.Err()
	default:
	}

	c.initCursor()

	c.cursor.Seek(append(c.bucket.prefix[:c.bucket.nameLen], seek...))
	if !c.cursor.Valid() {
		c.k = nil
		return c.k, 0, c.err
	}
	item := c.cursor.Item()
	c.k = item.Key()[c.bucket.nameLen:]

	return c.k, uint32(item.ValueSize()), c.err
}

func (c *badgerNoValuesCursor) SeekTo(seek []byte) ([]byte, uint32, error) {
	return c.Seek(seek)
}

func (c *badgerNoValuesCursor) Next() ([]byte, uint32, error) {
	select {
	case <-c.ctx.Done():
		return nil, 0, c.ctx.Err()
	default:
	}

	c.cursor.Next()
	if !c.cursor.Valid() {
		c.k = nil
		return c.k, 0, c.err
	}
	item := c.cursor.Item()
	c.k = item.Key()[c.bucket.nameLen:]
	return c.k, uint32(item.ValueSize()), c.err
}
