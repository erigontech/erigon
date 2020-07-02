package ethdb

import (
	"bytes"
	"context"
	"fmt"
	"os"
	"path"
	"strings"
	"sync"
	"time"

	"github.com/ledgerwatch/bolt"
	"github.com/ledgerwatch/turbo-geth/common"
	"github.com/ledgerwatch/turbo-geth/common/dbutils"
	"github.com/ledgerwatch/turbo-geth/log"
	"github.com/ledgerwatch/turbo-geth/metrics"
)

var (
	boltTxPool     = sync.Pool{New: func() interface{} { return &boltTx{} }}
	boltCursorPool = sync.Pool{New: func() interface{} { return &boltCursor{} }}
)

var valueBytesMetrics []metrics.Gauge
var keyBytesMetrics []metrics.Gauge
var totalBytesPutMetrics []metrics.Gauge
var totalBytesDeleteMetrics []metrics.Gauge
var keyNMetrics []metrics.Gauge

func init() {
	if metrics.Enabled {
		for i := range dbutils.Buckets {
			b := strings.ToLower(string(dbutils.Buckets[i]))
			b = strings.Replace(b, "-", "_", -1)
			valueBytesMetrics = append(valueBytesMetrics, metrics.NewRegisteredGauge("db/bucket/value_bytes/"+b, nil))
			keyBytesMetrics = append(keyBytesMetrics, metrics.NewRegisteredGauge("db/bucket/key_bytes/"+b, nil))
			totalBytesPutMetrics = append(totalBytesPutMetrics, metrics.NewRegisteredGauge("db/bucket/bytes_put_total/"+b, nil))
			totalBytesDeleteMetrics = append(totalBytesDeleteMetrics, metrics.NewRegisteredGauge("db/bucket/bytes_delete_total/"+b, nil))
			keyNMetrics = append(keyNMetrics, metrics.NewRegisteredGauge("db/bucket/keys/"+b, nil))
		}
	}
}

func collectBoltMetrics(ctx context.Context, db *bolt.DB, ticker *time.Ticker) {
	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
		}

		stats := db.Stats()
		boltPagesFreeGauge.Update(int64(stats.FreePageN))
		boltPagesPendingGauge.Update(int64(stats.PendingPageN))
		boltPagesAllocGauge.Update(int64(stats.FreeAlloc))
		boltFreelistInuseGauge.Update(int64(stats.FreelistInuse))

		boltTxGauge.Update(int64(stats.TxN))
		boltTxOpenGauge.Update(int64(stats.OpenTxN))
		boltTxCursorGauge.Update(int64(stats.TxStats.CursorCount))

		boltRebalanceGauge.Update(int64(stats.TxStats.Rebalance))
		boltRebalanceTimer.Update(stats.TxStats.RebalanceTime)

		boltSplitGauge.Update(int64(stats.TxStats.Split))
		boltSpillGauge.Update(int64(stats.TxStats.Spill))
		boltSpillTimer.Update(stats.TxStats.SpillTime)

		boltWriteGauge.Update(int64(stats.TxStats.Write))
		boltWriteTimer.Update(stats.TxStats.WriteTime)

		if len(valueBytesMetrics) == 0 {
			continue
		}
		writeStats := db.WriteStats()
		for i := range dbutils.Buckets {
			st, ok := writeStats[string(dbutils.Buckets[i])]
			if !ok {
				continue
			}

			valueBytesMetrics[i].Update(int64(st.ValueBytesN))
			keyBytesMetrics[i].Update(int64(st.KeyBytesN))
			totalBytesPutMetrics[i].Update(int64(st.TotalBytesPut))
			totalBytesDeleteMetrics[i].Update(int64(st.TotalBytesDelete))
			keyNMetrics[i].Update(int64(st.KeyN))
		}
	}
}

type boltOpts struct {
	Bolt *bolt.Options
	path string
}

type BoltKV struct {
	opts        boltOpts
	bolt        *bolt.DB
	log         log.Logger
	stopMetrics context.CancelFunc
	wg          *sync.WaitGroup
}

type boltTx struct {
	ctx     context.Context
	db      *BoltKV
	bolt    *bolt.Tx
	cursors []*boltCursor
}

type boltBucket struct {
	tx      *boltTx
	bolt    *bolt.Bucket
	id      int
	nameLen uint
}

type boltCursor struct {
	ctx    context.Context
	bucket boltBucket
	prefix []byte

	bolt *bolt.Cursor
}

type noValuesBoltCursor struct {
	*boltCursor
}

func (opts boltOpts) InMem() boltOpts {
	opts.Bolt.MemOnly = true
	return opts
}

func (opts boltOpts) ReadOnly() boltOpts {
	opts.Bolt.ReadOnly = true
	return opts
}

func (opts boltOpts) Path(path string) boltOpts {
	opts.path = path
	return opts
}

func (opts boltOpts) Open() (KV, error) {
	if !opts.Bolt.MemOnly {
		if err := os.MkdirAll(path.Dir(opts.path), 0744); err != nil {
			return nil, fmt.Errorf("could not create dir: %s, %w", opts.path, err)
		}
	}

	boltDB, err := bolt.Open(opts.path, 0600, opts.Bolt)
	if err != nil {
		return nil, err
	}
	if !opts.Bolt.ReadOnly {
		if err := boltDB.Update(func(tx *bolt.Tx) error {
			for _, name := range dbutils.Buckets {
				_, createErr := tx.CreateBucketIfNotExists(name, false)
				if createErr != nil {
					return createErr
				}
			}
			return nil
		}); err != nil {
			return nil, err
		}
	}

	db := &BoltKV{
		opts: opts,
		bolt: boltDB,
		log:  log.New("bolt_db", opts.path),
		wg:   &sync.WaitGroup{},
	}

	if metrics.Enabled {
		ctx, cancel := context.WithCancel(context.Background())
		db.stopMetrics = cancel
		db.wg.Add(1)
		go func() {
			defer db.wg.Done()
			ticker := time.NewTicker(3 * time.Second)
			defer ticker.Stop()
			collectBoltMetrics(ctx, boltDB, ticker)
		}()
	}

	return db, nil
}

func (opts boltOpts) MustOpen() KV {
	db, err := opts.Open()
	if err != nil {
		panic(err)
	}
	return db
}

func NewBolt() boltOpts {
	o := boltOpts{Bolt: bolt.DefaultOptions}
	o.Bolt.KeysPrefixCompressionDisable = true
	return o
}

// Close closes BoltKV
// All transactions must be closed before closing the database.
func (db *BoltKV) Close() {
	if db.stopMetrics != nil {
		db.stopMetrics()
	}

	db.wg.Wait()

	if db.bolt != nil {
		if err := db.bolt.Close(); err != nil {
			db.log.Warn("failed to close bolt DB", "err", err)
		} else {
			db.log.Info("bolt database closed")
		}
	}
}

func (db *BoltKV) DiskSize(_ context.Context) (common.StorageSize, error) {
	return common.StorageSize(db.bolt.Size()), nil
}

func (db *BoltKV) BucketsStat(_ context.Context) (map[string]common.StorageBucketWriteStats, error) {
	res := map[string]common.StorageBucketWriteStats{}
	for name, stats := range db.bolt.WriteStats() {
		res[name] = common.StorageBucketWriteStats{
			KeyN:             common.StorageCounter(stats.KeyN),
			KeyBytesN:        common.StorageSize(stats.KeyBytesN),
			ValueBytesN:      common.StorageSize(stats.ValueBytesN),
			TotalPut:         common.StorageCounter(stats.TotalPut),
			TotalDelete:      common.StorageCounter(stats.TotalDelete),
			TotalBytesPut:    common.StorageSize(stats.TotalBytesPut),
			TotalBytesDelete: common.StorageSize(stats.TotalBytesDelete),
		}
	}
	return res, nil
}

func (db *BoltKV) IdealBatchSize() int {
	return 50 * 1024 * 1024 // 50 Mb
}

func (db *BoltKV) Get(ctx context.Context, bucket, key []byte) (val []byte, err error) {
	err = db.bolt.View(func(tx *bolt.Tx) error {
		v, _ := tx.Bucket(bucket).Get(key)
		if v != nil {
			val = make([]byte, len(v))
			copy(val, v)
		}
		return nil
	})
	if err != nil {
		return nil, err
	}
	return val, nil
}

func (db *BoltKV) Has(ctx context.Context, bucket, key []byte) (bool, error) {
	var has bool
	err := db.bolt.View(func(tx *bolt.Tx) error {
		b := tx.Bucket(bucket)
		if b == nil {
			has = false
		} else {
			v, _ := b.Get(key)
			has = v != nil
		}
		return nil
	})
	return has, err
}

func (db *BoltKV) Begin(ctx context.Context, writable bool) (Tx, error) {
	t := boltTxPool.Get().(*boltTx)
	t.ctx = ctx
	t.db = db
	var err error
	t.bolt, err = db.bolt.Begin(writable)
	return t, err
}

func (db *BoltKV) View(ctx context.Context, f func(tx Tx) error) (err error) {
	t := boltTxPool.Get().(*boltTx)
	defer boltTxPool.Put(t)
	t.ctx = ctx
	t.db = db
	defer t.closeCursors()
	return db.bolt.View(func(tx *bolt.Tx) error {
		t.bolt = tx
		return f(t)
	})
}

func (db *BoltKV) Update(ctx context.Context, f func(tx Tx) error) (err error) {
	t := boltTxPool.Get().(*boltTx)
	defer boltTxPool.Put(t)
	t.ctx = ctx
	t.db = db
	defer t.closeCursors()
	return db.bolt.Update(func(tx *bolt.Tx) error {
		t.bolt = tx
		return f(t)
	})
}

func (tx *boltTx) Commit(ctx context.Context) error {
	defer tx.closeCursors()
	// could not put tx back to pool, because tx can be used by app code after commit
	return tx.bolt.Commit()
}

func (tx *boltTx) Rollback() {
	defer tx.closeCursors()
	// could not put tx back to pool, because tx can be used by app code after rollback
	if err := tx.bolt.Rollback(); err != nil {
		tx.db.log.Warn("bolt rollback failed", "err", err)
	}
}

func (tx *boltTx) closeCursors() {
	for _, c := range tx.cursors {
		boltCursorPool.Put(c)
	}
	tx.cursors = tx.cursors[:0]
}

func (tx *boltTx) Yield() {
	tx.bolt.Yield()
}

func (tx *boltTx) Bucket(name []byte) Bucket {
	b := boltBucket{tx: tx, nameLen: uint(len(name)), id: dbutils.BucketsIndex[string(name)]}
	b.bolt = tx.bolt.Bucket(name)
	return b
}

func (c *boltCursor) Prefix(v []byte) Cursor {
	c.prefix = v
	return c
}

func (c *boltCursor) MatchBits(n uint) Cursor {
	panic("not implemented yet")
}

func (c *boltCursor) Prefetch(v uint) Cursor {
	// nothing to do
	return c
}

func (c *boltCursor) NoValues() NoValuesCursor {
	return &noValuesBoltCursor{boltCursor: c}
}

func (b boltBucket) Size() (uint64, error) {
	st := b.bolt.Stats()
	return uint64((st.BranchPageN + st.BranchOverflowN + st.LeafPageN) * os.Getpagesize()), nil
}

func (b boltBucket) Clear() error {
	err := b.tx.bolt.DeleteBucket(dbutils.Buckets[b.id])
	if err != nil {
		return err
	}
	_, err = b.tx.bolt.CreateBucket(dbutils.Buckets[b.id], false)
	if err != nil {
		return err
	}
	return nil
}

func (b boltBucket) Get(key []byte) (val []byte, err error) {
	select {
	case <-b.tx.ctx.Done():
		return nil, b.tx.ctx.Err()
	default:
	}

	val, _ = b.bolt.Get(key)
	return val, err
}

func (b boltBucket) Put(key []byte, value []byte) error {
	select {
	case <-b.tx.ctx.Done():
		return b.tx.ctx.Err()
	default:
	}
	return b.bolt.Put(key, value)
}

func (b boltBucket) Delete(key []byte) error {
	select {
	case <-b.tx.ctx.Done():
		return b.tx.ctx.Err()
	default:
	}

	return b.bolt.Delete(key)
}

func (b boltBucket) Cursor() Cursor {
	c := boltCursorPool.Get().(*boltCursor)
	c.ctx = b.tx.ctx
	c.bucket = b
	c.prefix = nil
	c.bolt = b.bolt.Cursor()
	// add to auto-close on end of transactions
	if b.tx.cursors == nil {
		b.tx.cursors = make([]*boltCursor, 0, 1)
	}
	b.tx.cursors = append(b.tx.cursors, c)
	return c
}

func (c *boltCursor) First() (k, v []byte, err error) {
	if len(c.prefix) == 0 {
		k, v = c.bolt.First()
		return k, v, nil
	}
	k, v = c.bolt.Seek(c.prefix)
	if !bytes.HasPrefix(k, c.prefix) {
		return nil, nil, nil
	}
	return k, v, nil
}

func (c *boltCursor) Seek(seek []byte) (k, v []byte, err error) {
	select {
	case <-c.ctx.Done():
		return []byte{}, nil, c.ctx.Err()
	default:
	}

	k, v = c.bolt.Seek(seek)
	if c.prefix != nil {
		if !bytes.HasPrefix(k, c.prefix) {
			return nil, nil, nil
		}
	}
	return k, v, nil
}

func (c *boltCursor) SeekTo(seek []byte) (k, v []byte, err error) {
	select {
	case <-c.ctx.Done():
		return []byte{}, nil, c.ctx.Err()
	default:
	}

	k, v = c.bolt.SeekTo(seek)
	if c.prefix != nil {
		if !bytes.HasPrefix(k, c.prefix) {
			return nil, nil, nil
		}
	}
	return k, v, nil
}

func (c *boltCursor) Next() (k, v []byte, err error) {
	select {
	case <-c.ctx.Done():
		return []byte{}, nil, c.ctx.Err()
	default:
	}

	k, v = c.bolt.Next()
	if c.prefix != nil {
		if !bytes.HasPrefix(k, c.prefix) {
			k, v = nil, nil
		}
	}
	return k, v, nil
}

func (c *boltCursor) Delete(key []byte) error {
	select {
	case <-c.ctx.Done():
		return c.ctx.Err()
	default:
	}

	return c.bolt.Delete2(key)
}

func (c *boltCursor) Put(key []byte, value []byte) error {
	select {
	case <-c.ctx.Done():
		return c.ctx.Err()
	default:
	}

	return c.bolt.Put(key, value)
}

func (c *boltCursor) Append(key []byte, value []byte) error {
	return c.Put(key, value)
}

func (c *boltCursor) Walk(walker func(k, v []byte) (bool, error)) error {
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

func (c *noValuesBoltCursor) Walk(walker func(k []byte, vSize uint32) (bool, error)) error {
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

func (c *noValuesBoltCursor) First() (k []byte, vSize uint32, err error) {
	var v []byte
	if len(c.prefix) == 0 {
		k, v = c.bolt.First()
		return k, uint32(len(v)), nil
	}

	k, v = c.bolt.Seek(c.prefix)
	if !bytes.HasPrefix(k, c.prefix) {
		return nil, 0, nil
	}
	return k, uint32(len(v)), nil
}

func (c *noValuesBoltCursor) Seek(seek []byte) (k []byte, vSize uint32, err error) {
	select {
	case <-c.ctx.Done():
		return []byte{}, 0, c.ctx.Err() // on error key should be != nil
	default:
	}

	var v []byte
	k, v = c.bolt.Seek(seek)
	if c.prefix != nil {
		if !bytes.HasPrefix(k, c.prefix) {
			return nil, 0, nil
		}
	}
	return k, uint32(len(v)), nil
}

func (c *noValuesBoltCursor) Next() (k []byte, vSize uint32, err error) {
	select {
	case <-c.ctx.Done():
		return []byte{}, 0, c.ctx.Err()
	default:
	}

	var v []byte
	k, v = c.bolt.Next()
	if c.prefix != nil {
		if !bytes.HasPrefix(k, c.prefix) {
			return nil, 0, nil
		}
	}
	return k, uint32(len(v)), nil
}
