package ethdb

import (
	"context"
	"fmt"
	"net"
	"time"

	"github.com/ledgerwatch/turbo-geth/common"
	"github.com/ledgerwatch/turbo-geth/ethdb/remote"
	"github.com/ledgerwatch/turbo-geth/log"
	"google.golang.org/grpc"
	"google.golang.org/grpc/test/bufconn"
)

// generate the messages
//go:generate protoc --go_out=. "./remote/kv.proto"

// generate the services
//go:generate protoc --go-grpc_out=. "./remote/kv.proto"

type remote2Opts struct {
	DialAddress string
	inMemConn   *bufconn.Listener
}

type Remote2KV struct {
	opts   remote2Opts
	client remote.KvClient
	conn   *grpc.ClientConn
	log    log.Logger
}

type remote2Tx struct {
	ctx context.Context
	db  *Remote2KV

	handle     uint64
	finalizers []context.CancelFunc
}

type remote2Bucket struct {
	tx           *remote2Tx
	bucketHandle uint64
	initialized  bool
	name         []byte
}

type remote2Cursor struct {
	ctx          context.Context
	bucket       *remote2Bucket
	cursorHandle uint64
	initialized  bool
	prefetch     uint32
	prefix       []byte

	stream             remote.Kv_SeekClient
	streamClose        context.CancelFunc
	streamingRequested bool
}

type remote2NoValuesCursor struct {
	*remote2Cursor
}

func (opts remote2Opts) ReadOnly() remote2Opts {
	return opts
}

func (opts remote2Opts) Path(path string) remote2Opts {
	opts.DialAddress = path
	return opts
}

func (opts remote2Opts) InMem(listener *bufconn.Listener) remote2Opts {
	opts.inMemConn = listener
	return opts
}

func (opts remote2Opts) Open() (KV, error) {
	var dialOpts grpc.DialOption = grpc.EmptyDialOption{}
	if opts.inMemConn != nil {
		dialOpts = grpc.WithContextDialer(func(ctx context.Context, url string) (net.Conn, error) {
			return opts.inMemConn.Dial()
		})
	}

	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()

	conn, err := grpc.DialContext(ctx, opts.DialAddress, grpc.WithInsecure(), dialOpts)
	if err != nil {
		return nil, err
	}

	return &Remote2KV{
		opts:   opts,
		conn:   conn,
		client: remote.NewKvClient(conn),
		log:    log.New("remote_db", opts.DialAddress),
	}, nil
}

func (opts remote2Opts) MustOpen() KV {
	db, err := opts.Open()
	if err != nil {
		panic(err)
	}
	return db
}

func NewRemote2() remote2Opts {
	return remote2Opts{}
}

// Close
// All transactions must be closed before closing the database.
func (db *Remote2KV) Close() {
	if db.conn != nil {
		if err := db.conn.Close(); err != nil {
			db.log.Warn("failed to close remote DB", "err", err)
		} else {
			db.log.Info("remote database closed")
		}
	}
}

func (db *Remote2KV) DiskSize(ctx context.Context) (common.StorageSize, error) {
	//return db.remote.DiskSize(ctx)
	panic(1)
}

func (db *Remote2KV) BucketsStat(ctx context.Context) (map[string]common.StorageBucketWriteStats, error) {
	//return db.remote.BucketsStat(ctx)
	panic(1)
}

func (db *Remote2KV) IdealBatchSize() int {
	panic("not supported")
}

func (db *Remote2KV) Begin(ctx context.Context, writable bool) (Tx, error) {
	panic("remote db doesn't support managed transactions")
}

func (db *Remote2KV) View(ctx context.Context, f func(tx Tx) error) (err error) {
	txCtx, cancel := context.WithCancel(ctx)
	defer cancel() // rollback in any case

	stream, err := db.client.View(txCtx)
	if err != nil {
		return err
	}

	//nolint:errcheck
	defer stream.Send(&remote.ViewRequest{}) // signal server to rollback

	tx, err := stream.Recv()
	if err != nil {
		return err
	}
	t := &remote2Tx{ctx: ctx, db: db, handle: tx.TxHandle}
	defer t.close()

	return f(t)
}

func (db *Remote2KV) Update(ctx context.Context, f func(tx Tx) error) (err error) {
	return fmt.Errorf("remote db provider doesn't support .Update method")
}

func (tx *remote2Tx) Commit(ctx context.Context) error {
	panic("remote db is read-only")
}

func (tx *remote2Tx) close() {
	for i := range tx.finalizers {
		tx.finalizers[i]()
	}
}

func (tx *remote2Tx) Rollback() {
	panic("remote db is read-only")
}

func (tx *remote2Tx) Bucket(name []byte) Bucket {
	return &remote2Bucket{tx: tx, name: name}
}

func (c *remote2Cursor) init() error {
	if !c.bucket.initialized {
		if err := c.bucket.init(); err != nil {
			return err
		}
	}

	reply, err := c.bucket.tx.db.client.Cursor(c.ctx, &remote.CursorRequest{BucketHandle: c.bucket.bucketHandle, Prefix: c.prefix})
	if err != nil {
		return err
	}

	c.cursorHandle = reply.CursorHandle
	c.initialized = true
	return nil
}

func (c *remote2Cursor) Prefix(v []byte) Cursor {
	c.prefix = v
	return c
}

func (c *remote2Cursor) MatchBits(n uint) Cursor {
	panic("not implemented yet")
}

func (c *remote2Cursor) Prefetch(v uint) Cursor {
	c.prefetch = uint32(v)
	return c
}

func (c *remote2Cursor) NoValues() NoValuesCursor {
	//c.remote = c.remote.NoValues()
	return &remote2NoValuesCursor{remote2Cursor: c}
}

func (b *remote2Bucket) init() error {
	reply, err := b.tx.db.client.Bucket(b.tx.ctx, &remote.BucketRequest{Name: b.name, TxHandle: b.tx.handle})
	if err != nil {
		return err
	}
	if reply.BucketHandle == 0 {
		return fmt.Errorf("unexpected bucketHandle: 0")
	}
	b.bucketHandle = reply.BucketHandle
	b.initialized = true
	return nil
}

func (b *remote2Bucket) Size() (uint64, error) {
	panic("not implemented")
}

func (b *remote2Bucket) Clear() error {
	panic("not supporte")
}

func (b *remote2Bucket) Get(key []byte) (val []byte, err error) {
	if !b.initialized {
		err = b.init()
		if err != nil {
			return nil, err
		}
	}

	reply, err := b.tx.db.client.Get(b.tx.ctx, &remote.GetRequest{BucketHandle: b.bucketHandle, Key: key})
	if err != nil {
		fmt.Printf("%s\n", err)
		return nil, err
	}
	return reply.Value, nil
}

func (b *remote2Bucket) Put(key []byte, value []byte) error {
	panic("not supported")
}

func (b *remote2Bucket) Delete(key []byte) error {
	panic("not supported")
}

func (b *remote2Bucket) Cursor() Cursor {
	return &remote2Cursor{bucket: b, ctx: b.tx.ctx}
}

func (c *remote2Cursor) Put(key []byte, value []byte) error {
	panic("not supported")
}

func (c *remote2Cursor) Append(key []byte, value []byte) error {
	panic("not supported")
}

func (c *remote2Cursor) Delete(key []byte) error {
	panic("not supported")
}

func (c *remote2Cursor) First() ([]byte, []byte, error) {
	return c.Seek(c.prefix)
}

func (c *remote2Cursor) Seek(seek []byte) ([]byte, []byte, error) {
	if !c.initialized {
		if err := c.init(); err != nil {
			return []byte{}, nil, err
		}
	}

	if c.stream != nil {
		c.streamClose()
	}

	streamCtx, cancel := context.WithCancel(c.ctx) // cancel signaling for server that need stop streaming
	c.streamClose = cancel
	c.bucket.tx.finalizers = append(c.bucket.tx.finalizers, c.streamClose)

	var err error
	c.stream, err = c.bucket.tx.db.client.Seek(streamCtx)
	if err != nil {
		return []byte{}, nil, err
	}
	err = c.stream.Send(&remote.SeekRequest{CursorHandle: c.cursorHandle, SeekKey: seek, StartSreaming: false})
	if err != nil {
		return []byte{}, nil, err
	}

	pair, err := c.stream.Recv()
	if err != nil {
		return []byte{}, nil, err
	}

	return pair.Key, pair.Value, nil
}

func (c *remote2Cursor) SeekTo(seek []byte) ([]byte, []byte, error) {
	return c.Seek(seek)
}

func (c *remote2Cursor) Next() ([]byte, []byte, error) {
	if !c.initialized {
		return c.First()
	}

	if !c.streamingRequested {
		doStream := c.prefetch > 1
		if err := c.stream.Send(&remote.SeekRequest{StartSreaming: doStream}); err != nil {
			return []byte{}, nil, err
		}
		c.streamingRequested = doStream
	}

	pair, err := c.stream.Recv()
	if err != nil {
		return []byte{}, nil, err
	}
	return pair.Key, pair.Value, nil
}

func (c *remote2Cursor) Walk(walker func(k, v []byte) (bool, error)) error {
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

func (c *remote2NoValuesCursor) Walk(walker func(k []byte, vSize uint32) (bool, error)) error {
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

func (c *remote2NoValuesCursor) First() ([]byte, uint32, error) {
	return c.Seek(c.prefix)
}

func (c *remote2NoValuesCursor) Seek(seek []byte) ([]byte, uint32, error) {
	k, v, err := c.remote2Cursor.Seek(seek)
	if err != nil {
		return []byte{}, 0, err
	}

	return k, uint32(len(v)), err
}

func (c *remote2NoValuesCursor) Next() ([]byte, uint32, error) {
	k, v, err := c.remote2Cursor.Next()
	if err != nil {
		return []byte{}, 0, err
	}

	return k, uint32(len(v)), err
}
