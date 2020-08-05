package ethdb

import (
	"bytes"
	"context"
	"fmt"
	"net"
	"time"

	"github.com/ledgerwatch/turbo-geth/ethdb/remote"
	"github.com/ledgerwatch/turbo-geth/log"
	"google.golang.org/grpc"
	"google.golang.org/grpc/backoff"
	"google.golang.org/grpc/test/bufconn"
)

// generate the messages
//go:generate protoc --go_out=. "./remote/kv.proto"
//go:generate protoc --go_out=. "./remote/db.proto"

// generate the services
//go:generate protoc --go-grpc_out=. "./remote/kv.proto"
//go:generate protoc --go-grpc_out=. "./remote/db.proto"

type remote2Opts struct {
	DialAddress string
	inMemConn   *bufconn.Listener // for tests
}

type Remote2KV struct {
	opts     remote2Opts
	remoteKV remote.KVClient
	remoteDB remote.DBClient
	conn     *grpc.ClientConn
	log      log.Logger
}

type remote2Tx struct {
	ctx     context.Context
	db      *Remote2KV
	cursors []*remote2Cursor
}

type remote2Bucket struct {
	tx   *remote2Tx
	name []byte
}

type remote2Cursor struct {
	initialized        bool
	streamingRequested bool
	prefetch           uint32
	ctx                context.Context
	bucket             *remote2Bucket
	prefix             []byte
	stream             remote.KV_SeekClient
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
	var dialOpts = []grpc.DialOption{
		grpc.WithConnectParams(grpc.ConnectParams{Backoff: backoff.DefaultConfig}),
		grpc.WithInsecure(),
	}

	if opts.inMemConn != nil {
		dialOpts = append(dialOpts, grpc.WithContextDialer(func(ctx context.Context, url string) (net.Conn, error) {
			return opts.inMemConn.Dial()
		}))
	}

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	conn, err := grpc.DialContext(ctx, opts.DialAddress, dialOpts...)
	if err != nil {
		return nil, err
	}

	return &Remote2KV{
		opts:     opts,
		conn:     conn,
		remoteKV: remote.NewKVClient(conn),
		remoteDB: remote.NewDBClient(conn),
		log:      log.New("remote_db", opts.DialAddress),
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
		db.conn = nil
	}
}

func (db *Remote2KV) DiskSize(ctx context.Context) (uint64, error) {
	sizeReply, err := db.remoteDB.Size(ctx, &remote.SizeRequest{})
	if err != nil {
		return 0, err
	}
	return sizeReply.Size, nil
}

func (db *Remote2KV) IdealBatchSize() int {
	panic("not supported")
}

func (db *Remote2KV) Begin(ctx context.Context, writable bool) (Tx, error) {
	panic("remote db doesn't support managed transactions")
}

func (db *Remote2KV) View(ctx context.Context, f func(tx Tx) error) (err error) {
	t := &remote2Tx{ctx: ctx, db: db}
	defer t.Rollback()

	return f(t)
}

func (db *Remote2KV) Update(ctx context.Context, f func(tx Tx) error) (err error) {
	return fmt.Errorf("remote db provider doesn't support .Update method")
}

func (tx *remote2Tx) Commit(ctx context.Context) error {
	panic("remote db is read-only")
}

func (tx *remote2Tx) Rollback() {
	for _, c := range tx.cursors {
		if c.stream != nil {
			_ = c.stream.CloseSend()
			c.stream = nil
		}
	}
}

func (tx *remote2Tx) Bucket(name []byte) Bucket {
	return &remote2Bucket{tx: tx, name: name}
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

func (b *remote2Bucket) Size() (uint64, error) {
	sizeReply, err := b.tx.db.remoteDB.BucketSize(b.tx.ctx, &remote.BucketSizeRequest{BucketName: b.name})
	if err != nil {
		return 0, err
	}
	return sizeReply.Size, nil
}

func (b *remote2Bucket) Clear() error {
	panic("not supporte")
}

func (b *remote2Bucket) Get(key []byte) (val []byte, err error) {
	c := b.Cursor()
	defer func() {
		if v, ok := c.(*remote2Cursor); ok {
			if v.stream == nil {
				return
			}
			_ = v.stream.CloseSend()
		}
	}()

	k, v, err := c.Seek(key)
	if err != nil {
		fmt.Printf("errr3: %s\n", err)
		return nil, err
	}
	if !bytes.Equal(key, k) {
		return nil, nil
	}
	return v, nil
}

func (b *remote2Bucket) Put(key []byte, value []byte) error {
	panic("not supported")
}

func (b *remote2Bucket) Delete(key []byte) error {
	panic("not supported")
}

func (b *remote2Bucket) Cursor() Cursor {
	c := &remote2Cursor{bucket: b, ctx: b.tx.ctx}
	b.tx.cursors = append(b.tx.cursors, c)
	return c
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

// Seek - doesn't start streaming (because much of code does only several .Seek cals without reading sequenceof data)
// .Next() - does request streaming (if configured by user)
func (c *remote2Cursor) Seek(seek []byte) ([]byte, []byte, error) {
	if c.stream != nil {
		_ = c.stream.CloseSend()
		c.stream = nil
	}
	c.initialized = true

	var err error
	c.stream, err = c.bucket.tx.db.remoteKV.Seek(c.ctx)
	if err != nil {
		fmt.Printf("errr2: %s\n", err)
		return []byte{}, nil, err
	}
	err = c.stream.Send(&remote.SeekRequest{BucketName: c.bucket.name, SeekKey: seek, Prefix: c.prefix, StartSreaming: false})
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

// Next - returns next data element from server, request streaming (if configured by user)
func (c *remote2Cursor) Next() ([]byte, []byte, error) {
	if !c.initialized {
		return c.First()
	}

	// if streaming not requested, server will send data only when remoteKV send message to bi-directional channel
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
