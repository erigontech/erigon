package ethdb

import (
	"bytes"
	"context"
	"fmt"
	"net"
	"time"

	"github.com/ledgerwatch/turbo-geth/common"
	"github.com/ledgerwatch/turbo-geth/ethdb/remote"
	"github.com/ledgerwatch/turbo-geth/log"
	"google.golang.org/grpc"
	"google.golang.org/grpc/backoff"
	"google.golang.org/grpc/test/bufconn"
)

// generate the messages
//go:generate protoc --go_out=. "./remote/kv.proto"
//go:generate protoc --go_out=. "./remote/db.proto"
//go:generate protoc --go_out=. "./remote/ethbackend.proto"

// generate the services
//go:generate protoc --go-grpc_out=. "./remote/kv.proto"
//go:generate protoc --go-grpc_out=. "./remote/db.proto"
//go:generate protoc --go-grpc_out=. "./remote/ethbackend.proto"

type remoteOpts struct {
	DialAddress string
	inMemConn   *bufconn.Listener // for tests
}

type RemoteKV struct {
	opts     remoteOpts
	remoteKV remote.KVClient
	remoteDB remote.DBClient
	conn     *grpc.ClientConn
	log      log.Logger
}

type remoteTx struct {
	ctx     context.Context
	db      *RemoteKV
	cursors []*remoteCursor
}

type remoteBucket struct {
	tx   *remoteTx
	name string
}

type remoteCursor struct {
	initialized        bool
	streamingRequested bool
	prefetch           uint32
	ctx                context.Context
	bucket             *remoteBucket
	prefix             []byte
	stream             remote.KV_SeekClient
}

type RemoteBackend struct {
	opts             remoteOpts
	remoteEthBackend remote.ETHBACKENDClient
	conn             *grpc.ClientConn
	log              log.Logger
}

type remoteNoValuesCursor struct {
	*remoteCursor
}

func (opts remoteOpts) ReadOnly() remoteOpts {
	return opts
}

func (opts remoteOpts) Path(path string) remoteOpts {
	opts.DialAddress = path
	return opts
}

func (opts remoteOpts) InMem(listener *bufconn.Listener) remoteOpts {
	opts.inMemConn = listener
	return opts
}

func (opts remoteOpts) Open() (KV, Backend, error) {
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
		return nil, nil, err
	}

	db := &RemoteKV{
		opts:     opts,
		conn:     conn,
		remoteKV: remote.NewKVClient(conn),
		remoteDB: remote.NewDBClient(conn),
		log:      log.New("remote_db", opts.DialAddress),
	}

	eth := &RemoteBackend{
		opts:             opts,
		remoteEthBackend: remote.NewETHBACKENDClient(conn),
		conn:             conn,
		log:              log.New("remote_db", opts.DialAddress),
	}

	return db, eth, nil
}

func (opts remoteOpts) MustOpen() (KV, Backend) {
	db, txPool, err := opts.Open()
	if err != nil {
		panic(err)
	}
	return db, txPool
}

func NewRemote() remoteOpts {
	return remoteOpts{}
}

// Close
// All transactions must be closed before closing the database.
func (db *RemoteKV) Close() {
	if db.conn != nil {
		if err := db.conn.Close(); err != nil {
			db.log.Warn("failed to close remote DB", "err", err)
		} else {
			db.log.Info("remote database closed")
		}
		db.conn = nil
	}
}

func (db *RemoteKV) DiskSize(ctx context.Context) (uint64, error) {
	sizeReply, err := db.remoteDB.Size(ctx, &remote.SizeRequest{})
	if err != nil {
		return 0, err
	}
	return sizeReply.Size, nil
}

func (db *RemoteKV) IdealBatchSize() int {
	panic("not supported")
}

func (db *RemoteKV) Begin(ctx context.Context, parent Tx, writable bool) (Tx, error) {
	panic("remote db doesn't support managed transactions")
}

func (db *RemoteKV) View(ctx context.Context, f func(tx Tx) error) (err error) {
	t := &remoteTx{ctx: ctx, db: db}
	defer t.Rollback()

	return f(t)
}

func (db *RemoteKV) Update(ctx context.Context, f func(tx Tx) error) (err error) {
	return fmt.Errorf("remote db provider doesn't support .Update method")
}

func (tx *remoteTx) Commit(ctx context.Context) error {
	panic("remote db is read-only")
}

func (tx *remoteTx) Rollback() {
	for _, c := range tx.cursors {
		if c.stream != nil {
			_ = c.stream.CloseSend()
			c.stream = nil
		}
	}
}

func (tx *remoteTx) Bucket(name string) Bucket {
	return &remoteBucket{tx: tx, name: name}
}

func (c *remoteCursor) Prefix(v []byte) Cursor {
	c.prefix = v
	return c
}

func (c *remoteCursor) MatchBits(n uint) Cursor {
	panic("not implemented yet")
}

func (c *remoteCursor) Prefetch(v uint) Cursor {
	c.prefetch = uint32(v)
	return c
}

func (c *remoteCursor) NoValues() NoValuesCursor {
	//c.remote = c.remote.NoValues()
	return &remoteNoValuesCursor{remoteCursor: c}
}

func (b *remoteBucket) Size() (uint64, error) {
	sizeReply, err := b.tx.db.remoteDB.BucketSize(b.tx.ctx, &remote.BucketSizeRequest{BucketName: b.name})
	if err != nil {
		return 0, err
	}
	return sizeReply.Size, nil
}

func (b *remoteBucket) Clear() error {
	panic("not supporte")
}

func (tx *remoteTx) Get(bucket string, key []byte) (val []byte, err error) {
	c := tx.Cursor(bucket)
	defer func() {
		if v, ok := c.(*remoteCursor); ok {
			if v.stream == nil {
				return
			}
			_ = v.stream.CloseSend()
		}
	}()

	return c.Get(key)
}

func (c *remoteCursor) Get(key []byte) (val []byte, err error) {
	k, v, err := c.Seek(key)
	if err != nil {
		return nil, err
	}
	if !bytes.Equal(key, k) {
		return nil, nil
	}
	return v, nil
}

func (b *remoteBucket) Get(key []byte) (val []byte, err error) {
	c := b.Cursor()
	defer func() {
		if v, ok := c.(*remoteCursor); ok {
			if v.stream == nil {
				return
			}
			_ = v.stream.CloseSend()
		}
	}()

	k, v, err := c.Seek(key)
	if err != nil {
		return nil, err
	}
	if !bytes.Equal(key, k) {
		return nil, nil
	}
	return v, nil
}

func (b *remoteBucket) Put(key []byte, value []byte) error {
	panic("not supported")
}

func (b *remoteBucket) Delete(key []byte) error {
	panic("not supported")
}

func (tx *remoteTx) Cursor(bucket string) Cursor {
	return tx.Bucket(bucket).(*remoteBucket).Cursor()
}

func (b *remoteBucket) Cursor() Cursor {
	c := &remoteCursor{bucket: b, ctx: b.tx.ctx}
	b.tx.cursors = append(b.tx.cursors, c)
	return c
}

func (c *remoteCursor) Put(key []byte, value []byte) error {
	panic("not supported")
}

func (c *remoteCursor) Append(key []byte, value []byte) error {
	panic("not supported")
}

func (c *remoteCursor) Delete(key []byte) error {
	panic("not supported")
}

func (c *remoteCursor) First() ([]byte, []byte, error) {
	return c.Seek(c.prefix)
}

// Seek - doesn't start streaming (because much of code does only several .Seek calls without reading sequence of data)
// .Next() - does request streaming (if configured by user)
func (c *remoteCursor) Seek(seek []byte) ([]byte, []byte, error) {
	if c.stream != nil {
		_ = c.stream.CloseSend()
		c.stream = nil
	}
	c.initialized = true

	var err error
	c.stream, err = c.bucket.tx.db.remoteKV.Seek(c.ctx)
	if err != nil {
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

// Next - returns next data element from server, request streaming (if configured by user)
func (c *remoteCursor) Next() ([]byte, []byte, error) {
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

func (c *remoteCursor) Last() ([]byte, []byte, error) {
	panic("not implemented yet")
}

func (c *remoteCursor) Walk(walker func(k, v []byte) (bool, error)) error {
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

func (c *remoteNoValuesCursor) Walk(walker func(k []byte, vSize uint32) (bool, error)) error {
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

func (c *remoteNoValuesCursor) First() ([]byte, uint32, error) {
	return c.Seek(c.prefix)
}

func (c *remoteNoValuesCursor) Seek(seek []byte) ([]byte, uint32, error) {
	k, v, err := c.remoteCursor.Seek(seek)
	if err != nil {
		return []byte{}, 0, err
	}

	return k, uint32(len(v)), err
}

func (c *remoteNoValuesCursor) Next() ([]byte, uint32, error) {
	k, v, err := c.remoteCursor.Next()
	if err != nil {
		return []byte{}, 0, err
	}

	return k, uint32(len(v)), err
}

func (back *RemoteBackend) AddLocal(signedTx []byte) ([]byte, error) {
	res, err := back.remoteEthBackend.Add(context.Background(), &remote.TxRequest{Signedtx: signedTx})
	if err != nil {
		return common.Hash{}.Bytes(), err
	}
	return res.Hash, nil
}

func (back *RemoteBackend) Etherbase() (common.Address, error) {
	res, err := back.remoteEthBackend.Etherbase(context.Background(), &remote.EtherbaseRequest{})
	if err != nil {
		return common.Address{}, err
	}

	return common.BytesToAddress(res.Hash), nil
}

func (back *RemoteBackend) NetVersion() uint64 {
	res, err := back.remoteEthBackend.NetVersion(context.Background(), &remote.NetVersionRequest{})
	if err != nil {
		log.Error("NetVersion call got an error", "err", err)
		return 0
	}

	return res.Id
}
