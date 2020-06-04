package ethdb

import (
	"context"
	"fmt"
	"io"

	"github.com/ledgerwatch/turbo-geth/common"
	"github.com/ledgerwatch/turbo-geth/ethdb/remote"
	"github.com/ledgerwatch/turbo-geth/log"
)

type remoteOpts struct {
	Remote remote.DbOpts
}

type remoteDB struct {
	opts   remoteOpts
	remote *remote.DB
	log    log.Logger
}

type remoteTx struct {
	ctx context.Context
	db  *remoteDB

	remote *remote.Tx
}

type remoteBucket struct {
	tx *remoteTx

	nameLen uint
	remote  *remote.Bucket
}

type remoteCursor struct {
	ctx    context.Context
	bucket remoteBucket

	remote *remote.Cursor

	k   []byte
	v   []byte
	err error
}

type remoteNoValuesCursor struct {
	remoteCursor
}

func (opts remoteOpts) ReadOnly() remoteOpts {
	return opts
}

func (opts remoteOpts) Path(path string) remoteOpts {
	opts.Remote = opts.Remote.Addr(path)
	return opts
}

//
//
// Example text code:
//  writeDb, errOpen = ethdb.NewBolt().InMem().Open(ctx)
//	require.NoError(t, errOpen)
//	serverIn, clientOut := io.Pipe()
//	clientIn, serverOut := io.Pipe()
//	readDBs, errOpen = ethdb.NewRemote().InMem(clientIn, clientOut).Open(ctx)
//	require.NoError(t, errOpen)
//  go func() {
// 	    if err := remotedbserver.Server(ctx, writeDb, serverIn, serverOut, nil); err != nil {
//		    require.NoError(t, err)
//  	}
//  }()
//
func (opts remoteOpts) InMem(in io.Reader, out io.Writer) remoteOpts {
	opts.Remote.DialFunc = func(ctx context.Context) (io.Reader, io.Writer, io.Closer, error) {
		return in, out, nil, nil
	}
	return opts
}

func (opts remoteOpts) Open(ctx context.Context) (KV, error) {
	db, err := remote.Open(ctx, opts.Remote)
	if err != nil {
		return nil, err
	}

	return &remoteDB{
		opts:   opts,
		remote: db,
		log:    log.New("remote_db", opts.Remote.DialAddress),
	}, nil
}

func (opts remoteOpts) MustOpen(ctx context.Context) KV {
	db, err := opts.Open(ctx)
	if err != nil {
		panic(err)
	}
	return db
}

func NewRemote() remoteOpts {
	return remoteOpts{Remote: remote.DefaultOpts}
}

// Close closes BoltKV
// All transactions must be closed before closing the database.
func (db *remoteDB) Close() {
	if err := db.remote.Close(); err != nil {
		db.log.Warn("failed to close remote DB", "err", err)
	} else {
		db.log.Info("remote database closed")
	}
}

func (db *remoteDB) DiskSize(ctx context.Context) (common.StorageSize, error) {
	return db.remote.DiskSize(ctx)
}

func (db *remoteDB) BucketsStat(ctx context.Context) (map[string]common.StorageBucketWriteStats, error) {
	return db.remote.BucketsStat(ctx)
}

func (db *remoteDB) Begin(ctx context.Context, writable bool) (Tx, error) {
	panic("remote db doesn't support managed transactions")
}

func (db *remoteDB) View(ctx context.Context, f func(tx Tx) error) (err error) {
	t := &remoteTx{db: db, ctx: ctx}
	return db.remote.View(ctx, func(tx *remote.Tx) error {
		t.remote = tx
		return f(t)
	})
}

func (db *remoteDB) Update(ctx context.Context, f func(tx Tx) error) (err error) {
	return fmt.Errorf("remote db provider doesn't support .Update method")
}

func (tx *remoteTx) Commit(ctx context.Context) error {
	panic("remote db is read-only")
}

func (tx *remoteTx) Rollback() error {
	panic("remote db is read-only")
}

func (tx *remoteTx) Bucket(name []byte) Bucket {
	b := remoteBucket{tx: tx, nameLen: uint(len(name))}
	b.remote = tx.remote.Bucket(name)
	return b
}

func (tx *remoteTx) cleanup() {
	// nothing to cleanup
}

func (c *remoteCursor) Prefix(v []byte) Cursor {
	c.remote = c.remote.Prefix(v)
	return c
}

func (c *remoteCursor) MatchBits(n uint) Cursor {
	panic("not implemented yet")
}

func (c *remoteCursor) Prefetch(v uint) Cursor {
	c.remote = c.remote.Prefetch(v)
	return c
}

func (c *remoteCursor) NoValues() NoValuesCursor {
	c.remote = c.remote.NoValues()
	return &remoteNoValuesCursor{remoteCursor: *c}
}

func (b remoteBucket) Get(key []byte) (val []byte, err error) {
	val, err = b.remote.Get(key)
	return val, err
}

func (b remoteBucket) Put(key []byte, value []byte) error {
	panic("not supported")
}

func (b remoteBucket) Delete(key []byte) error {
	panic("not supported")
}

func (b remoteBucket) Cursor() Cursor {
	c := &remoteCursor{bucket: b, ctx: b.tx.ctx, remote: b.remote.Cursor()}
	return c
}

func (c *remoteCursor) Put(key []byte, value []byte) error {
	panic("not supported")
}

func (c *remoteCursor) Delete(key []byte) error {
	panic("not supported")
}

func (c *remoteCursor) First() ([]byte, []byte, error) {
	c.k, c.v, c.err = c.remote.First()
	if c.err != nil {
		return []byte{}, c.v, c.err // on error key should be != nil
	}
	return c.k, c.v, nil
}

func (c *remoteCursor) Seek(seek []byte) ([]byte, []byte, error) {
	c.k, c.v, c.err = c.remote.Seek(seek)
	if c.err != nil {
		return []byte{}, c.v, c.err
	}
	return c.k, c.v, nil
}

func (c *remoteCursor) SeekTo(seek []byte) ([]byte, []byte, error) {
	c.k, c.v, c.err = c.remote.SeekTo(seek)
	if c.err != nil {
		return []byte{}, c.v, c.err
	}
	return c.k, c.v, nil
}

func (c *remoteCursor) Next() ([]byte, []byte, error) {
	c.k, c.v, c.err = c.remote.Next()
	return c.k, c.v, c.err
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
	var vSize uint32
	c.k, vSize, c.err = c.remote.FirstKey()
	if c.err != nil {
		return []byte{}, vSize, c.err
	}
	return c.k, vSize, nil
}

func (c *remoteNoValuesCursor) Seek(seek []byte) ([]byte, uint32, error) {
	var vSize uint32
	c.k, vSize, c.err = c.remote.SeekKey(seek)
	if c.err != nil {
		return []byte{}, vSize, c.err
	}
	return c.k, vSize, nil
}

func (c *remoteNoValuesCursor) Next() ([]byte, uint32, error) {
	var vSize uint32
	c.k, vSize, c.err = c.remote.NextKey()
	if c.err != nil {
		return []byte{}, vSize, c.err
	}
	return c.k, vSize, nil
}
