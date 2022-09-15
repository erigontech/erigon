package remotedb

import (
	"bytes"
	"context"
	"fmt"
	"runtime"

	"github.com/ledgerwatch/log/v3"
	"golang.org/x/sync/semaphore"
	"google.golang.org/grpc"
	"google.golang.org/protobuf/types/known/emptypb"

	"github.com/ledgerwatch/erigon-lib/gointerfaces"
	"github.com/ledgerwatch/erigon-lib/gointerfaces/grpcutil"
	"github.com/ledgerwatch/erigon-lib/gointerfaces/remote"
	"github.com/ledgerwatch/erigon-lib/kv"
	"github.com/ledgerwatch/erigon-lib/kv/mdbx"
)

// generate the messages and services
type remoteOpts struct {
	bucketsCfg  mdbx.TableCfgFunc
	DialAddress string
	version     gointerfaces.Version
	remoteKV    remote.KVClient
	log         log.Logger
}

type RemoteKV struct {
	remoteKV     remote.KVClient
	log          log.Logger
	buckets      kv.TableCfg
	opts         remoteOpts
	roTxsLimiter *semaphore.Weighted
}

type remoteTx struct {
	stream             remote.KV_TxClient
	ctx                context.Context
	streamCancelFn     context.CancelFunc
	db                 *RemoteKV
	cursors            []*remoteCursor
	statelessCursors   map[string]kv.Cursor
	streamingRequested bool
	id                 uint64
}

type remoteCursor struct {
	ctx        context.Context
	stream     remote.KV_TxClient
	tx         *remoteTx
	bucketName string
	bucketCfg  kv.TableCfgItem
	id         uint32
}

type remoteCursorDupSort struct {
	*remoteCursor
}

func (opts remoteOpts) ReadOnly() remoteOpts {
	return opts
}

func (opts remoteOpts) WithBucketsConfig(f mdbx.TableCfgFunc) remoteOpts {
	opts.bucketsCfg = f
	return opts
}

func (opts remoteOpts) Open() (*RemoteKV, error) {
	db := &RemoteKV{
		opts:         opts,
		remoteKV:     opts.remoteKV,
		log:          log.New("remote_db", opts.DialAddress),
		buckets:      kv.TableCfg{},
		roTxsLimiter: semaphore.NewWeighted(int64(runtime.GOMAXPROCS(-1)) - 1), // 1 less than max to allow unlocking
	}
	customBuckets := opts.bucketsCfg(kv.ChaindataTablesCfg)
	for name, cfg := range customBuckets { // copy map to avoid changing global variable
		db.buckets[name] = cfg
	}

	return db, nil
}

func (opts remoteOpts) MustOpen() kv.RwDB {
	db, err := opts.Open()
	if err != nil {
		panic(err)
	}
	return db
}

// NewRemote defines new remove KV connection (without actually opening it)
// version parameters represent the version the KV client is expecting,
// compatibility check will be performed when the KV connection opens
func NewRemote(v gointerfaces.Version, logger log.Logger, remoteKV remote.KVClient) remoteOpts {
	return remoteOpts{bucketsCfg: mdbx.WithChaindataTables, version: v, log: logger, remoteKV: remoteKV}
}

func (db *RemoteKV) PageSize() uint64        { panic("not implemented") }
func (db *RemoteKV) AllBuckets() kv.TableCfg { return db.buckets }

func (db *RemoteKV) EnsureVersionCompatibility() bool {
	versionReply, err := db.remoteKV.Version(context.Background(), &emptypb.Empty{}, grpc.WaitForReady(true))
	if err != nil {
		db.log.Error("getting Version", "error", err)
		return false
	}
	if !gointerfaces.EnsureVersion(db.opts.version, versionReply) {
		db.log.Error("incompatible interface versions", "client", db.opts.version.String(),
			"server", fmt.Sprintf("%d.%d.%d", versionReply.Major, versionReply.Minor, versionReply.Patch))
		return false
	}
	db.log.Info("interfaces compatible", "client", db.opts.version.String(),
		"server", fmt.Sprintf("%d.%d.%d", versionReply.Major, versionReply.Minor, versionReply.Patch))
	return true
}

func (db *RemoteKV) Close() {}

func (db *RemoteKV) BeginRo(ctx context.Context) (txn kv.Tx, err error) {
	select {
	case <-ctx.Done():
		return nil, ctx.Err()
	default:
	}

	if semErr := db.roTxsLimiter.Acquire(ctx, 1); semErr != nil {
		return nil, semErr
	}

	defer func() {
		// ensure we release the semaphore on error
		if txn == nil {
			db.roTxsLimiter.Release(1)
		}
	}()

	streamCtx, streamCancelFn := context.WithCancel(ctx) // We create child context for the stream so we can cancel it to prevent leak
	stream, err := db.remoteKV.Tx(streamCtx)
	if err != nil {
		streamCancelFn()
		return nil, err
	}
	msg, err := stream.Recv()
	if err != nil {
		streamCancelFn()
		return nil, err
	}
	return &remoteTx{ctx: ctx, db: db, stream: stream, streamCancelFn: streamCancelFn, id: msg.TxID}, nil
}

func (db *RemoteKV) BeginRw(ctx context.Context) (kv.RwTx, error) {
	return nil, fmt.Errorf("remote db provider doesn't support .BeginRw method")
}

func (db *RemoteKV) View(ctx context.Context, f func(tx kv.Tx) error) (err error) {
	tx, err := db.BeginRo(ctx)
	if err != nil {
		return err
	}
	defer tx.Rollback()

	return f(tx)
}

func (db *RemoteKV) Update(ctx context.Context, f func(tx kv.RwTx) error) (err error) {
	return fmt.Errorf("remote db provider doesn't support .Update method")
}

func (tx *remoteTx) ViewID() uint64  { return tx.id }
func (tx *remoteTx) CollectMetrics() {}
func (tx *remoteTx) IncrementSequence(bucket string, amount uint64) (uint64, error) {
	panic("not implemented yet")
}
func (tx *remoteTx) ReadSequence(bucket string) (uint64, error) {
	panic("not implemented yet")
}
func (tx *remoteTx) Append(bucket string, k, v []byte) error    { panic("no write methods") }
func (tx *remoteTx) AppendDup(bucket string, k, v []byte) error { panic("no write methods") }

func (tx *remoteTx) Commit() error {
	panic("remote db is read-only")
}

func (tx *remoteTx) Rollback() {
	// don't close opened cursors - just close stream, server will cleanup everything well
	tx.closeGrpcStream()
	tx.db.roTxsLimiter.Release(1)
}
func (tx *remoteTx) DBSize() (uint64, error) { panic("not implemented") }

func (tx *remoteTx) statelessCursor(bucket string) (kv.Cursor, error) {
	if tx.statelessCursors == nil {
		tx.statelessCursors = make(map[string]kv.Cursor)
	}
	c, ok := tx.statelessCursors[bucket]
	if !ok {
		var err error
		c, err = tx.Cursor(bucket)
		if err != nil {
			return nil, err
		}
		tx.statelessCursors[bucket] = c
	}
	return c, nil
}

func (tx *remoteTx) BucketSize(name string) (uint64, error) { panic("not implemented") }

// TODO: this must be optimized - and implemented as single command on server, with server-side buffered streaming
func (tx *remoteTx) ForEach(bucket string, fromPrefix []byte, walker func(k, v []byte) error) error {
	c, err := tx.Cursor(bucket)
	if err != nil {
		return err
	}
	defer c.Close()

	for k, v, err := c.Seek(fromPrefix); k != nil; k, v, err = c.Next() {
		if err != nil {
			return err
		}
		if err := walker(k, v); err != nil {
			return err
		}
	}
	return nil
}

// TODO: this must be optimized - and implemented as single command on server, with server-side buffered streaming
func (tx *remoteTx) ForPrefix(bucket string, prefix []byte, walker func(k, v []byte) error) error {
	c, err := tx.Cursor(bucket)
	if err != nil {
		return err
	}
	defer c.Close()

	for k, v, err := c.Seek(prefix); k != nil; k, v, err = c.Next() {
		if err != nil {
			return err
		}
		if !bytes.HasPrefix(k, prefix) {
			break
		}
		if err := walker(k, v); err != nil {
			return err
		}
	}
	return nil
}

func (tx *remoteTx) ForAmount(bucket string, fromPrefix []byte, amount uint32, walker func(k, v []byte) error) error {
	if amount == 0 {
		return nil
	}
	c, err := tx.Cursor(bucket)
	if err != nil {
		return err
	}
	defer c.Close()

	for k, v, err := c.Seek(fromPrefix); k != nil && amount > 0; k, v, err = c.Next() {
		if err != nil {
			return err
		}
		if err := walker(k, v); err != nil {
			return err
		}
		amount--
	}
	return nil
}

func (tx *remoteTx) GetOne(bucket string, key []byte) (val []byte, err error) {
	c, err := tx.statelessCursor(bucket)
	if err != nil {
		return nil, err
	}
	_, val, err = c.SeekExact(key)
	return val, err
}

func (tx *remoteTx) Has(bucket string, key []byte) (bool, error) {
	c, err := tx.statelessCursor(bucket)
	if err != nil {
		return false, err
	}
	k, _, err := c.Seek(key)
	if err != nil {
		return false, err
	}
	return bytes.Equal(key, k), nil
}

func (c *remoteCursor) SeekExact(key []byte) (k, val []byte, err error) {
	return c.seekExact(key)
}

func (c *remoteCursor) Prev() ([]byte, []byte, error) {
	return c.prev()
}

func (tx *remoteTx) Cursor(bucket string) (kv.Cursor, error) {
	b := tx.db.buckets[bucket]
	c := &remoteCursor{tx: tx, ctx: tx.ctx, bucketName: bucket, bucketCfg: b, stream: tx.stream}
	tx.cursors = append(tx.cursors, c)
	if err := c.stream.Send(&remote.Cursor{Op: remote.Op_OPEN, BucketName: c.bucketName}); err != nil {
		return nil, err
	}
	msg, err := c.stream.Recv()
	if err != nil {
		return nil, err
	}
	c.id = msg.CursorID
	return c, nil
}

func (c *remoteCursor) Put(key []byte, value []byte) error            { panic("not supported") }
func (c *remoteCursor) PutNoOverwrite(key []byte, value []byte) error { panic("not supported") }
func (c *remoteCursor) Append(key []byte, value []byte) error         { panic("not supported") }
func (c *remoteCursor) Delete(k []byte) error                         { panic("not supported") }
func (c *remoteCursor) DeleteCurrent() error                          { panic("not supported") }
func (c *remoteCursor) Count() (uint64, error)                        { panic("not supported") }

func (c *remoteCursor) first() ([]byte, []byte, error) {
	if err := c.stream.Send(&remote.Cursor{Cursor: c.id, Op: remote.Op_FIRST}); err != nil {
		return []byte{}, nil, err
	}
	pair, err := c.stream.Recv()
	if err != nil {
		return []byte{}, nil, err
	}
	return pair.K, pair.V, nil
}

func (c *remoteCursor) next() ([]byte, []byte, error) {
	if err := c.stream.Send(&remote.Cursor{Cursor: c.id, Op: remote.Op_NEXT}); err != nil {
		return []byte{}, nil, err
	}
	pair, err := c.stream.Recv()
	if err != nil {
		return []byte{}, nil, err
	}
	return pair.K, pair.V, nil
}
func (c *remoteCursor) nextDup() ([]byte, []byte, error) {
	if err := c.stream.Send(&remote.Cursor{Cursor: c.id, Op: remote.Op_NEXT_DUP}); err != nil {
		return []byte{}, nil, err
	}
	pair, err := c.stream.Recv()
	if err != nil {
		return []byte{}, nil, err
	}
	return pair.K, pair.V, nil
}
func (c *remoteCursor) nextNoDup() ([]byte, []byte, error) {
	if err := c.stream.Send(&remote.Cursor{Cursor: c.id, Op: remote.Op_NEXT_NO_DUP}); err != nil {
		return []byte{}, nil, err
	}
	pair, err := c.stream.Recv()
	if err != nil {
		return []byte{}, nil, err
	}
	return pair.K, pair.V, nil
}
func (c *remoteCursor) prev() ([]byte, []byte, error) {
	if err := c.stream.Send(&remote.Cursor{Cursor: c.id, Op: remote.Op_PREV}); err != nil {
		return []byte{}, nil, err
	}
	pair, err := c.stream.Recv()
	if err != nil {
		return []byte{}, nil, err
	}
	return pair.K, pair.V, nil
}
func (c *remoteCursor) prevDup() ([]byte, []byte, error) {
	if err := c.stream.Send(&remote.Cursor{Cursor: c.id, Op: remote.Op_PREV_DUP}); err != nil {
		return []byte{}, nil, err
	}
	pair, err := c.stream.Recv()
	if err != nil {
		return []byte{}, nil, err
	}
	return pair.K, pair.V, nil
}
func (c *remoteCursor) prevNoDup() ([]byte, []byte, error) {
	if err := c.stream.Send(&remote.Cursor{Cursor: c.id, Op: remote.Op_PREV_NO_DUP}); err != nil {
		return []byte{}, nil, err
	}
	pair, err := c.stream.Recv()
	if err != nil {
		return []byte{}, nil, err
	}
	return pair.K, pair.V, nil
}
func (c *remoteCursor) last() ([]byte, []byte, error) {
	if err := c.stream.Send(&remote.Cursor{Cursor: c.id, Op: remote.Op_LAST}); err != nil {
		return []byte{}, nil, err
	}
	pair, err := c.stream.Recv()
	if err != nil {
		return []byte{}, nil, err
	}
	return pair.K, pair.V, nil
}
func (c *remoteCursor) setRange(k []byte) ([]byte, []byte, error) {
	if err := c.stream.Send(&remote.Cursor{Cursor: c.id, Op: remote.Op_SEEK, K: k}); err != nil {
		return []byte{}, nil, err
	}
	pair, err := c.stream.Recv()
	if err != nil {
		return []byte{}, nil, err
	}
	return pair.K, pair.V, nil
}
func (c *remoteCursor) seekExact(k []byte) ([]byte, []byte, error) {
	if err := c.stream.Send(&remote.Cursor{Cursor: c.id, Op: remote.Op_SEEK_EXACT, K: k}); err != nil {
		return []byte{}, nil, err
	}
	pair, err := c.stream.Recv()
	if err != nil {
		return []byte{}, nil, err
	}
	return pair.K, pair.V, nil
}
func (c *remoteCursor) getBothRange(k, v []byte) ([]byte, error) {
	if err := c.stream.Send(&remote.Cursor{Cursor: c.id, Op: remote.Op_SEEK_BOTH, K: k, V: v}); err != nil {
		return nil, err
	}
	pair, err := c.stream.Recv()
	if err != nil {
		return nil, err
	}
	return pair.V, nil
}
func (c *remoteCursor) seekBothExact(k, v []byte) ([]byte, []byte, error) {
	if err := c.stream.Send(&remote.Cursor{Cursor: c.id, Op: remote.Op_SEEK_BOTH_EXACT, K: k, V: v}); err != nil {
		return []byte{}, nil, err
	}
	pair, err := c.stream.Recv()
	if err != nil {
		return []byte{}, nil, err
	}
	return pair.K, pair.V, nil
}
func (c *remoteCursor) firstDup() ([]byte, error) {
	if err := c.stream.Send(&remote.Cursor{Cursor: c.id, Op: remote.Op_FIRST_DUP}); err != nil {
		return nil, err
	}
	pair, err := c.stream.Recv()
	if err != nil {
		return nil, err
	}
	return pair.V, nil
}
func (c *remoteCursor) lastDup() ([]byte, error) {
	if err := c.stream.Send(&remote.Cursor{Cursor: c.id, Op: remote.Op_LAST_DUP}); err != nil {
		return nil, err
	}
	pair, err := c.stream.Recv()
	if err != nil {
		return nil, err
	}
	return pair.V, nil
}
func (c *remoteCursor) getCurrent() ([]byte, []byte, error) {
	if err := c.stream.Send(&remote.Cursor{Cursor: c.id, Op: remote.Op_CURRENT}); err != nil {
		return []byte{}, nil, err
	}
	pair, err := c.stream.Recv()
	if err != nil {
		return []byte{}, nil, err
	}
	return pair.K, pair.V, nil
}

func (c *remoteCursor) Current() ([]byte, []byte, error) {
	return c.getCurrent()
}

// Seek - doesn't start streaming (because much of code does only several .Seek calls without reading sequence of data)
// .Next() - does request streaming (if configured by user)
func (c *remoteCursor) Seek(seek []byte) ([]byte, []byte, error) {
	return c.setRange(seek)
}

func (c *remoteCursor) First() ([]byte, []byte, error) {
	return c.first()
}

// Next - returns next data element from server, request streaming (if configured by user)
func (c *remoteCursor) Next() ([]byte, []byte, error) {
	return c.next()
}

func (c *remoteCursor) Last() ([]byte, []byte, error) {
	return c.last()
}

func (tx *remoteTx) closeGrpcStream() {
	if tx.stream == nil {
		return
	}
	defer tx.streamCancelFn() // hard cancel stream if graceful wasn't successful

	if tx.streamingRequested {
		// if streaming is in progress, can't use `CloseSend` - because
		// server will not read it right not - it busy with streaming data
		// TODO: set flag 'tx.streamingRequested' to false when got terminator from server (nil key or os.EOF)
		tx.streamCancelFn()
	} else {
		// try graceful close stream
		err := tx.stream.CloseSend()
		if err != nil {
			doLog := !grpcutil.IsEndOfStream(err)
			if doLog {
				log.Warn("couldn't send msg CloseSend to server", "err", err)
			}
		} else {
			_, err = tx.stream.Recv()
			if err != nil {
				doLog := !grpcutil.IsEndOfStream(err)
				if doLog {
					log.Warn("received unexpected error from server after CloseSend", "err", err)
				}
			}
		}
	}
	tx.stream = nil
	tx.streamingRequested = false
}

func (c *remoteCursor) Close() {
	if c.stream == nil {
		return
	}
	st := c.stream
	c.stream = nil
	if err := st.Send(&remote.Cursor{Cursor: c.id, Op: remote.Op_CLOSE}); err == nil {
		_, _ = st.Recv()
	}
}

func (tx *remoteTx) CursorDupSort(bucket string) (kv.CursorDupSort, error) {
	b := tx.db.buckets[bucket]
	c := &remoteCursor{tx: tx, ctx: tx.ctx, bucketName: bucket, bucketCfg: b, stream: tx.stream}
	tx.cursors = append(tx.cursors, c)
	if err := c.stream.Send(&remote.Cursor{Op: remote.Op_OPEN_DUP_SORT, BucketName: c.bucketName}); err != nil {
		return nil, err
	}
	msg, err := c.stream.Recv()
	if err != nil {
		return nil, err
	}
	c.id = msg.CursorID
	return &remoteCursorDupSort{remoteCursor: c}, nil
}

func (c *remoteCursorDupSort) SeekBothExact(key, value []byte) ([]byte, []byte, error) {
	return c.seekBothExact(key, value)
}

func (c *remoteCursorDupSort) SeekBothRange(key, value []byte) ([]byte, error) {
	return c.getBothRange(key, value)
}

func (c *remoteCursorDupSort) DeleteExact(k1, k2 []byte) error      { panic("not supported") }
func (c *remoteCursorDupSort) AppendDup(k []byte, v []byte) error   { panic("not supported") }
func (c *remoteCursorDupSort) PutNoDupData(key, value []byte) error { panic("not supported") }
func (c *remoteCursorDupSort) DeleteCurrentDuplicates() error       { panic("not supported") }
func (c *remoteCursorDupSort) CountDuplicates() (uint64, error)     { panic("not supported") }

func (c *remoteCursorDupSort) FirstDup() ([]byte, error) {
	return c.firstDup()
}
func (c *remoteCursorDupSort) NextDup() ([]byte, []byte, error) {
	return c.nextDup()
}
func (c *remoteCursorDupSort) NextNoDup() ([]byte, []byte, error) {
	return c.nextNoDup()
}
func (c *remoteCursorDupSort) PrevDup() ([]byte, []byte, error) {
	return c.prevDup()
}
func (c *remoteCursorDupSort) PrevNoDup() ([]byte, []byte, error) {
	return c.prevNoDup()
}
func (c *remoteCursorDupSort) LastDup() ([]byte, error) {
	return c.lastDup()
}
