/*
   Copyright 2021 Erigon contributors

   Licensed under the Apache License, Version 2.0 (the "License");
   you may not use this file except in compliance with the License.
   You may obtain a copy of the License at

       http://www.apache.org/licenses/LICENSE-2.0

   Unless required by applicable law or agreed to in writing, software
   distributed under the License is distributed on an "AS IS" BASIS,
   WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
   See the License for the specific language governing permissions and
   limitations under the License.
*/

package remotedb

import (
	"bytes"
	"context"
	"encoding/binary"
	"fmt"
	"runtime"

	"github.com/ledgerwatch/erigon-lib/kv/iter"
	"github.com/ledgerwatch/erigon-lib/kv/order"
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
	remoteKV    remote.KVClient
	log         log.Logger
	bucketsCfg  mdbx.TableCfgFunc
	DialAddress string
	version     gointerfaces.Version
}

type RemoteKV struct {
	remoteKV     remote.KVClient
	log          log.Logger
	buckets      kv.TableCfg
	roTxsLimiter *semaphore.Weighted
	opts         remoteOpts
}

type remoteTx struct {
	stream             remote.KV_TxClient
	ctx                context.Context
	streamCancelFn     context.CancelFunc
	db                 *RemoteKV
	statelessCursors   map[string]kv.Cursor
	cursors            []*remoteCursor
	streams            []kv.Closer
	viewID, id         uint64
	streamingRequested bool
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
	targetSemCount := int64(runtime.GOMAXPROCS(-1)) - 1
	if targetSemCount <= 1 {
		targetSemCount = 2
	}

	db := &RemoteKV{
		opts:         opts,
		remoteKV:     opts.remoteKV,
		log:          log.New("remote_db", opts.DialAddress),
		buckets:      kv.TableCfg{},
		roTxsLimiter: semaphore.NewWeighted(targetSemCount), // 1 less than max to allow unlocking
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
func (db *RemoteKV) ReadOnly() bool          { return true }
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
	return &remoteTx{ctx: ctx, db: db, stream: stream, streamCancelFn: streamCancelFn, viewID: msg.ViewID, id: msg.TxID}, nil
}

func (db *RemoteKV) BeginRw(ctx context.Context) (kv.RwTx, error) {
	return nil, fmt.Errorf("remote db provider doesn't support .BeginRw method")
}
func (db *RemoteKV) BeginRwNosync(ctx context.Context) (kv.RwTx, error) {
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
func (db *RemoteKV) UpdateNosync(ctx context.Context, f func(tx kv.RwTx) error) (err error) {
	return fmt.Errorf("remote db provider doesn't support .Update method")
}

func (tx *remoteTx) ViewID() uint64  { return tx.viewID }
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
	for _, c := range tx.streams {
		c.Close()
	}
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

func (tx *remoteTx) ForEach(bucket string, fromPrefix []byte, walker func(k, v []byte) error) error {
	it, err := tx.Range(bucket, fromPrefix, nil)
	if err != nil {
		return err
	}
	for it.HasNext() {
		k, v, err := it.Next()
		if err != nil {
			return err
		}
		if err := walker(k, v); err != nil {
			return err
		}
	}
	return nil
}

func (tx *remoteTx) ForPrefix(bucket string, prefix []byte, walker func(k, v []byte) error) error {
	it, err := tx.Prefix(bucket, prefix)
	if err != nil {
		return err
	}
	for it.HasNext() {
		k, v, err := it.Next()
		if err != nil {
			return err
		}
		if err := walker(k, v); err != nil {
			return err
		}
	}
	return nil
}

// TODO: this must be deprecated
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

func (tx *remoteTx) GetOne(bucket string, k []byte) (val []byte, err error) {
	c, err := tx.statelessCursor(bucket)
	if err != nil {
		return nil, err
	}
	_, val, err = c.SeekExact(k)
	return val, err
}

func (tx *remoteTx) Has(bucket string, k []byte) (bool, error) {
	c, err := tx.statelessCursor(bucket)
	if err != nil {
		return false, err
	}
	kk, _, err := c.Seek(k)
	if err != nil {
		return false, err
	}
	return bytes.Equal(k, kk), nil
}

func (c *remoteCursor) SeekExact(k []byte) (key, val []byte, err error) {
	return c.seekExact(k)
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

func (tx *remoteTx) ListBuckets() ([]string, error) {
	return nil, fmt.Errorf("function ListBuckets is not implemented for remoteTx")
}

func (c *remoteCursor) Put(k []byte, v []byte) error            { panic("not supported") }
func (c *remoteCursor) PutNoOverwrite(k []byte, v []byte) error { panic("not supported") }
func (c *remoteCursor) Append(k []byte, v []byte) error         { panic("not supported") }
func (c *remoteCursor) Delete(k []byte) error                   { panic("not supported") }
func (c *remoteCursor) DeleteCurrent() error                    { panic("not supported") }
func (c *remoteCursor) Count() (uint64, error) {
	if err := c.stream.Send(&remote.Cursor{Cursor: c.id, Op: remote.Op_COUNT}); err != nil {
		return 0, err
	}
	pair, err := c.stream.Recv()
	if err != nil {
		return 0, err
	}
	return binary.BigEndian.Uint64(pair.V), nil

}

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

func (c *remoteCursorDupSort) SeekBothExact(k, v []byte) ([]byte, []byte, error) {
	return c.seekBothExact(k, v)
}

func (c *remoteCursorDupSort) SeekBothRange(k, v []byte) ([]byte, error) {
	return c.getBothRange(k, v)
}

func (c *remoteCursorDupSort) DeleteExact(k1, k2 []byte) error    { panic("not supported") }
func (c *remoteCursorDupSort) AppendDup(k []byte, v []byte) error { panic("not supported") }
func (c *remoteCursorDupSort) PutNoDupData(k, v []byte) error     { panic("not supported") }
func (c *remoteCursorDupSort) DeleteCurrentDuplicates() error     { panic("not supported") }
func (c *remoteCursorDupSort) CountDuplicates() (uint64, error)   { panic("not supported") }

func (c *remoteCursorDupSort) FirstDup() ([]byte, error)          { return c.firstDup() }
func (c *remoteCursorDupSort) NextDup() ([]byte, []byte, error)   { return c.nextDup() }
func (c *remoteCursorDupSort) NextNoDup() ([]byte, []byte, error) { return c.nextNoDup() }
func (c *remoteCursorDupSort) PrevDup() ([]byte, []byte, error)   { return c.prevDup() }
func (c *remoteCursorDupSort) PrevNoDup() ([]byte, []byte, error) { return c.prevNoDup() }
func (c *remoteCursorDupSort) LastDup() ([]byte, error)           { return c.lastDup() }

// Temporal Methods
func (tx *remoteTx) HistoryGet(name kv.History, k []byte, ts uint64) (v []byte, ok bool, err error) {
	reply, err := tx.db.remoteKV.HistoryGet(tx.ctx, &remote.HistoryGetReq{TxId: tx.id, Table: string(name), K: k, Ts: ts})
	if err != nil {
		return nil, false, err
	}
	return reply.V, reply.Ok, nil
}

func (tx *remoteTx) IndexRange(name kv.InvertedIdx, k []byte, fromTs, toTs, limit int) (timestamps iter.U64, err error) {
	return iter.PaginateU64(func(pageToken string) (arr []uint64, nextPageToken string, err error) {
		req := &remote.IndexRangeReq{TxId: tx.id, Table: string(name), K: k, FromTs: int64(fromTs), ToTs: int64(toTs), Limit: int64(limit)}
		reply, err := tx.db.remoteKV.IndexRange(tx.ctx, req)
		if err != nil {
			return nil, "", err
		}
		return reply.Timestamps, reply.NextPageToken, nil
	}), nil
}

func (tx *remoteTx) Prefix(table string, prefix []byte) (iter.KV, error) {
	nextPrefix, ok := kv.NextSubtree(prefix)
	if !ok {
		return tx.Range(table, prefix, nil)
	}
	return tx.Range(table, prefix, nextPrefix)
}

/*
func (tx *remoteTx) IndexStream(name kv.InvertedIdx, k []byte, fromTs, toTs, limit int) (timestamps iter.U64, err error) {
	//TODO: maybe add ctx.WithCancel
	stream, err := tx.db.remoteKV.IndexStream(tx.ctx, &remote.IndexRangeReq{TxId: tx.id, Table: string(name), K: k, FromTs: int64(fromTs), ToTs: int64(toTs), Limit: int32(limit)})
	if err != nil {
		return nil, err
	}
	it := &grpc2U64Stream[*remote.IndexRangeReply]{
		grpc2UnaryStream[*remote.IndexRangeReply, uint64]{stream: stream, unwrap: func(msg *remote.IndexRangeReply) []uint64 { return msg.Timestamps }},
	}
	tx.streams = append(tx.streams, it)
	return it, nil
}

/*
func (tx *remoteTx) streamOrderLimit(table string, fromPrefix, toPrefix []byte, asc order.By, limit int) (iter.KV, error) {
	req := &remote.RangeReq{TxId: tx.id, Table: table, FromPrefix: fromPrefix, ToPrefix: toPrefix, OrderAscend: bool(asc), Limit: int32(limit)}
	stream, err := tx.db.remoteKV.Stream(tx.ctx, req)
	if err != nil {
		return nil, err
	}
	it := &grpc2Pairs[*remote.Pairs]{stream: stream}
	tx.streams = append(tx.streams, it)
	return it, nil
}

func (tx *remoteTx) Stream(table string, fromPrefix, toPrefix []byte) (iter.KV, error) {
	return tx.StreamAscend(table, fromPrefix, toPrefix, -1)
}
func (tx *remoteTx) StreamAscend(table string, fromPrefix, toPrefix []byte, limit int) (iter.KV, error) {
	return tx.streamOrderLimit(table, fromPrefix, toPrefix, true, limit)
}
func (tx *remoteTx) StreamDescend(table string, fromPrefix, toPrefix []byte, limit int) (iter.KV, error) {
	return tx.streamOrderLimit(table, fromPrefix, toPrefix, false, limit)
}
*/

func (tx *remoteTx) rangeOrderLimit(table string, fromPrefix, toPrefix []byte, asc order.By, limit int) (iter.KV, error) {
	return iter.PaginateKV(func(pageToken string) (keys [][]byte, values [][]byte, nextPageToken string, err error) {
		req := &remote.RangeReq{TxId: tx.id, Table: table, FromPrefix: fromPrefix, ToPrefix: toPrefix, OrderAscend: bool(asc), Limit: int64(limit)}
		reply, err := tx.db.remoteKV.Range(tx.ctx, req)
		if err != nil {
			return nil, nil, "", err
		}
		return reply.Keys, reply.Values, reply.NextPageToken, nil
	}), nil
}
func (tx *remoteTx) Range(table string, fromPrefix, toPrefix []byte) (iter.KV, error) {
	return tx.rangeOrderLimit(table, fromPrefix, toPrefix, order.Asc, -1)
}
func (tx *remoteTx) RangeAscend(table string, fromPrefix, toPrefix []byte, limit int) (iter.KV, error) {
	return tx.rangeOrderLimit(table, fromPrefix, toPrefix, order.Asc, limit)
}
func (tx *remoteTx) RangeDescend(table string, fromPrefix, toPrefix []byte, limit int) (iter.KV, error) {
	return tx.rangeOrderLimit(table, fromPrefix, toPrefix, order.Desc, limit)
}
func (tx *remoteTx) RangeDupSort(table string, key []byte, fromPrefix, toPrefix []byte, asc order.By, limit int) (iter.KV, error) {
	panic("not implemented yet")
}

/*
type grpcStream[Msg any] interface {
	Recv() (Msg, error)
	CloseSend() error
}

type parisMsg interface {
	GetKeys() [][]byte
	GetValues() [][]byte
}
type grpc2Pairs[Msg parisMsg] struct {
	stream     grpcStream[Msg]
	lastErr    error
	lastKeys   [][]byte
	lastValues [][]byte
	i          int
}

func (it *grpc2Pairs[Msg]) NextBatch() ([][]byte, [][]byte, error) {
	keys := it.lastKeys[it.i:]
	values := it.lastValues[it.i:]
	it.i = len(it.lastKeys)
	return keys, values, nil
}
func (it *grpc2Pairs[Msg]) HasNext() bool {
	if it.lastErr != nil {
		return true
	}
	if it.i < len(it.lastKeys) {
		return true
	}

	it.i = 0
	msg, err := it.stream.Recv()
	if err != nil {
		if errors.Is(err, io.EOF) {
			return false
		}
		it.lastErr = err
		return true
	}
	it.lastKeys = msg.GetKeys()
	it.lastValues = msg.GetValues()
	return len(it.lastKeys) > 0
}
func (it *grpc2Pairs[Msg]) Close() {
	//_ = it.stream.CloseSend()
}
func (it *grpc2Pairs[Msg]) Next() ([]byte, []byte, error) {
	if it.lastErr != nil {
		return nil, nil, it.lastErr
	}
	k := it.lastKeys[it.i]
	v := it.lastValues[it.i]
	it.i++
	return k, v, nil
}

type grpc2U64Stream[Msg any] struct {
	grpc2UnaryStream[Msg, uint64]
}

func (it *grpc2U64Stream[Msg]) ToBitmap() (*roaring64.Bitmap, error) {
	bm := roaring64.New()
	for it.HasNext() {
		batch, err := it.NextBatch()
		if err != nil {
			return nil, err
		}
		bm.AddMany(batch)
	}
	return bm, nil
}

type grpc2UnaryStream[Msg any, Res any] struct {
	stream  grpcStream[Msg]
	unwrap  func(Msg) []Res
	lastErr error
	last    []Res
	i       int
}

func (it *grpc2UnaryStream[Msg, Res]) NextBatch() ([]Res, error) {
	v := it.last[it.i:]
	it.i = len(it.last)
	return v, nil
}
func (it *grpc2UnaryStream[Msg, Res]) HasNext() bool {
	if it.lastErr != nil {
		return true
	}
	if it.i < len(it.last) {
		return true
	}

	it.i = 0
	msg, err := it.stream.Recv()
	if err != nil {
		if errors.Is(err, io.EOF) {
			return false
		}
		it.lastErr = err
		return true
	}
	it.last = it.unwrap(msg)
	return len(it.last) > 0
}
func (it *grpc2UnaryStream[Msg, Res]) Close() {
	//_ = it.stream.CloseSend()
}
func (it *grpc2UnaryStream[Msg, Res]) Next() (Res, error) {
	v := it.last[it.i]
	it.i++
	return v, nil
}
*/
