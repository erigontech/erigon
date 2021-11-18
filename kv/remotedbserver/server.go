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

package remotedbserver

import (
	"context"
	"fmt"
	"io"
	"sync"
	"time"

	"github.com/ledgerwatch/erigon-lib/gointerfaces/remote"
	"github.com/ledgerwatch/erigon-lib/gointerfaces/types"
	"github.com/ledgerwatch/erigon-lib/kv"
	"github.com/ledgerwatch/log/v3"
	"google.golang.org/protobuf/types/known/emptypb"
)

const MaxTxTTL = 60 * time.Second

// KvServiceAPIVersion - use it to track changes in API
// 1.1.0 - added pending transactions, add methods eth_getRawTransactionByHash, eth_retRawTransactionByBlockHashAndIndex, eth_retRawTransactionByBlockNumberAndIndex| Yes     |                                            |
// 1.2.0 - Added separated services for mining and txpool methods
// 2.0.0 - Rename all buckets
// 3.0.0 - ??
// 4.0.0 - Server send tx.ViewID() after open tx
// 5.0 - BlockTransaction table now has canonical ids (txs of non-canonical blocks moving to NonCanonicalTransaction table)
var KvServiceAPIVersion = &types.VersionReply{Major: 5, Minor: 0, Patch: 0}

type KvServer struct {
	remote.UnimplementedKVServer // must be embedded to have forward compatible implementations.

	kv                 kv.RwDB
	stateChangeStreams *StateChangeStreams
	ctx                context.Context
}

func NewKvServer(ctx context.Context, kv kv.RwDB) *KvServer {
	return &KvServer{kv: kv, stateChangeStreams: newStateChangeStreams(), ctx: ctx}
}

// Version returns the service-side interface version number
func (s *KvServer) Version(context.Context, *emptypb.Empty) (*types.VersionReply, error) {
	dbSchemaVersion := &kv.DBSchemaVersion
	if KvServiceAPIVersion.Major > dbSchemaVersion.Major {
		return KvServiceAPIVersion, nil
	}
	if dbSchemaVersion.Major > KvServiceAPIVersion.Major {
		return dbSchemaVersion, nil
	}
	if KvServiceAPIVersion.Minor > dbSchemaVersion.Minor {
		return KvServiceAPIVersion, nil
	}
	if dbSchemaVersion.Minor > KvServiceAPIVersion.Minor {
		return dbSchemaVersion, nil
	}
	return dbSchemaVersion, nil
}

func (s *KvServer) Tx(stream remote.KV_TxServer) error {
	tx, errBegin := s.kv.BeginRo(stream.Context())
	if errBegin != nil {
		return fmt.Errorf("server-side error: %w", errBegin)
	}
	rollback := func() {
		tx.Rollback()
	}
	defer rollback()

	if err := stream.Send(&remote.Pair{TxID: tx.ViewID()}); err != nil {
		return fmt.Errorf("server-side error: %w", err)
	}

	var CursorID uint32
	type CursorInfo struct {
		bucket string
		c      kv.Cursor
		k, v   []byte //fields to save current position of cursor - used when Tx reopen
	}
	cursors := map[uint32]*CursorInfo{}

	txTicker := time.NewTicker(MaxTxTTL)
	defer txTicker.Stop()

	// send all items to client, if k==nil - still send it to client and break loop
	for {
		in, recvErr := stream.Recv()
		if recvErr != nil {
			if recvErr == io.EOF { // termination
				return nil
			}
			return fmt.Errorf("server-side error: %w", recvErr)
		}

		//TODO: protect against client - which doesn't send any requests
		select {
		default:
		case <-txTicker.C:
			for _, c := range cursors { // save positions of cursor, will restore after Tx reopening
				k, v, err := c.c.Current()
				if err != nil {
					return err
				}
				c.k = bytesCopy(k)
				c.v = bytesCopy(v)
			}

			tx.Rollback()
			tx, errBegin = s.kv.BeginRo(stream.Context())
			if errBegin != nil {
				return fmt.Errorf("server-side error, BeginRo: %w", errBegin)
			}

			for _, c := range cursors { // restore all cursors position
				var err error
				c.c, err = tx.Cursor(c.bucket)
				if err != nil {
					return err
				}
				switch casted := c.c.(type) {
				case kv.CursorDupSort:
					v, err := casted.SeekBothRange(c.k, c.v)
					if err != nil {
						return fmt.Errorf("server-side error: %w", err)
					}
					if v == nil { // it may happen that key where we stopped disappeared after transaction reopen, then just move to next key
						_, _, err = casted.Next()
						if err != nil {
							return fmt.Errorf("server-side error: %w", err)
						}
					}
				case kv.Cursor:
					if _, _, err := c.c.Seek(c.k); err != nil {
						return fmt.Errorf("server-side error: %w", err)
					}
				}
			}
		}

		var c kv.Cursor
		if in.BucketName == "" {
			cInfo, ok := cursors[in.Cursor]
			if !ok {
				return fmt.Errorf("server-side error: unknown Cursor=%d, Op=%s", in.Cursor, in.Op)
			}
			c = cInfo.c
		}
		switch in.Op {
		case remote.Op_OPEN:
			CursorID++
			var err error
			c, err = tx.Cursor(in.BucketName)
			if err != nil {
				return err
			}
			cursors[CursorID] = &CursorInfo{
				bucket: in.BucketName,
				c:      c,
			}
			if err := stream.Send(&remote.Pair{CursorID: CursorID}); err != nil {
				return fmt.Errorf("server-side error: %w", err)
			}
			continue
		case remote.Op_CLOSE:
			cInfo, ok := cursors[in.Cursor]
			if !ok {
				return fmt.Errorf("server-side error: unknown Cursor=%d, Op=%s", in.Cursor, in.Op)
			}
			cInfo.c.Close()
			delete(cursors, in.Cursor)
			if err := stream.Send(&remote.Pair{}); err != nil {
				return fmt.Errorf("server-side error: %w", err)
			}
			continue
		default:
		}

		if err := handleOp(c, stream, in); err != nil {
			return fmt.Errorf("server-side error: %w", err)
		}
	}
}

func handleOp(c kv.Cursor, stream remote.KV_TxServer, in *remote.Cursor) error {
	var k, v []byte
	var err error
	switch in.Op {
	case remote.Op_FIRST:
		k, v, err = c.First()
	case remote.Op_FIRST_DUP:
		v, err = c.(kv.CursorDupSort).FirstDup()
	case remote.Op_SEEK:
		k, v, err = c.Seek(in.K)
	case remote.Op_SEEK_BOTH:
		v, err = c.(kv.CursorDupSort).SeekBothRange(in.K, in.V)
	case remote.Op_CURRENT:
		k, v, err = c.Current()
	case remote.Op_LAST:
		k, v, err = c.Last()
	case remote.Op_LAST_DUP:
		v, err = c.(kv.CursorDupSort).LastDup()
	case remote.Op_NEXT:
		k, v, err = c.Next()
	case remote.Op_NEXT_DUP:
		k, v, err = c.(kv.CursorDupSort).NextDup()
	case remote.Op_NEXT_NO_DUP:
		k, v, err = c.(kv.CursorDupSort).NextNoDup()
	case remote.Op_PREV:
		k, v, err = c.Prev()
	//case remote.Op_PREV_DUP:
	//	k, v, err = c.(ethdb.CursorDupSort).Prev()
	//	if err != nil {
	//		return err
	//	}
	//case remote.Op_PREV_NO_DUP:
	//	k, v, err = c.Prev()
	//	if err != nil {
	//		return err
	//	}
	case remote.Op_SEEK_EXACT:
		k, v, err = c.SeekExact(in.K)
	case remote.Op_SEEK_BOTH_EXACT:
		k, v, err = c.(kv.CursorDupSort).SeekBothExact(in.K, in.V)
	default:
		return fmt.Errorf("unknown operation: %s", in.Op)
	}
	if err != nil {
		return err
	}

	if err := stream.Send(&remote.Pair{K: k, V: v}); err != nil {
		return err
	}

	return nil
}

func bytesCopy(b []byte) []byte {
	if b == nil {
		return nil
	}
	copiedBytes := make([]byte, len(b))
	copy(copiedBytes, b)
	return copiedBytes
}

func (s *KvServer) StateChanges(req *remote.StateChangeRequest, server remote.KV_StateChangesServer) error {
	clean := s.stateChangeStreams.Add(server)
	defer clean()
	select {
	case <-s.ctx.Done():
		return nil
	case <-server.Context().Done():
		return nil
	}
}

func (s *KvServer) SendStateChanges(ctx context.Context, sc *remote.StateChangeBatch) {
	if err := s.stateChangeStreams.Broadcast(ctx, sc); err != nil {
		log.Warn("Sending new peer notice to core P2P failed", "error", err)
	}
}

type StateChangeStreams struct {
	mu      sync.RWMutex
	id      uint
	streams map[uint]remote.KV_StateChangesServer
}

func newStateChangeStreams() *StateChangeStreams {
	return &StateChangeStreams{}
}

func (s *StateChangeStreams) Add(stream remote.KV_StateChangesServer) (remove func()) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.streams == nil {
		s.streams = make(map[uint]remote.KV_StateChangesServer)
	}
	s.id++
	id := s.id
	s.streams[id] = stream
	return func() { s.remove(id) }
}

func (s *StateChangeStreams) doBroadcast(ctx context.Context, reply *remote.StateChangeBatch) (ids []uint, errs []error) {
	s.mu.RLock()
	defer s.mu.RUnlock()
Loop:
	for id, stream := range s.streams {
		select {
		case <-ctx.Done():
			errs = append(errs, ctx.Err())
			break Loop
		default:
		}
		err := stream.Send(reply)
		if err != nil {
			select {
			case <-stream.Context().Done():
				ids = append(ids, id)
			default:
			}
			errs = append(errs, err)
		}
	}
	return
}

func (s *StateChangeStreams) Broadcast(ctx context.Context, reply *remote.StateChangeBatch) (errs []error) {
	var ids []uint
	ids, errs = s.doBroadcast(ctx, reply)
	if len(ids) == 0 {
		return errs
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	for _, id := range ids {
		delete(s.streams, id)
	}
	return errs
}

func (s *StateChangeStreams) Len() int {
	s.mu.RLock()
	defer s.mu.RUnlock()
	return len(s.streams)
}

func (s *StateChangeStreams) remove(id uint) {
	s.mu.Lock()
	defer s.mu.Unlock()
	_, ok := s.streams[id]
	if !ok { // double-unsubscribe support
		return
	}
	delete(s.streams, id)
}
