package remotedbserver

import (
	"context"
	"fmt"

	"github.com/ledgerwatch/turbo-geth/ethdb"
	"github.com/ledgerwatch/turbo-geth/ethdb/remote"
)

type Server2 struct {
	remote.UnimplementedKvServer // must be embedded to have forward compatible implementations.

	lastHandle uint64

	kv ethdb.KV

	tx ethdb.Tx
	// Buckets opened by the client
	buckets map[uint64]ethdb.Bucket
	// Cursors opened by the client
	cursors map[uint64]ethdb.Cursor
	// List of cursors opened in each bucket
	cursorsByBucket map[uint64][]uint64
}

func NewServer2(kv ethdb.KV) *Server2 {
	return &Server2{
		kv:              kv,
		buckets:         make(map[uint64]ethdb.Bucket, 2),
		cursors:         make(map[uint64]ethdb.Cursor, 2),
		cursorsByBucket: make(map[uint64][]uint64, 2),
	}
}

func (s *Server2) Begin(ctx context.Context, in *remote.BeginRequest) (out *remote.BeginReply, err error) {
	s.tx, err = s.kv.Begin(ctx, false)
	if err != nil {
		return nil, err
	}
	return &remote.BeginReply{}, nil
}

func (s *Server2) Rollback(ctx context.Context, in *remote.RollbackRequest) (out *remote.RollbackReply, err error) {
	s.tx.Rollback()
	return &remote.RollbackReply{}, nil
}

func (s *Server2) Bucket(ctx context.Context, in *remote.BucketRequest) (out *remote.BucketReply, err error) {
	bucket := s.tx.Bucket(in.Name)
	s.lastHandle++
	s.buckets[s.lastHandle] = bucket
	return &remote.BucketReply{Handle: s.lastHandle}, nil
}

func (s *Server2) Get(ctx context.Context, in *remote.GetRequest) (out *remote.GetReply, err error) {
	bucket, ok := s.buckets[in.BucketHandle]
	if !ok {
		return nil, fmt.Errorf("bucket not found: %d", in.BucketHandle)
	}
	v, err := bucket.Get(in.Key)
	return &remote.GetReply{Value: v}, err
}

func (s *Server2) Cursor(ctx context.Context, in *remote.CursorRequest) (out *remote.CursorReply, err error) {
	bucket, ok := s.buckets[in.BucketHandle]
	if !ok {
		return nil, fmt.Errorf("bucket not found for remote.CmdGet: %d", in.BucketHandle)
	}

	c := bucket.Cursor()
	s.lastHandle++
	cursorHandle := s.lastHandle

	s.cursors[cursorHandle] = c
	if cursorHandles, ok := s.cursorsByBucket[in.BucketHandle]; ok {
		cursorHandles = append(cursorHandles, cursorHandle)
		s.cursorsByBucket[in.BucketHandle] = cursorHandles
	} else {
		s.cursorsByBucket[in.BucketHandle] = []uint64{cursorHandle}
	}
	return &remote.CursorReply{CursorHandle: cursorHandle}, nil
}

func (s *Server2) Seek(ctx context.Context, in *remote.SeekRequest) (out *remote.SeekReply, err error) {
	c, ok := s.cursors[in.CursorHandle]
	if !ok {
		return nil, fmt.Errorf("cursor not found: %d", in.CursorHandle)
	}

	k, v, err := c.Seek(in.Seek)
	if err != nil {
		return nil, err
	}

	return &remote.SeekReply{Key: k, Value: v}, nil
}

func (s *Server2) First(in *remote.FirstRequest, out remote.Kv_FirstServer) error {
	c, ok := s.cursors[in.CursorHandle]
	if !ok {
		return fmt.Errorf("cursor not found: %d", in.CursorHandle)
	}

	for k, v, err := c.First(); k != nil && in.NumKeys > 0; k, v, err = c.Next() {
		if err != nil {
			return err
		}
		if err := out.Send(&remote.FirstReply{Key: k, Value: v}); err != nil {
			return err
		}

		in.NumKeys--
	}
	return nil
}

func (s *Server2) Next(in *remote.NextRequest, out remote.Kv_NextServer) error {
	c, ok := s.cursors[in.CursorHandle]
	if !ok {
		return fmt.Errorf("cursor not found: %d", in.CursorHandle)
	}

	for k, v, err := c.Next(); k != nil && in.NumKeys > 0; k, v, err = c.Next() {
		if err != nil {
			return err
		}
		if err := out.Send(&remote.NextReply{Key: k, Value: v}); err != nil {
			return err
		}

		in.NumKeys--
	}
	return nil
}
