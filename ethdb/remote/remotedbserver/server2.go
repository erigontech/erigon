package remotedbserver

import (
	"context"
	"fmt"
	"io"
	"sync"

	"github.com/ledgerwatch/turbo-geth/ethdb"
	"github.com/ledgerwatch/turbo-geth/ethdb/remote"
)

type Server2 struct {
	remote.UnimplementedKvServer // must be embedded to have forward compatible implementations.

	lastHandle uint64

	kv ethdb.KV

	sync.Mutex
	// Transactions opened by the client
	txs map[uint64]ethdb.Tx
	// Buckets opened by the client
	buckets map[uint64]ethdb.Bucket
	// Cursors opened by the client
	cursors map[uint64]ethdb.Cursor
	// List of cursors opened in each bucket
	cursorsByBucket map[uint64][]uint64
	// List of buckets opened in each transaction
	bucketByTx map[uint64][]uint64
}

func NewServer2(kv ethdb.KV) *Server2 {
	return &Server2{
		kv:              kv,
		txs:             make(map[uint64]ethdb.Tx, 16),
		buckets:         make(map[uint64]ethdb.Bucket, 16),
		cursors:         make(map[uint64]ethdb.Cursor, 16),
		bucketByTx:      make(map[uint64][]uint64, 16),
		cursorsByBucket: make(map[uint64][]uint64, 16),
	}
}

func (s *Server2) View(stream remote.Kv_ViewServer) error {
	txHandle, err := s.begin()
	if err != nil {
		return err
	}
	defer func() {
		s.rollback(txHandle)
	}()

	if err := stream.Send(&remote.ViewReply{TxHandle: txHandle}); err != nil {
		return err
	}
	fmt.Printf("Wait for client\n")
	_, _ = stream.Recv() // block until client gone, send message or cancel context
	return nil
}

func (s *Server2) begin() (uint64, error) {
	tx, err := s.kv.Begin(context.Background(), false)
	if err != nil {
		return 0, err
	}
	s.Lock()
	defer s.Unlock()
	s.lastHandle++
	txHandle := s.lastHandle
	s.txs[txHandle] = tx
	return txHandle, nil
}

func (s *Server2) rollback(txHandle uint64) {
	s.Lock()
	defer s.Unlock()
	fmt.Printf("Rollback\n")

	buckets := s.bucketByTx[txHandle]
	// Remove all the buckets
	for _, bucketHandle := range buckets {
		for _, cursorHandle := range s.cursorsByBucket[bucketHandle] {
			if casted, ok := s.cursors[cursorHandle].(io.Closer); ok && casted != nil {
				_ = casted.Close()
			}

			delete(s.cursors, cursorHandle)
		}
		delete(s.cursorsByBucket, bucketHandle)
		delete(s.buckets, bucketHandle)
	}
	tx := s.txs[txHandle]
	if tx != nil {
		tx.Rollback()
		fmt.Printf("Rollback DOne\n")
	}
	delete(s.bucketByTx, txHandle)
	delete(s.txs, txHandle)
}

func (s *Server2) Bucket(ctx context.Context, in *remote.BucketRequest) (out *remote.BucketReply, err error) {
	s.Lock()
	defer s.Unlock()
	bucket := s.txs[in.TxHandle].Bucket(in.Name)
	s.lastHandle++
	bucketHandle := s.lastHandle
	s.buckets[bucketHandle] = bucket
	s.bucketByTx[in.TxHandle] = append(s.bucketByTx[in.TxHandle], bucketHandle)
	return &remote.BucketReply{BucketHandle: bucketHandle}, nil
}

func (s *Server2) Cursor(ctx context.Context, in *remote.CursorRequest) (out *remote.CursorReply, err error) {
	s.Lock()
	defer s.Unlock()
	bucket, ok := s.buckets[in.BucketHandle]
	if !ok {
		return nil, fmt.Errorf("bucket not found for .Cursor(): %d", in.BucketHandle)
	}

	c := bucket.Cursor().Prefix(in.Prefix)
	s.lastHandle++
	cursorHandle := s.lastHandle
	s.cursors[cursorHandle] = c
	s.cursorsByBucket[in.BucketHandle] = append(s.cursorsByBucket[in.BucketHandle], cursorHandle)
	return &remote.CursorReply{CursorHandle: cursorHandle}, nil
}

func (s *Server2) cursor(cursorHandle uint64) (ethdb.Cursor, func(), error) {
	s.Lock()
	defer s.Unlock()

	c, ok := s.cursors[cursorHandle]
	if !ok {
		return nil, nil, fmt.Errorf("cursor not found for .Seek(): %d", cursorHandle)
	}
	closer := func() {}
	if casted, ok := c.(io.Closer); ok {
		closer = func() {
			_ = casted.Close()
		}
	}

	return c, closer, nil
}

func (s *Server2) Seek(stream remote.Kv_SeekServer) (err error) {
	defer func() {
		fmt.Printf("Cusrsor exit?\n")
	}()
	in, err := stream.Recv()
	if err != nil {
		return err
	}
	c, closer, err := s.cursor(in.CursorHandle)
	if err != nil {
		return err
	}
	defer closer()

	k, v, err := c.Seek(in.SeekKey)
	if err != nil {
		return err
	}

	for {
		if err := stream.Send(&remote.Pair{Key: k, Value: v}); err != nil {
			fmt.Printf("Seek 4, %s\n", err)
			return err
		}
		if k == nil {
			fmt.Printf("Seek 5\n")
			return nil
		}

		if !in.StartSreaming {
			in, err = stream.Recv()
			if err != nil {
				return err
			}
		}

		k, v, err = c.Next()
		if err != nil {
			return err
		}
	}
}

func (s *Server2) Get(ctx context.Context, in *remote.GetRequest) (out *remote.GetReply, err error) {
	bucket, ok := s.buckets[in.BucketHandle]
	if !ok {
		return nil, fmt.Errorf("bucket not found for .Cursor(): %d", in.BucketHandle)
	}

	v, err := bucket.Get(in.Key)
	if err != nil {
		return nil, err
	}
	return &remote.GetReply{Value: v}, nil

}
