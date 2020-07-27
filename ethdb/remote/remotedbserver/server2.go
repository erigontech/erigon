package remotedbserver

import (
	"context"
	"fmt"
	"io"
	"sync"

	"github.com/ledgerwatch/turbo-geth/common"
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
	}
	delete(s.bucketByTx, txHandle)
	delete(s.txs, txHandle)
}

func (s *Server2) bucket(txHandle uint64, name []byte) (ethdb.Bucket, uint64, error) {
	s.Lock()
	defer s.Unlock()
	bucket := s.txs[txHandle].Bucket(name)
	s.lastHandle++
	bucketHandle := s.lastHandle
	s.buckets[bucketHandle] = bucket
	s.bucketByTx[txHandle] = append(s.bucketByTx[txHandle], bucketHandle)
	return bucket, bucketHandle, nil
}

func (s *Server2) cursor(bucketHandle uint64, prefix []byte) (ethdb.Cursor, func(), error) {
	s.Lock()
	defer s.Unlock()
	bucket, ok := s.buckets[bucketHandle]
	if !ok {
		return nil, nil, fmt.Errorf("bucket not found: %d", bucketHandle)
	}

	c := bucket.Cursor().Prefix(prefix)
	s.lastHandle++
	cursorHandle := s.lastHandle
	s.cursors[cursorHandle] = c
	s.cursorsByBucket[bucketHandle] = append(s.cursorsByBucket[bucketHandle], cursorHandle)

	closer := func() {}
	if casted, ok := c.(io.Closer); ok && casted != nil {
		closer = func() {
			_ = casted.Close()
			delete(s.cursors, cursorHandle)
		}
	}

	return c, closer, nil
}

func (s *Server2) Seek(stream remote.Kv_SeekServer) error {
	in, recvErr := stream.Recv()
	if recvErr != nil {
		return recvErr
	}

	txHandle, err := s.begin()
	if err != nil {
		return err
	}
	defer func() {
		s.rollback(txHandle)
	}()

	b, bucketHandle, err := s.bucket(txHandle, in.BucketName)
	if err != nil {
		return nil
	}

	_ = b
	c, closer, err := s.cursor(bucketHandle, in.Prefix)
	if err != nil {
		return nil
	}
	defer closer()

	// send all items to client, if k==nil - stil send it to client and break loop
	for k, v, err := c.Seek(in.SeekKey); ; k, v, err = c.Next() {
		if err != nil {
			return err
		}

		err = stream.Send(&remote.Pair{Key: common.CopyBytes(k), Value: common.CopyBytes(v)})
		if err != nil {
			return err
		}
		if k == nil {
			return nil
		}

		// if client not requested stream then wait signal from him before send any item
		if !in.StartSreaming {
			in, err = stream.Recv()
			if err != nil {
				return err
			}
		}
	}
}
