package remotedbserver

import (
	"context"
	"time"

	"github.com/ledgerwatch/turbo-geth/common"
	"github.com/ledgerwatch/turbo-geth/ethdb"
	"github.com/ledgerwatch/turbo-geth/ethdb/remote"
)

const MaxTxTTL = time.Minute

type KvServer struct {
	remote.UnimplementedKvServer // must be embedded to have forward compatible implementations.

	kv ethdb.KV
}

func NewKvServer(kv ethdb.KV) *KvServer {
	return &KvServer{kv: kv}
}

func (s *KvServer) Seek(stream remote.Kv_SeekServer) error {
	in, recvErr := stream.Recv()
	if recvErr != nil {
		return recvErr
	}

	tx, err := s.kv.Begin(context.Background(), false)
	if err != nil {
		return err
	}
	rollback := func() {
		tx.Rollback()
	}
	defer rollback()

	bucketName, prefix := in.BucketName, in.Prefix // 'in' value will cahnge, but this params will immutable

	c := tx.Bucket(bucketName).Cursor().Prefix(prefix)

	t := time.Now()
	i := 0
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

		//TODO: protect against stale client
		i++
		if i%128 == 0 && time.Since(t) > MaxTxTTL {
			tx.Rollback()
			tx, err = s.kv.Begin(context.Background(), false)
			if err != nil {
				return err
			}
			c = tx.Bucket(bucketName).Cursor().Prefix(prefix)
			_, _, _ = c.Seek(k)
		}
	}
}
