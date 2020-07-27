package remotedbserver

import (
	"context"

	"github.com/ledgerwatch/turbo-geth/common"
	"github.com/ledgerwatch/turbo-geth/ethdb"
	"github.com/ledgerwatch/turbo-geth/ethdb/remote"
)

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
	defer tx.Rollback()

	c := tx.Bucket(in.BucketName).Cursor().Prefix(in.Prefix)

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
