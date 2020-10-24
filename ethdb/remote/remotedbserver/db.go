package remotedbserver

import (
	"context"

	"github.com/ledgerwatch/turbo-geth/ethdb"
	"github.com/ledgerwatch/turbo-geth/ethdb/remote"
)

type DBServer struct {
	remote.UnimplementedDBServer // must be embedded to have forward compatible implementations.

	kv ethdb.KV
}

func NewDBServer(kv ethdb.KV) *DBServer {
	return &DBServer{kv: kv}
}

func (s *DBServer) Size(ctx context.Context, in *remote.SizeRequest) (*remote.SizeReply, error) {
	sz, err := s.kv.(ethdb.HasStats).DiskSize(ctx)
	if err != nil {
		return nil, err
	}
	return &remote.SizeReply{Size: sz}, nil
}

func (s *DBServer) BucketSize(ctx context.Context, in *remote.BucketSizeRequest) (*remote.BucketSizeReply, error) {
	out := &remote.BucketSizeReply{}
	if err := s.kv.View(ctx, func(tx ethdb.Tx) error {
		sz, err := tx.BucketSize(in.BucketName)
		if err != nil {
			return err
		}
		out.Size = sz
		return nil
	}); err != nil {
		return nil, err
	}
	return out, nil
}
