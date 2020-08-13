package remotedbserver

import (
	"context"
	"fmt"
	"io"
	"net"
	"time"

	grpc_middleware "github.com/grpc-ecosystem/go-grpc-middleware"
	grpc_recovery "github.com/grpc-ecosystem/go-grpc-middleware/recovery"
	grpc_prometheus "github.com/grpc-ecosystem/go-grpc-prometheus"
	"google.golang.org/grpc"

	"github.com/ledgerwatch/turbo-geth/common"
	"github.com/ledgerwatch/turbo-geth/core"
	"github.com/ledgerwatch/turbo-geth/ethdb"
	"github.com/ledgerwatch/turbo-geth/ethdb/remote"
	"github.com/ledgerwatch/turbo-geth/log"
	"github.com/ledgerwatch/turbo-geth/metrics"
)

const MaxTxTTL = time.Minute

type KvServer struct {
	remote.UnimplementedKVServer // must be embedded to have forward compatible implementations.

	kv ethdb.KV
}

func StartGrpc(kv ethdb.KV, eth core.Backend, addr string) {
	log.Info("Starting private RPC server", "on", addr)
	lis, err := net.Listen("tcp", addr)
	if err != nil {
		logger.Error("Could not create listener", "address", addr, "err", err)
		return
	}

	kvSrv := NewKvServer(kv)
	dbSrv := NewDBServer(kv)
	ethBackendSrv := NewEthBackendServer(eth)
	var (
		streamInterceptors []grpc.StreamServerInterceptor
		unaryInterceptors  []grpc.UnaryServerInterceptor
	)
	if metrics.Enabled {
		streamInterceptors = append(streamInterceptors, grpc_prometheus.StreamServerInterceptor)
		unaryInterceptors = append(unaryInterceptors, grpc_prometheus.UnaryServerInterceptor)
	}
	streamInterceptors = append(streamInterceptors, grpc_recovery.StreamServerInterceptor())
	unaryInterceptors = append(unaryInterceptors, grpc_recovery.UnaryServerInterceptor())

	grpcServer := grpc.NewServer(
		grpc.NumStreamWorkers(20),  // reduce amount of goroutines
		grpc.WriteBufferSize(1024), // reduce buffers to save mem
		grpc.ReadBufferSize(1024),
		grpc.MaxConcurrentStreams(40), // to force clients reduce concurency level
		grpc.StreamInterceptor(grpc_middleware.ChainStreamServer(streamInterceptors...)),
		grpc.UnaryInterceptor(grpc_middleware.ChainUnaryServer(unaryInterceptors...)),
	)
	remote.RegisterKVServer(grpcServer, kvSrv)
	remote.RegisterDBServer(grpcServer, dbSrv)
	remote.RegisterETHBACKENDServer(grpcServer, ethBackendSrv)

	if metrics.Enabled {
		grpc_prometheus.Register(grpcServer)
	}

	go func() {
		if err := grpcServer.Serve(lis); err != nil {
			logger.Error("private RPC server fail", "err", err)
		}
	}()
}

func NewKvServer(kv ethdb.KV) *KvServer {
	return &KvServer{kv: kv}
}

func (s *KvServer) Seek(stream remote.KV_SeekServer) error {
	in, recvErr := stream.Recv()
	if recvErr != nil {
		return recvErr
	}

	tx, err := s.kv.Begin(context.Background(), nil, false)
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
	fmt.Println("seek in.SeekKey", common.Bytes2Hex(in.SeekKey))
	// send all items to client, if k==nil - stil send it to client and break loop
	for k, v, err := c.Seek(in.SeekKey); ; k, v, err = c.Next() {
		if err != nil {
			return err
		}

		fmt.Println("ethdb/remote/remotedbserver/server2.go:103 seek", common.Bytes2Hex(k))
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
				if err == io.EOF {
					return nil
				}
				return err
			}
		}

		//TODO: protect against stale client
		i++
		if i%128 == 0 && time.Since(t) > MaxTxTTL {
			tx.Rollback()
			tx, err = s.kv.Begin(context.Background(), nil, false)
			if err != nil {
				return err
			}
			c = tx.Bucket(bucketName).Cursor().Prefix(prefix)
			_, _, _ = c.Seek(k)
		}
	}
}
