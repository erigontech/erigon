package remotedbserver

import (
	"fmt"
	"io"
	"net"
	"time"

	grpc_middleware "github.com/grpc-ecosystem/go-grpc-middleware"
	grpc_recovery "github.com/grpc-ecosystem/go-grpc-middleware/recovery"
	grpc_prometheus "github.com/grpc-ecosystem/go-grpc-prometheus"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials"

	"github.com/ledgerwatch/turbo-geth/common"
	"github.com/ledgerwatch/turbo-geth/core"
	"github.com/ledgerwatch/turbo-geth/ethdb"
	"github.com/ledgerwatch/turbo-geth/ethdb/remote"
	"github.com/ledgerwatch/turbo-geth/log"
	"github.com/ledgerwatch/turbo-geth/metrics"
)

const MaxTxTTL = 30 * time.Second

type KvServer struct {
	remote.UnstableKVService // must be embedded to have forward compatible implementations.

	kv ethdb.KV
}

func StartGrpc(kv ethdb.KV, eth core.Backend, addr string, creds *credentials.TransportCredentials) (*grpc.Server, error) {
	log.Info("Starting private RPC server", "on", addr)
	lis, err := net.Listen("tcp", addr)
	if err != nil {
		return nil, fmt.Errorf("could not create listener: %w, addr=%s", err, addr)
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
	var grpcServer *grpc.Server
	if creds == nil {
		grpcServer = grpc.NewServer(
			grpc.NumStreamWorkers(20),  // reduce amount of goroutines
			grpc.WriteBufferSize(1024), // reduce buffers to save mem
			grpc.ReadBufferSize(1024),
			grpc.MaxConcurrentStreams(40), // to force clients reduce concurency level
			grpc.StreamInterceptor(grpc_middleware.ChainStreamServer(streamInterceptors...)),
			grpc.UnaryInterceptor(grpc_middleware.ChainUnaryServer(unaryInterceptors...)),
		)
	} else {
		grpcServer = grpc.NewServer(
			grpc.NumStreamWorkers(20),  // reduce amount of goroutines
			grpc.WriteBufferSize(1024), // reduce buffers to save mem
			grpc.ReadBufferSize(1024),
			grpc.MaxConcurrentStreams(40), // to force clients reduce concurency level
			grpc.StreamInterceptor(grpc_middleware.ChainStreamServer(streamInterceptors...)),
			grpc.UnaryInterceptor(grpc_middleware.ChainUnaryServer(unaryInterceptors...)),
			grpc.Creds(*creds),
		)
	}
	remote.RegisterKVService(grpcServer, remote.NewKVService(kvSrv))
	remote.RegisterDBService(grpcServer, remote.NewDBService(dbSrv))
	remote.RegisterETHBACKENDService(grpcServer, remote.NewETHBACKENDService(ethBackendSrv))

	if metrics.Enabled {
		grpc_prometheus.Register(grpcServer)
	}

	go func() {
		if err := grpcServer.Serve(lis); err != nil {
			log.Error("private RPC server fail", "err", err)
		}
	}()

	return grpcServer, nil
}

func NewKvServer(kv ethdb.KV) *KvServer {
	return &KvServer{kv: kv}
}

func (s *KvServer) Seek(stream remote.KV_SeekServer) error {
	in, recvErr := stream.Recv()
	if recvErr != nil {
		return recvErr
	}
	tx, err := s.kv.Begin(stream.Context(), nil, false)
	if err != nil {
		return err
	}
	rollback := func() {
		tx.Rollback()
	}
	defer rollback()

	bucketName, prefix := in.BucketName, in.Prefix // 'in' value will cahnge, but this params will immutable

	var c ethdb.Cursor

	txTicker := time.NewTicker(MaxTxTTL)
	defer txTicker.Stop()

	isDupsort := len(in.SeekValue) != 0
	var k, v []byte
	if !isDupsort {
		c = tx.Cursor(bucketName).Prefix(prefix)
		k, v, err = c.Seek(in.SeekKey)
		if err != nil {
			return err
		}
	} else {
		cd := tx.CursorDupSort(bucketName)
		k, v, err = cd.SeekBothRange(in.SeekKey, in.SeekValue)
		if err != nil {
			return err
		}
		if k == nil { // it may happen that key where we stopped disappeared after transaction reopen, then just move to next key
			k, v, err = cd.Next()
			if err != nil {
				return err
			}
		}
		c = cd
	}

	// send all items to client, if k==nil - still send it to client and break loop
	for {
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

			if len(in.SeekValue) > 0 {
				k, v, err = c.(ethdb.CursorDupSort).SeekBothRange(in.SeekKey, in.SeekValue)
				if err != nil {
					return err
				}
				if k == nil { // it may happen that key where we stopped disappeared after transaction reopen, then just move to next key
					k, v, err = c.Next()
					if err != nil {
						return err
					}
				}
			} else if len(in.SeekKey) > 0 {
				k, v, err = c.Seek(in.SeekKey)
				if err != nil {
					return err
				}
			} else {
				k, v, err = c.Next()
				if err != nil {
					return err
				}
			}
		} else {
			k, v, err = c.Next()
			if err != nil {
				return err
			}
		}

		//TODO: protect against client - which doesn't send any requests
		select {
		default:
		case <-txTicker.C:
			tx.Rollback()
			tx, err = s.kv.Begin(stream.Context(), nil, false)
			if err != nil {
				return err
			}
			if isDupsort {
				dc := tx.CursorDupSort(bucketName)
				k, v, err = dc.SeekBothRange(k, v)
				if err != nil {
					return err
				}
				if k == nil { // it may happen that key where we stopped disappeared after transaction reopen, then just move to next key
					k, v, err = dc.Next()
					if err != nil {
						return err
					}
				}
				c = dc
			} else {
				c = tx.Cursor(bucketName).Prefix(prefix)
				k, v, err = c.Seek(k)
				if err != nil {
					return err
				}
			}
		}
	}
}
