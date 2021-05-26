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
	"github.com/ledgerwatch/erigon/common"
	"github.com/ledgerwatch/erigon/common/dbutils"
	"github.com/ledgerwatch/erigon/ethdb"
	"github.com/ledgerwatch/erigon/gointerfaces/remote"
	"github.com/ledgerwatch/erigon/gointerfaces/txpool"
	"github.com/ledgerwatch/erigon/gointerfaces/types"
	"github.com/ledgerwatch/erigon/log"
	"github.com/ledgerwatch/erigon/metrics"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials"
	"google.golang.org/grpc/keepalive"
	"google.golang.org/protobuf/types/known/emptypb"
)

const MaxTxTTL = 30 * time.Second

// KvServiceAPIVersion - use it to track changes in API
// 1.1.0 - added pending transactions, add methods eth_getRawTransactionByHash, eth_retRawTransactionByBlockHashAndIndex, eth_retRawTransactionByBlockNumberAndIndex| Yes     |                                            |
// 1.2.0 - Added separated services for mining and txpool methods
// 2.0.0 - Rename all buckets
var KvServiceAPIVersion = &types.VersionReply{Major: 2, Minor: 0, Patch: 0}

type KvServer struct {
	remote.UnimplementedKVServer // must be embedded to have forward compatible implementations.

	kv   ethdb.RwKV
	mdbx bool
}

func StartGrpc(kv *KvServer, ethBackendSrv *EthBackendServer, txPoolServer *TxPoolServer, miningServer *MiningServer, addr string, rateLimit uint32, creds *credentials.TransportCredentials) (*grpc.Server, error) {
	log.Info("Starting private RPC server", "on", addr)
	lis, err := net.Listen("tcp", addr)
	if err != nil {
		return nil, fmt.Errorf("could not create listener: %w, addr=%s", err, addr)
	}

	var (
		streamInterceptors []grpc.StreamServerInterceptor
		unaryInterceptors  []grpc.UnaryServerInterceptor
	)
	streamInterceptors = append(streamInterceptors, grpc_recovery.StreamServerInterceptor())
	unaryInterceptors = append(unaryInterceptors, grpc_recovery.UnaryServerInterceptor())

	if metrics.Enabled {
		streamInterceptors = append(streamInterceptors, grpc_prometheus.StreamServerInterceptor)
		unaryInterceptors = append(unaryInterceptors, grpc_prometheus.UnaryServerInterceptor)
	}

	var grpcServer *grpc.Server
	//cpus := uint32(runtime.GOMAXPROCS(-1))
	opts := []grpc.ServerOption{
		//grpc.NumStreamWorkers(cpus), // reduce amount of goroutines
		grpc.WriteBufferSize(1024), // reduce buffers to save mem
		grpc.ReadBufferSize(1024),
		grpc.MaxConcurrentStreams(rateLimit), // to force clients reduce concurrency level
		// Don't drop the connection, settings accordign to this comment on GitHub
		// https://github.com/grpc/grpc-go/issues/3171#issuecomment-552796779
		grpc.KeepaliveEnforcementPolicy(keepalive.EnforcementPolicy{
			MinTime:             10 * time.Second,
			PermitWithoutStream: true,
		}),
		grpc.StreamInterceptor(grpc_middleware.ChainStreamServer(streamInterceptors...)),
		grpc.UnaryInterceptor(grpc_middleware.ChainUnaryServer(unaryInterceptors...)),
	}
	if creds == nil {
		// no specific opts
	} else {
		opts = append(opts, grpc.Creds(*creds))
	}
	grpcServer = grpc.NewServer(opts...)
	remote.RegisterETHBACKENDServer(grpcServer, ethBackendSrv)
	txpool.RegisterTxpoolServer(grpcServer, txPoolServer)
	txpool.RegisterMiningServer(grpcServer, miningServer)
	remote.RegisterKVServer(grpcServer, kv)

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

func NewKvServer(kv ethdb.RwKV, mdbx bool) *KvServer {
	return &KvServer{kv: kv, mdbx: mdbx}
}

// Version returns the service-side interface version number
func (s *KvServer) Version(context.Context, *emptypb.Empty) (*types.VersionReply, error) {
	var dbSchemaVersion *types.VersionReply
	if s.mdbx {
		dbSchemaVersion = &dbutils.DBSchemaVersionMDBX
	} else {
		dbSchemaVersion = &dbutils.DBSchemaVersionLMDB
	}
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

	var CursorID uint32
	type CursorInfo struct {
		bucket string
		c      ethdb.Cursor
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
				c.k = common.CopyBytes(k)
				c.v = common.CopyBytes(v)
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
				case ethdb.CursorDupSort:
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
				case ethdb.Cursor:
					if _, _, err := c.c.Seek(c.k); err != nil {
						return fmt.Errorf("server-side error: %w", err)
					}
				}
			}
		}

		var c ethdb.Cursor
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

func handleOp(c ethdb.Cursor, stream remote.KV_TxServer, in *remote.Cursor) error {
	var k, v []byte
	var err error
	switch in.Op {
	case remote.Op_FIRST:
		k, v, err = c.First()
	case remote.Op_FIRST_DUP:
		v, err = c.(ethdb.CursorDupSort).FirstDup()
	case remote.Op_SEEK:
		k, v, err = c.Seek(in.K)
	case remote.Op_SEEK_BOTH:
		v, err = c.(ethdb.CursorDupSort).SeekBothRange(in.K, in.V)
	case remote.Op_CURRENT:
		k, v, err = c.Current()
	case remote.Op_LAST:
		k, v, err = c.Last()
	case remote.Op_LAST_DUP:
		v, err = c.(ethdb.CursorDupSort).LastDup()
	case remote.Op_NEXT:
		k, v, err = c.Next()
	case remote.Op_NEXT_DUP:
		k, v, err = c.(ethdb.CursorDupSort).NextDup()
	case remote.Op_NEXT_NO_DUP:
		k, v, err = c.(ethdb.CursorDupSort).NextNoDup()
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
		k, v, err = c.(ethdb.CursorDupSort).SeekBothExact(in.K, in.V)
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
