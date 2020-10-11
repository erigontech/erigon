package remotedbserver

import (
	"fmt"
	"io"
	"net"
	"runtime"
	"time"

	grpc_middleware "github.com/grpc-ecosystem/go-grpc-middleware"
	grpc_recovery "github.com/grpc-ecosystem/go-grpc-middleware/recovery"
	grpc_prometheus "github.com/grpc-ecosystem/go-grpc-prometheus"
	"github.com/ledgerwatch/turbo-geth/common"
	"github.com/ledgerwatch/turbo-geth/core"
	"github.com/ledgerwatch/turbo-geth/ethdb"
	"github.com/ledgerwatch/turbo-geth/ethdb/remote"
	"github.com/ledgerwatch/turbo-geth/log"
	"github.com/ledgerwatch/turbo-geth/metrics"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials"
	"google.golang.org/grpc/keepalive"
)

const MaxTxTTL = 30 * time.Second

type KvServer struct {
	remote.UnstableKVService // must be embedded to have forward compatible implementations.

	kv ethdb.KV
}

type Kv2Server struct {
	remote.UnstableKV2Service // must be embedded to have forward compatible implementations.

	kv ethdb.KV
}

func StartGrpc(kv ethdb.KV, eth core.Backend, addr string, creds *credentials.TransportCredentials) (*grpc.Server, error) {
	log.Info("Starting private RPC server", "on", addr)
	lis, err := net.Listen("tcp", addr)
	if err != nil {
		return nil, fmt.Errorf("could not create listener: %w, addr=%s", err, addr)
	}

	kvSrv := NewKvServer(kv)
	kv2Srv := NewKv2Server(kv)
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
	cpus := uint32(runtime.GOMAXPROCS(-1))
	opts := []grpc.ServerOption{
		grpc.NumStreamWorkers(cpus), // reduce amount of goroutines
		grpc.WriteBufferSize(1024),  // reduce buffers to save mem
		grpc.ReadBufferSize(1024),
		grpc.MaxConcurrentStreams(100), // to force clients reduce concurrency level
		grpc.KeepaliveParams(keepalive.ServerParameters{
			Time: 10 * time.Minute,
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
	remote.RegisterKVService(grpcServer, remote.NewKVService(kvSrv))
	remote.RegisterDBService(grpcServer, remote.NewDBService(dbSrv))
	remote.RegisterETHBACKENDService(grpcServer, remote.NewETHBACKENDService(ethBackendSrv))
	remote.RegisterKV2Service(grpcServer, remote.NewKV2Service(kv2Srv))

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
		return fmt.Errorf("server-side error: %w", err)
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
			return fmt.Errorf("server-side error: %w", err)
		}
	} else {
		cd := tx.CursorDupSort(bucketName)
		k, v, err = cd.SeekBothRange(in.SeekKey, in.SeekValue)
		if err != nil {
			return fmt.Errorf("server-side error: %w", err)
		}
		if k == nil { // it may happen that key where we stopped disappeared after transaction reopen, then just move to next key
			k, v, err = cd.Next()
			if err != nil {
				return fmt.Errorf("server-side error: %w", err)
			}
		}
		c = cd
	}

	// send all items to client, if k==nil - still send it to client and break loop
	for {
		err = stream.Send(&remote.Pair{Key: common.CopyBytes(k), Value: common.CopyBytes(v)})
		if err != nil {
			return fmt.Errorf("server-side error: %w", err)
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
				return fmt.Errorf("server-side error: %w", err)
			}

			if len(in.SeekValue) > 0 {
				k, v, err = c.(ethdb.CursorDupSort).SeekBothRange(in.SeekKey, in.SeekValue)
				if err != nil {
					return fmt.Errorf("server-side error: %w", err)
				}
				if k == nil { // it may happen that key where we stopped disappeared after transaction reopen, then just move to next key
					k, v, err = c.Next()
					if err != nil {
						return fmt.Errorf("server-side error: %w", err)
					}
				}
			} else if len(in.SeekKey) > 0 {
				k, v, err = c.Seek(in.SeekKey)
				if err != nil {
					return fmt.Errorf("server-side error: %w", err)
				}
			} else {
				k, v, err = c.Next()
				if err != nil {
					return fmt.Errorf("server-side error: %w", err)
				}
			}
		} else {
			k, v, err = c.Next()
			if err != nil {
				return fmt.Errorf("server-side error: %w", err)
			}
		}

		//TODO: protect against client - which doesn't send any requests
		select {
		default:
		case <-txTicker.C:
			tx.Rollback()
			tx, err = s.kv.Begin(stream.Context(), nil, false)
			if err != nil {
				return fmt.Errorf("server-side error: %w", err)

			}
			if isDupsort {
				dc := tx.CursorDupSort(bucketName)
				k, v, err = dc.SeekBothRange(k, v)
				if err != nil {
					return fmt.Errorf("server-side error: %w", err)
				}
				if k == nil { // it may happen that key where we stopped disappeared after transaction reopen, then just move to next key
					k, v, err = dc.Next()
					if err != nil {
						return fmt.Errorf("server-side error: %w", err)
					}
				}
				c = dc
			} else {
				c = tx.Cursor(bucketName).Prefix(prefix)
				k, v, err = c.Seek(k)
				if err != nil {
					return fmt.Errorf("server-side error: %w", err)
				}
			}
		}
	}
}

func NewKv2Server(kv ethdb.KV) *Kv2Server {
	return &Kv2Server{kv: kv}
}

func (s *Kv2Server) Tx(stream remote.KV2_TxServer) error {
	tx, errBegin := s.kv.Begin(stream.Context(), nil, false)
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
			tx, errBegin = s.kv.Begin(stream.Context(), nil, false)
			if errBegin != nil {
				return fmt.Errorf("server-side error: %w", errBegin)
			}

			for _, c := range cursors { // restore all cursors position
				c.c = tx.Cursor(c.bucket)
				switch casted := c.c.(type) {
				case ethdb.CursorDupFixed:
					k, _, err := casted.SeekBothRange(c.k, c.v)
					if err != nil {
						return fmt.Errorf("server-side error: %w", err)
					}
					if k == nil { // it may happen that key where we stopped disappeared after transaction reopen, then just move to next key
						_, _, err = casted.Next()
						if err != nil {
							return fmt.Errorf("server-side error: %w", err)
						}
					}
				case ethdb.CursorDupSort:
					k, _, err := casted.SeekBothRange(c.k, c.v)
					if err != nil {
						return fmt.Errorf("server-side error: %w", err)
					}
					if k == nil { // it may happen that key where we stopped disappeared after transaction reopen, then just move to next key
						_, _, err = casted.Next()
						if err != nil {
							return fmt.Errorf("server-side error: %w", err)
						}
					}
				case ethdb.Cursor:
					_, _, err := c.c.Seek(c.k)
					if err != nil {
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
			cursors[CursorID] = &CursorInfo{
				c: tx.Cursor(in.BucketName),
			}
			if err := stream.Send(&remote.Pair2{CursorID: CursorID}); err != nil {
				return fmt.Errorf("server-side error: %w", err)
			}
			continue
		case remote.Op_CLOSE:
			cursors[in.Cursor].c.Close()
			delete(cursors, in.Cursor)
			if err := stream.Send(&remote.Pair2{}); err != nil {
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

func handleOp(c ethdb.Cursor, stream remote.KV2_TxServer, in *remote.Cursor) error {
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
		k, v, err = c.(ethdb.CursorDupSort).SeekBothRange(in.K, in.V)
	case remote.Op_CURRENT:
		k, v, err = c.Current()
	case remote.Op_GET_MULTIPLE:
		v, err = c.(ethdb.CursorDupFixed).GetMulti()
	case remote.Op_LAST:
		k, v, err = c.Last()
	case remote.Op_LAST_DUP:
		v, err = c.(ethdb.CursorDupSort).LastDup(in.K)
	case remote.Op_NEXT:
		k, v, err = c.Next()
	case remote.Op_NEXT_DUP:
		k, v, err = c.(ethdb.CursorDupSort).NextDup()
	case remote.Op_NEXT_MULTIPLE:
		k, v, err = c.(ethdb.CursorDupFixed).NextMulti()
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
		v, err = c.SeekExact(in.K)
	case remote.Op_SEEK_BOTH_EXACT:
		k, v, err = c.(ethdb.CursorDupSort).SeekBothExact(in.K, in.V)
	default:
		return fmt.Errorf("unknown operation: %s", in.Op)
	}
	if err != nil {
		return err
	}

	if err := stream.Send(&remote.Pair2{K: common.CopyBytes(k), V: common.CopyBytes(v)}); err != nil {
		return err
	}

	return nil
}
