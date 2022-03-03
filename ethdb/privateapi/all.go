package privateapi

import (
	"fmt"
	"net"
	"time"

	grpc_middleware "github.com/grpc-ecosystem/go-grpc-middleware"
	grpc_recovery "github.com/grpc-ecosystem/go-grpc-middleware/recovery"
	"github.com/ledgerwatch/erigon-lib/kv/remotedbserver"
	"google.golang.org/grpc/keepalive"
	"google.golang.org/grpc/reflection"

	//grpc_prometheus "github.com/grpc-ecosystem/go-grpc-prometheus"
	"github.com/ledgerwatch/erigon-lib/gointerfaces/remote"
	txpool_proto "github.com/ledgerwatch/erigon-lib/gointerfaces/txpool"
	"github.com/ledgerwatch/log/v3"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials"
	"google.golang.org/grpc/health"
	"google.golang.org/grpc/health/grpc_health_v1"
)

func NewServer(rateLimit uint32, creds credentials.TransportCredentials) *grpc.Server {
	var (
		streamInterceptors []grpc.StreamServerInterceptor
		unaryInterceptors  []grpc.UnaryServerInterceptor
	)
	streamInterceptors = append(streamInterceptors, grpc_recovery.StreamServerInterceptor())
	unaryInterceptors = append(unaryInterceptors, grpc_recovery.UnaryServerInterceptor())

	//if metrics.Enabled {
	//	streamInterceptors = append(streamInterceptors, grpc_prometheus.StreamServerInterceptor)
	//	unaryInterceptors = append(unaryInterceptors, grpc_prometheus.UnaryServerInterceptor)
	//}

	//cpus := uint32(runtime.GOMAXPROCS(-1))
	opts := []grpc.ServerOption{
		//grpc.NumStreamWorkers(cpus),          // reduce amount of goroutines
		grpc.MaxConcurrentStreams(320_000), // to force clients reduce concurrency level
		// Don't drop the connection, settings accordign to this comment on GitHub
		// https://github.com/grpc/grpc-go/issues/3171#issuecomment-552796779
		grpc.KeepaliveEnforcementPolicy(keepalive.EnforcementPolicy{
			MinTime:             10 * time.Second,
			PermitWithoutStream: true,
		}),
		grpc.StreamInterceptor(grpc_middleware.ChainStreamServer(streamInterceptors...)),
		grpc.UnaryInterceptor(grpc_middleware.ChainUnaryServer(unaryInterceptors...)),
		grpc.Creds(creds),
	}
	grpcServer := grpc.NewServer(opts...)
	reflection.Register(grpcServer)

	//if metrics.Enabled {
	//	grpc_prometheus.Register(grpcServer)
	//}

	return grpcServer
}

func StartGrpc(kv *remotedbserver.KvServer, ethBackendSrv *EthBackendServer, txPoolServer txpool_proto.TxpoolServer,
	miningServer txpool_proto.MiningServer, addr string, rateLimit uint32, creds credentials.TransportCredentials,
	healthCheck bool) (*grpc.Server, error) {
	log.Info("Starting private RPC server", "on", addr)
	lis, err := net.Listen("tcp", addr)
	if err != nil {
		return nil, fmt.Errorf("could not create listener: %w, addr=%s", err, addr)
	}

	grpcServer := NewServer(rateLimit, creds)
	remote.RegisterETHBACKENDServer(grpcServer, ethBackendSrv)
	if txPoolServer != nil {
		txpool_proto.RegisterTxpoolServer(grpcServer, txPoolServer)
	}
	if miningServer != nil {
		txpool_proto.RegisterMiningServer(grpcServer, miningServer)
	}
	remote.RegisterKVServer(grpcServer, kv)
	var healthServer *health.Server
	if healthCheck {
		healthServer = health.NewServer()
		grpc_health_v1.RegisterHealthServer(grpcServer, healthServer)
	}
	go func() {
		if healthCheck {
			defer healthServer.Shutdown()
		}
		defer ethBackendSrv.StopProposer()
		if err := grpcServer.Serve(lis); err != nil {
			log.Error("private RPC server fail", "err", err)
		}
	}()

	return grpcServer, nil
}
