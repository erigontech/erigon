package privateapi

import (
	"fmt"
	"net"

	"github.com/ledgerwatch/erigon-lib/gointerfaces/grpcutil"
	"github.com/ledgerwatch/erigon-lib/gointerfaces/remote"

	txpool_proto "github.com/ledgerwatch/erigon-lib/gointerfaces/txpool"
	"github.com/ledgerwatch/erigon-lib/kv/remotedbserver"
	"github.com/ledgerwatch/log/v3"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials"
	"google.golang.org/grpc/health"
	"google.golang.org/grpc/health/grpc_health_v1"
)

func StartGrpc(kv *remotedbserver.KvServer, ethBackendSrv *EthBackendServer, txPoolServer txpool_proto.TxpoolServer,
	miningServer txpool_proto.MiningServer, addr string, rateLimit uint32, creds credentials.TransportCredentials,
	healthCheck bool, logger log.Logger) (*grpc.Server, error) {
	logger.Info("Starting private RPC server", "on", addr)
	lis, err := net.Listen("tcp", addr)
	if err != nil {
		return nil, fmt.Errorf("could not create listener: %w, addr=%s", err, addr)
	}

	grpcServer := grpcutil.NewServer(rateLimit, creds)
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
		if err := grpcServer.Serve(lis); err != nil {
			logger.Error("private RPC server fail", "err", err)
		}
	}()

	return grpcServer, nil
}
