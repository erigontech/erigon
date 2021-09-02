package privateapi

import (
	"fmt"
	"net"

	"github.com/ledgerwatch/erigon-lib/gointerfaces/grpcutil"
	//grpc_prometheus "github.com/grpc-ecosystem/go-grpc-prometheus"
	"github.com/ledgerwatch/erigon-lib/gointerfaces/remote"
	txpool_proto "github.com/ledgerwatch/erigon-lib/gointerfaces/txpool"
	"github.com/ledgerwatch/erigon/ethdb/remotedbserver"
	"github.com/ledgerwatch/log/v3"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials"
)

func StartGrpc(kv *remotedbserver.KvServer, ethBackendSrv *EthBackendServer, txPoolServer txpool_proto.TxpoolServer, miningServer txpool_proto.MiningServer, addr string, rateLimit uint32, creds *credentials.TransportCredentials) (*grpc.Server, error) {
	log.Info("Starting private RPC server", "on", addr)
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

	go func() {
		if err := grpcServer.Serve(lis); err != nil {
			log.Error("private RPC server fail", "err", err)
		}
	}()

	return grpcServer, nil
}
