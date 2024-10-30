// Copyright 2024 The Erigon Authors
// This file is part of Erigon.
//
// Erigon is free software: you can redistribute it and/or modify
// it under the terms of the GNU Lesser General Public License as published by
// the Free Software Foundation, either version 3 of the License, or
// (at your option) any later version.
//
// Erigon is distributed in the hope that it will be useful,
// but WITHOUT ANY WARRANTY; without even the implied warranty of
// MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE. See the
// GNU Lesser General Public License for more details.
//
// You should have received a copy of the GNU Lesser General Public License
// along with Erigon. If not, see <http://www.gnu.org/licenses/>.

package privateapi

import (
	"fmt"
	"net"

	"github.com/erigontech/erigon-lib/gointerfaces/grpcutil"
	remote "github.com/erigontech/erigon-lib/gointerfaces/remoteproto"
	"github.com/erigontech/erigon/v3/polygon/bridge"
	"github.com/erigontech/erigon/v3/polygon/heimdall"

	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials"
	"google.golang.org/grpc/health"
	"google.golang.org/grpc/health/grpc_health_v1"

	"github.com/erigontech/erigon-lib/gointerfaces/txpoolproto"
	"github.com/erigontech/erigon-lib/kv/remotedbserver"
	"github.com/erigontech/erigon-lib/log/v3"
)

func StartGrpc(kv *remotedbserver.KvServer, ethBackendSrv *EthBackendServer, txPoolServer txpoolproto.TxpoolServer,
	miningServer txpoolproto.MiningServer, bridgeServer *bridge.BackendServer, heimdallServer *heimdall.BackendServer,
	addr string, rateLimit uint32, creds credentials.TransportCredentials, healthCheck bool, logger log.Logger) (*grpc.Server, error) {
	logger.Info("Starting private RPC server", "on", addr)
	lis, err := net.Listen("tcp", addr)
	if err != nil {
		return nil, fmt.Errorf("could not create listener: %w, addr=%s", err, addr)
	}

	grpcServer := grpcutil.NewServer(rateLimit, creds)
	remote.RegisterETHBACKENDServer(grpcServer, ethBackendSrv)
	if txPoolServer != nil {
		txpoolproto.RegisterTxpoolServer(grpcServer, txPoolServer)
	}
	if miningServer != nil {
		txpoolproto.RegisterMiningServer(grpcServer, miningServer)
	}
	if bridgeServer != nil {
		remote.RegisterBridgeBackendServer(grpcServer, bridgeServer)
	}
	if heimdallServer != nil {
		remote.RegisterHeimdallBackendServer(grpcServer, heimdallServer)
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
