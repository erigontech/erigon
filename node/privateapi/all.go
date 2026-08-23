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
	"context"

	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials"

	"github.com/erigontech/erigon/common/log/v3"
	"github.com/erigontech/erigon/db/kv/remotedbserver"
	"github.com/erigontech/erigon/node/gointerfaces/grpcutil"
	"github.com/erigontech/erigon/node/gointerfaces/remoteproto"
	"github.com/erigontech/erigon/node/gointerfaces/txpoolproto"
)

func StartGrpc(ctx context.Context, kv *remotedbserver.KvServer, ethBackendSrv *EthBackendServer, txPoolServer txpoolproto.TxpoolServer,
	miningServer txpoolproto.MiningServer,
	addr string, rateLimit uint32, creds credentials.TransportCredentials, healthCheck bool, logger log.Logger) (*grpc.Server, error) {
	logger.Info("Starting private RPC server", "on", addr)

	grpcServer := grpcutil.NewServer(rateLimit, creds)
	remoteproto.RegisterETHBACKENDServer(grpcServer, ethBackendSrv)
	if txPoolServer != nil {
		txpoolproto.RegisterTxpoolServer(grpcServer, txPoolServer)
	}
	if miningServer != nil {
		txpoolproto.RegisterMiningServer(grpcServer, miningServer)
	}
	remoteproto.RegisterKVServer(grpcServer, kv)

	if err := grpcutil.StartServer(ctx, grpcServer, addr, healthCheck, logger, "private RPC server fail"); err != nil {
		return nil, err
	}
	return grpcServer, nil
}
