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

package rpcservices

import (
	"context"
	"fmt"
	"time"

	"google.golang.org/grpc"
	"google.golang.org/protobuf/types/known/emptypb"

	txpool2 "github.com/erigontech/erigon/txnprovider/txpool"

	"github.com/erigontech/erigon-lib/gointerfaces"
	"github.com/erigontech/erigon-lib/gointerfaces/grpcutil"
	txpooproto "github.com/erigontech/erigon-lib/gointerfaces/txpoolproto"
	"github.com/erigontech/erigon-lib/log/v3"
)

type TxPoolService struct {
	txpooproto.TxpoolClient
	log     log.Logger
	version gointerfaces.Version
}

func NewTxPoolService(client txpooproto.TxpoolClient) *TxPoolService {
	return &TxPoolService{
		TxpoolClient: client,
		version:      gointerfaces.VersionFromProto(txpool2.TxPoolAPIVersion),
		log:          log.New("remote_service", "tx_pool"),
	}
}

func (s *TxPoolService) EnsureVersionCompatibility() bool {
Start:
	versionReply, err := s.Version(context.Background(), &emptypb.Empty{}, grpc.WaitForReady(true))
	if err != nil {
		if grpcutil.ErrIs(err, txpool2.ErrPoolDisabled) {
			time.Sleep(3 * time.Second)
			goto Start
		}
		s.log.Error("ensure version", "err", err)
		return false
	}
	if !gointerfaces.EnsureVersion(s.version, versionReply) {
		s.log.Error("incompatible interface versions", "client", s.version.String(),
			"server", fmt.Sprintf("%d.%d.%d", versionReply.Major, versionReply.Minor, versionReply.Patch))
		return false
	}
	s.log.Info("interfaces compatible", "client", s.version.String(),
		"server", fmt.Sprintf("%d.%d.%d", versionReply.Major, versionReply.Minor, versionReply.Patch))
	return true
}
