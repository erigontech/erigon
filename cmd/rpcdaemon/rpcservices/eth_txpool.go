package rpcservices

import (
	"context"
	"fmt"
	"time"

	"github.com/ledgerwatch/erigon-lib/gointerfaces"
	"github.com/ledgerwatch/erigon-lib/gointerfaces/grpcutil"
	txpooproto "github.com/ledgerwatch/erigon-lib/gointerfaces/txpool"
	txpool2 "github.com/ledgerwatch/erigon/zk/txpool"
	"github.com/ledgerwatch/log/v3"
	"google.golang.org/grpc"
	"google.golang.org/protobuf/types/known/emptypb"
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
