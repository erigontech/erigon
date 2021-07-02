package services

import (
	"context"
	"fmt"

	"github.com/ledgerwatch/erigon-lib/gointerfaces"
	"github.com/ledgerwatch/erigon-lib/gointerfaces/txpool"
	"github.com/ledgerwatch/erigon/ethdb/remote/remotedbserver"
	"github.com/ledgerwatch/erigon/log"
	"google.golang.org/grpc"
	"google.golang.org/protobuf/types/known/emptypb"
)

type TxPoolService struct {
	txpool.TxpoolClient
	log     log.Logger
	version gointerfaces.Version
}

func NewTxPoolService(cc grpc.ClientConnInterface) *TxPoolService {
	return &TxPoolService{
		TxpoolClient: txpool.NewTxpoolClient(cc),
		version:      gointerfaces.VersionFromProto(remotedbserver.TxPoolAPIVersion),
		log:          log.New("remote_service", "tx_pool"),
	}
}

func (s *TxPoolService) EnsureVersionCompatibility() bool {
	versionReply, err := s.Version(context.Background(), &emptypb.Empty{}, grpc.WaitForReady(true))
	if err != nil {
		s.log.Error("ensure version", "error", err)
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
