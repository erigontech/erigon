package rpcservices

import (
	"context"
	"fmt"

	"github.com/gateway-fm/cdk-erigon-lib/gointerfaces"
	"github.com/gateway-fm/cdk-erigon-lib/gointerfaces/txpool"
	"github.com/ledgerwatch/erigon/ethdb/privateapi"
	"github.com/ledgerwatch/log/v3"
	"google.golang.org/grpc"
	"google.golang.org/protobuf/types/known/emptypb"
)

type MiningService struct {
	txpool.MiningClient
	log     log.Logger
	version gointerfaces.Version
}

func NewMiningService(client txpool.MiningClient) *MiningService {
	return &MiningService{
		MiningClient: client,
		version:      gointerfaces.VersionFromProto(privateapi.MiningAPIVersion),
		log:          log.New("remote_service", "mining"),
	}
}

func (s *MiningService) EnsureVersionCompatibility() bool {
	versionReply, err := s.Version(context.Background(), &emptypb.Empty{}, grpc.WaitForReady(true))
	if err != nil {
		s.log.Error("getting Version", "err", err)
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
