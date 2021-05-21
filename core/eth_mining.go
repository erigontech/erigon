package core

import (
	"context"
	"fmt"

	"github.com/ledgerwatch/erigon/gointerfaces"
	"github.com/ledgerwatch/erigon/gointerfaces/txpool"
	"github.com/ledgerwatch/erigon/log"
	"google.golang.org/grpc"
	"google.golang.org/protobuf/types/known/emptypb"
)

var MiningAPIVersion = gointerfaces.Version{Major: 1, Minor: 0, Patch: 0}

type MiningService struct {
	txpool.MiningClient
	log     log.Logger
	version gointerfaces.Version
}

func NewMiningService(cc grpc.ClientConnInterface) *MiningService {
	return &MiningService{
		MiningClient: txpool.NewMiningClient(cc),
		version:      MiningAPIVersion,
		log:          log.New("mining_service"),
	}
}

func (s *MiningService) EnsureVersionCompatibility() bool {
	versionReply, err := s.Version(context.Background(), &emptypb.Empty{}, grpc.WaitForReady(true))
	if err != nil {
		s.log.Error("getting Version info from remove KV", "error", err)
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
