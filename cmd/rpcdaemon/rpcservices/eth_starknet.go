package rpcservices

import (
	"github.com/ledgerwatch/erigon-lib/gointerfaces"
	"github.com/ledgerwatch/erigon-lib/gointerfaces/starknet"
	types2 "github.com/ledgerwatch/erigon-lib/gointerfaces/types"
	"github.com/ledgerwatch/log/v3"
	"google.golang.org/grpc"
)

// StarknetAPIVersion
var StarknetAPIVersion = &types2.VersionReply{Major: 1, Minor: 0, Patch: 0}

type StarknetService struct {
	starknet.CAIROVMClient
	log     log.Logger
	version gointerfaces.Version
}

func NewStarknetService(cc grpc.ClientConnInterface) *StarknetService {
	return &StarknetService{
		CAIROVMClient: starknet.NewCAIROVMClient(cc),
		version:       gointerfaces.VersionFromProto(StarknetAPIVersion),
		log:           log.New("remote_service", "starknet"),
	}
}

func (s *StarknetService) EnsureVersionCompatibility() bool {
	//TODO: add version check
	return true
}
