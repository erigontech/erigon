package services

import (
	"context"
	"errors"
	"fmt"
	"io"

	"github.com/ledgerwatch/erigon-lib/gointerfaces"
	"github.com/ledgerwatch/erigon-lib/gointerfaces/remote"
	"github.com/ledgerwatch/erigon/common"
	"github.com/ledgerwatch/erigon/ethdb/privateapi"
	"github.com/ledgerwatch/log/v3"
	"google.golang.org/grpc"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/types/known/emptypb"
)

// ApiBackend - interface which must be used by API layer
// implementation can work with local Ethereum object or with Remote (grpc-based) one
// this is reason why all methods are accepting context and returning error
type ApiBackend interface {
	Etherbase(ctx context.Context) (common.Address, error)
	NetVersion(ctx context.Context) (uint64, error)
	NetPeerCount(ctx context.Context) (uint64, error)
	ProtocolVersion(ctx context.Context) (uint64, error)
	ClientVersion(ctx context.Context) (string, error)
	Subscribe(ctx context.Context, cb func(*remote.SubscribeReply)) error
}

type RemoteBackend struct {
	remoteEthBackend remote.ETHBACKENDClient
	log              log.Logger
	version          gointerfaces.Version
}

func NewRemoteBackend(cc grpc.ClientConnInterface) *RemoteBackend {
	return &RemoteBackend{
		remoteEthBackend: remote.NewETHBACKENDClient(cc),
		version:          gointerfaces.VersionFromProto(privateapi.EthBackendAPIVersion),
		log:              log.New("remote_service", "eth_backend"),
	}
}

func (back *RemoteBackend) EnsureVersionCompatibility() bool {
	versionReply, err := back.remoteEthBackend.Version(context.Background(), &emptypb.Empty{}, grpc.WaitForReady(true))
	if err != nil {

		back.log.Error("getting Version", "error", err)
		return false
	}
	if !gointerfaces.EnsureVersion(back.version, versionReply) {
		back.log.Error("incompatible interface versions", "client", back.version.String(),
			"server", fmt.Sprintf("%d.%d.%d", versionReply.Major, versionReply.Minor, versionReply.Patch))
		return false
	}
	back.log.Info("interfaces compatible", "client", back.version.String(),
		"server", fmt.Sprintf("%d.%d.%d", versionReply.Major, versionReply.Minor, versionReply.Patch))
	return true
}

func (back *RemoteBackend) Etherbase(ctx context.Context) (common.Address, error) {
	res, err := back.remoteEthBackend.Etherbase(ctx, &remote.EtherbaseRequest{})
	if err != nil {
		if s, ok := status.FromError(err); ok {
			return common.Address{}, errors.New(s.Message())
		}
		return common.Address{}, err
	}

	return gointerfaces.ConvertH160toAddress(res.Address), nil
}

func (back *RemoteBackend) NetVersion(ctx context.Context) (uint64, error) {
	res, err := back.remoteEthBackend.NetVersion(ctx, &remote.NetVersionRequest{})
	if err != nil {
		if s, ok := status.FromError(err); ok {
			return 0, errors.New(s.Message())
		}
		return 0, err
	}

	return res.Id, nil
}

func (back *RemoteBackend) NetPeerCount(ctx context.Context) (uint64, error) {
	res, err := back.remoteEthBackend.NetPeerCount(ctx, &remote.NetPeerCountRequest{})
	if err != nil {
		if s, ok := status.FromError(err); ok {
			return 0, errors.New(s.Message())
		}
		return 0, err
	}

	return res.Count, nil
}

func (back *RemoteBackend) ProtocolVersion(ctx context.Context) (uint64, error) {
	res, err := back.remoteEthBackend.ProtocolVersion(ctx, &remote.ProtocolVersionRequest{})
	if err != nil {
		if s, ok := status.FromError(err); ok {
			return 0, errors.New(s.Message())
		}
		return 0, err
	}

	return res.Id, nil
}

func (back *RemoteBackend) ClientVersion(ctx context.Context) (string, error) {
	res, err := back.remoteEthBackend.ClientVersion(ctx, &remote.ClientVersionRequest{})
	if err != nil {
		if s, ok := status.FromError(err); ok {
			return "", errors.New(s.Message())
		}
		return "", err
	}

	return res.NodeName, nil
}

func (back *RemoteBackend) Subscribe(ctx context.Context, onNewEvent func(*remote.SubscribeReply)) error {
	subscription, err := back.remoteEthBackend.Subscribe(ctx, &remote.SubscribeRequest{}, grpc.WaitForReady(true))
	if err != nil {
		if s, ok := status.FromError(err); ok {
			return errors.New(s.Message())
		}
		return err
	}
	for {
		event, err := subscription.Recv()
		if err == io.EOF {
			log.Info("rpcdaemon: the subscription channel was closed")
			break
		}
		if err != nil {
			return err
		}

		onNewEvent(event)
	}
	return nil
}
