package remotedbserver

import (
	"bytes"
	"context"

	"github.com/ledgerwatch/turbo-geth/common"
	"github.com/ledgerwatch/turbo-geth/core"
	"github.com/ledgerwatch/turbo-geth/core/types"
	"github.com/ledgerwatch/turbo-geth/gointerfaces"
	"github.com/ledgerwatch/turbo-geth/gointerfaces/remote"
	types2 "github.com/ledgerwatch/turbo-geth/gointerfaces/types"
	"github.com/ledgerwatch/turbo-geth/log"
	"github.com/ledgerwatch/turbo-geth/params"
	"github.com/ledgerwatch/turbo-geth/rlp"
	"google.golang.org/protobuf/types/known/emptypb"
)

type EthBackendServer struct {
	remote.UnimplementedETHBACKENDServer // must be embedded to have forward compatible implementations.

	eth       core.EthBackend
	events    *Events
	gitCommit string
}

func NewEthBackendServer(eth core.EthBackend, events *Events, gitCommit string) *EthBackendServer {
	return &EthBackendServer{eth: eth, events: events, gitCommit: gitCommit}
}

func (s *EthBackendServer) Version(context.Context, *emptypb.Empty) (*types2.VersionReply, error) {
	return &types2.VersionReply{Major: 2, Minor: 0, Patch: 0}, nil
}

func (s *EthBackendServer) Etherbase(_ context.Context, _ *remote.EtherbaseRequest) (*remote.EtherbaseReply, error) {
	out := &remote.EtherbaseReply{Address: gointerfaces.ConvertAddressToH160(common.Address{})}

	base, err := s.eth.Etherbase()
	if err != nil {
		return out, err
	}

	out.Address = gointerfaces.ConvertAddressToH160(base)
	return out, nil
}

func (s *EthBackendServer) NetVersion(_ context.Context, _ *remote.NetVersionRequest) (*remote.NetVersionReply, error) {
	id, err := s.eth.NetVersion()
	if err != nil {
		return &remote.NetVersionReply{}, err
	}
	return &remote.NetVersionReply{Id: id}, nil
}

func (s *EthBackendServer) Subscribe(r *remote.SubscribeRequest, subscribeServer remote.ETHBACKEND_SubscribeServer) error {
	log.Debug("establishing event subscription channel with the RPC daemon")
	s.events.AddHeaderSubscription(func(h *types.Header) error {
		select {
		case <-subscribeServer.Context().Done():
			return subscribeServer.Context().Err()
		default:
		}

		var buf bytes.Buffer
		if err := rlp.Encode(&buf, h); err != nil {
			log.Warn("error while marshaling a header", "err", err)
			return err
		}
		payload := buf.Bytes()

		err := subscribeServer.Send(&remote.SubscribeReply{
			Type: remote.Event_HEADER,
			Data: payload,
		})

		// we only close the wg on error because if we successfully sent an event,
		// that means that the channel wasn't closed and is ready to
		// receive more events.
		// if rpcdaemon disconnects, we will receive an error here
		// next time we try to send an event
		if err != nil {
			log.Info("event subscription channel was closed", "reason", err)
		}
		return err
	})

	//s.events.AddPendingLogsSubscription(func(data types.Logs) error {
	//	select {
	//	case <-subscribeServer.Context().Done():
	//		return subscribeServer.Context().Err()
	//	default:
	//	}
	//
	//	var buf bytes.Buffer
	//	if err := rlp.Encode(&buf, data); err != nil {
	//		log.Warn("error while marshaling a pending logs", "err", err)
	//		return err
	//	}
	//	payload := buf.Bytes()
	//
	//	err := subscribeServer.Send(&remote.SubscribeReply{
	//		Type: remote.Event_PENDING_LOGS,
	//		Data: payload,
	//	})
	//
	//	// we only close the wg on error because if we successfully sent an event,
	//	// that means that the channel wasn't closed and is ready to
	//	// receive more events.
	//	// if rpcdaemon disconnects, we will receive an error here
	//	// next time we try to send an event
	//	if err != nil {
	//		log.Info("event subscription channel was closed", "reason", err)
	//	}
	//	return err
	//})

	//s.events.AddPendingBlockSubscription(func(data *types.Block) error {
	//	select {
	//	case <-subscribeServer.Context().Done():
	//		return subscribeServer.Context().Err()
	//	default:
	//	}
	//
	//	var buf bytes.Buffer
	//	if err := rlp.Encode(&buf, data); err != nil {
	//		log.Warn("error while marshaling a pending block", "err", err)
	//		return err
	//	}
	//	payload := buf.Bytes()
	//
	//	err := subscribeServer.Send(&remote.SubscribeReply{
	//		Type: remote.Event_PENDING_BLOCK,
	//		Data: payload,
	//	})
	//
	//	// we only close the wg on error because if we successfully sent an event,
	//	// that means that the channel wasn't closed and is ready to
	//	// receive more events.
	//	// if rpcdaemon disconnects, we will receive an error here
	//	// next time we try to send an event
	//	if err != nil {
	//		log.Info("event subscription channel was closed", "reason", err)
	//	}
	//	return err
	//})

	log.Info("event subscription channel established with the RPC daemon")
	<-subscribeServer.Context().Done()
	log.Info("event subscription channel closed with the RPC daemon")
	return nil
}

func (s *EthBackendServer) ProtocolVersion(_ context.Context, _ *remote.ProtocolVersionRequest) (*remote.ProtocolVersionReply, error) {
	// Hardcoding to avoid import cycle
	return &remote.ProtocolVersionReply{Id: 66}, nil
}

func (s *EthBackendServer) ClientVersion(_ context.Context, _ *remote.ClientVersionRequest) (*remote.ClientVersionReply, error) {
	return &remote.ClientVersionReply{NodeName: common.MakeName("TurboGeth", params.VersionWithCommit(s.gitCommit, ""))}, nil
}
