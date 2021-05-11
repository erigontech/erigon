package remotedbserver

import (
	"bytes"
	"context"
	"errors"

	"github.com/ledgerwatch/turbo-geth/common"
	"github.com/ledgerwatch/turbo-geth/common/hexutil"
	"github.com/ledgerwatch/turbo-geth/consensus/ethash"
	"github.com/ledgerwatch/turbo-geth/core"
	"github.com/ledgerwatch/turbo-geth/core/types"
	"github.com/ledgerwatch/turbo-geth/gointerfaces"
	"github.com/ledgerwatch/turbo-geth/gointerfaces/remote"
	"github.com/ledgerwatch/turbo-geth/log"
	"github.com/ledgerwatch/turbo-geth/params"
	"github.com/ledgerwatch/turbo-geth/rlp"
)

type EthBackendServer struct {
	remote.UnimplementedETHBACKENDServer // must be embedded to have forward compatible implementations.

	eth       core.EthBackend
	events    *Events
	ethash    *ethash.API
	gitCommit string
}

func NewEthBackendServer(eth core.EthBackend, events *Events, ethashApi *ethash.API, gitCommit string) *EthBackendServer {
	return &EthBackendServer{eth: eth, events: events, ethash: ethashApi, gitCommit: gitCommit}
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

	s.events.AddPendingLogsSubscription(func(data types.Logs) error {
		select {
		case <-subscribeServer.Context().Done():
			return subscribeServer.Context().Err()
		default:
		}

		var buf bytes.Buffer
		if err := rlp.Encode(&buf, data); err != nil {
			log.Warn("error while marshaling a pending logs", "err", err)
			return err
		}
		payload := buf.Bytes()

		err := subscribeServer.Send(&remote.SubscribeReply{
			Type: remote.Event_PENDING_LOGS,
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

	s.events.AddPendingBlockSubscription(func(data *types.Block) error {
		select {
		case <-subscribeServer.Context().Done():
			return subscribeServer.Context().Err()
		default:
		}

		var buf bytes.Buffer
		if err := rlp.Encode(&buf, data); err != nil {
			log.Warn("error while marshaling a pending block", "err", err)
			return err
		}
		payload := buf.Bytes()

		err := subscribeServer.Send(&remote.SubscribeReply{
			Type: remote.Event_PENDING_BLOCK,
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

	log.Info("event subscription channel established with the RPC daemon")
	<-subscribeServer.Context().Done()
	log.Info("event subscription channel closed with the RPC daemon")
	return nil
}

func (s *EthBackendServer) GetWork(context.Context, *remote.GetWorkRequest) (*remote.GetWorkReply, error) {
	if s.ethash == nil {
		return nil, errors.New("not supported, consensus engine is not ethash")
	}
	res, err := s.ethash.GetWork()
	if err != nil {
		return nil, err
	}
	return &remote.GetWorkReply{HeaderHash: res[0], SeedHash: res[1], Target: res[2], BlockNumber: res[3]}, nil
}

func (s *EthBackendServer) SubmitWork(_ context.Context, req *remote.SubmitWorkRequest) (*remote.SubmitWorkReply, error) {
	if s.ethash == nil {
		return nil, errors.New("not supported, consensus engine is not ethash")
	}
	var nonce types.BlockNonce
	copy(nonce[:], req.BlockNonce)
	ok := s.ethash.SubmitWork(nonce, common.BytesToHash(req.PowHash), common.BytesToHash(req.Digest))
	return &remote.SubmitWorkReply{Ok: ok}, nil
}

func (s *EthBackendServer) SetHashRate(_ context.Context, req *remote.SubmitHashRateRequest) (*remote.SubmitHashRateReply, error) {
	if s.ethash == nil {
		return nil, errors.New("not supported, consensus engine is not ethash")
	}
	ok := s.ethash.SubmitHashRate(hexutil.Uint64(req.Rate), common.BytesToHash(req.Id))
	return &remote.SubmitHashRateReply{Ok: ok}, nil
}

func (s *EthBackendServer) GetHashRate(_ context.Context, req *remote.GetHashRateRequest) (*remote.GetHashRateReply, error) {
	if s.ethash == nil {
		return nil, errors.New("not supported, consensus engine is not ethash")
	}
	return &remote.GetHashRateReply{HashRate: s.ethash.GetHashrate()}, nil
}

func (s *EthBackendServer) Mining(_ context.Context, req *remote.MiningRequest) (*remote.MiningReply, error) {
	if s.ethash == nil {
		return nil, errors.New("not supported, consensus engine is not ethash")
	}
	return &remote.MiningReply{Enabled: s.eth.IsMining(), Running: true}, nil
}

func (s *EthBackendServer) ProtocolVersion(_ context.Context, _ *remote.ProtocolVersionRequest) (*remote.ProtocolVersionReply, error) {
	// Hardcoding to avoid import cycle
	return &remote.ProtocolVersionReply{Id: 66}, nil
}

func (s *EthBackendServer) ClientVersion(_ context.Context, _ *remote.ClientVersionRequest) (*remote.ClientVersionReply, error) {
	return &remote.ClientVersionReply{NodeName: common.MakeName("TurboGeth", params.VersionWithCommit(s.gitCommit, ""))}, nil
}
