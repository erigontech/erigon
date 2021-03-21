package remotedbserver

import (
	"context"
	"encoding/json"
	"errors"
	"sync"

	"github.com/ledgerwatch/turbo-geth/common"
	"github.com/ledgerwatch/turbo-geth/common/hexutil"
	"github.com/ledgerwatch/turbo-geth/consensus/ethash"
	"github.com/ledgerwatch/turbo-geth/core"
	"github.com/ledgerwatch/turbo-geth/core/types"
	"github.com/ledgerwatch/turbo-geth/gointerfaces"
	"github.com/ledgerwatch/turbo-geth/gointerfaces/remote"
	"github.com/ledgerwatch/turbo-geth/log"
	"github.com/ledgerwatch/turbo-geth/rlp"
)

type EthBackendServer struct {
	remote.UnimplementedETHBACKENDServer // must be embedded to have forward compatible implementations.

	eth    core.EthBackend
	events *Events
	ethash *ethash.API
}

func NewEthBackendServer(eth core.EthBackend, events *Events, ethashApi *ethash.API) *EthBackendServer {
	return &EthBackendServer{eth: eth, events: events, ethash: ethashApi}
}

func (s *EthBackendServer) Add(_ context.Context, in *remote.TxRequest) (*remote.AddReply, error) {
	signedTx := new(types.Transaction)
	out := &remote.AddReply{Hash: gointerfaces.ConvertHashToH256(common.Hash{})}

	if err := rlp.DecodeBytes(in.Signedtx, signedTx); err != nil {
		return out, err
	}

	if err := s.eth.TxPool().AddLocal(signedTx); err != nil {
		return out, err
	}

	out.Hash = gointerfaces.ConvertHashToH256(signedTx.Hash())
	return out, nil
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
	wg := sync.WaitGroup{}
	wg.Add(1)
	s.events.AddHeaderSubscription(func(h *types.Header) error {
		select {
		case <-subscribeServer.Context().Done():
			wg.Done()
			return subscribeServer.Context().Err()
		default:
		}

		payload, err := json.Marshal(h)
		if err != nil {
			log.Warn("error while marshaling a header", "err", err)
			return err
		}

		err = subscribeServer.Send(&remote.SubscribeReply{
			Type: uint64(EventTypeHeader),
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
	wg.Wait()
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
