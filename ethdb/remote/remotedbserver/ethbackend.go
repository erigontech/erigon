package remotedbserver

import (
	"context"
	"encoding/json"
	"sync"

	"github.com/ledgerwatch/turbo-geth/common"
	"github.com/ledgerwatch/turbo-geth/core"
	"github.com/ledgerwatch/turbo-geth/core/types"
	"github.com/ledgerwatch/turbo-geth/eth/stagedsync"
	"github.com/ledgerwatch/turbo-geth/ethdb/remote"
	"github.com/ledgerwatch/turbo-geth/log"
	"github.com/ledgerwatch/turbo-geth/rlp"
)

const (
	chainChanSize = 10
)

type EthBackendServer struct {
	remote.UnimplementedETHBACKENDServer // must be embedded to have forward compatible implementations.

	eth        core.Backend
	stagedSync *stagedsync.StagedSync
}

func NewEthBackendServer(eth core.Backend, stagedSync *stagedsync.StagedSync) *EthBackendServer {
	return &EthBackendServer{eth: eth, stagedSync: stagedSync}
}

func (s *EthBackendServer) Add(_ context.Context, in *remote.TxRequest) (*remote.AddReply, error) {
	signedTx := new(types.Transaction)
	out := &remote.AddReply{Hash: common.Hash{}.Bytes()}

	if err := rlp.DecodeBytes(in.Signedtx, signedTx); err != nil {
		return out, err
	}

	if err := s.eth.TxPool().AddLocal(signedTx); err != nil {
		return out, err
	}

	out.Hash = signedTx.Hash().Bytes()
	return out, nil
}

func (s *EthBackendServer) Etherbase(_ context.Context, _ *remote.EtherbaseRequest) (*remote.EtherbaseReply, error) {
	out := &remote.EtherbaseReply{Hash: common.Hash{}.Bytes()}

	base, err := s.eth.Etherbase()
	if err != nil {
		return out, err
	}

	out.Hash = base.Hash().Bytes()
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

	chainCh := make(chan stagedsync.ChainHeadEvent, chainChanSize)
	s.stagedSync.SubscribeChainHeadEvent(chainCh)

	go func() {
		defer wg.Done()
		for {
			select {
			case <-subscribeServer.Context().Done():
				return
			case h := <-chainCh:
				payload, err := json.Marshal(h.Header)
				if err != nil {
					log.Warn("error while marshaling a header", "err", err)
					return
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
					return
				}
			}
		}
	}()

	log.Info("event subscription channel established with the RPC daemon")
	wg.Wait()
	if err := subscribeServer.Context().Err(); err != nil {
		log.Warn("event subscription channel was closed", "reason", err)
	} else {
		log.Info("event subscription channel closed with the RPC daemon")
	}
	return nil
}
