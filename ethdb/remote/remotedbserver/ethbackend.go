package remotedbserver

import (
	"context"
	"fmt"
	"time"

	"github.com/ledgerwatch/turbo-geth/common"
	"github.com/ledgerwatch/turbo-geth/core"
	"github.com/ledgerwatch/turbo-geth/core/types"
	"github.com/ledgerwatch/turbo-geth/ethdb/remote"
	"github.com/ledgerwatch/turbo-geth/rlp"
)

type EthBackendServer struct {
	remote.UnimplementedETHBACKENDServer // must be embedded to have forward compatible implementations.

	eth core.Backend
}

func NewEthBackendServer(eth core.Backend) *EthBackendServer {
	return &EthBackendServer{eth: eth}
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

func (s *EthBackendServer) Subscribe(_ *remote.SubscribeRequest, subscribeServer remote.ETHBACKEND_SubscribeServer) error {
	for i := 0; i < 10; i++ {
		time.Sleep(1 * time.Second)
		err := subscribeServer.Send(&remote.SubscribeReply{Data: []byte(fmt.Sprintf("iteraton %d", i))})
		if err != nil {
			return err
		}
	}

	return nil
}
