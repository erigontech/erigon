package core

import (
	"context"
	"io"

	"github.com/ledgerwatch/turbo-geth/common"
	"github.com/ledgerwatch/turbo-geth/core/types"
	"github.com/ledgerwatch/turbo-geth/ethdb"
	"github.com/ledgerwatch/turbo-geth/gointerfaces"
	"github.com/ledgerwatch/turbo-geth/gointerfaces/remote"
	"github.com/ledgerwatch/turbo-geth/log"
	"github.com/ledgerwatch/turbo-geth/rlp"
)

type EthBackend struct {
	Backend
}

type Backend interface {
	TxPool() *TxPool
	Etherbase() (common.Address, error)
	NetVersion() (uint64, error)
}

func NewEthBackend(eth Backend) *EthBackend {
	return &EthBackend{eth}
}

func (back *EthBackend) AddLocal(signedtx []byte) ([]byte, error) {
	tx := new(types.Transaction)
	if err := rlp.DecodeBytes(signedtx, tx); err != nil {
		return common.Hash{}.Bytes(), err
	}

	return tx.Hash().Bytes(), back.TxPool().AddLocal(tx)
}

func (back *EthBackend) Subscribe(func(*remote.SubscribeReply)) error {
	// do nothing
	return nil
}

type RemoteBackend struct {
	remoteEthBackend remote.ETHBACKENDClient
	log              log.Logger
}

func NewRemoteBackend(kv ethdb.KV) *RemoteBackend {
	return &RemoteBackend{
		remoteEthBackend: remote.NewETHBACKENDClient(kv.(*ethdb.RemoteKV).GrpcConn()),
		log:              log.New("remote_db"),
	}
}

func (back *RemoteBackend) AddLocal(signedTx []byte) ([]byte, error) {
	res, err := back.remoteEthBackend.Add(context.Background(), &remote.TxRequest{Signedtx: signedTx})
	if err != nil {
		return common.Hash{}.Bytes(), err
	}
	return gointerfaces.ConvertH256ToHash(res.Hash).Bytes(), nil
}

func (back *RemoteBackend) Etherbase() (common.Address, error) {
	res, err := back.remoteEthBackend.Etherbase(context.Background(), &remote.EtherbaseRequest{})
	if err != nil {
		return common.Address{}, err
	}

	return gointerfaces.ConvertH160toAddress(res.Address), nil
}

func (back *RemoteBackend) NetVersion() (uint64, error) {
	res, err := back.remoteEthBackend.NetVersion(context.Background(), &remote.NetVersionRequest{})
	if err != nil {
		return 0, err
	}

	return res.Id, nil
}

func (back *RemoteBackend) Subscribe(onNewEvent func(*remote.SubscribeReply)) error {
	subscription, err := back.remoteEthBackend.Subscribe(context.Background(), &remote.SubscribeRequest{})
	if err != nil {
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
