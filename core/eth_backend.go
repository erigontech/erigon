package core

import (
	"context"
	"errors"
	"io"

	"github.com/ledgerwatch/turbo-geth/common"
	"github.com/ledgerwatch/turbo-geth/common/hexutil"
	"github.com/ledgerwatch/turbo-geth/consensus/ethash"
	"github.com/ledgerwatch/turbo-geth/core/types"
	"github.com/ledgerwatch/turbo-geth/ethdb"
	"github.com/ledgerwatch/turbo-geth/gointerfaces"
	"github.com/ledgerwatch/turbo-geth/gointerfaces/remote"
	"github.com/ledgerwatch/turbo-geth/log"
	"github.com/ledgerwatch/turbo-geth/rlp"
	"google.golang.org/grpc/status"
)

// ApiBackend - interface which must be used by API layer
// implementation can work with local Ethereum object or with Remote (grpc-based) one
// this is reason why all methods are accepting context and returning error
type ApiBackend interface {
	AddLocal(context.Context, []byte) ([]byte, error)
	Etherbase(ctx context.Context) (common.Address, error)
	NetVersion(ctx context.Context) (uint64, error)
	Subscribe(ctx context.Context, cb func(*remote.SubscribeReply)) error

	GetWork(ctx context.Context) ([4]string, error)
	SubmitWork(ctx context.Context, nonce types.BlockNonce, hash, digest common.Hash) (bool, error)
	SubmitHashRate(ctx context.Context, rate hexutil.Uint64, id common.Hash) (bool, error)
	GetHashRate(ctx context.Context) (uint64, error)
}

type EthBackend interface {
	TxPool() *TxPool
	Etherbase() (common.Address, error)
	NetVersion() (uint64, error)
}

type EthBackendImpl struct {
	eth    EthBackend
	ethash *ethash.API
}

func NewEthBackend(eth EthBackend, ethashApi *ethash.API) *EthBackendImpl {
	return &EthBackendImpl{eth: eth, ethash: ethashApi}
}

func (back *EthBackendImpl) AddLocal(_ context.Context, signedtx []byte) ([]byte, error) {
	tx := new(types.Transaction)
	if err := rlp.DecodeBytes(signedtx, tx); err != nil {
		return common.Hash{}.Bytes(), err
	}

	return tx.Hash().Bytes(), back.eth.TxPool().AddLocal(tx)
}

func (back *EthBackendImpl) Etherbase(_ context.Context) (common.Address, error) {
	return back.eth.Etherbase()
}
func (back *EthBackendImpl) NetVersion(_ context.Context) (uint64, error) {
	return back.eth.NetVersion()
}
func (back *EthBackendImpl) Subscribe(_ context.Context, cb func(*remote.SubscribeReply)) error {
	// do nothing
	return nil
}

func (back *EthBackendImpl) GetWork(ctx context.Context) ([4]string, error) {
	return back.ethash.GetWork()
}
func (back *EthBackendImpl) SubmitWork(ctx context.Context, nonce types.BlockNonce, hash, digest common.Hash) (bool, error) {
	return back.ethash.SubmitWork(nonce, hash, digest), nil
}
func (back *EthBackendImpl) SubmitHashRate(ctx context.Context, rate hexutil.Uint64, id common.Hash) (bool, error) {
	return back.ethash.SubmitHashRate(rate, id), nil
}
func (back *EthBackendImpl) GetHashRate(ctx context.Context) (uint64, error) {
	return back.ethash.GetHashrate(), nil
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

func (back *RemoteBackend) AddLocal(ctx context.Context, signedTx []byte) ([]byte, error) {
	res, err := back.remoteEthBackend.Add(ctx, &remote.TxRequest{Signedtx: signedTx})
	if err != nil {
		if s, ok := status.FromError(err); ok {
			return common.Hash{}.Bytes(), errors.New(s.Message())
		}
		return common.Hash{}.Bytes(), err
	}
	return gointerfaces.ConvertH256ToHash(res.Hash).Bytes(), nil
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

func (back *RemoteBackend) Subscribe(ctx context.Context, onNewEvent func(*remote.SubscribeReply)) error {
	subscription, err := back.remoteEthBackend.Subscribe(ctx, &remote.SubscribeRequest{})
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

func (back *RemoteBackend) GetWork(ctx context.Context) ([4]string, error) {
	var res [4]string
	repl, err := back.remoteEthBackend.GetWork(ctx, &remote.GetWorkRequest{})
	if err != nil {
		if s, ok := status.FromError(err); ok {
			return res, errors.New(s.Message())
		}
		return res, err
	}
	res[0] = repl.HeaderHash
	res[1] = repl.SeedHash
	res[2] = repl.Target
	res[3] = repl.BlockNumber
	return res, nil
}

func (back *RemoteBackend) SubmitWork(ctx context.Context, nonce types.BlockNonce, hash, digest common.Hash) (bool, error) {
	repl, err := back.remoteEthBackend.SubmitWork(ctx, &remote.SubmitWorkRequest{BlockNonce: nonce[:], PowHash: hash.Bytes(), Digest: digest.Bytes()})
	if err != nil {
		if s, ok := status.FromError(err); ok {
			return false, errors.New(s.Message())
		}
		return false, err
	}
	return repl.Ok, err
}

func (back *RemoteBackend) SubmitHashRate(ctx context.Context, rate hexutil.Uint64, id common.Hash) (bool, error) {
	repl, err := back.remoteEthBackend.SubmitHashRate(ctx, &remote.SubmitHashRateRequest{Rate: uint64(rate), Id: id.Bytes()})
	if err != nil {
		if s, ok := status.FromError(err); ok {
			return false, errors.New(s.Message())
		}
		return false, err
	}
	return repl.Ok, err
}

func (back *RemoteBackend) GetHashRate(ctx context.Context) (uint64, error) {
	repl, err := back.remoteEthBackend.GetHashRate(ctx, &remote.GetHashRateRequest{})
	if err != nil {
		if s, ok := status.FromError(err); ok {
			return 0, errors.New(s.Message())
		}
		return 0, err
	}
	return repl.HashRate, err
}
