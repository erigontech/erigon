package remotedbserver

import (
	"context"

	"github.com/ledgerwatch/turbo-geth/common"
	"github.com/ledgerwatch/turbo-geth/core"
	"github.com/ledgerwatch/turbo-geth/core/types"
	"github.com/ledgerwatch/turbo-geth/ethdb/remote"
	"github.com/ledgerwatch/turbo-geth/internal/ethapi"
	"github.com/ledgerwatch/turbo-geth/rlp"
)

type TxPoolServer struct {
	remote.UnimplementedTXPOOLServer // must be embedded to have forward compatible implementations.

	txPool    *core.TxPool
	nonceLock *ethapi.AddrLocker
}

func NewTxPoolServer(txPool *core.TxPool) *TxPoolServer {
	return &TxPoolServer{txPool: txPool, nonceLock: new(ethapi.AddrLocker)}
}

func (s *TxPoolServer) Add(_ context.Context, in *remote.TxRequest) (*remote.AddReply, error) {
	signedTx := new(types.Transaction)
	out := &remote.AddReply{Hash: common.Hash{}.Bytes()}

	if err := rlp.DecodeBytes(in.Signedtx, signedTx); err != nil {
		return out, err
	}

	if err := s.txPool.AddLocal(signedTx); err != nil {
		return out, err
	}

	out.Hash = signedTx.Hash().Bytes()
	return out, nil
}
