package remotedbserver

import (
	"bytes"
	"context"
	"fmt"

	"github.com/ledgerwatch/turbo-geth/common"
	"github.com/ledgerwatch/turbo-geth/core"
	"github.com/ledgerwatch/turbo-geth/core/types"
	"github.com/ledgerwatch/turbo-geth/event"
	"github.com/ledgerwatch/turbo-geth/gointerfaces"
	proto_txpool "github.com/ledgerwatch/turbo-geth/gointerfaces/txpool"
	"github.com/ledgerwatch/turbo-geth/log"
	"github.com/ledgerwatch/turbo-geth/rlp"
)

type txPool interface {
	Get(hash common.Hash) types.Transaction
	AddLocals(txs []types.Transaction) []error
	SubscribeNewTxsEvent(ch chan<- core.NewTxsEvent) event.Subscription
}

type TxPoolServer struct {
	proto_txpool.UnimplementedTxpoolServer
	ctx    context.Context
	txPool txPool
}

func NewTxPoolServer(ctx context.Context, txPool txPool) *TxPoolServer {
	return &TxPoolServer{ctx: ctx, txPool: txPool}
}
func (s *TxPoolServer) FindUnknown(ctx context.Context, in *proto_txpool.TxHashes) (*proto_txpool.TxHashes, error) {
	return nil, fmt.Errorf("unimplemented")
	/*
		var underpriced int
		for i := range in.Hashes {
			h := gointerfaces.ConvertH256ToHash(in.Hashes[i])
			if s.txPool.Has(h) {
				continue
			}
			if s.underpriced.Contains(h) {
				underpriced++
				continue
			}
			reply.Hashes = append(reply.Hashes, in.Hashes[i])
		}
		txAnnounceInMeter.Mark(int64(len(in.Hashes)))
		txAnnounceKnownMeter.Mark(int64(len(in.Hashes) - len(reply.Hashes)))
		txAnnounceUnderpricedMeter.Mark(int64(underpriced))
	*/
}

func (s *TxPoolServer) Add(ctx context.Context, in *proto_txpool.AddRequest) (*proto_txpool.AddReply, error) {
	reply := &proto_txpool.AddReply{Imported: make([]proto_txpool.ImportResult, len(in.RlpTxs)), Errors: make([]string, len(in.RlpTxs))}
	txs, err := types.UnmarshalTransactionsFromBinary(in.RlpTxs)
	if err != nil {
		return nil, err
	}
	errs := s.txPool.AddLocals(txs)
	for i, err := range errs {
		if err == nil {
			continue
		}

		reply.Errors[i] = err.Error()

		// Track a few interesting failure types
		switch err {
		case nil: // Noop, but need to handle to not count these

		case core.ErrAlreadyKnown:
			reply.Imported[i] = proto_txpool.ImportResult_ALREADY_EXISTS
		case core.ErrUnderpriced, core.ErrReplaceUnderpriced:
			reply.Imported[i] = proto_txpool.ImportResult_FEE_TOO_LOW
		case core.ErrInvalidSender, core.ErrGasLimit, core.ErrNegativeValue, core.ErrOversizedData:
			reply.Imported[i] = proto_txpool.ImportResult_INVALID
		default:
			reply.Imported[i] = proto_txpool.ImportResult_INTERNAL_ERROR
		}
	}
	return reply, nil
}

func (s *TxPoolServer) OnAdd(req *proto_txpool.OnAddRequest, stream proto_txpool.Txpool_OnAddServer) error {
	txsCh := make(chan core.NewTxsEvent, 1024)
	defer close(txsCh)
	sub := s.txPool.SubscribeNewTxsEvent(txsCh)
	defer sub.Unsubscribe()

	var buf bytes.Buffer
	for tx := range txsCh {
		buf.Reset()
		if err := rlp.Encode(&buf, tx); err != nil {
			log.Warn("error while marshaling a pending transaction", "err", err)
			return err
		}
		if err := stream.Send(&proto_txpool.OnAddReply{RplTxs: [][]byte{common.CopyBytes(buf.Bytes())}}); err != nil {
			return err
		}
	}
	return nil
}

func (s *TxPoolServer) Transactions(ctx context.Context, in *proto_txpool.TransactionsRequest) (*proto_txpool.TransactionsReply, error) {
	buf := bytes.NewBuffer(nil)
	reply := &proto_txpool.TransactionsReply{RlpTxs: make([][]byte, len(in.Hashes))}
	for i := range in.Hashes {
		txn := s.txPool.Get(gointerfaces.ConvertH256ToHash(in.Hashes[i]))
		if txn == nil {
			reply.RlpTxs[i] = nil
			continue
		}
		buf.Reset()
		if err := rlp.Encode(buf, txn); err != nil {
			return nil, err
		}
		reply.RlpTxs[i] = common.CopyBytes(buf.Bytes())
	}

	return reply, nil
}
