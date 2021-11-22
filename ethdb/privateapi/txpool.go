package privateapi

import (
	"bytes"
	"context"
	"fmt"

	"github.com/ledgerwatch/erigon-lib/gointerfaces"
	proto_txpool "github.com/ledgerwatch/erigon-lib/gointerfaces/txpool"
	types2 "github.com/ledgerwatch/erigon-lib/gointerfaces/types"
	"github.com/ledgerwatch/erigon/common"
	"github.com/ledgerwatch/erigon/core"
	"github.com/ledgerwatch/erigon/core/types"
	"github.com/ledgerwatch/erigon/event"
	"github.com/ledgerwatch/erigon/rlp"
	"github.com/ledgerwatch/log/v3"
	"google.golang.org/protobuf/types/known/emptypb"
)

// TxPoolAPIVersion
var TxPoolAPIVersion = &types2.VersionReply{Major: 1, Minor: 0, Patch: 0}

type txPool interface {
	Get(hash common.Hash) types.Transaction
	AddLocals(txs []types.Transaction) []error
	Content() (map[common.Address]types.Transactions, map[common.Address]types.Transactions)
	CountContent() (uint, uint)
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

func (s *TxPoolServer) Version(context.Context, *emptypb.Empty) (*types2.VersionReply, error) {
	return MiningAPIVersion, nil
}
func (s *TxPoolServer) All(context.Context, *proto_txpool.AllRequest) (*proto_txpool.AllReply, error) {
	pending, queued := s.txPool.Content()
	reply := &proto_txpool.AllReply{}
	reply.Txs = make([]*proto_txpool.AllReply_Tx, 0, 32)
	for addr, list := range pending {
		addrBytes := addr.Bytes()
		for i := range list {
			b, err := rlp.EncodeToBytes(list[i])
			if err != nil {
				return nil, err
			}
			reply.Txs = append(reply.Txs, &proto_txpool.AllReply_Tx{
				Sender: addrBytes,
				Type:   proto_txpool.AllReply_PENDING,
				RlpTx:  b,
			})
		}
	}

	for addr, list := range queued {
		addrBytes := addr.Bytes()
		for i := range list {
			b, err := rlp.EncodeToBytes(list[i])
			if err != nil {
				return nil, err
			}
			reply.Txs = append(reply.Txs, &proto_txpool.AllReply_Tx{
				Sender: addrBytes,
				Type:   proto_txpool.AllReply_QUEUED,
				RlpTx:  b,
			})
		}
	}

	return reply, nil
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
	txs, err := types.DecodeTransactions(in.RlpTxs)
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
	var rplTxs [][]byte
	for txs := range txsCh {
		rplTxs = rplTxs[:0]
		for _, tx := range txs.Txs {
			buf.Reset()
			if err := tx.MarshalBinary(&buf); err != nil {
				log.Warn("error while marshaling a pending transaction", "err", err)
				return err
			}
			rplTxs = append(rplTxs, common.CopyBytes(buf.Bytes()))
		}
		if err := stream.Send(&proto_txpool.OnAddReply{RplTxs: rplTxs}); err != nil {
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
		if err := txn.MarshalBinary(buf); err != nil {
			return nil, err
		}
		reply.RlpTxs[i] = common.CopyBytes(buf.Bytes())
	}

	return reply, nil
}

func (s *TxPoolServer) Status(_ context.Context, _ *proto_txpool.StatusRequest) (*proto_txpool.StatusReply, error) {
	pending, queued := s.txPool.CountContent()
	return &proto_txpool.StatusReply{
		PendingCount: uint32(pending),
		QueuedCount:  uint32(queued),
	}, nil
}
