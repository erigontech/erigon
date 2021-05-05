package remotedbserver

import (
	"bytes"
	"context"
	"fmt"
	"sync"

	"github.com/ledgerwatch/turbo-geth/common"
	"github.com/ledgerwatch/turbo-geth/core"
	"github.com/ledgerwatch/turbo-geth/core/types"
	"github.com/ledgerwatch/turbo-geth/event"
	"github.com/ledgerwatch/turbo-geth/gointerfaces"
	proto_txpool "github.com/ledgerwatch/turbo-geth/gointerfaces/txpool"
	"github.com/ledgerwatch/turbo-geth/log"
)

type txPool interface {
	Get(hash common.Hash) types.Transaction
	AddLocals(txs []types.Transaction) []error
	SubscribeNewTxsEvent(ch chan<- core.NewTxsEvent) event.Subscription
}

type TxPoolServer struct {
	proto_txpool.UnimplementedTxpoolServer
	ctx                 context.Context
	txPool              txPool
	pendingBlocksPubSub types.BlocksPubSub
	minedBlocksPubSub   types.BlocksPubSub
	minedBlockStreams   MinedBlockStreams
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

func (s *TxPoolServer) SendPendingBlock(block *types.Block) { s.pendingBlocksPubSub.Pub(block) }

func (s *TxPoolServer) OnPendingBlock(req *proto_txpool.OnPendingBlockRequest, reply proto_txpool.Txpool_OnPendingBlockServer) error {
	ch, unsubscribe := s.pendingBlocksPubSub.Sub()
	defer unsubscribe()

	var buf bytes.Buffer
	for b := range ch {
		buf.Reset()
		if err := b.EncodeRLP(&buf); err != nil {
			return err
		}

		if err := reply.Send(&proto_txpool.OnPendingBlockReply{RplBlock: common.CopyBytes(buf.Bytes())}); err != nil {
			return err
		}
	}
	return nil
}

func (s *TxPoolServer) SendMinedBlock(block *types.Block) {
	var buf bytes.Buffer
	if err := block.EncodeRLP(&buf); err != nil {
		//log error
		return
	}
	reply := &proto_txpool.OnMinedBlockReply{RplBlock: common.CopyBytes(buf.Bytes())}
	s.minedBlockStreams.Pub(reply)
}

func (s *TxPoolServer) OnMinedBlock(req *proto_txpool.OnMinedBlockRequest, reply proto_txpool.Txpool_OnMinedBlockServer) error {
	s.minedBlockStreams.Add(reply)
	<-reply.Context().Done()
	return reply.Context().Err()
}

// BlocksPubSub - it's safe to use this class as non-pointer, do double-unsubscribe
type MinedBlockStreams struct {
	sync.Mutex
	id    uint
	chans map[uint]proto_txpool.Txpool_OnMinedBlockServer
}

func (s *MinedBlockStreams) Add(stream proto_txpool.Txpool_OnMinedBlockServer) (unsubscribe func()) {
	s.Lock()
	defer s.Unlock()
	if s.chans == nil {
		s.chans = make(map[uint]proto_txpool.Txpool_OnMinedBlockServer)
	}
	s.id++
	id := s.id
	s.chans[id] = stream
	return func() { s.unsubscribe(id) }
}

func (s *MinedBlockStreams) Pub(reply *proto_txpool.OnMinedBlockReply) {
	s.Lock()
	defer s.Unlock()
	for _, stream := range s.chans {
		if err := stream.Send(reply); err != nil {
			fmt.Printf("errr from stream: %s\n", err)
			_ = err //TODO: log me
		}
	}
}

func (s *MinedBlockStreams) unsubscribe(id uint) {
	s.Lock()
	defer s.Unlock()
	_, ok := s.chans[id]
	if !ok { // double-unsubscribe support
		return
	}
	delete(s.chans, id)
}
