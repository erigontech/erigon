package txpool

import (
	"bytes"
	"context"

	"github.com/ledgerwatch/turbo-geth/common"
	"github.com/ledgerwatch/turbo-geth/core"
	"github.com/ledgerwatch/turbo-geth/gointerfaces"
	proto_txpool "github.com/ledgerwatch/turbo-geth/gointerfaces/txpool"
	"github.com/ledgerwatch/turbo-geth/rlp"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

type Server struct {
	proto_txpool.UnimplementedTxpoolServer
	ctx    context.Context
	txPool *core.TxPool
}

func NewServer(ctx context.Context, txPool *core.TxPool) *Server {
	return &Server{ctx: ctx, txPool: txPool}
}

func (s *Server) FindUnknownTransactions(ctx context.Context, in *proto_txpool.TxHashes) (*proto_txpool.TxHashes, error) {
	return nil, status.Errorf(codes.Unimplemented, "method FindUnknownTransactions not implemented")
}
func (s *Server) ImportTransactions(ctx context.Context, in *proto_txpool.ImportRequest) (*proto_txpool.ImportReply, error) {
	return nil, status.Errorf(codes.Unimplemented, "method ImportTransactions not implemented")
}

func (s *Server) GetTransactions(ctx context.Context, in *proto_txpool.GetTransactionsRequest) (*proto_txpool.GetTransactionsReply, error) {
	buf := bytes.NewBuffer(nil)
	reply := &proto_txpool.GetTransactionsReply{Txs: make([][]byte, len(in.Hashes))}
	for i := range in.Hashes {
		txn := s.txPool.Get(gointerfaces.ConvertH256ToHash(in.Hashes[i]))
		if txn == nil {
			reply.Txs[i] = nil
			continue
		}
		buf.Reset()
		if err := rlp.Encode(buf, txn); err != nil {
			return nil, err
		}
		reply.Txs[i] = common.CopyBytes(buf.Bytes())
	}

	return reply, nil
}
