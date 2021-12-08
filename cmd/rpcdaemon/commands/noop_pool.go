package commands

import (
	"context"
	"github.com/ledgerwatch/erigon-lib/gointerfaces/txpool"
	txpool_proto "github.com/ledgerwatch/erigon-lib/gointerfaces/txpool"
	types2 "github.com/ledgerwatch/erigon-lib/gointerfaces/types"
	"google.golang.org/grpc"
	"google.golang.org/protobuf/types/known/emptypb"
)

func NewNoopTxPoolClient() *NoopTxPool {
	return &NoopTxPool{}
}

type NoopTxPool struct {
	AddCalls []*txpool.AddRequest
}

func (n *NoopTxPool) Version(ctx context.Context, in *emptypb.Empty, opts ...grpc.CallOption) (*types2.VersionReply, error) {
	panic("implement me")
}

func (n *NoopTxPool) FindUnknown(ctx context.Context, in *txpool.TxHashes, opts ...grpc.CallOption) (*txpool.TxHashes, error) {
	panic("implement me")
}

func (n *NoopTxPool) Add(ctx context.Context, in *txpool.AddRequest, opts ...grpc.CallOption) (*txpool.AddReply, error) {
	n.AddCalls = append(n.AddCalls, in)

	reply := &txpool.AddReply{Imported: []txpool_proto.ImportResult{txpool_proto.ImportResult_SUCCESS}}

	return reply, nil
}

func (n *NoopTxPool) Transactions(ctx context.Context, in *txpool.TransactionsRequest, opts ...grpc.CallOption) (*txpool.TransactionsReply, error) {
	panic("implement me")
}

func (n *NoopTxPool) All(ctx context.Context, in *txpool.AllRequest, opts ...grpc.CallOption) (*txpool.AllReply, error) {
	panic("implement me")
}

func (n *NoopTxPool) Pending(ctx context.Context, in *emptypb.Empty, opts ...grpc.CallOption) (*txpool.PendingReply, error) {
	panic("implement me")
}

func (n *NoopTxPool) OnAdd(ctx context.Context, in *txpool.OnAddRequest, opts ...grpc.CallOption) (txpool.Txpool_OnAddClient, error) {
	panic("implement me")
}

func (n *NoopTxPool) Status(ctx context.Context, in *txpool.StatusRequest, opts ...grpc.CallOption) (*txpool.StatusReply, error) {
	panic("implement me")
}

func (n *NoopTxPool) Nonce(ctx context.Context, in *txpool.NonceRequest, opts ...grpc.CallOption) (*txpool.NonceReply, error) {
	panic("implement me")
}
