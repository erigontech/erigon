package direct

import (
	"context"

	txpool_proto "github.com/ledgerwatch/erigon-lib/gointerfaces/txpool"
	"github.com/ledgerwatch/erigon-lib/gointerfaces/types"
	"github.com/ledgerwatch/log/v3"
	"google.golang.org/grpc"
	"google.golang.org/protobuf/types/known/emptypb"
)

var _ txpool_proto.TxpoolClient = (*TxPoolClient)(nil)

type TxPoolClient struct {
	server txpool_proto.TxpoolServer
}

func NewTxPoolClient(server txpool_proto.TxpoolServer) *TxPoolClient {
	return &TxPoolClient{server}
}

func (s *TxPoolClient) Version(ctx context.Context, in *emptypb.Empty, opts ...grpc.CallOption) (*types.VersionReply, error) {
	return s.server.Version(ctx, in)
}

func (s *TxPoolClient) FindUnknown(ctx context.Context, in *txpool_proto.TxHashes, opts ...grpc.CallOption) (*txpool_proto.TxHashes, error) {
	return s.server.FindUnknown(ctx, in)
}

func (s *TxPoolClient) Add(ctx context.Context, in *txpool_proto.AddRequest, opts ...grpc.CallOption) (*txpool_proto.AddReply, error) {
	return s.server.Add(ctx, in)
}

func (s *TxPoolClient) Transactions(ctx context.Context, in *txpool_proto.TransactionsRequest, opts ...grpc.CallOption) (*txpool_proto.TransactionsReply, error) {
	return s.server.Transactions(ctx, in)
}

func (s *TxPoolClient) All(ctx context.Context, in *txpool_proto.AllRequest, opts ...grpc.CallOption) (*txpool_proto.AllReply, error) {
	return s.server.All(ctx, in)
}

func (s *TxPoolClient) Pending(ctx context.Context, in *emptypb.Empty, opts ...grpc.CallOption) (*txpool_proto.PendingReply, error) {
	return s.server.Pending(ctx, in)
}

// -- start OnAdd

func (s *TxPoolClient) OnAdd(ctx context.Context, in *txpool_proto.OnAddRequest, opts ...grpc.CallOption) (txpool_proto.Txpool_OnAddClient, error) {
	messageCh := make(chan *txpool_proto.OnAddReply, 16384)
	streamServer := &TxPoolOnAddS{messageCh: messageCh, ctx: ctx}
	go func() {
		if err := s.server.OnAdd(in, streamServer); err != nil {
			log.Warn("[direct] stream returns", "err", err)
		}
		close(messageCh)
	}()
	return &TxPoolOnAddC{messageCh: messageCh, ctx: ctx}, nil
}

type TxPoolOnAddS struct {
	messageCh chan *txpool_proto.OnAddReply
	ctx       context.Context
	grpc.ServerStream
}

func (s *TxPoolOnAddS) Send(m *txpool_proto.OnAddReply) error {
	s.messageCh <- m
	return nil
}
func (s *TxPoolOnAddS) Context() context.Context {
	return s.ctx
}

type TxPoolOnAddC struct {
	messageCh chan *txpool_proto.OnAddReply
	ctx       context.Context
	grpc.ClientStream
}

func (c *TxPoolOnAddC) Recv() (*txpool_proto.OnAddReply, error) {
	m := <-c.messageCh
	return m, nil
}
func (c *TxPoolOnAddC) Context() context.Context {
	return c.ctx
}

// -- end OnAdd

func (s *TxPoolClient) Status(ctx context.Context, in *txpool_proto.StatusRequest, opts ...grpc.CallOption) (*txpool_proto.StatusReply, error) {
	return s.server.Status(ctx, in)
}

func (s *TxPoolClient) Nonce(ctx context.Context, in *txpool_proto.NonceRequest, opts ...grpc.CallOption) (*txpool_proto.NonceReply, error) {
	return s.server.Nonce(ctx, in)
}
