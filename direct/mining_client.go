package direct

import (
	"context"

	txpool_proto "github.com/ledgerwatch/erigon-lib/gointerfaces/txpool"
	"github.com/ledgerwatch/erigon-lib/gointerfaces/types"
	"github.com/ledgerwatch/log/v3"
	"google.golang.org/grpc"
	"google.golang.org/protobuf/types/known/emptypb"
)

var _ txpool_proto.MiningClient = (*MiningClientDirect)(nil)

type MiningClientDirect struct {
	server txpool_proto.MiningServer
}

func NewMiningClientDirect(server txpool_proto.MiningServer) *MiningClientDirect {
	return &MiningClientDirect{server: server}
}

func (s *MiningClientDirect) Version(ctx context.Context, in *emptypb.Empty, opts ...grpc.CallOption) (*types.VersionReply, error) {
	return s.server.Version(ctx, in)
}

// -- start OnPendingBlock

func (s *MiningClientDirect) OnPendingBlock(ctx context.Context, in *txpool_proto.OnPendingBlockRequest, opts ...grpc.CallOption) (txpool_proto.Mining_OnPendingBlockClient, error) {
	messageCh := make(chan *txpool_proto.OnPendingBlockReply, 16384)
	streamServer := &MiningOnPendingBlockS{messageCh: messageCh, ctx: ctx}
	go func() {
		if err := s.server.OnPendingBlock(in, streamServer); err != nil {
			log.Warn("[direct] stream returns", "err", err)
		}
		close(messageCh)
	}()
	return &MiningOnPendingBlockC{messageCh: messageCh, ctx: ctx}, nil
}

type MiningOnPendingBlockS struct {
	messageCh chan *txpool_proto.OnPendingBlockReply
	ctx       context.Context
	grpc.ServerStream
}

func (s *MiningOnPendingBlockS) Send(m *txpool_proto.OnPendingBlockReply) error {
	s.messageCh <- m
	return nil
}
func (s *MiningOnPendingBlockS) Context() context.Context {
	return s.ctx
}

type MiningOnPendingBlockC struct {
	messageCh chan *txpool_proto.OnPendingBlockReply
	ctx       context.Context
	grpc.ClientStream
}

func (c *MiningOnPendingBlockC) Recv() (*txpool_proto.OnPendingBlockReply, error) {
	m := <-c.messageCh
	return m, nil
}
func (c *MiningOnPendingBlockC) Context() context.Context {
	return c.ctx
}

// -- end OnPendingBlock
// -- start OnMinedBlock

func (s *MiningClientDirect) OnMinedBlock(ctx context.Context, in *txpool_proto.OnMinedBlockRequest, opts ...grpc.CallOption) (txpool_proto.Mining_OnMinedBlockClient, error) {
	messageCh := make(chan *txpool_proto.OnMinedBlockReply, 16384)
	streamServer := &MiningOnMinedBlockS{messageCh: messageCh, ctx: ctx}
	go func() {
		if err := s.server.OnMinedBlock(in, streamServer); err != nil {
			log.Warn("[direct] stream returns", "err", err)
		}
		close(messageCh)
	}()
	return &MiningOnMinedBlockC{messageCh: messageCh, ctx: ctx}, nil
}

type MiningOnMinedBlockS struct {
	messageCh chan *txpool_proto.OnMinedBlockReply
	ctx       context.Context
	grpc.ServerStream
}

func (s *MiningOnMinedBlockS) Send(m *txpool_proto.OnMinedBlockReply) error {
	s.messageCh <- m
	return nil
}
func (s *MiningOnMinedBlockS) Context() context.Context {
	return s.ctx
}

type MiningOnMinedBlockC struct {
	messageCh chan *txpool_proto.OnMinedBlockReply
	ctx       context.Context
	grpc.ClientStream
}

func (c *MiningOnMinedBlockC) Recv() (*txpool_proto.OnMinedBlockReply, error) {
	m := <-c.messageCh
	return m, nil
}
func (c *MiningOnMinedBlockC) Context() context.Context {
	return c.ctx
}

// -- end OnMinedBlock
// -- end OnPendingLogs

func (s *MiningClientDirect) OnPendingLogs(ctx context.Context, in *txpool_proto.OnPendingLogsRequest, opts ...grpc.CallOption) (txpool_proto.Mining_OnPendingLogsClient, error) {
	messageCh := make(chan *txpool_proto.OnPendingLogsReply, 16384)
	streamServer := &MiningOnPendingLogsS{messageCh: messageCh, ctx: ctx}
	go func() {
		if err := s.server.OnPendingLogs(in, streamServer); err != nil {
			log.Warn("[direct] stream returns", "err", err)
		}
		close(messageCh)
	}()
	return &MiningOnPendingLogsC{messageCh: messageCh, ctx: ctx}, nil
}

type MiningOnPendingLogsS struct {
	messageCh chan *txpool_proto.OnPendingLogsReply
	ctx       context.Context
	grpc.ServerStream
}

func (s *MiningOnPendingLogsS) Send(m *txpool_proto.OnPendingLogsReply) error {
	s.messageCh <- m
	return nil
}
func (s *MiningOnPendingLogsS) Context() context.Context {
	return s.ctx
}

type MiningOnPendingLogsC struct {
	messageCh chan *txpool_proto.OnPendingLogsReply
	ctx       context.Context
	grpc.ClientStream
}

func (c *MiningOnPendingLogsC) Recv() (*txpool_proto.OnPendingLogsReply, error) {
	m := <-c.messageCh
	return m, nil
}
func (c *MiningOnPendingLogsC) Context() context.Context {
	return c.ctx
}

// -- end OnPendingLogs

func (s *MiningClientDirect) GetWork(ctx context.Context, in *txpool_proto.GetWorkRequest, opts ...grpc.CallOption) (*txpool_proto.GetWorkReply, error) {
	return s.server.GetWork(ctx, in)
}

func (s *MiningClientDirect) SubmitWork(ctx context.Context, in *txpool_proto.SubmitWorkRequest, opts ...grpc.CallOption) (*txpool_proto.SubmitWorkReply, error) {
	return s.server.SubmitWork(ctx, in)
}

func (s *MiningClientDirect) SubmitHashRate(ctx context.Context, in *txpool_proto.SubmitHashRateRequest, opts ...grpc.CallOption) (*txpool_proto.SubmitHashRateReply, error) {
	return s.server.SubmitHashRate(ctx, in)
}

func (s *MiningClientDirect) HashRate(ctx context.Context, in *txpool_proto.HashRateRequest, opts ...grpc.CallOption) (*txpool_proto.HashRateReply, error) {
	return s.server.HashRate(ctx, in)
}

func (s *MiningClientDirect) Mining(ctx context.Context, in *txpool_proto.MiningRequest, opts ...grpc.CallOption) (*txpool_proto.MiningReply, error) {
	return s.server.Mining(ctx, in)
}
