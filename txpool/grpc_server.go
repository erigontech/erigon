package txpool

import (
	"context"
	"fmt"
	"net"
	"sync"
	"time"

	grpc_middleware "github.com/grpc-ecosystem/go-grpc-middleware"
	grpc_recovery "github.com/grpc-ecosystem/go-grpc-middleware/recovery"
	"github.com/ledgerwatch/erigon-lib/gointerfaces"
	txpool_proto "github.com/ledgerwatch/erigon-lib/gointerfaces/txpool"
	types2 "github.com/ledgerwatch/erigon-lib/gointerfaces/types"
	"github.com/ledgerwatch/erigon-lib/kv"
	"github.com/ledgerwatch/log/v3"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials"
	"google.golang.org/grpc/keepalive"
	"google.golang.org/protobuf/types/known/emptypb"
)

// TxPoolAPIVersion
var TxPoolAPIVersion = &types2.VersionReply{Major: 1, Minor: 0, Patch: 0}

type txPool interface {
	GetRlp(tx kv.Tx, hash []byte) ([]byte, error)
	AddLocals(ctx context.Context, newTxs TxSlots) ([]DiscardReason, error)
	DeprecatedForEach(_ context.Context, f func(rlp, sender []byte, t SubPoolType), tx kv.Tx) error
	CountContent() (int, int, int)
	IdHashKnown(tx kv.Tx, hash []byte) (bool, error)
}

type GrpcServer struct {
	txpool_proto.UnimplementedTxpoolServer
	ctx             context.Context
	txPool          txPool
	db              kv.RoDB
	NewSlotsStreams *NewSlotsStreams
}

func NewGrpcServer(ctx context.Context, txPool txPool, db kv.RoDB) *GrpcServer {
	return &GrpcServer{ctx: ctx, txPool: txPool, db: db, NewSlotsStreams: &NewSlotsStreams{}}
}

func (s *GrpcServer) Version(context.Context, *emptypb.Empty) (*types2.VersionReply, error) {
	return TxPoolAPIVersion, nil
}
func convertSubPoolType(t SubPoolType) txpool_proto.AllReply_Type {
	switch t {
	case PendingSubPool:
		return txpool_proto.AllReply_PENDING
	case BaseFeeSubPool:
		return txpool_proto.AllReply_PENDING
	case QueuedSubPool:
		return txpool_proto.AllReply_QUEUED
	default:
		panic("unknown")
	}
}
func (s *GrpcServer) All(ctx context.Context, _ *txpool_proto.AllRequest) (*txpool_proto.AllReply, error) {
	tx, err := s.db.BeginRo(ctx)
	if err != nil {
		return nil, err
	}
	defer tx.Rollback()
	reply := &txpool_proto.AllReply{}
	reply.Txs = make([]*txpool_proto.AllReply_Tx, 0, 32)
	if err := s.txPool.DeprecatedForEach(ctx, func(rlp, sender []byte, t SubPoolType) {
		reply.Txs = append(reply.Txs, &txpool_proto.AllReply_Tx{
			Sender: sender,
			Type:   convertSubPoolType(t),
			RlpTx:  rlp,
		})
	}, tx); err != nil {
		return nil, err
	}
	return reply, nil
}

func (s *GrpcServer) FindUnknown(ctx context.Context, in *txpool_proto.TxHashes) (*txpool_proto.TxHashes, error) {
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

func (s *GrpcServer) Add(ctx context.Context, in *txpool_proto.AddRequest) (*txpool_proto.AddReply, error) {
	tx, err := s.db.BeginRo(context.Background())
	if err != nil {
		return nil, err
	}
	defer tx.Rollback()

	var slots TxSlots
	slots.Resize(uint(len(in.RlpTxs)))
	parseCtx := NewTxParseContext()
	parseCtx.Reject(func(hash []byte) bool {
		known, _ := s.txPool.IdHashKnown(tx, hash)
		return known
	})
	for i := range in.RlpTxs {
		slots.txs[i] = &TxSlot{}
		slots.isLocal[i] = true
		if _, err := parseCtx.ParseTransaction(in.RlpTxs[i], 0, slots.txs[i], slots.senders.At(i)); err != nil {
			log.Warn("stream.Recv", "err", err)
			continue
		}
	}

	reply := &txpool_proto.AddReply{Imported: make([]txpool_proto.ImportResult, len(in.RlpTxs)), Errors: make([]string, len(in.RlpTxs))}
	discardReasons, err := s.txPool.AddLocals(ctx, slots)
	if err != nil {
		return nil, err
	}
	//TODO: concept of discardReasons not really implemented yet
	_ = discardReasons
	/*
		for i, err := range discardReasons {
			if err == nil {
				continue
			}

			reply.Errors[i] = err.Error()

			// Track a few interesting failure types
			switch err {
			case Success: // Noop, but need to handle to not count these

			//case core.ErrAlreadyKnown:
			//	reply.Imported[i] = txpool_proto.ImportResult_ALREADY_EXISTS
			//case core.ErrUnderpriced, core.ErrReplaceUnderpriced:
			//	reply.Imported[i] = txpool_proto.ImportResult_FEE_TOO_LOW
			//case core.ErrInvalidSender, core.ErrGasLimit, core.ErrNegativeValue, core.ErrOversizedData:
			//	reply.Imported[i] = txpool_proto.ImportResult_INVALID
			default:
				reply.Imported[i] = txpool_proto.ImportResult_INTERNAL_ERROR
			}
		}
	*/
	return reply, nil
}

func (s *GrpcServer) OnAdd(req *txpool_proto.OnAddRequest, stream txpool_proto.Txpool_OnAddServer) error {
	//txpool.Loop does send messages to this streams
	remove := s.NewSlotsStreams.Add(stream)
	defer remove()
	select {
	case <-stream.Context().Done():
		return stream.Context().Err()
	case <-s.ctx.Done():
		return s.ctx.Err()
	}
}

func (s *GrpcServer) Transactions(ctx context.Context, in *txpool_proto.TransactionsRequest) (*txpool_proto.TransactionsReply, error) {
	tx, err := s.db.BeginRo(ctx)
	if err != nil {
		return nil, err
	}
	defer tx.Rollback()

	reply := &txpool_proto.TransactionsReply{RlpTxs: make([][]byte, len(in.Hashes))}
	for i := range in.Hashes {
		h := gointerfaces.ConvertH256ToHash(in.Hashes[i])
		txnRlp, err := s.txPool.GetRlp(tx, h[:])
		if err != nil {
			return nil, err
		}
		reply.RlpTxs[i] = txnRlp
	}

	return reply, nil
}

func (s *GrpcServer) Status(_ context.Context, _ *txpool_proto.StatusRequest) (*txpool_proto.StatusReply, error) {
	pending, baseFee, queued := s.txPool.CountContent()
	return &txpool_proto.StatusReply{
		PendingCount: uint32(pending),
		QueuedCount:  uint32(queued),
		BaseFeeCount: uint32(baseFee),
	}, nil
}

// NewSlotsStreams - it's safe to use this class as non-pointer
type NewSlotsStreams struct {
	chans map[uint]txpool_proto.Txpool_OnAddServer
	mu    sync.Mutex
	id    uint
}

func (s *NewSlotsStreams) Add(stream txpool_proto.Txpool_OnAddServer) (remove func()) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.chans == nil {
		s.chans = make(map[uint]txpool_proto.Txpool_OnAddServer)
	}
	s.id++
	id := s.id
	s.chans[id] = stream
	return func() { s.remove(id) }
}

func (s *NewSlotsStreams) Broadcast(reply *txpool_proto.OnAddReply) {
	s.mu.Lock()
	defer s.mu.Unlock()
	for id, stream := range s.chans {
		err := stream.Send(reply)
		if err != nil {
			log.Debug("failed send to mined block stream", "err", err)
			select {
			case <-stream.Context().Done():
				delete(s.chans, id)
			default:
			}
		}
	}
}

func (s *NewSlotsStreams) remove(id uint) {
	s.mu.Lock()
	defer s.mu.Unlock()
	_, ok := s.chans[id]
	if !ok { // double-unsubscribe support
		return
	}
	delete(s.chans, id)
}

func StartGrpc(txPoolServer txpool_proto.TxpoolServer, miningServer txpool_proto.MiningServer, addr string, creds *credentials.TransportCredentials) (*grpc.Server, error) {
	lis, err := net.Listen("tcp", addr)
	if err != nil {
		return nil, fmt.Errorf("could not create listener: %w, addr=%s", err, addr)
	}

	var (
		streamInterceptors []grpc.StreamServerInterceptor
		unaryInterceptors  []grpc.UnaryServerInterceptor
	)
	streamInterceptors = append(streamInterceptors, grpc_recovery.StreamServerInterceptor())
	unaryInterceptors = append(unaryInterceptors, grpc_recovery.UnaryServerInterceptor())

	//if metrics.Enabled {
	//	streamInterceptors = append(streamInterceptors, grpc_prometheus.StreamServerInterceptor)
	//	unaryInterceptors = append(unaryInterceptors, grpc_prometheus.UnaryServerInterceptor)
	//}

	var grpcServer *grpc.Server
	//cpus := uint32(runtime.GOMAXPROCS(-1))
	opts := []grpc.ServerOption{
		//grpc.NumStreamWorkers(cpus), // reduce amount of goroutines
		grpc.WriteBufferSize(1024), // reduce buffers to save mem
		grpc.ReadBufferSize(1024),
		grpc.MaxConcurrentStreams(kv.ReadersLimit - 128), // to force clients reduce concurrency level
		// Don't drop the connection, settings accordign to this comment on GitHub
		// https://github.com/grpc/grpc-go/issues/3171#issuecomment-552796779
		grpc.KeepaliveEnforcementPolicy(keepalive.EnforcementPolicy{
			MinTime:             10 * time.Second,
			PermitWithoutStream: true,
		}),
		grpc.StreamInterceptor(grpc_middleware.ChainStreamServer(streamInterceptors...)),
		grpc.UnaryInterceptor(grpc_middleware.ChainUnaryServer(unaryInterceptors...)),
	}
	if creds == nil {
		// no specific opts
	} else {
		opts = append(opts, grpc.Creds(*creds))
	}
	grpcServer = grpc.NewServer(opts...)
	if txPoolServer != nil {
		txpool_proto.RegisterTxpoolServer(grpcServer, txPoolServer)
	}
	if miningServer != nil {
		txpool_proto.RegisterMiningServer(grpcServer, miningServer)
	}

	//if metrics.Enabled {
	//	grpc_prometheus.Register(grpcServer)
	//}

	go func() {
		if err := grpcServer.Serve(lis); err != nil {
			log.Error("private RPC server fail", "err", err)
		}
	}()
	log.Info("Started gRPC server", "on", addr)
	return grpcServer, nil
}
