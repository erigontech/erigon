package heimdallgrpc

import (
	"context"
	"fmt"
	"net"
	"time"

	"github.com/ledgerwatch/erigon/consensus/bor"
	"github.com/ledgerwatch/log/v3"
	proto "github.com/maticnetwork/polyproto/heimdall"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/types/known/emptypb"
)

type HeimdallGRPCServer struct {
	proto.UnimplementedHeimdallServer
	heimdall bor.IHeimdallClient
	logger   log.Logger
}

func (h *HeimdallGRPCServer) Span(ctx context.Context, in *proto.SpanRequest) (*proto.SpanResponse, error) {
	_ /*result*/, err := h.heimdall.Span(ctx, in.ID)

	if err != nil {
		h.logger.Error("Error while fetching span")
		return nil, err
	}

	resp := &proto.SpanResponse{}
	//TODO
	//resp.Result = parseSpan(result.Result)
	//resp.Height = fmt.Sprint(result.Height)

	return resp, nil
}

func (h *HeimdallGRPCServer) FetchCheckpointCount(ctx context.Context, in *emptypb.Empty) (*proto.FetchCheckpointCountResponse, error) {
	count, err := h.heimdall.FetchCheckpointCount(ctx)

	if err != nil {
		h.logger.Error("Error while fetching checkpoint count")
		return nil, err
	}

	resp := &proto.FetchCheckpointCountResponse{}
	resp.Height = fmt.Sprint(count)

	return resp, nil
}

func (h *HeimdallGRPCServer) FetchCheckpoint(ctx context.Context, in *proto.FetchCheckpointRequest) (*proto.FetchCheckpointResponse, error) {

	_ /*checkpoint*/, err := h.heimdall.FetchCheckpoint(ctx, in.ID)

	if err != nil {
		h.logger.Error("Error while fetching checkpoint")
		return nil, err
	}

	/* TODO

	var hash [32]byte

	copy(hash[:], checkPoint.RootHash.Bytes())

	var address [20]byte

	copy(address[:], checkPoint.Proposer.Bytes())
	*/

	resp := &proto.FetchCheckpointResponse{}

	/* TODO
	resp.Height = fmt.Sprint(result.Height)
	resp.Result = &proto.Checkpoint{
		StartBlock: checkPoint.StartBlock,
		EndBlock:   checkPoint.EndBlock,
		RootHash:   protoutils.ConvertHashToH256(hash),
		Proposer:   protoutils.ConvertAddressToH160(address),
		Timestamp:  timestamppb.New(time.Unix(int64(checkPoint.TimeStamp), 0)),
		BorChainID: checkPoint.BorChainID,
	}
	*/
	return resp, nil
}

func (h *HeimdallGRPCServer) StateSyncEvents(req *proto.StateSyncEventsRequest, reply proto.Heimdall_StateSyncEventsServer) error {
	//TODO
	//fromId := req.FromID
	return nil
}

// StartHeimdallServer creates a heimdall GRPC server - which is implemented via the passed in client
// interface.  It is intended for use in testing where more than a single test validator is required rather
// than to replace the maticnetwork implementation
func StartHeimdallServer(shutDownCtx context.Context, heimdall bor.IHeimdallClient, addr string, logger log.Logger) error {
	grpcServer := grpc.NewServer(withLoggingUnaryInterceptor(logger))
	proto.RegisterHeimdallServer(grpcServer,
		&HeimdallGRPCServer{
			heimdall: heimdall,
			logger:   logger,
		})

	lis, err := net.Listen("tcp", addr)
	if err != nil {
		return err
	}

	go func() {
		if err := grpcServer.Serve(lis); err != nil {
			logger.Error("failed to serve grpc server", "err", err)
		}

		<-shutDownCtx.Done()
		grpcServer.Stop()
		lis.Close()
		logger.Info("GRPC Server stopped", "addr", addr)
	}()

	logger.Info("GRPC Server started", "addr", addr)

	return nil
}

func withLoggingUnaryInterceptor(logger log.Logger) grpc.ServerOption {
	return grpc.UnaryInterceptor(func(ctx context.Context, req interface{}, info *grpc.UnaryServerInfo, handler grpc.UnaryHandler) (interface{}, error) {
		start := time.Now()

		h, err := handler(ctx, req)
		if err != nil {
			err = status.Errorf(codes.Internal, err.Error())
		}

		logger.Info("Request", "method", info.FullMethod, "duration", time.Since(start), "error", err)

		return h, err
	})
}
