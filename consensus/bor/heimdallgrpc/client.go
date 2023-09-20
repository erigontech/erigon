package heimdallgrpc

import (
	"time"

	grpc_retry "github.com/grpc-ecosystem/go-grpc-middleware/retry"
	"github.com/ledgerwatch/log/v3"
	proto "github.com/maticnetwork/polyproto/heimdall"

	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials/insecure"
)

const (
	stateFetchLimit = 50
)

type HeimdallGRPCClient struct {
	conn   *grpc.ClientConn
	client proto.HeimdallClient
	logger log.Logger
}

func NewHeimdallGRPCClient(address string, logger log.Logger) *HeimdallGRPCClient {
	opts := []grpc_retry.CallOption{
		grpc_retry.WithMax(10000),
		grpc_retry.WithBackoff(grpc_retry.BackoffLinear(5 * time.Second)),
		grpc_retry.WithCodes(codes.Internal, codes.Unavailable, codes.Aborted, codes.NotFound),
	}

	conn, err := grpc.Dial(address,
		grpc.WithStreamInterceptor(grpc_retry.StreamClientInterceptor(opts...)),
		grpc.WithUnaryInterceptor(grpc_retry.UnaryClientInterceptor(opts...)),
		grpc.WithTransportCredentials(insecure.NewCredentials()),
	)
	if err != nil {
		logger.Crit("Failed to connect to Heimdall gRPC", "error", err)
	}

	logger.Info("Connected to Heimdall gRPC server", "address", address)

	return &HeimdallGRPCClient{
		conn:   conn,
		client: proto.NewHeimdallClient(conn),
		logger: logger,
	}
}

func (h *HeimdallGRPCClient) Close() {
	h.logger.Debug("Shutdown detected, Closing Heimdall gRPC client")
	h.conn.Close()
}
