package execution_client

import (
	"context"
	"fmt"

	"github.com/c2h5oh/datasize"
	"github.com/ledgerwatch/erigon-lib/gointerfaces"
	"github.com/ledgerwatch/erigon-lib/gointerfaces/execution"
	"github.com/ledgerwatch/erigon/cl/cltypes"
	"github.com/ledgerwatch/erigon/cmd/erigon-el/eth1"
	"github.com/ledgerwatch/erigon/common"
	"github.com/ledgerwatch/erigon/core/types"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/keepalive"
)

//var connAddr = "127.0.0.1:8989"

type ExecutionClient struct {
	client execution.ExecutionClient
	ctx    context.Context
}

func NewExecutionClient(ctx context.Context, addr string) (*ExecutionClient, error) {
	// creating grpc client connection
	var dialOpts []grpc.DialOption

	dialOpts = []grpc.DialOption{
		grpc.WithDefaultCallOptions(grpc.MaxCallRecvMsgSize(int(16 * datasize.MB))),
		grpc.WithKeepaliveParams(keepalive.ClientParameters{}),
	}

	dialOpts = append(dialOpts, grpc.WithTransportCredentials(insecure.NewCredentials()))
	conn, err := grpc.DialContext(ctx, addr, dialOpts...)
	if err != nil {
		return nil, fmt.Errorf("creating client connection to execution client: %w", err)
	}

	return &ExecutionClient{
		client: execution.NewExecutionClient(conn),
		ctx:    ctx,
	}, nil
}

// InsertHeaders will send block bodies to execution client
func (ec *ExecutionClient) InsertHeaders(headers []*types.Header) error {
	grpcHeaders := make([]*execution.Header, 0, len(headers))
	for _, header := range headers {
		grpcHeaders = append(grpcHeaders, eth1.HeaderToHeaderRPC(header))
	}
	_, err := ec.client.InsertHeaders(ec.ctx, &execution.InsertHeadersRequest{Headers: grpcHeaders})
	return err
}

// InsertBodies will send block bodies to execution client
func (ec *ExecutionClient) InsertBodies(bodies []*types.RawBody, blockHashes []common.Hash, blockNumbers []uint64) error {
	if len(bodies) != len(blockHashes) || len(bodies) != len(blockNumbers) {
		return fmt.Errorf("unbalanced inputs")
	}
	grpcBodies := make([]*execution.BlockBody, 0, len(bodies))
	for i, body := range bodies {
		grpcBodies = append(grpcBodies, &execution.BlockBody{
			BlockHash:    gointerfaces.ConvertHashToH256(blockHashes[i]),
			BlockNumber:  blockNumbers[i],
			Transactions: body.Transactions,
		})
	}
	_, err := ec.client.InsertBodies(ec.ctx, &execution.InsertBodiesRequest{Bodies: grpcBodies})
	return err
}

func (ec *ExecutionClient) InsertExecutionPayload(payload *cltypes.ExecutionPayload) error {
	if err := ec.InsertHeaders([]*types.Header{payload.Header()}); err != nil {
		return err
	}
	return ec.InsertBodies([]*types.RawBody{payload.BlockBody()}, []common.Hash{payload.BlockHash}, []uint64{payload.BlockNumber})
}

func (ec *ExecutionClient) ForkChoiceUpdate(headHash common.Hash) (*execution.ForkChoiceReceipt, error) {
	return ec.client.UpdateForkChoice(ec.ctx, gointerfaces.ConvertHashToH256(headHash))
}

func (ec *ExecutionClient) IsCanonical(hash common.Hash) (bool, error) {
	resp, err := ec.client.IsCanonicalHash(ec.ctx, gointerfaces.ConvertHashToH256(hash))
	if err != nil {
		return false, err
	}
	return resp.Canonical, nil
}
