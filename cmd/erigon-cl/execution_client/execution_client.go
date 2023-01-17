package execution_client

import (
	"context"
	"fmt"
	"time"

	"github.com/c2h5oh/datasize"
	libcommon "github.com/ledgerwatch/erigon-lib/common"
	"github.com/ledgerwatch/erigon-lib/gointerfaces"
	"github.com/ledgerwatch/erigon-lib/gointerfaces/execution"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/keepalive"

	"github.com/ledgerwatch/erigon/cl/cltypes"
	"github.com/ledgerwatch/erigon/cmd/erigon-el/eth1"
	"github.com/ledgerwatch/erigon/core/types"
)

// ExecutionClient interfaces with the Erigon-EL component consensus side.
type ExecutionClient struct {
	client execution.ExecutionClient
	ctx    context.Context
}

// NewExecutionClient establishes a client-side connection with Erigon-EL
func NewExecutionClient(ctx context.Context, addr string) (*ExecutionClient, error) {
	// Set up dial options for the gRPC client connection
	var dialOpts []grpc.DialOption
	dialOpts = []grpc.DialOption{
		grpc.WithDefaultCallOptions(grpc.MaxCallRecvMsgSize(int(16 * datasize.MB))),
		grpc.WithKeepaliveParams(keepalive.ClientParameters{
			Time:                5 * time.Minute,
			Timeout:             10 * time.Minute,
			PermitWithoutStream: true,
		}),
	}

	// Add transport credentials to the dial options
	dialOpts = append(dialOpts, grpc.WithTransportCredentials(insecure.NewCredentials()))

	// Create the gRPC client connection
	conn, err := grpc.DialContext(ctx, addr, dialOpts...)
	if err != nil {
		// Return an error if the connection fails
		return nil, fmt.Errorf("creating client connection to execution client: %w", err)
	}

	// Return a new ExecutionClient struct with the gRPC client and context set as fields
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
func (ec *ExecutionClient) InsertBodies(bodies []*types.RawBody, blockHashes []libcommon.Hash, blockNumbers []uint64) error {
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

// InsertExecutionPayloads insert a segment of execution payloads
func (ec *ExecutionClient) InsertExecutionPayloads(payloads []*cltypes.Eth1Block) error {
	headers := make([]*types.Header, 0, len(payloads))
	bodies := make([]*types.RawBody, 0, len(payloads))
	blockHashes := make([]libcommon.Hash, 0, len(payloads))
	blockNumbers := make([]uint64, 0, len(payloads))

	for _, payload := range payloads {
		headers = append(headers, payload.Header)
		bodies = append(bodies, payload.Body)
		blockHashes = append(blockHashes, payload.Header.BlockHashCL)
		blockNumbers = append(blockNumbers, payload.NumberU64())
	}

	if err := ec.InsertHeaders(headers); err != nil {
		return err
	}
	return ec.InsertBodies(bodies, blockHashes, blockNumbers)
}

func (ec *ExecutionClient) ForkChoiceUpdate(headHash libcommon.Hash) (*execution.ForkChoiceReceipt, error) {
	return ec.client.UpdateForkChoice(ec.ctx, gointerfaces.ConvertHashToH256(headHash))
}

func (ec *ExecutionClient) IsCanonical(hash libcommon.Hash) (bool, error) {
	resp, err := ec.client.IsCanonicalHash(ec.ctx, gointerfaces.ConvertHashToH256(hash))
	if err != nil {
		return false, err
	}
	return resp.Canonical, nil
}
