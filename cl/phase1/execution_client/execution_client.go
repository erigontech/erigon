package execution_client

import (
	"context"
	"encoding/binary"
	"fmt"
	"math/big"
	"time"

	"github.com/c2h5oh/datasize"
	"github.com/holiman/uint256"
	libcommon "github.com/ledgerwatch/erigon-lib/common"
	"github.com/ledgerwatch/erigon-lib/gointerfaces"
	"github.com/ledgerwatch/erigon-lib/gointerfaces/execution"
	types2 "github.com/ledgerwatch/erigon-lib/gointerfaces/types"
	"github.com/ledgerwatch/log/v3"

	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/keepalive"

	"github.com/ledgerwatch/erigon/cl/cltypes"
	"github.com/ledgerwatch/erigon/cl/phase1/execution_client/rpc_helper"
	"github.com/ledgerwatch/erigon/core/types"
	"github.com/ledgerwatch/erigon/turbo/engineapi/engine_types"
)

const fcuTimeout = 12 * time.Second

// ExecutionClient interfaces with the Erigon-EL component consensus side.
type ExecutionClient struct {
	client execution.ExecutionClient
	ctx    context.Context
}

func HeaderRpcToHeader(header *execution.Header) (*types.Header, error) {
	var blockNonce types.BlockNonce
	binary.BigEndian.PutUint64(blockNonce[:], header.Nonce)
	h := &types.Header{
		ParentHash:  gointerfaces.ConvertH256ToHash(header.ParentHash),
		UncleHash:   gointerfaces.ConvertH256ToHash(header.OmmerHash),
		Coinbase:    gointerfaces.ConvertH160toAddress(header.Coinbase),
		Root:        gointerfaces.ConvertH256ToHash(header.StateRoot),
		TxHash:      gointerfaces.ConvertH256ToHash(header.TransactionHash),
		ReceiptHash: gointerfaces.ConvertH256ToHash(header.ReceiptRoot),
		Bloom:       gointerfaces.ConvertH2048ToBloom(header.LogsBloom),
		Difficulty:  gointerfaces.ConvertH256ToUint256Int(header.Difficulty).ToBig(),
		Number:      big.NewInt(int64(header.BlockNumber)),
		GasLimit:    header.GasLimit,
		GasUsed:     header.GasUsed,
		Time:        header.Timestamp,
		Extra:       header.ExtraData,
		MixDigest:   gointerfaces.ConvertH256ToHash(header.PrevRandao),
		Nonce:       blockNonce,
	}
	if header.BaseFeePerGas != nil {
		h.BaseFee = gointerfaces.ConvertH256ToUint256Int(header.BaseFeePerGas).ToBig()
	}
	if header.WithdrawalHash != nil {
		h.WithdrawalsHash = new(libcommon.Hash)
		*h.WithdrawalsHash = gointerfaces.ConvertH256ToHash(header.WithdrawalHash)
	}
	blockHash := gointerfaces.ConvertH256ToHash(header.BlockHash)
	if blockHash != h.Hash() {
		return nil, fmt.Errorf("block %d, %x has invalid hash. expected: %x", header.BlockNumber, h.Hash(), blockHash)
	}
	return h, nil
}

func HeaderToHeaderRPC(header *types.Header) *execution.Header {
	difficulty := new(uint256.Int)
	difficulty.SetFromBig(header.Difficulty)

	var baseFeeReply *types2.H256
	if header.BaseFee != nil {
		var baseFee uint256.Int
		baseFee.SetFromBig(header.BaseFee)
		baseFeeReply = gointerfaces.ConvertUint256IntToH256(&baseFee)
	}
	var withdrawalHashReply *types2.H256
	if header.WithdrawalsHash != nil {
		withdrawalHashReply = gointerfaces.ConvertHashToH256(*header.WithdrawalsHash)
	}
	return &execution.Header{
		ParentHash:      gointerfaces.ConvertHashToH256(header.ParentHash),
		Coinbase:        gointerfaces.ConvertAddressToH160(header.Coinbase),
		StateRoot:       gointerfaces.ConvertHashToH256(header.Root),
		TransactionHash: gointerfaces.ConvertHashToH256(header.TxHash),
		LogsBloom:       gointerfaces.ConvertBytesToH2048(header.Bloom[:]),
		ReceiptRoot:     gointerfaces.ConvertHashToH256(header.ReceiptHash),
		PrevRandao:      gointerfaces.ConvertHashToH256(header.MixDigest),
		BlockNumber:     header.Number.Uint64(),
		Nonce:           header.Nonce.Uint64(),
		GasLimit:        header.GasLimit,
		GasUsed:         header.GasUsed,
		Timestamp:       header.Time,
		ExtraData:       header.Extra,
		Difficulty:      gointerfaces.ConvertUint256IntToH256(difficulty),
		BlockHash:       gointerfaces.ConvertHashToH256(header.Hash()),
		OmmerHash:       gointerfaces.ConvertHashToH256(header.UncleHash),
		BaseFeePerGas:   baseFeeReply,
		WithdrawalHash:  withdrawalHashReply,
	}

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
		grpcHeaders = append(grpcHeaders, HeaderToHeaderRPC(header))
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
			Withdrawals:  engine_types.ConvertWithdrawalsToRpc(body.Withdrawals),
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
		rlpHeader, err := payload.RlpHeader()
		if err != nil {
			return err
		}
		headers = append(headers, rlpHeader)
		bodies = append(bodies, payload.Body())
		blockHashes = append(blockHashes, payload.BlockHash)
		blockNumbers = append(blockNumbers, payload.BlockNumber)
	}

	if err := ec.InsertHeaders(headers); err != nil {
		return err
	}
	return ec.InsertBodies(bodies, blockHashes, blockNumbers)
}

func (ec *ExecutionClient) ForkChoiceUpdate(headHash libcommon.Hash) (*execution.ForkChoiceReceipt, error) {
	log.Debug("[ExecutionClientRpc] Calling EL", "method", rpc_helper.ForkChoiceUpdatedV1)

	return ec.client.UpdateForkChoice(ec.ctx, &execution.ForkChoice{
		HeadBlockHash: gointerfaces.ConvertHashToH256(headHash),
		Timeout:       uint64(fcuTimeout.Milliseconds()),
	})
}

func (ec *ExecutionClient) IsCanonical(hash libcommon.Hash) (bool, error) {
	resp, err := ec.client.IsCanonicalHash(ec.ctx, gointerfaces.ConvertHashToH256(hash))
	if err != nil {
		return false, err
	}
	return resp.Canonical, nil
}

func (ec *ExecutionClient) ReadHeader(number uint64, blockHash libcommon.Hash) (*types.Header, error) {
	resp, err := ec.client.GetHeader(ec.ctx, &execution.GetSegmentRequest{
		BlockNumber: &number,
		BlockHash:   gointerfaces.ConvertHashToH256(blockHash),
	})
	if err != nil {
		return nil, err
	}

	return HeaderRpcToHeader(resp.Header)
}

func (ec *ExecutionClient) ReadExecutionPayload(number uint64, blockHash libcommon.Hash) (*cltypes.Eth1Block, error) {
	header, err := ec.ReadHeader(number, blockHash)
	if err != nil {
		return nil, err
	}
	body, err := ec.ReadBody(number, blockHash)
	if err != nil {
		return nil, err
	}
	return cltypes.NewEth1BlockFromHeaderAndBody(header, body), nil
}

func (ec *ExecutionClient) ReadBody(number uint64, blockHash libcommon.Hash) (*types.RawBody, error) {
	resp, err := ec.client.GetBody(ec.ctx, &execution.GetSegmentRequest{
		BlockNumber: &number,
		BlockHash:   gointerfaces.ConvertHashToH256(blockHash),
	})
	if err != nil {
		return nil, err
	}
	uncles := make([]*types.Header, 0, len(resp.Body.Uncles))
	for _, uncle := range resp.Body.Uncles {
		h, err := HeaderRpcToHeader(uncle)
		if err != nil {
			return nil, err
		}
		uncles = append(uncles, h)
	}
	return &types.RawBody{
		Transactions: resp.Body.Transactions,
		Uncles:       uncles,
		Withdrawals:  engine_types.ConvertWithdrawalsFromRpc(resp.Body.Withdrawals),
	}, nil
}
