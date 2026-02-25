// Copyright 2025 The Erigon Authors
// This file is part of Erigon.
//
// Erigon is free software: you can redistribute it and/or modify
// it under the terms of the GNU Lesser General Public License as published by
// the Free Software Foundation, either version 3 of the License, or
// (at your option) any later version.
//
// Erigon is distributed in the hope that it will be useful,
// but WITHOUT ANY WARRANTY; without even the implied warranty of
// MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE. See the
// GNU Lesser General Public License for more details.
//
// You should have received a copy of the GNU Lesser General Public License
// along with Erigon. If not, see <http://www.gnu.org/licenses/>.

package engineapi

import (
	"context"
	"math/big"
	"testing"

	"github.com/holiman/uint256"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"google.golang.org/grpc"
	"google.golang.org/protobuf/types/known/emptypb"

	"github.com/erigontech/erigon/common"
	"github.com/erigontech/erigon/common/hexutil"
	"github.com/erigontech/erigon/common/log/v3"
	"github.com/erigontech/erigon/execution/chain"
	"github.com/erigontech/erigon/execution/engineapi/engine_types"
	"github.com/erigontech/erigon/execution/types"
	"github.com/erigontech/erigon/node/gointerfaces"
	"github.com/erigontech/erigon/node/gointerfaces/executionproto"
	"github.com/erigontech/erigon/node/gointerfaces/typesproto"
	"github.com/erigontech/erigon/rpc"
)

// ---------------------------------------------------------------------------
// Minimal stub implementing executionproto.ExecutionClient for unit tests.
// Only the methods called by BuildBlockV1 (GetHeader, AssembleBlock,
// GetAssembledBlock) are implemented; everything else panics.
// ---------------------------------------------------------------------------

type stubExecutionClient struct {
	executionproto.ExecutionClient // embed to satisfy the interface; unused methods will panic

	getHeaderFunc         func(ctx context.Context, in *executionproto.GetSegmentRequest, opts ...grpc.CallOption) (*executionproto.GetHeaderResponse, error)
	assembleBlockFunc     func(ctx context.Context, in *executionproto.AssembleBlockRequest, opts ...grpc.CallOption) (*executionproto.AssembleBlockResponse, error)
	getAssembledBlockFunc func(ctx context.Context, in *executionproto.GetAssembledBlockRequest, opts ...grpc.CallOption) (*executionproto.GetAssembledBlockResponse, error)
}

func (s *stubExecutionClient) GetHeader(ctx context.Context, in *executionproto.GetSegmentRequest, opts ...grpc.CallOption) (*executionproto.GetHeaderResponse, error) {
	if s.getHeaderFunc != nil {
		return s.getHeaderFunc(ctx, in, opts...)
	}
	return &executionproto.GetHeaderResponse{}, nil
}

func (s *stubExecutionClient) AssembleBlock(ctx context.Context, in *executionproto.AssembleBlockRequest, opts ...grpc.CallOption) (*executionproto.AssembleBlockResponse, error) {
	if s.assembleBlockFunc != nil {
		return s.assembleBlockFunc(ctx, in, opts...)
	}
	return &executionproto.AssembleBlockResponse{}, nil
}

func (s *stubExecutionClient) GetAssembledBlock(ctx context.Context, in *executionproto.GetAssembledBlockRequest, opts ...grpc.CallOption) (*executionproto.GetAssembledBlockResponse, error) {
	if s.getAssembledBlockFunc != nil {
		return s.getAssembledBlockFunc(ctx, in, opts...)
	}
	return &executionproto.GetAssembledBlockResponse{}, nil
}

// Methods that may be called during EngineServer construction but are not
// exercised in the test paths.

func (s *stubExecutionClient) CurrentHeader(ctx context.Context, _ *emptypb.Empty, _ ...grpc.CallOption) (*executionproto.GetHeaderResponse, error) {
	return &executionproto.GetHeaderResponse{}, nil
}

func (s *stubExecutionClient) GetTD(ctx context.Context, _ *executionproto.GetSegmentRequest, _ ...grpc.CallOption) (*executionproto.GetTDResponse, error) {
	return &executionproto.GetTDResponse{}, nil
}

// ---------------------------------------------------------------------------
// Test helpers
// ---------------------------------------------------------------------------

// allForksChainConfig returns a chain config where all forks (Shanghai, Cancun,
// Prague, Osaka, Amsterdam) are activated at timestamp 0.
func allForksChainConfig() *chain.Config {
	return &chain.Config{
		ChainID:                       big.NewInt(1337),
		HomesteadBlock:                big.NewInt(0),
		TangerineWhistleBlock:         big.NewInt(0),
		SpuriousDragonBlock:           big.NewInt(0),
		ByzantiumBlock:                big.NewInt(0),
		ConstantinopleBlock:           big.NewInt(0),
		PetersburgBlock:               big.NewInt(0),
		IstanbulBlock:                 big.NewInt(0),
		MuirGlacierBlock:              big.NewInt(0),
		BerlinBlock:                   big.NewInt(0),
		LondonBlock:                   big.NewInt(0),
		ArrowGlacierBlock:             big.NewInt(0),
		GrayGlacierBlock:              big.NewInt(0),
		TerminalTotalDifficulty:       big.NewInt(0),
		TerminalTotalDifficultyPassed: true,
		ShanghaiTime:                  big.NewInt(0),
		CancunTime:                    big.NewInt(0),
		PragueTime:                    big.NewInt(0),
		OsakaTime:                     big.NewInt(0),
		AmsterdamTime:                 big.NewInt(0),
		Ethash:                        new(chain.EthashConfig),
	}
}

// preShanghaiChainConfig returns a chain config where Shanghai and later forks
// are NOT activated (fork timestamps set far into the future via nil).
func preShanghaiChainConfig() *chain.Config {
	cfg := allForksChainConfig()
	cfg.ShanghaiTime = nil
	cfg.CancunTime = nil
	cfg.PragueTime = nil
	cfg.OsakaTime = nil
	cfg.AmsterdamTime = nil
	return cfg
}

// preCancunChainConfig returns a chain config where Shanghai is active but
// Cancun and later forks are NOT activated.
func preCancunChainConfig() *chain.Config {
	cfg := allForksChainConfig()
	cfg.CancunTime = nil
	cfg.PragueTime = nil
	cfg.OsakaTime = nil
	cfg.AmsterdamTime = nil
	return cfg
}

// makeParentHeader builds a minimal types.Header with the given timestamp and
// returns both the Go header (for hash computation) and its RPC representation.
// The RPC header's BlockHash is set to the real computed hash so that
// HeaderRpcToHeader validation passes.
func makeParentHeader(timestamp uint64) (*types.Header, *executionproto.Header) {
	h := &types.Header{
		ParentHash:  common.Hash{},
		UncleHash:   common.Hash{},
		Coinbase:    common.Address{},
		Root:        common.Hash{},
		TxHash:      common.Hash{},
		ReceiptHash: common.Hash{},
		Bloom:       types.Bloom{},
		GasLimit:    30_000_000,
		GasUsed:     0,
		Time:        timestamp,
		Extra:       nil,
		MixDigest:   common.Hash{},
	}
	h.Number.SetUint64(100)
	h.Difficulty.SetUint64(0)
	h.BaseFee = uint256.NewInt(1_000_000_000)

	realHash := h.Hash()

	zeroH256 := gointerfaces.ConvertHashToH256(common.Hash{})
	rpcHeader := &executionproto.Header{
		ParentHash:      zeroH256,
		Coinbase:        gointerfaces.ConvertAddressToH160(common.Address{}),
		StateRoot:       zeroH256,
		ReceiptRoot:     zeroH256,
		LogsBloom:       gointerfaces.ConvertBytesToH2048(make([]byte, 256)),
		PrevRandao:      zeroH256,
		BlockNumber:     100,
		GasLimit:        30_000_000,
		GasUsed:         0,
		Timestamp:       timestamp,
		ExtraData:       nil,
		Difficulty:      gointerfaces.ConvertUint256IntToH256(uint256.NewInt(0)),
		BlockHash:       gointerfaces.ConvertHashToH256(realHash),
		OmmerHash:       zeroH256,
		TransactionHash: zeroH256,
		BaseFeePerGas:   gointerfaces.ConvertUint256IntToH256(uint256.NewInt(1_000_000_000)),
	}
	return h, rpcHeader
}

// newTestingAPI creates a testingImpl backed by stub execution client.
func newTestingAPI(cfg *chain.Config, stub *stubExecutionClient, enabled bool) TestingAPI {
	srv := NewEngineServer(
		log.New(),
		cfg,
		stub,
		nil,   // blockDownloader
		false, // caplin
		false, // proposing
		true,  // consuming
		nil,   // txPool
		0,     // fcuTimeout
		0,     // maxReorgDepth
	)
	return NewTestingImpl(srv, enabled)
}

// validPayloadAttrs returns payload attributes valid for a post-Cancun chain
// with timestamp greater than parentTimestamp.
func validPayloadAttrs(parentTimestamp uint64) *engine_types.PayloadAttributes {
	beaconRoot := common.Hash{0xbe, 0xac}
	return &engine_types.PayloadAttributes{
		Timestamp:             hexutil.Uint64(parentTimestamp + 1),
		PrevRandao:            common.Hash{0xaa},
		SuggestedFeeRecipient: common.HexToAddress("0x1111111111111111111111111111111111111111"),
		Withdrawals:           make([]*types.Withdrawal, 0),
		ParentBeaconBlockRoot: &beaconRoot,
	}
}

// getHeaderReturning creates a getHeaderFunc that returns the given RPC header
// when the request hash matches expectedHash. Otherwise returns nil (unknown).
func getHeaderReturning(expectedHash common.Hash, rpcHeader *executionproto.Header) func(ctx context.Context, in *executionproto.GetSegmentRequest, opts ...grpc.CallOption) (*executionproto.GetHeaderResponse, error) {
	return func(_ context.Context, in *executionproto.GetSegmentRequest, _ ...grpc.CallOption) (*executionproto.GetHeaderResponse, error) {
		reqHash := gointerfaces.ConvertH256ToHash(in.BlockHash)
		if reqHash != expectedHash {
			return &executionproto.GetHeaderResponse{Header: nil}, nil
		}
		return &executionproto.GetHeaderResponse{
			Header: rpcHeader,
		}, nil
	}
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

func TestBuildBlockV1(t *testing.T) {
	t.Parallel()

	const parentTimestamp = 1000

	// Build a real parent header so that its computed hash matches BlockHash
	// (required by HeaderRpcToHeader validation in chainRW.GetHeaderByHash).
	parentHdr, parentRPC := makeParentHeader(parentTimestamp)
	parentHash := parentHdr.Hash()

	t.Run("disabled gate", func(t *testing.T) {
		t.Parallel()
		stub := &stubExecutionClient{}
		api := newTestingAPI(allForksChainConfig(), stub, false)
		resp, err := api.BuildBlockV1(context.Background(), parentHash, validPayloadAttrs(parentTimestamp), nil, nil)
		require.Nil(t, resp)
		require.Error(t, err)
		var rpcErr *rpc.InvalidParamsError
		require.ErrorAs(t, err, &rpcErr)
		assert.Contains(t, rpcErr.Message, "testing namespace is disabled")
	})

	t.Run("nil payloadAttributes", func(t *testing.T) {
		t.Parallel()
		stub := &stubExecutionClient{}
		api := newTestingAPI(allForksChainConfig(), stub, true)
		resp, err := api.BuildBlockV1(context.Background(), parentHash, nil, nil, nil)
		require.Nil(t, resp)
		require.Error(t, err)
		var rpcErr *rpc.InvalidParamsError
		require.ErrorAs(t, err, &rpcErr)
		assert.Contains(t, rpcErr.Message, "payloadAttributes must not be null")
	})

	t.Run("explicit non-empty transaction list", func(t *testing.T) {
		t.Parallel()
		stub := &stubExecutionClient{}
		api := newTestingAPI(allForksChainConfig(), stub, true)
		txs := []hexutil.Bytes{{0x01, 0x02}}
		resp, err := api.BuildBlockV1(context.Background(), parentHash, validPayloadAttrs(parentTimestamp), &txs, nil)
		require.Nil(t, resp)
		require.Error(t, err)
		var rpcErr *rpc.InvalidParamsError
		require.ErrorAs(t, err, &rpcErr)
		assert.Contains(t, rpcErr.Message, "explicit transaction list not yet supported")
	})

	t.Run("empty transaction list is allowed", func(t *testing.T) {
		t.Parallel()
		// An empty slice (not nil pointer) means "build an empty block".
		// This should pass the tx-list check and proceed to the parent-hash lookup.
		stub := &stubExecutionClient{
			getHeaderFunc: func(_ context.Context, _ *executionproto.GetSegmentRequest, _ ...grpc.CallOption) (*executionproto.GetHeaderResponse, error) {
				// Return nil header to trigger unknown parent error (separate from tx check).
				return &executionproto.GetHeaderResponse{Header: nil}, nil
			},
		}
		api := newTestingAPI(allForksChainConfig(), stub, true)
		txs := []hexutil.Bytes{} // empty slice, not nil
		resp, err := api.BuildBlockV1(context.Background(), parentHash, validPayloadAttrs(parentTimestamp), &txs, nil)
		require.Nil(t, resp)
		require.Error(t, err)
		// Should have passed the tx check and hit the unknown parent check.
		var rpcErr *rpc.InvalidParamsError
		require.ErrorAs(t, err, &rpcErr)
		assert.Contains(t, rpcErr.Message, "unknown parent hash")
	})

	t.Run("unknown parent hash", func(t *testing.T) {
		t.Parallel()
		stub := &stubExecutionClient{
			getHeaderFunc: func(_ context.Context, _ *executionproto.GetSegmentRequest, _ ...grpc.CallOption) (*executionproto.GetHeaderResponse, error) {
				return &executionproto.GetHeaderResponse{Header: nil}, nil
			},
		}
		api := newTestingAPI(allForksChainConfig(), stub, true)
		resp, err := api.BuildBlockV1(context.Background(), parentHash, validPayloadAttrs(parentTimestamp), nil, nil)
		require.Nil(t, resp)
		require.Error(t, err)
		var rpcErr *rpc.InvalidParamsError
		require.ErrorAs(t, err, &rpcErr)
		assert.Contains(t, rpcErr.Message, "unknown parent hash")
	})

	t.Run("timestamp equal to parent", func(t *testing.T) {
		t.Parallel()
		stub := &stubExecutionClient{
			getHeaderFunc: getHeaderReturning(parentHash, parentRPC),
		}
		api := newTestingAPI(allForksChainConfig(), stub, true)
		attrs := validPayloadAttrs(parentTimestamp)
		attrs.Timestamp = hexutil.Uint64(parentTimestamp) // equal, not greater
		resp, err := api.BuildBlockV1(context.Background(), parentHash, attrs, nil, nil)
		require.Nil(t, resp)
		require.Error(t, err)
		var rpcErr *rpc.InvalidParamsError
		require.ErrorAs(t, err, &rpcErr)
		assert.Contains(t, rpcErr.Message, "payload timestamp must be greater than parent")
	})

	t.Run("timestamp less than parent", func(t *testing.T) {
		t.Parallel()
		stub := &stubExecutionClient{
			getHeaderFunc: getHeaderReturning(parentHash, parentRPC),
		}
		api := newTestingAPI(allForksChainConfig(), stub, true)
		attrs := validPayloadAttrs(parentTimestamp)
		attrs.Timestamp = hexutil.Uint64(parentTimestamp - 1) // less than parent
		resp, err := api.BuildBlockV1(context.Background(), parentHash, attrs, nil, nil)
		require.Nil(t, resp)
		require.Error(t, err)
		var rpcErr *rpc.InvalidParamsError
		require.ErrorAs(t, err, &rpcErr)
		assert.Contains(t, rpcErr.Message, "payload timestamp must be greater than parent")
	})

	t.Run("missing parentBeaconBlockRoot for Cancun+", func(t *testing.T) {
		t.Parallel()
		stub := &stubExecutionClient{
			getHeaderFunc: getHeaderReturning(parentHash, parentRPC),
		}
		api := newTestingAPI(allForksChainConfig(), stub, true)
		attrs := validPayloadAttrs(parentTimestamp)
		attrs.ParentBeaconBlockRoot = nil // required for Cancun+
		resp, err := api.BuildBlockV1(context.Background(), parentHash, attrs, nil, nil)
		require.Nil(t, resp)
		require.Error(t, err)
		var rpcErr *rpc.InvalidParamsError
		require.ErrorAs(t, err, &rpcErr)
		assert.Contains(t, rpcErr.Message, "parentBeaconBlockRoot required for Cancun")
	})

	t.Run("unexpected parentBeaconBlockRoot pre-Cancun", func(t *testing.T) {
		t.Parallel()
		stub := &stubExecutionClient{
			getHeaderFunc: getHeaderReturning(parentHash, parentRPC),
		}
		// Use a pre-Cancun config (Shanghai only).
		api := newTestingAPI(preCancunChainConfig(), stub, true)
		attrs := validPayloadAttrs(parentTimestamp)
		// attrs already has ParentBeaconBlockRoot set, which is invalid pre-Cancun.
		resp, err := api.BuildBlockV1(context.Background(), parentHash, attrs, nil, nil)
		require.Nil(t, resp)
		require.Error(t, err)
		var rpcErr *rpc.InvalidParamsError
		require.ErrorAs(t, err, &rpcErr)
		assert.Contains(t, rpcErr.Message, "parentBeaconBlockRoot not supported before Cancun")
	})

	// These two tests exercise the waitForResponse busy-polling loop which
	// spins for SecondsPerSlot (12 s on mainnet) before timing out. They are
	// skipped under -short to keep `make test-short` fast; run them in full
	// mode to validate timeout behaviour.

	t.Run("execution service busy on AssembleBlock", func(t *testing.T) {
		t.Parallel()
		if testing.Short() {
			t.Skip("skipping busy-poll timeout test in -short mode")
		}
		stub := &stubExecutionClient{
			getHeaderFunc: getHeaderReturning(parentHash, parentRPC),
			assembleBlockFunc: func(_ context.Context, _ *executionproto.AssembleBlockRequest, _ ...grpc.CallOption) (*executionproto.AssembleBlockResponse, error) {
				return &executionproto.AssembleBlockResponse{
					Id:   0,
					Busy: true,
				}, nil
			},
		}
		api := newTestingAPI(allForksChainConfig(), stub, true)
		resp, err := api.BuildBlockV1(context.Background(), parentHash, validPayloadAttrs(parentTimestamp), nil, nil)
		require.Nil(t, resp)
		require.Error(t, err)
		assert.Contains(t, err.Error(), "execution service is busy")
	})

	t.Run("execution service busy on GetAssembledBlock", func(t *testing.T) {
		t.Parallel()
		if testing.Short() {
			t.Skip("skipping busy-poll timeout test in -short mode")
		}
		stub := &stubExecutionClient{
			getHeaderFunc: getHeaderReturning(parentHash, parentRPC),
			assembleBlockFunc: func(_ context.Context, _ *executionproto.AssembleBlockRequest, _ ...grpc.CallOption) (*executionproto.AssembleBlockResponse, error) {
				return &executionproto.AssembleBlockResponse{
					Id:   42,
					Busy: false,
				}, nil
			},
			getAssembledBlockFunc: func(_ context.Context, _ *executionproto.GetAssembledBlockRequest, _ ...grpc.CallOption) (*executionproto.GetAssembledBlockResponse, error) {
				return &executionproto.GetAssembledBlockResponse{
					Busy: true,
				}, nil
			},
		}
		api := newTestingAPI(allForksChainConfig(), stub, true)
		resp, err := api.BuildBlockV1(context.Background(), parentHash, validPayloadAttrs(parentTimestamp), nil, nil)
		require.Nil(t, resp)
		require.Error(t, err)
		assert.Contains(t, err.Error(), "execution service is busy retrieving assembled block")
	})

	t.Run("nil assembled block data", func(t *testing.T) {
		t.Parallel()
		stub := &stubExecutionClient{
			getHeaderFunc: getHeaderReturning(parentHash, parentRPC),
			assembleBlockFunc: func(_ context.Context, _ *executionproto.AssembleBlockRequest, _ ...grpc.CallOption) (*executionproto.AssembleBlockResponse, error) {
				return &executionproto.AssembleBlockResponse{Id: 42, Busy: false}, nil
			},
			getAssembledBlockFunc: func(_ context.Context, _ *executionproto.GetAssembledBlockRequest, _ ...grpc.CallOption) (*executionproto.GetAssembledBlockResponse, error) {
				return &executionproto.GetAssembledBlockResponse{
					Data: nil,
					Busy: false,
				}, nil
			},
		}
		api := newTestingAPI(allForksChainConfig(), stub, true)
		resp, err := api.BuildBlockV1(context.Background(), parentHash, validPayloadAttrs(parentTimestamp), nil, nil)
		require.Nil(t, resp)
		require.Error(t, err)
		assert.Contains(t, err.Error(), "no assembled block data available")
	})

	t.Run("happy path with mempool (nil transactions)", func(t *testing.T) {
		t.Parallel()
		const payloadID uint64 = 99
		blockHash := common.HexToHash("0xbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb")
		stateRoot := common.HexToHash("0xcccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccc")
		blockValue := uint256.NewInt(12345)

		stub := &stubExecutionClient{
			getHeaderFunc: getHeaderReturning(parentHash, parentRPC),
			assembleBlockFunc: func(_ context.Context, req *executionproto.AssembleBlockRequest, _ ...grpc.CallOption) (*executionproto.AssembleBlockResponse, error) {
				// Verify the request was built correctly.
				assert.Equal(t, parentHash, common.Hash(gointerfaces.ConvertH256ToHash(req.ParentHash)))
				assert.Equal(t, uint64(parentTimestamp+1), req.Timestamp)
				return &executionproto.AssembleBlockResponse{
					Id:   payloadID,
					Busy: false,
				}, nil
			},
			getAssembledBlockFunc: func(_ context.Context, req *executionproto.GetAssembledBlockRequest, _ ...grpc.CallOption) (*executionproto.GetAssembledBlockResponse, error) {
				assert.Equal(t, payloadID, req.Id)
				return &executionproto.GetAssembledBlockResponse{
					Busy: false,
					Data: &executionproto.AssembledBlockData{
						ExecutionPayload: &typesproto.ExecutionPayload{
							Version:       3, // Cancun+
							ParentHash:    gointerfaces.ConvertHashToH256(parentHash),
							Coinbase:      gointerfaces.ConvertAddressToH160(common.Address{}),
							StateRoot:     gointerfaces.ConvertHashToH256(stateRoot),
							ReceiptRoot:   gointerfaces.ConvertHashToH256(common.Hash{}),
							LogsBloom:     gointerfaces.ConvertBytesToH2048(make([]byte, 256)),
							PrevRandao:    gointerfaces.ConvertHashToH256(common.Hash{0xaa}),
							BlockNumber:   101,
							GasLimit:      30_000_000,
							GasUsed:       21_000,
							Timestamp:     parentTimestamp + 1,
							ExtraData:     []byte("test"),
							BaseFeePerGas: gointerfaces.ConvertUint256IntToH256(uint256.NewInt(1_000_000_000)),
							BlockHash:     gointerfaces.ConvertHashToH256(blockHash),
							Transactions:  [][]byte{},
							BlobGasUsed:   ptrUint64(0),
							ExcessBlobGas: ptrUint64(0),
						},
						BlockValue:  gointerfaces.ConvertUint256IntToH256(blockValue),
						BlobsBundle: nil,
					},
				}, nil
			},
		}
		api := newTestingAPI(allForksChainConfig(), stub, true)
		resp, err := api.BuildBlockV1(context.Background(), parentHash, validPayloadAttrs(parentTimestamp), nil, nil)
		require.NoError(t, err)
		require.NotNil(t, resp)

		// Verify response fields.
		assert.Equal(t, blockHash, resp.ExecutionPayload.BlockHash)
		assert.Equal(t, parentHash, resp.ExecutionPayload.ParentHash)
		assert.Equal(t, hexutil.Uint64(101), resp.ExecutionPayload.BlockNumber)
		assert.Equal(t, hexutil.Uint64(parentTimestamp+1), resp.ExecutionPayload.Timestamp)
		assert.Equal(t, hexutil.Bytes("test"), resp.ExecutionPayload.ExtraData)
		assert.Equal(t, false, resp.ShouldOverrideBuilder)

		// Block value should match.
		require.NotNil(t, resp.BlockValue)
		assert.Equal(t, blockValue.ToBig().String(), resp.BlockValue.ToInt().String())
	})

	t.Run("happy path with extraData override", func(t *testing.T) {
		t.Parallel()
		stub := &stubExecutionClient{
			getHeaderFunc: getHeaderReturning(parentHash, parentRPC),
			assembleBlockFunc: func(_ context.Context, _ *executionproto.AssembleBlockRequest, _ ...grpc.CallOption) (*executionproto.AssembleBlockResponse, error) {
				return &executionproto.AssembleBlockResponse{Id: 1, Busy: false}, nil
			},
			getAssembledBlockFunc: func(_ context.Context, _ *executionproto.GetAssembledBlockRequest, _ ...grpc.CallOption) (*executionproto.GetAssembledBlockResponse, error) {
				return &executionproto.GetAssembledBlockResponse{
					Busy: false,
					Data: &executionproto.AssembledBlockData{
						ExecutionPayload: &typesproto.ExecutionPayload{
							Version:       3,
							ParentHash:    gointerfaces.ConvertHashToH256(parentHash),
							Coinbase:      gointerfaces.ConvertAddressToH160(common.Address{}),
							StateRoot:     gointerfaces.ConvertHashToH256(common.Hash{}),
							ReceiptRoot:   gointerfaces.ConvertHashToH256(common.Hash{}),
							LogsBloom:     gointerfaces.ConvertBytesToH2048(make([]byte, 256)),
							PrevRandao:    gointerfaces.ConvertHashToH256(common.Hash{}),
							BlockNumber:   101,
							GasLimit:      30_000_000,
							Timestamp:     parentTimestamp + 1,
							ExtraData:     []byte("original"),
							BaseFeePerGas: gointerfaces.ConvertUint256IntToH256(uint256.NewInt(1)),
							BlockHash:     gointerfaces.ConvertHashToH256(common.Hash{0x01}),
							BlobGasUsed:   ptrUint64(0),
							ExcessBlobGas: ptrUint64(0),
						},
						BlockValue: gointerfaces.ConvertUint256IntToH256(uint256.NewInt(0)),
					},
				}, nil
			},
		}
		api := newTestingAPI(allForksChainConfig(), stub, true)
		overrideData := hexutil.Bytes("overridden-extra")
		resp, err := api.BuildBlockV1(context.Background(), parentHash, validPayloadAttrs(parentTimestamp), nil, &overrideData)
		require.NoError(t, err)
		require.NotNil(t, resp)
		assert.Equal(t, hexutil.Bytes("overridden-extra"), resp.ExecutionPayload.ExtraData)
	})

	t.Run("missing withdrawals for Shanghai", func(t *testing.T) {
		t.Parallel()
		// Shanghai requires withdrawals to be non-nil.
		stub := &stubExecutionClient{
			getHeaderFunc: getHeaderReturning(parentHash, parentRPC),
		}
		// Use pre-Cancun (Shanghai-only) config.
		api := newTestingAPI(preCancunChainConfig(), stub, true)
		attrs := &engine_types.PayloadAttributes{
			Timestamp:             hexutil.Uint64(parentTimestamp + 1),
			PrevRandao:            common.Hash{0xaa},
			SuggestedFeeRecipient: common.HexToAddress("0x1111111111111111111111111111111111111111"),
			Withdrawals:           nil, // missing for Shanghai
			ParentBeaconBlockRoot: nil, // correct for pre-Cancun
		}
		resp, err := api.BuildBlockV1(context.Background(), parentHash, attrs, nil, nil)
		require.Nil(t, resp)
		require.Error(t, err)
		var rpcErr *rpc.InvalidParamsError
		require.ErrorAs(t, err, &rpcErr)
		assert.Contains(t, rpcErr.Message, "missing withdrawals")
	})

	t.Run("withdrawals before Shanghai", func(t *testing.T) {
		t.Parallel()
		stub := &stubExecutionClient{
			getHeaderFunc: getHeaderReturning(parentHash, parentRPC),
		}
		// Use pre-Shanghai config.
		api := newTestingAPI(preShanghaiChainConfig(), stub, true)
		attrs := &engine_types.PayloadAttributes{
			Timestamp:             hexutil.Uint64(parentTimestamp + 1),
			PrevRandao:            common.Hash{0xaa},
			SuggestedFeeRecipient: common.HexToAddress("0x1111111111111111111111111111111111111111"),
			Withdrawals:           make([]*types.Withdrawal, 0), // present but pre-Shanghai
			ParentBeaconBlockRoot: nil,
		}
		resp, err := api.BuildBlockV1(context.Background(), parentHash, attrs, nil, nil)
		require.Nil(t, resp)
		require.Error(t, err)
		var rpcErr *rpc.InvalidParamsError
		require.ErrorAs(t, err, &rpcErr)
		assert.Contains(t, rpcErr.Message, "withdrawals before Shanghai")
	})
}

func ptrUint64(v uint64) *uint64 {
	return &v
}
