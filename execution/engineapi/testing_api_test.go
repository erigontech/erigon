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

	"github.com/erigontech/erigon/cl/clparams"
	"github.com/erigontech/erigon/common"
	"github.com/erigontech/erigon/common/hexutil"
	"github.com/erigontech/erigon/common/log/v3"
	"github.com/erigontech/erigon/execution/builder"
	"github.com/erigontech/erigon/execution/chain"
	"github.com/erigontech/erigon/execution/engineapi/engine_types"
	"github.com/erigontech/erigon/execution/execmodule"
	"github.com/erigontech/erigon/execution/types"
	"github.com/erigontech/erigon/rpc"
)

// ---------------------------------------------------------------------------
// Minimal stub implementing execmodule.ExecutionModule for unit tests.
// Only the methods called by BuildBlockV1 (GetHeader, AssembleBlock,
// GetAssembledBlock) are wired to configurable closures; everything else
// is a no-op that satisfies the interface.
// ---------------------------------------------------------------------------

type stubExecutionModule struct {
	getHeaderFunc         func(ctx context.Context, blockHash *common.Hash, blockNumber *uint64) (*types.Header, error)
	assembleBlockFunc     func(ctx context.Context, params *builder.Parameters) (execmodule.AssembleBlockResult, error)
	getAssembledBlockFunc func(ctx context.Context, payloadID uint64) (execmodule.AssembledBlockResult, error)
	getForkChoiceFunc     func(ctx context.Context) (execmodule.ForkChoiceState, error)
}

var _ execmodule.ExecutionModule = (*stubExecutionModule)(nil)

func (s *stubExecutionModule) GetHeader(ctx context.Context, blockHash *common.Hash, blockNumber *uint64) (*types.Header, error) {
	if s.getHeaderFunc != nil {
		return s.getHeaderFunc(ctx, blockHash, blockNumber)
	}
	return nil, nil
}

func (s *stubExecutionModule) AssembleBlock(ctx context.Context, params *builder.Parameters) (execmodule.AssembleBlockResult, error) {
	if s.assembleBlockFunc != nil {
		return s.assembleBlockFunc(ctx, params)
	}
	return execmodule.AssembleBlockResult{}, nil
}

func (s *stubExecutionModule) GetAssembledBlock(ctx context.Context, payloadID uint64) (execmodule.AssembledBlockResult, error) {
	if s.getAssembledBlockFunc != nil {
		return s.getAssembledBlockFunc(ctx, payloadID)
	}
	return execmodule.AssembledBlockResult{}, nil
}

// --- No-op implementations for the rest of the interface ---

func (s *stubExecutionModule) InsertBlocks(_ context.Context, _ []*types.RawBlock) (execmodule.ExecutionStatus, error) {
	return execmodule.ExecutionStatusSuccess, nil
}
func (s *stubExecutionModule) ValidateChain(_ context.Context, _ common.Hash, _ uint64) (execmodule.ValidationResult, error) {
	return execmodule.ValidationResult{}, nil
}
func (s *stubExecutionModule) UpdateForkChoice(_ context.Context, _, _, _ common.Hash) (execmodule.ForkChoiceResult, error) {
	return execmodule.ForkChoiceResult{}, nil
}
func (s *stubExecutionModule) GetForkChoice(ctx context.Context) (execmodule.ForkChoiceState, error) {
	if s.getForkChoiceFunc != nil {
		return s.getForkChoiceFunc(ctx)
	}
	return execmodule.ForkChoiceState{}, nil
}
func (s *stubExecutionModule) CurrentHeader(_ context.Context) (*types.Header, error) {
	return nil, nil
}
func (s *stubExecutionModule) GetBody(_ context.Context, _ *common.Hash, _ *uint64) (*types.RawBody, error) {
	return nil, nil
}
func (s *stubExecutionModule) HasBlock(_ context.Context, _ *common.Hash, _ *uint64) (bool, error) {
	return false, nil
}
func (s *stubExecutionModule) GetBodiesByRange(_ context.Context, _, _ uint64) ([]*types.RawBody, error) {
	return nil, nil
}
func (s *stubExecutionModule) GetBodiesByHashes(_ context.Context, _ []common.Hash) ([]*types.RawBody, error) {
	return nil, nil
}
func (s *stubExecutionModule) GetPayloadBodiesByHash(_ context.Context, _ []common.Hash) ([]*execmodule.PayloadBody, error) {
	return nil, nil
}
func (s *stubExecutionModule) GetPayloadBodiesByRange(_ context.Context, _, _ uint64) ([]*execmodule.PayloadBody, error) {
	return nil, nil
}
func (s *stubExecutionModule) IsCanonicalHash(_ context.Context, _ common.Hash) (bool, error) {
	return false, nil
}
func (s *stubExecutionModule) GetHeaderHashNumber(_ context.Context, _ common.Hash) (*uint64, error) {
	return nil, nil
}
func (s *stubExecutionModule) GetTD(_ context.Context, _ *common.Hash, _ *uint64) (*big.Int, error) {
	return nil, nil
}
func (s *stubExecutionModule) Ready(_ context.Context) (bool, error) { return true, nil }
func (s *stubExecutionModule) FrozenBlocks(_ context.Context) (uint64, bool, error) {
	return 0, false, nil
}

// ---------------------------------------------------------------------------
// Test helpers
// ---------------------------------------------------------------------------

// allForksChainConfig returns a chain config where all forks (Shanghai, Cancun,
// Prague, Osaka, Amsterdam) are activated at timestamp 0.
func allForksChainConfig() *chain.Config {
	return &chain.Config{
		ChainID:                       big.NewInt(1337),
		HomesteadBlock:                common.NewUint64(0),
		TangerineWhistleBlock:         common.NewUint64(0),
		SpuriousDragonBlock:           common.NewUint64(0),
		ByzantiumBlock:                common.NewUint64(0),
		ConstantinopleBlock:           common.NewUint64(0),
		PetersburgBlock:               common.NewUint64(0),
		IstanbulBlock:                 common.NewUint64(0),
		MuirGlacierBlock:              common.NewUint64(0),
		BerlinBlock:                   common.NewUint64(0),
		LondonBlock:                   common.NewUint64(0),
		ArrowGlacierBlock:             common.NewUint64(0),
		GrayGlacierBlock:              common.NewUint64(0),
		TerminalTotalDifficulty:       big.NewInt(0),
		TerminalTotalDifficultyPassed: true,
		ShanghaiTime:                  common.NewUint64(0),
		CancunTime:                    common.NewUint64(0),
		PragueTime:                    common.NewUint64(0),
		OsakaTime:                     common.NewUint64(0),
		AmsterdamTime:                 common.NewUint64(0),
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

// makeParentHeader builds a minimal types.Header with the given timestamp.
// The returned header can be hashed via .Hash() to get a stable parent hash.
func makeParentHeader(timestamp uint64) *types.Header {
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
	return h
}

// getHeaderReturning creates a getHeaderFunc that returns hdr when the request
// hash matches expectedHash.  Otherwise returns nil (unknown).
func getHeaderReturning(expectedHash common.Hash, hdr *types.Header) func(ctx context.Context, blockHash *common.Hash, blockNumber *uint64) (*types.Header, error) {
	return func(_ context.Context, blockHash *common.Hash, _ *uint64) (*types.Header, error) {
		if blockHash == nil || *blockHash != expectedHash {
			return nil, nil
		}
		return hdr, nil
	}
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

// newTestingAPI creates a testingImpl backed by a stub execution module.
func newTestingAPI(cfg *chain.Config, stub *stubExecutionModule) TestingAPI {
	srv := NewEngineServer(
		log.New(),
		cfg,
		stub,
		nil,   // blockDownloader
		false, // caplin
		false, // internalCL
		false, // proposing
		true,  // consuming
		nil,   // txPool
		0,     // fcuTimeout
		0,     // maxReorgDepth
	)
	return NewTestingImpl(srv)
}

// makeAssembledBlock builds a minimal BlockWithReceipts for use in stub
// getAssembledBlockFunc implementations.  header fields are set to the
// supplied values; withdrawals are set to an empty (non-nil) slice so that
// the Shanghai withdrawal field is present.
func makeAssembledBlock(blockHash, parentHash, stateRoot common.Hash, blockNumber, timestamp uint64, extraData []byte, gasLimit, gasUsed uint64, blobGasUsed, excessBlobGas uint64) *types.BlockWithReceipts {
	h := &types.Header{
		ParentHash:  parentHash,
		Root:        stateRoot,
		ReceiptHash: common.Hash{},
		Bloom:       types.Bloom{},
		GasLimit:    gasLimit,
		GasUsed:     gasUsed,
		Time:        timestamp,
		Extra:       extraData,
	}
	h.Number.SetUint64(blockNumber)
	h.Difficulty.SetUint64(0)
	h.BaseFee = uint256.NewInt(1_000_000_000)
	h.BlobGasUsed = &blobGasUsed
	h.ExcessBlobGas = &excessBlobGas

	blk := types.NewBlockFromStorage(blockHash, h, nil, nil, []*types.Withdrawal{})
	return &types.BlockWithReceipts{
		Block:    blk,
		Requests: make(types.FlatRequests, 0), // empty but non-nil: valid for Prague+
	}
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

func TestBuildBlockV1(t *testing.T) {
	t.Parallel()

	const parentTimestamp = 1000

	// Build a real parent header so that its computed hash matches BlockHash
	// (required by GetHeader lookup in chainRW).
	parentHdr := makeParentHeader(parentTimestamp)
	parentHash := parentHdr.Hash()

	t.Run("nil payloadAttributes", func(t *testing.T) {
		t.Parallel()
		stub := &stubExecutionModule{}
		api := newTestingAPI(allForksChainConfig(), stub)
		resp, err := api.BuildBlockV1(context.Background(), parentHash, nil, nil, nil)
		require.Nil(t, resp)
		require.Error(t, err)
		var rpcErr *rpc.InvalidParamsError
		require.ErrorAs(t, err, &rpcErr)
		assert.Contains(t, rpcErr.Message, "payloadAttributes must not be null")
	})

	t.Run("explicit non-empty transaction list", func(t *testing.T) {
		t.Parallel()
		stub := &stubExecutionModule{}
		api := newTestingAPI(allForksChainConfig(), stub)
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
		stub := &stubExecutionModule{
			getHeaderFunc: func(_ context.Context, _ *common.Hash, _ *uint64) (*types.Header, error) {
				// Return nil header to trigger unknown parent error (separate from tx check).
				return nil, nil
			},
		}
		api := newTestingAPI(allForksChainConfig(), stub)
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
		stub := &stubExecutionModule{
			getHeaderFunc: func(_ context.Context, _ *common.Hash, _ *uint64) (*types.Header, error) {
				return nil, nil
			},
		}
		api := newTestingAPI(allForksChainConfig(), stub)
		resp, err := api.BuildBlockV1(context.Background(), parentHash, validPayloadAttrs(parentTimestamp), nil, nil)
		require.Nil(t, resp)
		require.Error(t, err)
		var rpcErr *rpc.InvalidParamsError
		require.ErrorAs(t, err, &rpcErr)
		assert.Contains(t, rpcErr.Message, "unknown parent hash")
	})

	t.Run("timestamp equal to parent", func(t *testing.T) {
		t.Parallel()
		stub := &stubExecutionModule{
			getHeaderFunc: getHeaderReturning(parentHash, parentHdr),
		}
		api := newTestingAPI(allForksChainConfig(), stub)
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
		stub := &stubExecutionModule{
			getHeaderFunc: getHeaderReturning(parentHash, parentHdr),
		}
		api := newTestingAPI(allForksChainConfig(), stub)
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
		stub := &stubExecutionModule{
			getHeaderFunc: getHeaderReturning(parentHash, parentHdr),
		}
		api := newTestingAPI(allForksChainConfig(), stub)
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
		stub := &stubExecutionModule{
			getHeaderFunc: getHeaderReturning(parentHash, parentHdr),
		}
		// Use a pre-Cancun config (Shanghai only).
		api := newTestingAPI(preCancunChainConfig(), stub)
		attrs := validPayloadAttrs(parentTimestamp)
		// attrs already has ParentBeaconBlockRoot set, which is invalid pre-Cancun.
		resp, err := api.BuildBlockV1(context.Background(), parentHash, attrs, nil, nil)
		require.Nil(t, resp)
		require.Error(t, err)
		var rpcErr *rpc.InvalidParamsError
		require.ErrorAs(t, err, &rpcErr)
		assert.Contains(t, rpcErr.Message, "parentBeaconBlockRoot not supported before Cancun")
	})

	// These two tests exercise the waitForResponse busy-polling loop.
	// We use an Aura config (5 s slot) to keep the timeout short while
	// still validating the busy-path logic.
	shortSlotCfg := allForksChainConfig()
	shortSlotCfg.Aura = &chain.AuRaConfig{} // SecondsPerSlot() == 5
	shortSlotCfg.Ethash = nil               // Aura and Ethash are mutually exclusive

	t.Run("execution service busy on AssembleBlock", func(t *testing.T) {
		t.Parallel()
		stub := &stubExecutionModule{
			getHeaderFunc: getHeaderReturning(parentHash, parentHdr),
			assembleBlockFunc: func(_ context.Context, _ *builder.Parameters) (execmodule.AssembleBlockResult, error) {
				return execmodule.AssembleBlockResult{Busy: true}, nil
			},
		}
		api := newTestingAPI(shortSlotCfg, stub)
		resp, err := api.BuildBlockV1(context.Background(), parentHash, validPayloadAttrs(parentTimestamp), nil, nil)
		require.Nil(t, resp)
		require.Error(t, err)
		assert.Contains(t, err.Error(), "execution service is busy")
	})

	t.Run("execution service busy on GetAssembledBlock", func(t *testing.T) {
		t.Parallel()
		stub := &stubExecutionModule{
			getHeaderFunc: getHeaderReturning(parentHash, parentHdr),
			assembleBlockFunc: func(_ context.Context, _ *builder.Parameters) (execmodule.AssembleBlockResult, error) {
				return execmodule.AssembleBlockResult{PayloadID: 42, Busy: false}, nil
			},
			getAssembledBlockFunc: func(_ context.Context, _ uint64) (execmodule.AssembledBlockResult, error) {
				return execmodule.AssembledBlockResult{Busy: true}, nil
			},
		}
		api := newTestingAPI(shortSlotCfg, stub)
		resp, err := api.BuildBlockV1(context.Background(), parentHash, validPayloadAttrs(parentTimestamp), nil, nil)
		require.Nil(t, resp)
		require.Error(t, err)
		assert.Contains(t, err.Error(), "execution service is busy retrieving assembled block")
	})

	t.Run("nil assembled block data", func(t *testing.T) {
		t.Parallel()
		stub := &stubExecutionModule{
			getHeaderFunc: getHeaderReturning(parentHash, parentHdr),
			assembleBlockFunc: func(_ context.Context, _ *builder.Parameters) (execmodule.AssembleBlockResult, error) {
				return execmodule.AssembleBlockResult{PayloadID: 42, Busy: false}, nil
			},
			getAssembledBlockFunc: func(_ context.Context, _ uint64) (execmodule.AssembledBlockResult, error) {
				return execmodule.AssembledBlockResult{Block: nil, Busy: false}, nil
			},
		}
		api := newTestingAPI(allForksChainConfig(), stub)
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

		stub := &stubExecutionModule{
			getHeaderFunc: getHeaderReturning(parentHash, parentHdr),
			assembleBlockFunc: func(_ context.Context, params *builder.Parameters) (execmodule.AssembleBlockResult, error) {
				// Verify the request was built correctly.
				assert.Equal(t, parentHash, params.ParentHash)
				assert.Equal(t, uint64(parentTimestamp+1), params.Timestamp)
				return execmodule.AssembleBlockResult{PayloadID: payloadID, Busy: false}, nil
			},
			getAssembledBlockFunc: func(_ context.Context, id uint64) (execmodule.AssembledBlockResult, error) {
				assert.Equal(t, payloadID, id)
				blk := makeAssembledBlock(blockHash, parentHash, stateRoot, 101, parentTimestamp+1, []byte("test"), 30_000_000, 21_000, 0, 0)
				return execmodule.AssembledBlockResult{Block: blk, BlockValue: blockValue, Busy: false}, nil
			},
		}
		api := newTestingAPI(allForksChainConfig(), stub)
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
		stub := &stubExecutionModule{
			getHeaderFunc: getHeaderReturning(parentHash, parentHdr),
			assembleBlockFunc: func(_ context.Context, _ *builder.Parameters) (execmodule.AssembleBlockResult, error) {
				return execmodule.AssembleBlockResult{PayloadID: 1, Busy: false}, nil
			},
			getAssembledBlockFunc: func(_ context.Context, _ uint64) (execmodule.AssembledBlockResult, error) {
				blk := makeAssembledBlock(common.Hash{0x01}, parentHash, common.Hash{}, 101, parentTimestamp+1, []byte("original"), 30_000_000, 0, 0, 0)
				return execmodule.AssembledBlockResult{
					Block:      blk,
					BlockValue: uint256.NewInt(0),
					Busy:       false,
				}, nil
			},
		}
		api := newTestingAPI(allForksChainConfig(), stub)
		overrideData := hexutil.Bytes("overridden-extra")
		resp, err := api.BuildBlockV1(context.Background(), parentHash, validPayloadAttrs(parentTimestamp), nil, &overrideData)
		require.NoError(t, err)
		require.NotNil(t, resp)
		assert.Equal(t, hexutil.Bytes("overridden-extra"), resp.ExecutionPayload.ExtraData)
	})

	t.Run("missing withdrawals for Shanghai", func(t *testing.T) {
		t.Parallel()
		// Shanghai requires withdrawals to be non-nil.
		stub := &stubExecutionModule{
			getHeaderFunc: getHeaderReturning(parentHash, parentHdr),
		}
		// Use pre-Cancun (Shanghai-only) config.
		api := newTestingAPI(preCancunChainConfig(), stub)
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
		stub := &stubExecutionModule{
			getHeaderFunc: getHeaderReturning(parentHash, parentHdr),
		}
		// Use pre-Shanghai config.
		api := newTestingAPI(preShanghaiChainConfig(), stub)
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

func TestForkchoiceUpdatedV2PayloadAttributesWithdrawalsValidation(t *testing.T) {
	t.Parallel()

	t.Run("missing withdrawals for Shanghai returns invalid payload attributes", func(t *testing.T) {
		forkchoiceState := &engine_types.ForkChoiceState{
			HeadHash:           common.Hash{0x1},
			SafeBlockHash:      common.Hash{0x2},
			FinalizedBlockHash: common.Hash{0x3},
		}
		srv := NewEngineServer(
			log.New(),
			preCancunChainConfig(),
			&stubExecutionModule{
				getForkChoiceFunc: func(_ context.Context) (execmodule.ForkChoiceState, error) {
					return execmodule.ForkChoiceState{
						HeadHash:      forkchoiceState.HeadHash,
						SafeHash:      forkchoiceState.SafeBlockHash,
						FinalizedHash: forkchoiceState.FinalizedBlockHash,
					}, nil
				},
			},
			nil,
			false,
			false,
			false,
			true,
			nil,
			0,
			0,
		)

		resp, err := srv.forkchoiceUpdated(context.Background(), forkchoiceState, &engine_types.PayloadAttributes{
			Timestamp:             hexutil.Uint64(1001),
			PrevRandao:            common.Hash{0xaa},
			SuggestedFeeRecipient: common.HexToAddress("0x1111111111111111111111111111111111111111"),
			Withdrawals:           nil,
		}, clparams.CapellaVersion)
		require.Nil(t, resp)
		require.Error(t, err)
		require.Equal(t, -38003, err.(rpc.Error).ErrorCode())
	})

	t.Run("withdrawals before Shanghai returns invalid payload attributes", func(t *testing.T) {
		forkchoiceState := &engine_types.ForkChoiceState{
			HeadHash:           common.Hash{0x4},
			SafeBlockHash:      common.Hash{0x5},
			FinalizedBlockHash: common.Hash{0x6},
		}
		srv := NewEngineServer(
			log.New(),
			preShanghaiChainConfig(),
			&stubExecutionModule{
				getForkChoiceFunc: func(_ context.Context) (execmodule.ForkChoiceState, error) {
					return execmodule.ForkChoiceState{
						HeadHash:      forkchoiceState.HeadHash,
						SafeHash:      forkchoiceState.SafeBlockHash,
						FinalizedHash: forkchoiceState.FinalizedBlockHash,
					}, nil
				},
			},
			nil,
			false,
			false,
			false,
			true,
			nil,
			0,
			0,
		)

		resp, err := srv.forkchoiceUpdated(context.Background(), forkchoiceState, &engine_types.PayloadAttributes{
			Timestamp:             hexutil.Uint64(1001),
			PrevRandao:            common.Hash{0xaa},
			SuggestedFeeRecipient: common.HexToAddress("0x1111111111111111111111111111111111111111"),
			Withdrawals:           make([]*types.Withdrawal, 0),
		}, clparams.CapellaVersion)
		require.Nil(t, resp)
		require.Error(t, err)
		require.Equal(t, -38003, err.(rpc.Error).ErrorCode())
	})
}

func ptrUint64(v uint64) *uint64 {
	return &v
}
