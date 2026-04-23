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
	"bytes"
	"context"
	"crypto/ecdsa"
	"math/big"
	"testing"
	"time"

	"github.com/holiman/uint256"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/erigontech/erigon/cl/clparams"
	"github.com/erigontech/erigon/common"
	"github.com/erigontech/erigon/common/crypto"
	"github.com/erigontech/erigon/common/hexutil"
	"github.com/erigontech/erigon/common/log/v3"
	"github.com/erigontech/erigon/execution/builder"
	"github.com/erigontech/erigon/execution/chain"
	"github.com/erigontech/erigon/execution/engineapi/engine_block_downloader"
	"github.com/erigontech/erigon/execution/engineapi/engine_types"
	"github.com/erigontech/erigon/execution/execmodule"
	"github.com/erigontech/erigon/execution/types"
	"github.com/erigontech/erigon/node/ethconfig"
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
	return NewTestingImpl(srv, log.New(), nil)
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

// makeSignedRawTx builds, signs, and RLP-encodes a legacy transaction.
// The result is the raw bytes suitable for passing to BuildBlockV1 as a tx entry.
func makeSignedRawTx(t *testing.T, key *ecdsa.PrivateKey, cfg *chain.Config, nonce, blockNumber, timestamp uint64) hexutil.Bytes {
	t.Helper()
	to := common.HexToAddress("0x2222222222222222222222222222222222222222")
	tx := types.NewTransaction(nonce, to, uint256.NewInt(0), 21_000, uint256.NewInt(1_000_000_000), nil)
	signer := types.MakeSigner(cfg, blockNumber, timestamp)
	signed, err := types.SignTx(tx, *signer, key)
	require.NoError(t, err)
	var buf bytes.Buffer
	require.NoError(t, signed.MarshalBinary(&buf))
	return buf.Bytes()
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

	t.Run("explicit non-empty transaction list with invalid tx bytes", func(t *testing.T) {
		t.Parallel()
		stub := &stubExecutionModule{
			getHeaderFunc: getHeaderReturning(parentHash, parentHdr),
		}
		api := newTestingAPI(allForksChainConfig(), stub)
		// Invalid raw bytes that cannot be decoded as a transaction.
		txs := []hexutil.Bytes{{0x01, 0x02}}
		resp, err := api.BuildBlockV1(context.Background(), parentHash, validPayloadAttrs(parentTimestamp), &txs, nil)
		require.Nil(t, resp)
		require.Error(t, err)
		var rpcErr *rpc.InvalidParamsError
		require.ErrorAs(t, err, &rpcErr)
		assert.Contains(t, rpcErr.Message, "decode error")
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
	// We use context with a short deadline (100ms) so the busy-retry loop
	// terminates quickly while still validating the busy-path logic.
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
		ctx, cancel := context.WithTimeout(context.Background(), 100*time.Millisecond)
		defer cancel()
		resp, err := api.BuildBlockV1(ctx, parentHash, validPayloadAttrs(parentTimestamp), nil, nil)
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
		ctx, cancel := context.WithTimeout(context.Background(), 100*time.Millisecond)
		defer cancel()
		resp, err := api.BuildBlockV1(ctx, parentHash, validPayloadAttrs(parentTimestamp), nil, nil)
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

		require.NotNil(t, resp.BlockValue)
		assert.Equal(t, "12345", resp.BlockValue.ToInt().String())
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

	t.Run("nonce too high", func(t *testing.T) {
		t.Parallel()
		key, err := crypto.GenerateKey()
		require.NoError(t, err)
		// State nonce is 0 (default stub), but tx nonce is 1 → too high.
		rawTx := makeSignedRawTx(t, key, allForksChainConfig(), 1, 101, parentTimestamp+1)
		stub := &stubExecutionModule{
			getHeaderFunc: getHeaderReturning(parentHash, parentHdr),
		}
		api := newTestingAPI(allForksChainConfig(), stub)
		txs := []hexutil.Bytes{rawTx}
		resp, err := api.BuildBlockV1(context.Background(), parentHash, validPayloadAttrs(parentTimestamp), &txs, nil)
		require.Nil(t, resp)
		require.Error(t, err)
		var rpcErr *rpc.CustomError
		require.ErrorAs(t, err, &rpcErr)
		assert.Equal(t, rpc.ErrCodeDefault, rpcErr.Code)
		assert.Contains(t, rpcErr.Message, "nonce too high")
	})

	t.Run("nonce too low", func(t *testing.T) {
		t.Parallel()
		key, err := crypto.GenerateKey()
		require.NoError(t, err)
		cfg := allForksChainConfig()
		// Two txs from the same sender with the same nonce (0, 0).
		// After the first tx is accepted (expectedNonce→1), the second nonce=0 is too low.
		rawTx0 := makeSignedRawTx(t, key, cfg, 0, 101, parentTimestamp+1)
		rawTx1 := makeSignedRawTx(t, key, cfg, 0, 101, parentTimestamp+1)
		stub := &stubExecutionModule{
			getHeaderFunc: getHeaderReturning(parentHash, parentHdr),
		}
		api := newTestingAPI(cfg, stub)
		txs := []hexutil.Bytes{rawTx0, rawTx1}
		resp, err := api.BuildBlockV1(context.Background(), parentHash, validPayloadAttrs(parentTimestamp), &txs, nil)
		require.Nil(t, resp)
		require.Error(t, err)
		var rpcErr *rpc.CustomError
		require.ErrorAs(t, err, &rpcErr)
		assert.Equal(t, rpc.ErrCodeDefault, rpcErr.Code)
		assert.Contains(t, rpcErr.Message, "nonce too low")
	})

	t.Run("valid explicit tx list reaches assembler with CustomTxnProvider set", func(t *testing.T) {
		t.Parallel()
		key, err := crypto.GenerateKey()
		require.NoError(t, err)
		cfg := allForksChainConfig()
		// Two sequential txs from the same sender: nonces 0 and 1.
		rawTx0 := makeSignedRawTx(t, key, cfg, 0, 101, parentTimestamp+1)
		rawTx1 := makeSignedRawTx(t, key, cfg, 1, 101, parentTimestamp+1)

		var capturedParams *builder.Parameters
		stub := &stubExecutionModule{
			getHeaderFunc: getHeaderReturning(parentHash, parentHdr),
			assembleBlockFunc: func(_ context.Context, params *builder.Parameters) (execmodule.AssembleBlockResult, error) {
				capturedParams = params
				// Return busy=false with payloadID=1 so execution proceeds to GetAssembledBlock.
				return execmodule.AssembleBlockResult{PayloadID: 1, Busy: false}, nil
			},
			getAssembledBlockFunc: func(_ context.Context, _ uint64) (execmodule.AssembledBlockResult, error) {
				// Returning nil block triggers "no assembled block data available" error,
				// which is fine — we only care that AssembleBlock was reached.
				return execmodule.AssembledBlockResult{Block: nil, Busy: false}, nil
			},
		}
		api := newTestingAPI(cfg, stub)
		txs := []hexutil.Bytes{rawTx0, rawTx1}
		_, err = api.BuildBlockV1(context.Background(), parentHash, validPayloadAttrs(parentTimestamp), &txs, nil)
		// The call fails because getAssembledBlock returns nil block, but that's expected.
		require.Error(t, err)
		assert.Contains(t, err.Error(), "no assembled block data")
		// The important check: CustomTxnProvider was set and forwarded to AssembleBlock.
		require.NotNil(t, capturedParams, "AssembleBlock should have been called")
		require.NotNil(t, capturedParams.CustomTxnProvider)
	})

}

// ---------------------------------------------------------------------------
// staticTxnProvider tests
// ---------------------------------------------------------------------------

func TestStaticTxnProvider(t *testing.T) {
	t.Parallel()

	// stubTx is a minimal unsigned transaction; its content is irrelevant for
	// testing the atomic "consumed" semantics of staticTxnProvider.
	to := common.HexToAddress("0x2222222222222222222222222222222222222222")
	stubTx := types.NewTransaction(0, to, uint256.NewInt(0), 21_000, uint256.NewInt(1_000_000_000), nil)

	t.Run("returns transactions on first call", func(t *testing.T) {
		t.Parallel()
		p := &staticTxnProvider{txns: []types.Transaction{stubTx}}
		txns, err := p.ProvideTxns(context.Background())
		require.NoError(t, err)
		require.Len(t, txns, 1)
	})

	t.Run("returns nil on second call (consumed)", func(t *testing.T) {
		t.Parallel()
		p := &staticTxnProvider{txns: []types.Transaction{stubTx}}
		_, _ = p.ProvideTxns(context.Background()) // first call consumes
		txns, err := p.ProvideTxns(context.Background())
		require.NoError(t, err)
		require.Nil(t, txns)
	})

	t.Run("empty provider returns empty slice then nil", func(t *testing.T) {
		t.Parallel()
		p := &staticTxnProvider{txns: []types.Transaction{}}
		txns, err := p.ProvideTxns(context.Background())
		require.NoError(t, err)
		require.Empty(t, txns)

		txns, err = p.ProvideTxns(context.Background())
		require.NoError(t, err)
		require.Nil(t, txns)
	})
}

// ---------------------------------------------------------------------------
// assembleParams propagation tests
// ---------------------------------------------------------------------------

// nilGetAssembledBlock is a shared stub for getAssembledBlockFunc that always
// returns a nil block (triggers "no assembled block data" error). Used by tests
// that only care about what was passed to AssembleBlock, not the assembled result.
var nilGetAssembledBlock = func(_ context.Context, _ uint64) (execmodule.AssembledBlockResult, error) {
	return execmodule.AssembledBlockResult{Block: nil, Busy: false}, nil
}

// captureAssembleParams is a helper that returns an assembleBlockFunc which
// captures the Parameters passed to AssembleBlock.
func captureAssembleParams(dest **builder.Parameters) func(context.Context, *builder.Parameters) (execmodule.AssembleBlockResult, error) {
	return func(_ context.Context, params *builder.Parameters) (execmodule.AssembleBlockResult, error) {
		*dest = params
		return execmodule.AssembleBlockResult{PayloadID: 1, Busy: false}, nil
	}
}

func TestBuildBlockV1AssembleParamsVersion(t *testing.T) {
	t.Parallel()

	const parentTimestamp = 1000

	parentHdr := makeParentHeader(parentTimestamp)
	parentHash := parentHdr.Hash()

	t.Run("nil transactions produces nil CustomTxnProvider (mempool path)", func(t *testing.T) {
		t.Parallel()
		var captured *builder.Parameters
		stub := &stubExecutionModule{
			getHeaderFunc:         getHeaderReturning(parentHash, parentHdr),
			assembleBlockFunc:     captureAssembleParams(&captured),
			getAssembledBlockFunc: nilGetAssembledBlock,
		}
		api := newTestingAPI(allForksChainConfig(), stub)
		_, _ = api.BuildBlockV1(context.Background(), parentHash, validPayloadAttrs(parentTimestamp), nil, nil)
		require.NotNil(t, captured)
		assert.Nil(t, captured.CustomTxnProvider, "nil transactions should leave CustomTxnProvider nil (use mempool)")
	})

	t.Run("Cancun: ParentBeaconBlockRoot and Withdrawals propagated", func(t *testing.T) {
		t.Parallel()
		beaconRoot := common.Hash{0xbe, 0xef}
		withdrawals := []*types.Withdrawal{{Index: 1, Validator: 2, Address: common.Address{0x33}, Amount: 100}}
		attrs := &engine_types.PayloadAttributes{
			Timestamp:             hexutil.Uint64(parentTimestamp + 1),
			PrevRandao:            common.Hash{0xaa},
			SuggestedFeeRecipient: common.HexToAddress("0x1111111111111111111111111111111111111111"),
			Withdrawals:           withdrawals,
			ParentBeaconBlockRoot: &beaconRoot,
		}
		var captured *builder.Parameters
		stub := &stubExecutionModule{
			getHeaderFunc:         getHeaderReturning(parentHash, parentHdr),
			assembleBlockFunc:     captureAssembleParams(&captured),
			getAssembledBlockFunc: nilGetAssembledBlock,
		}
		api := newTestingAPI(allForksChainConfig(), stub)
		_, _ = api.BuildBlockV1(context.Background(), parentHash, attrs, nil, nil)
		require.NotNil(t, captured)
		require.NotNil(t, captured.ParentBeaconBlockRoot, "ParentBeaconBlockRoot must be set for Cancun+")
		assert.Equal(t, beaconRoot, *captured.ParentBeaconBlockRoot)
		require.NotNil(t, captured.Withdrawals, "Withdrawals must be set for Capella+")
		assert.Equal(t, withdrawals, captured.Withdrawals)
	})

	t.Run("Shanghai (pre-Cancun): Withdrawals set but no ParentBeaconBlockRoot", func(t *testing.T) {
		t.Parallel()
		withdrawals := []*types.Withdrawal{{Index: 5, Validator: 6, Address: common.Address{0x44}, Amount: 200}}
		attrs := &engine_types.PayloadAttributes{
			Timestamp:             hexutil.Uint64(parentTimestamp + 1),
			PrevRandao:            common.Hash{0xbb},
			SuggestedFeeRecipient: common.HexToAddress("0x1111111111111111111111111111111111111111"),
			Withdrawals:           withdrawals,
			ParentBeaconBlockRoot: nil,
		}
		var captured *builder.Parameters
		stub := &stubExecutionModule{
			getHeaderFunc:         getHeaderReturning(parentHash, parentHdr),
			assembleBlockFunc:     captureAssembleParams(&captured),
			getAssembledBlockFunc: nilGetAssembledBlock,
		}
		api := newTestingAPI(preCancunChainConfig(), stub)
		_, _ = api.BuildBlockV1(context.Background(), parentHash, attrs, nil, nil)
		require.NotNil(t, captured)
		assert.Nil(t, captured.ParentBeaconBlockRoot, "ParentBeaconBlockRoot must NOT be set pre-Cancun")
		require.NotNil(t, captured.Withdrawals, "Withdrawals must be set for Shanghai/Capella")
		assert.Equal(t, withdrawals, captured.Withdrawals)
	})
}

// ---------------------------------------------------------------------------
// Multi-sender nonce tracking test
// ---------------------------------------------------------------------------

func TestBuildBlockV1MultipleSendersNonce(t *testing.T) {
	t.Parallel()

	const parentTimestamp = 1000
	parentHdr := makeParentHeader(parentTimestamp)
	parentHash := parentHdr.Hash()

	t.Run("two senders with sequential nonces accepted", func(t *testing.T) {
		t.Parallel()
		key1, err := crypto.GenerateKey()
		require.NoError(t, err)
		key2, err := crypto.GenerateKey()
		require.NoError(t, err)
		cfg := allForksChainConfig()
		// Interleaved txs from two different senders, each starting at nonce 0.
		rawTx0 := makeSignedRawTx(t, key1, cfg, 0, 101, parentTimestamp+1)
		rawTx1 := makeSignedRawTx(t, key2, cfg, 0, 101, parentTimestamp+1)
		rawTx2 := makeSignedRawTx(t, key1, cfg, 1, 101, parentTimestamp+1)
		rawTx3 := makeSignedRawTx(t, key2, cfg, 1, 101, parentTimestamp+1)

		var captured *builder.Parameters
		stub := &stubExecutionModule{
			getHeaderFunc:         getHeaderReturning(parentHash, parentHdr),
			assembleBlockFunc:     captureAssembleParams(&captured),
			getAssembledBlockFunc: nilGetAssembledBlock,
		}
		api := newTestingAPI(cfg, stub)
		txs := []hexutil.Bytes{rawTx0, rawTx1, rawTx2, rawTx3}
		_, err = api.BuildBlockV1(context.Background(), parentHash, validPayloadAttrs(parentTimestamp), &txs, nil)
		// Fails at nil block, not at nonce check.
		require.Error(t, err)
		assert.Contains(t, err.Error(), "no assembled block data")
		require.NotNil(t, captured, "AssembleBlock should have been called")
		require.NotNil(t, captured.CustomTxnProvider)
	})

	t.Run("second sender nonce gap detected independently", func(t *testing.T) {
		t.Parallel()
		key1, err := crypto.GenerateKey()
		require.NoError(t, err)
		key2, err := crypto.GenerateKey()
		require.NoError(t, err)
		cfg := allForksChainConfig()
		rawTx0 := makeSignedRawTx(t, key1, cfg, 0, 101, parentTimestamp+1)
		// key2 skips nonce 0 → nonce 1 (too high).
		rawTx1 := makeSignedRawTx(t, key2, cfg, 1, 101, parentTimestamp+1)

		stub := &stubExecutionModule{
			getHeaderFunc: getHeaderReturning(parentHash, parentHdr),
		}
		api := newTestingAPI(cfg, stub)
		txs := []hexutil.Bytes{rawTx0, rawTx1}
		resp, err := api.BuildBlockV1(context.Background(), parentHash, validPayloadAttrs(parentTimestamp), &txs, nil)
		require.Nil(t, resp)
		require.Error(t, err)
		var rpcErr *rpc.CustomError
		require.ErrorAs(t, err, &rpcErr)
		assert.Equal(t, rpc.ErrCodeDefault, rpcErr.Code)
		assert.Contains(t, rpcErr.Message, "nonce too high")
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

// TestForkchoiceUpdatedV2ValidatesAttributesWhenSyncing pins the engine-api spec
// requirement that payloadAttributes validation runs regardless of fork-choice
// sync state. The hive `engine-withdrawals / Empty Withdrawals (Paris)` test
// exercises exactly this: CLMocker sends fcuV2 with nil withdrawals at a
// Shanghai timestamp while the EL reports SYNCING, and expects -38003.
func TestForkchoiceUpdatedV2ValidatesAttributesWhenSyncing(t *testing.T) {
	t.Parallel()

	forkchoiceState := &engine_types.ForkChoiceState{
		HeadHash:           common.Hash{0x1},
		SafeBlockHash:      common.Hash{0x2},
		FinalizedBlockHash: common.Hash{0x3},
	}
	stub := &stubExecutionModule{} // GetHeaderHashNumber returns nil -> HandleForkChoice -> SYNCING
	srv := NewEngineServer(
		log.New(),
		preCancunChainConfig(),
		stub,
		// minimal downloader just provides the badHeaders LRU used by
		// getQuickPayloadStatusIfPossible; other deps are not touched because
		// srv.test = true guards StartDownloading below.
		engine_block_downloader.NewEngineBlockDownloader(
			context.Background(), log.New(), stub, nil, nil, preCancunChainConfig(), ethconfig.Sync{}, nil,
		),
		false,
		false,
		false,
		true,
		nil,
		0,
		0,
	)
	srv.test = true // avoid invoking downloader.StartDownloading on the SYNCING path

	resp, err := srv.forkchoiceUpdated(context.Background(), forkchoiceState, &engine_types.PayloadAttributes{
		Timestamp:             hexutil.Uint64(1001),
		PrevRandao:            common.Hash{0xaa},
		SuggestedFeeRecipient: common.HexToAddress("0x1111111111111111111111111111111111111111"),
		Withdrawals:           nil, // invalid: Shanghai timestamp requires non-nil withdrawals
	}, clparams.CapellaVersion)
	require.Nil(t, resp)
	require.Error(t, err)
	require.Equal(t, -38003, err.(rpc.Error).ErrorCode())
}

func ptrUint64(v uint64) *uint64 {
	return &v
}
