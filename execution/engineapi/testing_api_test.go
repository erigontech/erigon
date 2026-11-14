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
	"fmt"
	"math/big"
	"runtime"
	"testing"
	"testing/synctest"
	"time"

	mapset "github.com/deckarep/golang-set/v2"
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
	"github.com/erigontech/erigon/execution/exec"
	"github.com/erigontech/erigon/execution/execmodule"
	"github.com/erigontech/erigon/execution/execmodule/execmoduletester"
	"github.com/erigontech/erigon/execution/protocol"
	"github.com/erigontech/erigon/execution/protocol/mdgas"
	"github.com/erigontech/erigon/execution/protocol/params"
	"github.com/erigontech/erigon/execution/rlp"
	"github.com/erigontech/erigon/execution/types"
	"github.com/erigontech/erigon/execution/types/accounts"
	"github.com/erigontech/erigon/execution/vm"
	"github.com/erigontech/erigon/execution/vm/program"
	"github.com/erigontech/erigon/node/ethconfig"
	"github.com/erigontech/erigon/rpc"
	"github.com/erigontech/erigon/txnprovider"
)

// stubExecutionModule supplies default responses for ExecutionModule methods.
// Tests can override individual methods by setting the corresponding callbacks.
type stubExecutionModule struct {
	getHeaderFunc          func(ctx context.Context, blockHash *common.Hash, blockNumber *uint64) (*types.Header, error)
	getHeadersByRangeFunc  func(ctx context.Context, start, count uint64) ([]*types.Header, error)
	headerNumberFunc       func(ctx context.Context, hash common.Hash) (*uint64, error)
	assembleBlockFunc      func(ctx context.Context, params *builder.Parameters) (execmodule.AssembleBlockResult, error)
	getAssembledBlockFunc  func(ctx context.Context, payloadID uint64) (execmodule.AssembledBlockResult, error)
	getForkChoiceFunc      func(ctx context.Context) (execmodule.ForkChoiceState, error)
	currentHeaderFunc      func(ctx context.Context) (*types.Header, error)
	insertBlocksFunc       func(ctx context.Context, blocks []*types.Block) (execmodule.ExecutionStatus, error)
	validateChainBlockFunc func(ctx context.Context, block *types.Block) (execmodule.ValidationResult, error)
	validateChainFunc      func(ctx context.Context, blockHash common.Hash, blockNumber uint64) (execmodule.ValidationResult, error)
	updateForkChoiceFunc   func(ctx context.Context, headHash, safeHash, finalizedHash common.Hash) (execmodule.ForkChoiceResult, error)
	inclusionListFunc      func(ctx context.Context) (types.Transactions, error)
	addSendersRecoveryFunc func(blockHash common.Hash, recovery *exec.SendersRecovery)
	isValidatedFunc        func(hash common.Hash) bool

	getPayloadBodiesByHashFunc func(ctx context.Context, hashes []common.Hash) ([]*execmodule.PayloadBody, error)
}

var _ execmodule.ExecutionModule = (*stubExecutionModule)(nil)

func (s *stubExecutionModule) GetHeader(ctx context.Context, blockHash *common.Hash, blockNumber *uint64) (*types.Header, error) {
	if s.getHeaderFunc != nil {
		return s.getHeaderFunc(ctx, blockHash, blockNumber)
	}
	return nil, nil
}

func (s *stubExecutionModule) GetHeadersByRange(ctx context.Context, start, count uint64) ([]*types.Header, error) {
	if s.getHeadersByRangeFunc != nil {
		return s.getHeadersByRangeFunc(ctx, start, count)
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

func (s *stubExecutionModule) AddSendersRecovery(blockHash common.Hash, recovery *exec.SendersRecovery) {
	if s.addSendersRecoveryFunc != nil {
		s.addSendersRecoveryFunc(blockHash, recovery)
	}
}

func (s *stubExecutionModule) InsertBlocks(ctx context.Context, blocks []*types.Block) (execmodule.ExecutionStatus, error) {
	if s.insertBlocksFunc != nil {
		return s.insertBlocksFunc(ctx, blocks)
	}
	return execmodule.ExecutionStatusSuccess, nil
}

func (s *stubExecutionModule) ValidateChain(ctx context.Context, block *types.Block) (execmodule.ValidationResult, error) {
	if s.validateChainBlockFunc != nil {
		return s.validateChainBlockFunc(ctx, block)
	}
	if s.validateChainFunc != nil {
		return s.validateChainFunc(ctx, block.Hash(), block.NumberU64())
	}
	return execmodule.ValidationResult{}, nil
}

func (s *stubExecutionModule) UpdateForkChoice(ctx context.Context, headHash, safeHash, finalizedHash common.Hash) (execmodule.ForkChoiceResult, error) {
	if s.updateForkChoiceFunc != nil {
		return s.updateForkChoiceFunc(ctx, headHash, safeHash, finalizedHash)
	}
	return execmodule.ForkChoiceResult{}, nil
}

func (s *stubExecutionModule) GetForkChoice(ctx context.Context) (execmodule.ForkChoiceState, error) {
	if s.getForkChoiceFunc != nil {
		return s.getForkChoiceFunc(ctx)
	}
	return execmodule.ForkChoiceState{}, nil
}

func (s *stubExecutionModule) CurrentHeader(ctx context.Context) (*types.Header, error) {
	if s.currentHeaderFunc != nil {
		return s.currentHeaderFunc(ctx)
	}
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

func (s *stubExecutionModule) GetPayloadBodiesByHash(ctx context.Context, hashes []common.Hash) ([]*execmodule.PayloadBody, error) {
	if s.getPayloadBodiesByHashFunc != nil {
		return s.getPayloadBodiesByHashFunc(ctx, hashes)
	}
	return nil, nil
}

func (s *stubExecutionModule) GetPayloadBodiesByRange(_ context.Context, _, _ uint64) ([]*execmodule.PayloadBody, error) {
	return nil, nil
}

func (s *stubExecutionModule) IsCanonicalHash(_ context.Context, _ common.Hash) (bool, error) {
	return false, nil
}

func (s *stubExecutionModule) IsValidated(hash common.Hash) bool {
	if s.isValidatedFunc != nil {
		return s.isValidatedFunc(hash)
	}
	return false
}

func (s *stubExecutionModule) GetHeaderHashNumber(ctx context.Context, hash common.Hash) (*uint64, error) {
	if s.headerNumberFunc != nil {
		return s.headerNumberFunc(ctx, hash)
	}
	return nil, nil
}

func (s *stubExecutionModule) GetTD(_ context.Context, _ *common.Hash, _ *uint64) (*uint256.Int, error) {
	return nil, nil
}
func (s *stubExecutionModule) Ready(_ context.Context) (bool, error) { return true, nil }
func (s *stubExecutionModule) FrozenBlocks(_ context.Context) (uint64, bool, error) {
	return 0, false, nil
}

func (s *stubExecutionModule) InclusionList(ctx context.Context) (types.Transactions, error) {
	if s.inclusionListFunc != nil {
		return s.inclusionListFunc(ctx)
	}
	return nil, nil
}

// ---------------------------------------------------------------------------
// Test helpers
// ---------------------------------------------------------------------------

// allForksChainConfig returns a chain config where all forks (Shanghai, Cancun,
// Prague, Osaka, Amsterdam) are activated at timestamp 0.
func allForksChainConfig() *chain.Config {
	return &chain.Config{
		ChainID:                       uint256.NewInt(1337),
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
		TerminalTotalDifficulty:       uint256.NewInt(0),
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

// prePragueChainConfig returns a chain config where Cancun is active but
// Prague and later forks are NOT activated.
func prePragueChainConfig() *chain.Config {
	cfg := allForksChainConfig()
	cfg.PragueTime = nil
	cfg.OsakaTime = nil
	cfg.AmsterdamTime = nil
	return cfg
}

// preOsakaChainConfig returns a chain config where Prague is active but
// Osaka and later forks are NOT activated.
func preOsakaChainConfig() *chain.Config {
	cfg := allForksChainConfig()
	cfg.OsakaTime = nil
	cfg.AmsterdamTime = nil
	return cfg
}

// preAmsterdamChainConfig returns a chain config where Osaka is active but
// Amsterdam (Glamsterdam) is NOT activated.
func preAmsterdamChainConfig() *chain.Config {
	cfg := allForksChainConfig()
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

// validPayloadAttrs returns payload attributes valid for an all-forks chain
// (including Glamsterdam) with timestamp greater than parentTimestamp.
func validPayloadAttrs(parentTimestamp uint64) *engine_types.PayloadAttributes {
	beaconRoot := common.Hash{0xbe, 0xac}
	slotNumber := hexutil.Uint64(1)
	targetGasLimit := hexutil.Uint64(30_000_000)
	return &engine_types.PayloadAttributes{
		Timestamp:             hexutil.Uint64(parentTimestamp + 1),
		PrevRandao:            common.Hash{0xaa},
		SuggestedFeeRecipient: common.HexToAddress("0x1111111111111111111111111111111111111111"),
		Withdrawals:           make([]*types.Withdrawal, 0),
		ParentBeaconBlockRoot: &beaconRoot,
		SlotNumber:            &slotNumber,
		TargetGasLimit:        &targetGasLimit,
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
		nil,   // blobGetter
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

	blk := types.NewBlockFromStorage(blockHash, h, nil, nil, []*types.Withdrawal{}, nil)
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

func TestAssembleTestingBlockRetrievalDeadline(t *testing.T) {
	for _, tc := range []struct {
		name      string
		discarded bool
	}{
		{"context error", false},
		{"discarded build", true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			synctest.Test(t, func(t *testing.T) {
				const parentTimestamp = 1000
				parent := makeParentHeader(parentTimestamp)
				stub := &stubExecutionModule{
					assembleBlockFunc: func(context.Context, *builder.Parameters) (execmodule.AssembleBlockResult, error) {
						return execmodule.AssembleBlockResult{PayloadID: 1}, nil
					},
					getAssembledBlockFunc: func(ctx context.Context, _ uint64) (execmodule.AssembledBlockResult, error) {
						<-ctx.Done()
						if tc.discarded {
							return execmodule.AssembledBlockResult{Unknown: true}, nil
						}
						return execmodule.AssembledBlockResult{}, ctx.Err()
					},
				}
				api := newTestingAPI(allForksChainConfig(), stub).(*testingImpl)
				ctx, cancel := context.WithCancel(t.Context())
				defer cancel()
				fallback := time.AfterFunc(2*time.Second, cancel)
				defer fallback.Stop()
				deadline := time.Now().Add(time.Second)
				txns := []hexutil.Bytes{}

				_, _, err := api.assembleTestingBlock(ctx, parent.Hash(), parent, validPayloadAttrs(parentTimestamp), &txns, nil, deadline)

				require.ErrorIs(t, err, context.DeadlineExceeded)
				require.Equal(t, deadline, time.Now())
				require.True(t, api.server.lock.TryLock(), "retrieval must release the engine lock at the deadline")
				api.server.lock.Unlock()
			})
		})
	}
}

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

	t.Run("extraData longer than 32 bytes", func(t *testing.T) {
		t.Parallel()
		stub := &stubExecutionModule{
			getHeaderFunc: getHeaderReturning(parentHash, parentHdr),
		}
		api := newTestingAPI(allForksChainConfig(), stub)
		tooLong := hexutil.Bytes(make([]byte, 33))
		resp, err := api.BuildBlockV1(context.Background(), parentHash, validPayloadAttrs(parentTimestamp), nil, &tooLong)
		require.Nil(t, resp)
		require.Error(t, err)
		var rpcErr *rpc.InvalidParamsError
		require.ErrorAs(t, err, &rpcErr)
		assert.Contains(t, rpcErr.Message, "extraData")
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

	t.Run("missing slotNumber for Glamsterdam+", func(t *testing.T) {
		t.Parallel()
		stub := &stubExecutionModule{
			getHeaderFunc: getHeaderReturning(parentHash, parentHdr),
		}
		api := newTestingAPI(allForksChainConfig(), stub)
		attrs := validPayloadAttrs(parentTimestamp)
		attrs.SlotNumber = nil // required for Glamsterdam+
		resp, err := api.BuildBlockV1(context.Background(), parentHash, attrs, nil, nil)
		require.Nil(t, resp)
		require.Error(t, err)
		var rpcErr *rpc.InvalidParamsError
		require.ErrorAs(t, err, &rpcErr)
		assert.Contains(t, rpcErr.Message, "slotNumber required for Glamsterdam")
	})

	t.Run("missing targetGasLimit for Glamsterdam+", func(t *testing.T) {
		t.Parallel()
		stub := &stubExecutionModule{
			getHeaderFunc: getHeaderReturning(parentHash, parentHdr),
		}
		api := newTestingAPI(allForksChainConfig(), stub)
		attrs := validPayloadAttrs(parentTimestamp)
		attrs.TargetGasLimit = nil // required for Glamsterdam+
		resp, err := api.BuildBlockV1(context.Background(), parentHash, attrs, nil, nil)
		require.Nil(t, resp)
		require.Error(t, err)
		var rpcErr *rpc.InvalidParamsError
		require.ErrorAs(t, err, &rpcErr)
		assert.Contains(t, rpcErr.Message, "targetGasLimit required for Glamsterdam")
	})

	t.Run("unexpected targetGasLimit pre-Glamsterdam", func(t *testing.T) {
		t.Parallel()
		stub := &stubExecutionModule{
			getHeaderFunc: getHeaderReturning(parentHash, parentHdr),
		}
		api := newTestingAPI(preAmsterdamChainConfig(), stub)
		attrs := validPayloadAttrs(parentTimestamp)
		attrs.SlotNumber = nil
		// attrs already has TargetGasLimit set, which is invalid pre-Glamsterdam.
		resp, err := api.BuildBlockV1(context.Background(), parentHash, attrs, nil, nil)
		require.Nil(t, resp)
		require.Error(t, err)
		var rpcErr *rpc.InvalidParamsError
		require.ErrorAs(t, err, &rpcErr)
		assert.Contains(t, rpcErr.Message, "targetGasLimit not supported before Glamsterdam")
	})

	t.Run("unexpected slotNumber pre-Glamsterdam", func(t *testing.T) {
		t.Parallel()
		stub := &stubExecutionModule{
			getHeaderFunc: getHeaderReturning(parentHash, parentHdr),
		}
		api := newTestingAPI(preAmsterdamChainConfig(), stub)
		attrs := validPayloadAttrs(parentTimestamp)
		attrs.TargetGasLimit = nil
		resp, err := api.BuildBlockV1(context.Background(), parentHash, attrs, nil, nil)
		require.Nil(t, resp)
		require.Error(t, err)
		var rpcErr *rpc.InvalidParamsError
		require.ErrorAs(t, err, &rpcErr)
		assert.Contains(t, rpcErr.Message, "slotNumber not supported before Glamsterdam")
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

	t.Run("extraData forwarded to the block builder", func(t *testing.T) {
		t.Parallel()
		var captured *builder.Parameters
		stub := &stubExecutionModule{
			getHeaderFunc: getHeaderReturning(parentHash, parentHdr),
			assembleBlockFunc: func(_ context.Context, params *builder.Parameters) (execmodule.AssembleBlockResult, error) {
				captured = params
				return execmodule.AssembleBlockResult{PayloadID: 1, Busy: false}, nil
			},
			getAssembledBlockFunc: func(_ context.Context, _ uint64) (execmodule.AssembledBlockResult, error) {
				blk := makeAssembledBlock(common.Hash{0x01}, parentHash, common.Hash{}, 101, parentTimestamp+1, []byte("overridden-extra"), 30_000_000, 0, 0, 0)
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
		require.NotNil(t, captured)
		assert.Equal(t, []byte("overridden-extra"), captured.ExtraData)
		assert.Equal(t, hexutil.Bytes("overridden-extra"), resp.ExecutionPayload.ExtraData)
	})

	t.Run("nil extraData builds with empty extra data", func(t *testing.T) {
		t.Parallel()
		var captured *builder.Parameters
		stub := &stubExecutionModule{
			getHeaderFunc: getHeaderReturning(parentHash, parentHdr),
			assembleBlockFunc: func(_ context.Context, params *builder.Parameters) (execmodule.AssembleBlockResult, error) {
				captured = params
				return execmodule.AssembleBlockResult{PayloadID: 1, Busy: false}, nil
			},
			getAssembledBlockFunc: func(_ context.Context, _ uint64) (execmodule.AssembledBlockResult, error) {
				blk := makeAssembledBlock(common.Hash{0x01}, parentHash, common.Hash{}, 101, parentTimestamp+1, []byte{}, 30_000_000, 0, 0, 0)
				return execmodule.AssembledBlockResult{Block: blk, BlockValue: uint256.NewInt(0), Busy: false}, nil
			},
		}
		api := newTestingAPI(allForksChainConfig(), stub)
		_, err := api.BuildBlockV1(context.Background(), parentHash, validPayloadAttrs(parentTimestamp), nil, nil)
		require.NoError(t, err)
		require.NotNil(t, captured)
		require.NotNil(t, captured.ExtraData, "nil extraData must force empty extra data, not the builder's configured default")
		assert.Empty(t, captured.ExtraData)
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

	// No signature is needed here: the provider selects transactions without validating them.
	to := common.HexToAddress("0x2222222222222222222222222222222222222222")
	stubTx := types.NewTransaction(0, to, uint256.NewInt(0), 21_000, uint256.NewInt(1_000_000_000), nil)

	t.Run("returns transactions once without a filter", func(t *testing.T) {
		t.Parallel()
		p := &staticTxnProvider{txns: []types.Transaction{stubTx}}
		txns, err := p.ProvideTxns(t.Context())
		require.NoError(t, err)
		require.Equal(t, []types.Transaction{stubTx}, txns)
		txns, err = p.ProvideTxns(t.Context())
		require.NoError(t, err)
		require.Nil(t, txns)
	})

	for _, tc := range []struct {
		name   string
		amount int
	}{
		{name: "duplicates across batches", amount: 1},
		{name: "duplicates within a batch", amount: 2},
		{name: "whole list", amount: 3},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			second := types.NewTransaction(1, to, uint256.NewInt(0), 21_000, uint256.NewInt(1_000_000_000), nil)
			requested := []types.Transaction{stubTx, stubTx, second}
			p := &staticTxnProvider{txns: requested}
			filter := mapset.NewSet[[32]byte]()
			txns, err := p.ProvideTxns(t.Context(), txnprovider.WithAmount(0), txnprovider.WithTxnIdsFilter(filter))
			require.NoError(t, err)
			require.Empty(t, txns)
			for offset := 0; offset < len(requested); offset += tc.amount {
				txns, err = p.ProvideTxns(t.Context(), txnprovider.WithAmount(tc.amount), txnprovider.WithTxnIdsFilter(filter))
				require.NoError(t, err)
				require.Equal(t, requested[offset:min(offset+tc.amount, len(requested))], txns)
				for _, tx := range txns {
					filter.Add(tx.Hash())
				}
			}
			filter.Clear()
			txns, err = p.ProvideTxns(t.Context(), txnprovider.WithTxnIdsFilter(filter))
			require.NoError(t, err)
			require.Nil(t, txns)
			require.Equal(t, requested, p.txns)
		})
	}

	t.Run("empty provider returns no transactions", func(t *testing.T) {
		t.Parallel()
		p := &staticTxnProvider{txns: []types.Transaction{}}
		txns, err := p.ProvideTxns(context.Background())
		require.NoError(t, err)
		require.Empty(t, txns)

		txns, err = p.ProvideTxns(context.Background())
		require.NoError(t, err)
		require.Nil(t, txns)
	})

	t.Run("appending does not overwrite unserved transactions", func(t *testing.T) {
		t.Parallel()
		p := &staticTxnProvider{txns: []types.Transaction{stubTx, stubTx}}
		txns, err := p.ProvideTxns(t.Context(), txnprovider.WithAmount(1))
		require.NoError(t, err)
		require.Equal(t, []types.Transaction{stubTx, nil}, append(txns, nil))
		txns, err = p.ProvideTxns(t.Context(), txnprovider.WithAmount(1))
		require.NoError(t, err)
		require.Equal(t, []types.Transaction{stubTx}, txns)
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
		slotNum := hexutil.Uint64(1)
		tgl := hexutil.Uint64(30_000_000)
		withdrawals := []*types.Withdrawal{{Index: 1, Validator: 2, Address: common.Address{0x33}, Amount: 100}}
		attrs := &engine_types.PayloadAttributes{
			Timestamp:             hexutil.Uint64(parentTimestamp + 1),
			PrevRandao:            common.Hash{0xaa},
			SuggestedFeeRecipient: common.HexToAddress("0x1111111111111111111111111111111111111111"),
			Withdrawals:           withdrawals,
			ParentBeaconBlockRoot: &beaconRoot,
			SlotNumber:            &slotNum,
			TargetGasLimit:        &tgl,
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

	t.Run("Glamsterdam: SlotNumber and TargetGasLimit propagated", func(t *testing.T) {
		t.Parallel()
		attrs := validPayloadAttrs(parentTimestamp)
		*attrs.SlotNumber = 7
		*attrs.TargetGasLimit = 45_000_000
		var captured *builder.Parameters
		stub := &stubExecutionModule{
			getHeaderFunc:         getHeaderReturning(parentHash, parentHdr),
			assembleBlockFunc:     captureAssembleParams(&captured),
			getAssembledBlockFunc: nilGetAssembledBlock,
		}
		api := newTestingAPI(allForksChainConfig(), stub)
		_, _ = api.BuildBlockV1(context.Background(), parentHash, attrs, nil, nil)
		require.NotNil(t, captured)
		require.NotNil(t, captured.SlotNumber, "SlotNumber must be forwarded for Glamsterdam+")
		assert.Equal(t, uint64(7), *captured.SlotNumber)
		require.NotNil(t, captured.TargetGasLimit, "TargetGasLimit must be forwarded for Glamsterdam+")
		assert.Equal(t, uint64(45_000_000), *captured.TargetGasLimit)
	})

	t.Run("pre-Glamsterdam: SlotNumber and TargetGasLimit not set", func(t *testing.T) {
		t.Parallel()
		attrs := validPayloadAttrs(parentTimestamp)
		attrs.SlotNumber = nil
		attrs.TargetGasLimit = nil
		var captured *builder.Parameters
		stub := &stubExecutionModule{
			getHeaderFunc:         getHeaderReturning(parentHash, parentHdr),
			assembleBlockFunc:     captureAssembleParams(&captured),
			getAssembledBlockFunc: nilGetAssembledBlock,
		}
		api := newTestingAPI(preAmsterdamChainConfig(), stub)
		_, _ = api.BuildBlockV1(context.Background(), parentHash, attrs, nil, nil)
		require.NotNil(t, captured)
		assert.Nil(t, captured.SlotNumber, "SlotNumber must NOT be set pre-Glamsterdam")
		assert.Nil(t, captured.TargetGasLimit, "TargetGasLimit must NOT be set pre-Glamsterdam")
	})
}

func TestDecodeTxnProviderPreservesNonces(t *testing.T) {
	t.Parallel()
	cfg := allForksChainConfig()
	cfg.BogotaTime = common.NewUint64(0)
	key, err := crypto.GenerateKey()
	require.NoError(t, err)
	sender := crypto.PubkeyToAddress(key.PublicKey)
	frame := func(nonceKey, seq uint64) hexutil.Bytes {
		tx := &types.FrameTransaction{
			ChainID: *cfg.ChainID, SenderAddress: sender, Nonce: seq,
			NonceKeys: []uint256.Int{*uint256.NewInt(nonceKey)},
			Frames:    []types.Frame{{Mode: types.FrameVerify, Flags: types.FrameApproveBoth}},
			Fees:      types.FrameFees{FeeCap: *uint256.NewInt(1_000_000_000)},
		}
		var encoded bytes.Buffer
		require.NoError(t, tx.MarshalBinary(&encoded))
		return encoded.Bytes()
	}
	ordinary0 := makeSignedRawTx(t, key, cfg, 0, 101, 1001)
	ordinary1 := makeSignedRawTx(t, key, cfg, 1, 101, 1001)
	for _, tc := range []struct {
		name string
		txs  []hexutil.Bytes
	}{
		{name: "keyed sequence above account nonce", txs: []hexutil.Bytes{frame(1, 7)}},
		{name: "keyed sequence below account nonce", txs: []hexutil.Bytes{ordinary0, frame(1, 0), ordinary1}},
		{name: "keyed frame preserves account nonce", txs: []hexutil.Bytes{frame(1, 0), ordinary0}},
		{name: "independent keys", txs: []hexutil.Bytes{frame(1, 0), frame(2, 0), ordinary0}},
		{name: "legacy frame consumes account nonce", txs: []hexutil.Bytes{frame(0, 0), ordinary1}},
		{name: "legacy frame with nonzero nonce", txs: []hexutil.Bytes{frame(0, 1)}},
		{name: "legacy frame with repeated nonce", txs: []hexutil.Bytes{ordinary0, frame(0, 0)}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			api := newTestingAPI(cfg, &stubExecutionModule{}).(*testingImpl)
			provider, err := api.decodeTxnProvider(&tc.txs, 101, 1001)
			require.NoError(t, err)
			provided, err := provider.ProvideTxns(t.Context())
			require.NoError(t, err)
			require.Len(t, provided, len(tc.txs))
			for i, tx := range provided {
				var encoded bytes.Buffer
				require.NoError(t, tx.MarshalBinary(&encoded))
				require.Equal(t, []byte(tc.txs[i]), encoded.Bytes())
			}
		})
	}
}

func TestBuildBlockV1FrameNonceAfterCreate(t *testing.T) {
	for _, tc := range []struct {
		name         string
		key          uint64
		otherCreator bool
		nonceOffset  int64
		commit       bool
		independent  bool
		duplicate    bool
	}{
		{name: "legacy"},
		{name: "legacy with independent successor", independent: true},
		{name: "keyed", key: 5},
		{name: "keyed with independent successor", key: 5, independent: true},
		{name: "other creator", otherCreator: true},
		{name: "other creator with independent successor", otherCreator: true, independent: true},
		{name: "nonce gap", key: 5, nonceOffset: 1},
		{name: "consumed nonce", key: 5, nonceOffset: -1},
		{name: "commit nonce gap", key: 5, nonceOffset: 1, commit: true},
		{name: "duplicate before nonce gap", key: 5, nonceOffset: 1, duplicate: true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			cfg := chain.AllProtocolChanges.Copy()
			cfg.BogotaTime = common.NewUint64(0)
			sender := common.HexToAddress("0x1000")
			creator := sender
			if tc.otherCreator {
				creator = common.HexToAddress("0x2000")
			}
			create := program.New().Push0().Push0().Push0().Op(vm.CREATE, vm.POP, vm.STOP).Bytes()
			code := append([]byte{byte(vm.CALLDATASIZE), byte(vm.PUSH1), byte(4 + len(create)), byte(vm.JUMPI)}, create...)
			code = append(code, program.New().Op(vm.JUMPDEST).Push(types.FrameApproveBoth).Push0().Push0().Op(vm.APPROVE).Bytes()...)
			genesis := &types.Genesis{
				Config: cfg, GasLimit: 30_000_000, BaseFee: uint256.NewInt(1),
				Alloc: types.GenesisAlloc{
					sender:                  {Balance: big.NewInt(common.Ether), Code: code},
					types.FrameNonceManager: {Nonce: 1, Code: common.FromHex("60006000fd")},
				},
			}
			genesis.Alloc[creator] = types.GenesisAccount{Balance: big.NewInt(common.Ether), Code: code}
			independent := common.HexToAddress("0x3000")
			if tc.independent {
				genesis.Alloc[independent] = types.GenesisAccount{Balance: big.NewInt(common.Ether), Code: code}
			}
			m := execmoduletester.New(t, execmoduletester.WithGenesisSpec(genesis))
			parentChain, err := m.GenerateChain(1, nil)
			require.NoError(t, err)
			require.NoError(t, m.InsertChain(parentChain))
			parent := parentChain.TopBlock
			approval := types.Frame{Mode: types.FrameVerify, Flags: types.FrameApproveBoth, Data: []byte{1}, Limits: mdgas.MdGas{Execution: 1_000, State: params.StateGasPerStorageSet}}
			first := &types.FrameTransaction{
				ChainID: *cfg.ChainID, NonceKeys: []uint256.Int{*uint256.NewInt(tc.key)}, SenderAddress: sender,
				Fees: types.FrameFees{FeeCap: uint256.Int{1}},
				Frames: []types.Frame{
					{Mode: types.FrameDefault, Target: &creator, Limits: mdgas.MdGas{Execution: 200_000, State: 1_000_000}},
					approval,
				},
			}
			// CREATE increments the creator's nonce, and legacy approval increments
			// it once more when the creator is also the transaction sender.
			secondNonce := uint64(1)
			if tc.key == 0 && creator == sender {
				secondNonce++
			}
			secondNonce = uint64(int64(secondNonce) + tc.nonceOffset)
			second := &types.FrameTransaction{
				ChainID: *cfg.ChainID, NonceKeys: []uint256.Int{{}}, SenderAddress: creator, Nonce: secondNonce,
				Fees: types.FrameFees{FeeCap: uint256.Int{1}}, Frames: []types.Frame{approval},
			}
			frames := []*types.FrameTransaction{first, second}
			if tc.duplicate {
				frames = []*types.FrameTransaction{first, first, second}
			}
			if tc.independent {
				frames = append(frames, &types.FrameTransaction{
					ChainID: *cfg.ChainID, NonceKeys: []uint256.Int{{}}, SenderAddress: independent,
					Fees: types.FrameFees{FeeCap: uint256.Int{1}}, Frames: []types.Frame{approval},
				})
			}
			txs := make([]hexutil.Bytes, 0, len(frames))
			for _, tx := range frames {
				var encoded bytes.Buffer
				require.NoError(t, tx.MarshalBinary(&encoded))
				txs = append(txs, encoded.Bytes())
			}
			server := NewEngineServer(m.Log, cfg, m.ExecModule, nil, false, false, true, true, nil, nil, 0, 0)
			api := NewTestingImpl(server)
			attrs := validPayloadAttrs(parent.Time())
			slot := hexutil.Uint64(parent.NumberU64() + 1)
			attrs.SlotNumber = &slot
			if tc.commit {
				hash, err := api.CommitBlockV1(t.Context(), attrs, &txs, nil)
				require.ErrorContains(t, err, "transaction 1")
				require.ErrorContains(t, err, second.Hash().String())
				require.Zero(t, hash)
				head, err := m.ExecModule.CurrentHeader(t.Context())
				require.NoError(t, err)
				require.Equal(t, parent.Hash(), head.Hash())
				return
			}
			response, err := api.BuildBlockV1(t.Context(), parent.Hash(), attrs, &txs, nil)
			if tc.nonceOffset != 0 {
				require.ErrorContains(t, err, "transaction 1")
				require.ErrorContains(t, err, frames[1].Hash().String())
				if tc.duplicate {
					require.ErrorContains(t, err, "nonce too low")
				}
				require.Nil(t, response)
				return
			}
			require.NoError(t, err)
			require.Equal(t, txs, response.ExecutionPayload.Transactions)
		})
	}
}

func TestBuildBlockV1FrameNonceAfterOrdinaryCreate(t *testing.T) {
	cfg := chain.AllProtocolChanges.Copy()
	cfg.BogotaTime = common.NewUint64(0)
	key, err := crypto.GenerateKey()
	require.NoError(t, err)
	sender := crypto.PubkeyToAddress(key.PublicKey)
	creator := common.HexToAddress("0x1000")
	create := program.New().Push0().Push0().Push0().Op(vm.CREATE, vm.POP, vm.STOP).Bytes()
	code := append([]byte{byte(vm.CALLDATASIZE), byte(vm.PUSH1), byte(4 + len(create)), byte(vm.JUMPI)}, create...)
	code = append(code, program.New().Op(vm.JUMPDEST).Push(types.FrameApproveBoth).Push0().Push0().Op(vm.APPROVE).Bytes()...)
	genesis := &types.Genesis{
		Config: cfg, GasLimit: 30_000_000, BaseFee: uint256.NewInt(1),
		Alloc: types.GenesisAlloc{
			sender:  {Balance: big.NewInt(common.Ether)},
			creator: {Balance: big.NewInt(common.Ether), Code: code},
		},
	}
	m := execmoduletester.New(t, execmoduletester.WithGenesisSpec(genesis))
	parentChain, err := m.GenerateChain(1, nil)
	require.NoError(t, err)
	require.NoError(t, m.InsertChain(parentChain))
	parent := parentChain.TopBlock
	ordinary, err := types.SignTx(types.NewTransaction(0, creator, uint256.NewInt(0), 1_000_000, uint256.NewInt(1), nil),
		*types.MakeSigner(cfg, parent.NumberU64()+1, parent.Time()+1), key)
	require.NoError(t, err)
	frame := &types.FrameTransaction{
		ChainID: *cfg.ChainID, NonceKeys: []uint256.Int{{}}, SenderAddress: creator, Nonce: 1,
		Fees: types.FrameFees{FeeCap: uint256.Int{1}},
		Frames: []types.Frame{{
			Mode: types.FrameVerify, Flags: types.FrameApproveBoth, Data: []byte{1},
			Limits: mdgas.MdGas{Execution: 1_000},
		}},
	}
	var txs []hexutil.Bytes
	for _, tx := range []types.Transaction{ordinary, frame} {
		var encoded bytes.Buffer
		require.NoError(t, tx.MarshalBinary(&encoded))
		txs = append(txs, encoded.Bytes())
	}
	server := NewEngineServer(m.Log, cfg, m.ExecModule, nil, false, false, true, true, nil, nil, 0, 0)
	api := NewTestingImpl(server)
	attrs := validPayloadAttrs(parent.Time())
	slot := hexutil.Uint64(parent.NumberU64() + 1)
	attrs.SlotNumber = &slot
	response, err := api.BuildBlockV1(t.Context(), parent.Hash(), attrs, &txs, nil)
	require.NoError(t, err)
	require.Equal(t, txs, response.ExecutionPayload.Transactions)
}

func TestTestingBlockNonceValidation(t *testing.T) {
	type requestedTxn struct {
		sender int
		nonce  uint64
	}
	for _, tc := range []struct {
		name    string
		txns    []requestedTxn
		wantErr string
		commit  bool
	}{
		{name: "sequential nonces", txns: []requestedTxn{{0, 0}, {0, 1}}},
		{name: "interleaved senders", txns: []requestedTxn{{0, 0}, {1, 0}, {0, 1}, {1, 1}}},
		{name: "nonce too high", txns: []requestedTxn{{0, 1}}, wantErr: "nonce too high"},
		{name: "nonce too low", txns: []requestedTxn{{0, 0}, {0, 0}}, wantErr: "nonce too low"},
		{name: "second sender nonce gap", txns: []requestedTxn{{0, 0}, {1, 1}}, wantErr: "nonce too high"},
		{name: "commit nonce gap", txns: []requestedTxn{{0, 1}}, wantErr: "nonce too high", commit: true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			cfg := chain.AllProtocolChanges.Copy()
			keys := make([]*ecdsa.PrivateKey, 2)
			alloc := make(types.GenesisAlloc)
			for i := range keys {
				key, err := crypto.GenerateKey()
				require.NoError(t, err)
				keys[i] = key
				alloc[crypto.PubkeyToAddress(key.PublicKey)] = types.GenesisAccount{Balance: big.NewInt(common.Ether)}
			}
			genesis := &types.Genesis{Config: cfg, GasLimit: 30_000_000, BaseFee: uint256.NewInt(1), Alloc: alloc}
			m := execmoduletester.New(t, execmoduletester.WithGenesisSpec(genesis))
			parentChain, err := m.GenerateChain(1, nil)
			require.NoError(t, err)
			require.NoError(t, m.InsertChain(parentChain))
			parent := parentChain.TopBlock
			txs := make([]hexutil.Bytes, len(tc.txns))
			for i, tx := range tc.txns {
				txs[i] = makeSignedRawTx(t, keys[tx.sender], cfg, tx.nonce, parent.NumberU64()+1, parent.Time()+1)
			}
			server := NewEngineServer(m.Log, cfg, m.ExecModule, nil, false, false, true, true, nil, nil, 0, 0)
			api := NewTestingImpl(server)
			attrs := validPayloadAttrs(parent.Time())
			slot := hexutil.Uint64(parent.NumberU64() + 1)
			attrs.SlotNumber = &slot
			if tc.commit {
				var hash common.Hash
				hash, err = api.CommitBlockV1(t.Context(), attrs, &txs, nil)
				require.Zero(t, hash)
			} else {
				var response *engine_types.GetPayloadResponse
				response, err = api.BuildBlockV1(t.Context(), parent.Hash(), attrs, &txs, nil)
				if tc.wantErr == "" {
					require.NoError(t, err)
					require.Equal(t, txs, response.ExecutionPayload.Transactions)
					return
				}
				require.Nil(t, response)
			}
			require.ErrorContains(t, err, tc.wantErr)
			invalidIndex := len(txs) - 1
			invalidTx, decodeErr := types.DecodeTransaction(txs[invalidIndex])
			require.NoError(t, decodeErr)
			require.ErrorContains(t, err, fmt.Sprintf("transaction %d (%s)", invalidIndex, invalidTx.Hash()))
			head, err := m.ExecModule.CurrentHeader(t.Context())
			require.NoError(t, err)
			require.Equal(t, parent.Hash(), head.Hash())
		})
	}
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
		var rpcErr rpc.Error
		require.ErrorAs(t, err, &rpcErr)
		require.Equal(t, -38003, rpcErr.ErrorCode())
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
		var rpcErr rpc.Error
		require.ErrorAs(t, err, &rpcErr)
		require.Equal(t, -38003, rpcErr.ErrorCode())
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
	var rpcErr rpc.Error
	require.ErrorAs(t, err, &rpcErr)
	require.Equal(t, -38003, rpcErr.ErrorCode())
}

// TestForkchoiceUpdatedV3DefersAttributesValidationWhenSyncing pins the
// engine API spec rule that the "Extend point (8)" checks added by
// engine_forkchoiceUpdatedV3 (cancun.md) — including the
// `parentBeaconBlockRoot != null` requirement — only run when the head is
// VALID. The hive `engine-cancun / Invalid PayloadAttributes, Missing
// BeaconRoot, Syncing=True` test exercises exactly this: CLMocker sends
// fcuV3 with nil parentBeaconBlockRoot to a head the EL hasn't seen, and
// expects {payloadStatus: SYNCING} with no JSON-RPC error.
func TestForkchoiceUpdatedV3DefersAttributesValidationWhenSyncing(t *testing.T) {
	t.Parallel()

	forkchoiceState := &engine_types.ForkChoiceState{
		HeadHash:           common.Hash{0x1},
		SafeBlockHash:      common.Hash{0x2},
		FinalizedBlockHash: common.Hash{0x3},
	}
	stub := &stubExecutionModule{}   // GetForkChoice/GetHeaderByHash stubs -> SYNCING
	cfg := preAmsterdamChainConfig() // V3 is the correct FCU version pre-Amsterdam
	srv := NewEngineServer(
		log.New(),
		cfg,
		stub,
		engine_block_downloader.NewEngineBlockDownloader(
			context.Background(), log.New(), stub, nil, nil, cfg, ethconfig.Sync{}, nil,
		),
		false,
		false,
		false,
		true,
		nil,
		nil,
		0,
		0,
	)
	srv.test = true // avoid invoking downloader.StartDownloading on the SYNCING path

	resp, err := srv.forkchoiceUpdated(context.Background(), forkchoiceState, &engine_types.PayloadAttributes{
		Timestamp:             hexutil.Uint64(1001),
		PrevRandao:            common.Hash{0xaa},
		SuggestedFeeRecipient: common.HexToAddress("0x1111111111111111111111111111111111111111"),
		Withdrawals:           make([]*types.Withdrawal, 0),
		ParentBeaconBlockRoot: nil, // V3 attrs MUST carry it, but head is unknown -> SYNCING wins
	}, clparams.DenebVersion)
	require.NoError(t, err)
	require.NotNil(t, resp)
	require.Equal(t, engine_types.SyncingStatus, resp.PayloadStatus.Status)
	require.Nil(t, resp.PayloadId)
}

// TestForkchoiceUpdatedV3RejectsMissingBeaconRootWhenValid pins the other
// half of the spec rule: when the head IS VALID, fcuV3 with a nil
// parentBeaconBlockRoot MUST return -38003 (cancun.md point 8.1).
func TestForkchoiceUpdatedV3RejectsMissingBeaconRootWhenValid(t *testing.T) {
	t.Parallel()

	forkchoiceState := &engine_types.ForkChoiceState{
		HeadHash:           common.Hash{0x1},
		SafeBlockHash:      common.Hash{0x2},
		FinalizedBlockHash: common.Hash{0x3},
	}
	srv := NewEngineServer(
		log.New(),
		preAmsterdamChainConfig(), // V3 is the correct FCU version pre-Amsterdam
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
		nil,
		0,
		0,
	)

	resp, err := srv.forkchoiceUpdated(context.Background(), forkchoiceState, &engine_types.PayloadAttributes{
		Timestamp:             hexutil.Uint64(1001),
		PrevRandao:            common.Hash{0xaa},
		SuggestedFeeRecipient: common.HexToAddress("0x1111111111111111111111111111111111111111"),
		Withdrawals:           make([]*types.Withdrawal, 0),
		ParentBeaconBlockRoot: nil,
	}, clparams.DenebVersion)
	require.Nil(t, resp)
	require.Error(t, err)
	var rpcErr rpc.Error
	require.ErrorAs(t, err, &rpcErr)
	require.Equal(t, -38003, rpcErr.ErrorCode())
}

// ---------------------------------------------------------------------------
// validatePayloadAttributes Pre/Post FCU — Amsterdam/Gloas version gate tests
// ---------------------------------------------------------------------------

func TestValidatePayloadAttributesPreFCU_AmsterdamGate(t *testing.T) {
	t.Parallel()

	t.Run("V3 at Amsterdam timestamp rejected", func(t *testing.T) {
		t.Parallel()
		srv := NewEngineServer(log.New(), allForksChainConfig(), &stubExecutionModule{}, nil, false, false, false, true, nil, nil, 0, 0)
		attrs := &engine_types.PayloadAttributes{
			Timestamp:             hexutil.Uint64(100), // Amsterdam active at ts=0
			ParentBeaconBlockRoot: &common.Hash{},
			Withdrawals:           make([]*types.Withdrawal, 0),
		}
		err := srv.validatePayloadAttributesPreFCU(clparams.FuluVersion, attrs)
		require.Error(t, err)
		var unsupported *rpc.UnsupportedForkError
		require.ErrorAs(t, err, &unsupported)
	})

	t.Run("V3 at pre-Amsterdam timestamp allowed", func(t *testing.T) {
		t.Parallel()
		srv := NewEngineServer(log.New(), preAmsterdamChainConfig(), &stubExecutionModule{}, nil, false, false, false, true, nil, nil, 0, 0)
		attrs := &engine_types.PayloadAttributes{
			Timestamp:             hexutil.Uint64(100),
			ParentBeaconBlockRoot: &common.Hash{},
			Withdrawals:           make([]*types.Withdrawal, 0),
		}
		err := srv.validatePayloadAttributesPreFCU(clparams.FuluVersion, attrs)
		require.NoError(t, err)
	})
}

func TestValidatePayloadAttributesPostFCU_AmsterdamGate(t *testing.T) {
	t.Parallel()

	t.Run("V4 at pre-Amsterdam timestamp rejected", func(t *testing.T) {
		t.Parallel()
		srv := NewEngineServer(log.New(), preAmsterdamChainConfig(), &stubExecutionModule{}, nil, false, false, false, true, nil, nil, 0, 0)
		slotNumber := hexutil.Uint64(1)
		targetGasLimit := hexutil.Uint64(30_000_000)
		attrs := &engine_types.PayloadAttributes{
			Timestamp:             hexutil.Uint64(100),
			ParentBeaconBlockRoot: &common.Hash{},
			Withdrawals:           make([]*types.Withdrawal, 0),
			SlotNumber:            &slotNumber,
			TargetGasLimit:        &targetGasLimit,
		}
		err := srv.validatePayloadAttributesPostFCU(clparams.GloasVersion, attrs)
		require.Error(t, err)
		var unsupported *rpc.UnsupportedForkError
		require.ErrorAs(t, err, &unsupported)
	})

	t.Run("V4 at Amsterdam timestamp allowed", func(t *testing.T) {
		t.Parallel()
		srv := NewEngineServer(log.New(), allForksChainConfig(), &stubExecutionModule{}, nil, false, false, false, true, nil, nil, 0, 0)
		slotNumber := hexutil.Uint64(1)
		targetGasLimit := hexutil.Uint64(30_000_000)
		attrs := &engine_types.PayloadAttributes{
			Timestamp:             hexutil.Uint64(100),
			ParentBeaconBlockRoot: &common.Hash{},
			Withdrawals:           make([]*types.Withdrawal, 0),
			SlotNumber:            &slotNumber,
			TargetGasLimit:        &targetGasLimit,
		}
		err := srv.validatePayloadAttributesPostFCU(clparams.GloasVersion, attrs)
		require.NoError(t, err)
	})

	t.Run("pre-V4 with SlotNumber rejected", func(t *testing.T) {
		t.Parallel()
		srv := NewEngineServer(log.New(), preAmsterdamChainConfig(), &stubExecutionModule{}, nil, false, false, false, true, nil, nil, 0, 0)
		slotNumber := hexutil.Uint64(1)
		attrs := &engine_types.PayloadAttributes{
			Timestamp:             hexutil.Uint64(100),
			ParentBeaconBlockRoot: &common.Hash{},
			Withdrawals:           make([]*types.Withdrawal, 0),
			SlotNumber:            &slotNumber,
		}
		err := srv.validatePayloadAttributesPostFCU(clparams.FuluVersion, attrs)
		require.Error(t, err)
		var rpcErr rpc.Error
		require.ErrorAs(t, err, &rpcErr)
		require.Equal(t, -38003, rpcErr.ErrorCode())
	})

	t.Run("pre-V4 without SlotNumber allowed", func(t *testing.T) {
		t.Parallel()
		srv := NewEngineServer(log.New(), preAmsterdamChainConfig(), &stubExecutionModule{}, nil, false, false, false, true, nil, nil, 0, 0)
		attrs := &engine_types.PayloadAttributes{
			Timestamp:             hexutil.Uint64(100),
			ParentBeaconBlockRoot: &common.Hash{},
			Withdrawals:           make([]*types.Withdrawal, 0),
		}
		err := srv.validatePayloadAttributesPostFCU(clparams.FuluVersion, attrs)
		require.NoError(t, err)
	})
}

func TestNewPayloadV4RejectsSlotNumber(t *testing.T) {
	t.Parallel()

	srv := NewEngineServer(log.New(), preAmsterdamChainConfig(), &stubExecutionModule{}, nil, false, false, false, true, nil, nil, 0, 0)
	zero := hexutil.Uint64(0)
	payload := &engine_types.ExecutionPayload{
		LogsBloom:     make(hexutil.Bytes, types.BloomByteLength),
		BaseFeePerGas: (*hexutil.U256)(uint256.NewInt(1)),
		Transactions:  []hexutil.Bytes{},
		Withdrawals:   []*types.Withdrawal{},
		BlobGasUsed:   &zero,
		ExcessBlobGas: &zero,
		SlotNumber:    &zero,
	}

	status, err := srv.NewPayloadV4(t.Context(), payload, []common.Hash{}, &common.Hash{}, []hexutil.Bytes{})
	require.Nil(t, status)
	require.Error(t, err)
	var rpcErr rpc.Error
	require.ErrorAs(t, err, &rpcErr)
	require.Equal(t, -32602, rpcErr.ErrorCode())
}

// A null baseFeePerGas decodes to nil and must reach validation, not panic while building the header.
func TestNewPayloadWithoutBaseFeeDoesNotPanic(t *testing.T) {
	t.Parallel()

	srv := NewEngineServer(log.New(), preAmsterdamChainConfig(), &stubExecutionModule{}, nil, false, false, false, true, nil, nil, 0, 0)
	zero := hexutil.Uint64(0)
	payload := &engine_types.ExecutionPayload{
		LogsBloom:     make(hexutil.Bytes, types.BloomByteLength),
		Transactions:  []hexutil.Bytes{},
		Withdrawals:   []*types.Withdrawal{},
		BlobGasUsed:   &zero,
		ExcessBlobGas: &zero,
	}

	require.NotPanics(t, func() {
		_, _ = srv.NewPayloadV4(t.Context(), payload, []common.Hash{}, &common.Hash{}, []hexutil.Bytes{})
	})
}

func signedElectraPayload(t *testing.T, config *chain.Config) (*engine_types.ExecutionPayload, common.Address) {
	t.Helper()
	key, err := crypto.GenerateKey()
	require.NoError(t, err)
	txn, err := types.SignTx(&types.LegacyTx{
		CommonTx: types.CommonTx{GasLimit: 21_000},
		GasPrice: *uint256.NewInt(1),
	}, *types.MakeSigner(config, 1, 0), key)
	require.NoError(t, err)

	zero := uint64(0)
	header := &types.Header{
		Number:                *uint256.NewInt(1),
		GasLimit:              30_000_000,
		BaseFee:               uint256.NewInt(1),
		BlobGasUsed:           &zero,
		ExcessBlobGas:         &zero,
		ParentBeaconBlockRoot: &common.Hash{},
		RequestsHash:          types.FlatRequests{}.Hash(),
	}
	block := types.NewBlock(header, []types.Transaction{txn}, nil, nil, []*types.Withdrawal{}, nil)
	resp, err := assembledBlockToPayloadResponse(&types.BlockWithReceipts{Block: block, Requests: types.FlatRequests{}}, uint256.NewInt(0), clparams.ElectraVersion)
	require.NoError(t, err)
	resp.ExecutionPayload.Withdrawals = []*types.Withdrawal{}
	return resp.ExecutionPayload, crypto.PubkeyToAddress(key.PublicKey)
}

func TestNewPayloadRegistersSendersRecovery(t *testing.T) {
	t.Parallel()

	config := preAmsterdamChainConfig()
	config.TerminalTotalDifficulty = nil
	payload, sender := signedElectraPayload(t, config)

	var registeredHash common.Hash
	var recovery *exec.SendersRecovery
	stub := &stubExecutionModule{addSendersRecoveryFunc: func(blockHash common.Hash, r *exec.SendersRecovery) {
		registeredHash, recovery = blockHash, r
	}}
	srv := NewEngineServer(log.New(), config, stub, nil, false, false, false, true, nil, nil, 0, 0)
	_, err := srv.NewPayloadV4(t.Context(), payload, []common.Hash{}, &common.Hash{}, []hexutil.Bytes{})
	require.ErrorContains(t, err, "not a proof-of-stake chain")

	require.Equal(t, payload.BlockHash, registeredHash)
	senders, err := recovery.Wait(t.Context())
	require.NoError(t, err)
	require.Equal(t, []accounts.Address{accounts.InternAddress(sender)}, senders)
}

func TestNewPayloadRegistersSendersRecoveryForBALPayload(t *testing.T) {
	config := allForksChainConfig()
	config.TerminalTotalDifficulty = nil
	key, err := crypto.GenerateKey()
	require.NoError(t, err)
	txn, err := types.SignTx(&types.LegacyTx{
		CommonTx: types.CommonTx{GasLimit: 21_000},
		GasPrice: *uint256.NewInt(1),
	}, *types.MakeSigner(config, 1, 0), key)
	require.NoError(t, err)
	balRLP, err := rlp.EncodeToBytes([]any{[]any{common.Address{0x2}, []any{}, []any{}, []any{}, []any{}, []any{}}})
	require.NoError(t, err)
	balHash := crypto.Keccak256Hash(balRLP)
	zero := uint64(0)
	header := &types.Header{
		Number:                *uint256.NewInt(1),
		GasLimit:              30_000_000,
		BaseFee:               uint256.NewInt(1),
		BlobGasUsed:           &zero,
		ExcessBlobGas:         &zero,
		ParentBeaconBlockRoot: &common.Hash{},
		RequestsHash:          types.FlatRequests{}.Hash(),
		BlockAccessListHash:   &balHash,
		SlotNumber:            &zero,
	}
	block := types.NewBlock(header, []types.Transaction{txn}, nil, nil, []*types.Withdrawal{}, nil)
	payload, err := engine_types.ExecutionPayloadFromBlock(block)
	require.NoError(t, err)
	payload.Withdrawals = []*types.Withdrawal{}
	balBytes := hexutil.Bytes(balRLP)
	payload.BlockAccessList = &balBytes

	var registeredHash common.Hash
	var recovery *exec.SendersRecovery
	stub := &stubExecutionModule{addSendersRecoveryFunc: func(blockHash common.Hash, r *exec.SendersRecovery) {
		registeredHash, recovery = blockHash, r
	}}
	srv := NewEngineServer(log.New(), config, stub, nil, false, false, false, true, nil, nil, 0, 0)
	_, err = srv.NewPayloadV5(t.Context(), payload, []common.Hash{}, &common.Hash{}, []hexutil.Bytes{})
	require.ErrorContains(t, err, "not a proof-of-stake chain")

	require.Equal(t, payload.BlockHash, registeredHash)
	senders, err := recovery.Wait(t.Context())
	require.NoError(t, err)
	require.Equal(t, []accounts.Address{accounts.InternAddress(crypto.PubkeyToAddress(key.PublicKey))}, senders)
}

func TestDecodePayloadTransactionsReportsTheFirstInvalidTx(t *testing.T) {
	config := preAmsterdamChainConfig()
	key, err := crypto.GenerateKey()
	require.NoError(t, err)
	signed := func(gas uint64) hexutil.Bytes {
		txn, err := types.SignTx(&types.LegacyTx{CommonTx: types.CommonTx{GasLimit: gas}, GasPrice: *uint256.NewInt(1)}, *types.MakeSigner(config, 1, 0), key)
		require.NoError(t, err)
		raw, err := types.MarshalTransactionsBinary(types.Transactions{txn})
		require.NoError(t, err)
		return raw[0]
	}
	undecodable := hexutil.Bytes{0x02, 0xff}
	rlpString := hexutil.Bytes{0x82, 0x02, 0x01}
	srv := NewEngineServer(log.New(), config, &stubExecutionModule{}, nil, false, false, false, true, nil, nil, 0, 0)
	for _, test := range []struct {
		name      string
		raw       []hexutil.Bytes
		amsterdam bool
		want      string
	}{
		{"decode error before RLP string", []hexutil.Bytes{signed(21_000), undecodable, rlpString}, false, "rlp"},
		{"RLP string before decode error", []hexutil.Bytes{signed(21_000), rlpString, undecodable}, false, "typed txn marshalled as RLP string"},
		{"gas limit before decode error", []hexutil.Bytes{signed(31_000_000), undecodable}, false, protocol.ErrGasLimitReached.Error()},
		{"Amsterdam state gas over block limit", []hexutil.Bytes{signed(30_000_001)}, true, protocol.ErrGasLimitReached.Error()},
	} {
		t.Run(test.name, func(t *testing.T) {
			_, _, _, status := srv.decodePayloadTransactions(test.raw, 30_000_000, test.amsterdam, false, nil)
			require.NotNil(t, status)
			require.Equal(t, engine_types.InvalidStatus, status.Status)
			require.ErrorContains(t, status.ValidationError.Error(), test.want)
		})
	}
}

func TestDecodePayloadTransactionsAcrossChunks(t *testing.T) {
	defer runtime.GOMAXPROCS(runtime.GOMAXPROCS(4))
	config := preAmsterdamChainConfig()
	key, err := crypto.GenerateKey()
	require.NoError(t, err)
	const n = 3*payloadDecodeChunk + 8
	payload := func() []hexutil.Bytes {
		raw := make([]hexutil.Bytes, n)
		for i := range raw {
			txn, err := types.SignTx(&types.LegacyTx{CommonTx: types.CommonTx{Nonce: uint64(i), GasLimit: 21_000}, GasPrice: *uint256.NewInt(1)}, *types.MakeSigner(config, 1, 0), key)
			require.NoError(t, err)
			bins, err := types.MarshalTransactionsBinary(types.Transactions{txn})
			require.NoError(t, err)
			raw[i] = bins[0]
		}
		return raw
	}
	overGas := func(raw []hexutil.Bytes, i int) {
		txn, err := types.SignTx(&types.LegacyTx{CommonTx: types.CommonTx{Nonce: uint64(i), GasLimit: 31_000_000}, GasPrice: *uint256.NewInt(1)}, *types.MakeSigner(config, 1, 0), key)
		require.NoError(t, err)
		bins, err := types.MarshalTransactionsBinary(types.Transactions{txn})
		require.NoError(t, err)
		raw[i] = bins[0]
	}
	srv := NewEngineServer(log.New(), config, &stubExecutionModule{}, nil, false, false, false, true, nil, nil, 0, 0)

	t.Run("every tx decoded in order", func(t *testing.T) {
		_, transactions, _, status := srv.decodePayloadTransactions(payload(), 30_000_000, false, false, nil)
		require.Nil(t, status)
		require.Len(t, transactions, n)
		for i, txn := range transactions {
			require.NotNil(t, txn, "tx %d was not decoded", i)
			require.Equal(t, uint64(i), txn.GetNonce())
		}
	})
	low, high := payloadDecodeChunk+5, 2*payloadDecodeChunk+5
	for _, test := range []struct {
		name string
		mark func(raw []hexutil.Bytes)
		want string
	}{
		{"gas limit in an earlier chunk than a decode error", func(raw []hexutil.Bytes) {
			overGas(raw, low)
			raw[high] = hexutil.Bytes{0x02, 0xff}
		}, protocol.ErrGasLimitReached.Error()},
		{"decode error in an earlier chunk than a gas limit", func(raw []hexutil.Bytes) {
			raw[low] = hexutil.Bytes{0x02, 0xff}
			overGas(raw, high)
		}, "rlp"},
		{"error in the partial last chunk", func(raw []hexutil.Bytes) {
			raw[n-1] = hexutil.Bytes{0x82, 0x02, 0x01}
		}, "typed txn marshalled as RLP string"},
	} {
		t.Run(test.name, func(t *testing.T) {
			raw := payload()
			test.mark(raw)
			_, _, _, status := srv.decodePayloadTransactions(raw, 30_000_000, false, false, nil)
			require.NotNil(t, status)
			require.Equal(t, engine_types.InvalidStatus, status.Status)
			require.ErrorContains(t, status.ValidationError.Error(), test.want)
		})
	}
}

func TestNewPayloadSkipsSendersRecoveryOnBlockHashMismatch(t *testing.T) {
	t.Parallel()

	config := preAmsterdamChainConfig()
	payload, _ := signedElectraPayload(t, config)
	payload.BlockHash = common.Hash{0x1}

	registered := false
	stub := &stubExecutionModule{addSendersRecoveryFunc: func(common.Hash, *exec.SendersRecovery) {
		registered = true
	}}
	srv := NewEngineServer(log.New(), config, stub, nil, false, false, false, true, nil, nil, 0, 0)
	status, err := srv.NewPayloadV4(t.Context(), payload, []common.Hash{}, &common.Hash{}, []hexutil.Bytes{})
	require.NoError(t, err)
	require.Equal(t, engine_types.InvalidStatus, status.Status)
	require.False(t, registered)
}

func TestNewPayloadV5RequiresBlockAccessListBeforeAmsterdam(t *testing.T) {
	t.Parallel()

	srv := NewEngineServer(log.New(), preAmsterdamChainConfig(), &stubExecutionModule{}, nil, false, false, false, true, nil, nil, 0, 0)
	zero := hexutil.Uint64(0)
	payload := &engine_types.ExecutionPayload{
		LogsBloom:     make(hexutil.Bytes, types.BloomByteLength),
		BaseFeePerGas: (*hexutil.U256)(uint256.NewInt(1)),
		Transactions:  []hexutil.Bytes{},
		Withdrawals:   []*types.Withdrawal{},
		BlobGasUsed:   &zero,
		ExcessBlobGas: &zero,
		SlotNumber:    &zero,
	}

	status, err := srv.NewPayloadV5(t.Context(), payload, []common.Hash{}, &common.Hash{}, []hexutil.Bytes{})
	require.Nil(t, status)
	require.Error(t, err)
	var invalidParams *rpc.InvalidParamsError
	require.ErrorAs(t, err, &invalidParams)
	require.Equal(t, "blockAccessList missing", invalidParams.Message)
}

func TestForkchoiceUpdatedReturnsSyncingForIncompleteExecution(t *testing.T) {
	t.Parallel()

	headHash := common.Hash{0x1}
	headNumber := uint64(1)
	header := &types.Header{}
	header.Number.SetUint64(headNumber)

	for _, executionStatus := range []execmodule.ExecutionStatus{
		execmodule.ExecutionStatusMissingSegment,
		execmodule.ExecutionStatusTooFarAway,
	} {
		t.Run(executionStatus.String(), func(t *testing.T) {
			t.Parallel()

			stub := &stubExecutionModule{
				getHeaderFunc: getHeaderReturning(headHash, header),
				headerNumberFunc: func(context.Context, common.Hash) (*uint64, error) {
					return &headNumber, nil
				},
				updateForkChoiceFunc: func(context.Context, common.Hash, common.Hash, common.Hash) (execmodule.ForkChoiceResult, error) {
					return execmodule.ForkChoiceResult{
						Status:          executionStatus,
						LatestValidHash: common.Hash{0x2},
						ValidationError: "not validated",
					}, nil
				},
			}
			cfg := preCancunChainConfig()
			ctx := context.Background()
			downloader := engine_block_downloader.NewEngineBlockDownloader(ctx, log.New(), stub, nil, nil, cfg, ethconfig.Sync{}, nil)
			srv := NewEngineServer(log.New(), cfg, stub, downloader, false, false, false, true, nil, nil, 0, 0)

			resp, err := srv.ForkchoiceUpdatedV1(ctx, &engine_types.ForkChoiceState{HeadHash: headHash}, nil)
			require.NoError(t, err)
			require.Equal(t, engine_types.SyncingStatus, resp.PayloadStatus.Status)
			require.Nil(t, resp.PayloadStatus.LatestValidHash)
			require.Nil(t, resp.PayloadStatus.ValidationError)
			require.Nil(t, resp.PayloadId)
		})
	}
}
