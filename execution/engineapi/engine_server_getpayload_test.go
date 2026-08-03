// Copyright 2026 The Erigon Authors
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
	"encoding/binary"
	"testing"

	"github.com/holiman/uint256"
	"github.com/stretchr/testify/require"

	"github.com/erigontech/erigon/cl/clparams"
	"github.com/erigontech/erigon/common"
	"github.com/erigontech/erigon/common/empty"
	"github.com/erigontech/erigon/common/hexutil"
	"github.com/erigontech/erigon/common/log/v3"
	"github.com/erigontech/erigon/execution/builder"
	"github.com/erigontech/erigon/execution/chain"
	"github.com/erigontech/erigon/execution/execmodule"
	"github.com/erigontech/erigon/execution/types"
	"github.com/erigontech/erigon/rpc"
)

func TestGetPayloadV4RejectsNilRequests(t *testing.T) {
	t.Parallel()

	const payloadID uint64 = 42
	stub := &getPayloadStubModule{
		getAssembledBlockFunc: func(_ context.Context, id uint64) (execmodule.AssembledBlockResult, error) {
			require.Equal(t, payloadID, id)
			return execmodule.AssembledBlockResult{
				Block: minimalPragueBlock(1, nil /* nil Requests */),
			}, nil
		},
	}

	srv := newProposingEngineServerForGetPayloadTests(stub)
	resp, err := srv.GetPayloadV4(context.Background(), payloadIDBytes(payloadID))

	require.Nil(t, resp)
	require.ErrorContains(t, err, "missing execution requests")
}

func TestGetPayloadV4AcceptsEmptyRequestsBundle(t *testing.T) {
	t.Parallel()

	const payloadID uint64 = 43
	stub := &getPayloadStubModule{
		getAssembledBlockFunc: func(_ context.Context, id uint64) (execmodule.AssembledBlockResult, error) {
			require.Equal(t, payloadID, id)
			return execmodule.AssembledBlockResult{
				Block: minimalPragueBlock(1, make(types.FlatRequests, 0)),
			}, nil
		},
	}

	srv := newProposingEngineServerForGetPayloadTests(stub)
	resp, err := srv.GetPayloadV4(context.Background(), payloadIDBytes(payloadID))

	require.NoError(t, err)
	require.NotNil(t, resp)
	require.NotNil(t, resp.ExecutionRequests)
	require.Len(t, resp.ExecutionRequests, 0)
}

func TestGetPayloadRejectsInvalidAmsterdamBlobsBundle(t *testing.T) {
	const payloadID uint64 = 44
	cfg := allForksChainConfig()
	to := common.Address{0x01}
	wrappedTxn := &types.BlobTxWrapper{}
	wrappedTxn.Tx.To = &to
	wrappedTxn.Tx.ChainID = *cfg.ChainID
	wrappedTxn.Tx.BlobVersionedHashes = []common.Hash{{0x01}, {0x01}}
	wrappedTxn.Commitments = make(types.BlobKzgs, 2)
	wrappedTxn.Blobs = make(types.Blobs, 2)
	wrappedTxn.Proofs = make(types.KZGProofs, 1)

	header := &types.Header{
		Number:   *uint256.NewInt(101),
		Time:     1,
		BaseFee:  uint256.NewInt(1_000_000_000),
		GasLimit: 30_000_000,
	}
	block := types.NewBlock(header, []types.Transaction{wrappedTxn}, nil, nil, nil)
	stub := &getPayloadStubModule{
		getAssembledBlockFunc: func(_ context.Context, id uint64) (execmodule.AssembledBlockResult, error) {
			require.Equal(t, payloadID, id)
			return execmodule.AssembledBlockResult{
				Block: &types.BlockWithReceipts{
					Block:    block,
					Requests: make(types.FlatRequests, 0),
				},
				BlockValue: uint256.NewInt(0),
			}, nil
		},
	}
	srv := NewEngineServer(log.New(), cfg, stub, nil, false, false, true, true, nil, nil, 0, 0)

	resp, err := srv.getPayload(context.Background(), payloadID, clparams.GloasVersion)

	require.ErrorContains(t, err, "built invalid blobsBundle")
	require.Nil(t, resp)
}

func TestGetPayloadRejectsParisShanghaiMismatch(t *testing.T) {
	t.Parallel()

	const payloadID uint64 = 44
	for _, tc := range []struct {
		name      string
		timestamp uint64
		version   clparams.StateVersion
	}{
		{"Shanghai payload with Paris schema", 100, clparams.BellatrixVersion},
		{"Paris payload with Shanghai schema", 99, clparams.CapellaVersion},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			stub := &getPayloadStubModule{
				getAssembledBlockFunc: func(_ context.Context, id uint64) (execmodule.AssembledBlockResult, error) {
					require.Equal(t, payloadID, id)
					return execmodule.AssembledBlockResult{
						Block:      minimalPragueBlock(tc.timestamp, nil),
						BlockValue: uint256.NewInt(0),
					}, nil
				},
			}
			srv := newProposingEngineServerWithConfig(parisShanghaiChainConfig(), stub)

			resp, err := srv.getPayload(context.Background(), payloadID, tc.version)

			require.Nil(t, resp)
			var unsupported *rpc.UnsupportedForkError
			require.ErrorAs(t, err, &unsupported)
		})
	}
}

func TestAssembledBlockToPayloadResponseIncludesCanonicalEmptyBAL(t *testing.T) {
	t.Parallel()

	baseFee := uint256.NewInt(1_000_000_000)
	emptyBALHash := empty.BlockAccessListHash
	header := &types.Header{
		Number:              *uint256.NewInt(101),
		Time:                1,
		BaseFee:             baseFee,
		GasLimit:            30_000_000,
		BlockAccessListHash: &emptyBALHash,
	}
	block := types.NewBlockWithHeader(header)
	br := &types.BlockWithReceipts{Block: block, BlockAccessList: make(types.BlockAccessList, 0), Requests: make(types.FlatRequests, 0)}

	resp, err := assembledBlockToPayloadResponse(br, uint256.NewInt(0), clparams.GloasVersion)
	require.NoError(t, err)

	emptyBAL, err := types.EncodeBlockAccessListBytes(make(types.BlockAccessList, 0))
	require.NoError(t, err)
	require.NotNil(t, resp.ExecutionPayload.BlockAccessList)
	require.Equal(t, hexutil.Bytes(emptyBAL), *resp.ExecutionPayload.BlockAccessList)
}

func newProposingEngineServerForGetPayloadTests(stub execmodule.ExecutionModule) *EngineServer {
	cfg := allForksChainConfig()
	// GetPayloadV4 is valid on Prague but invalid once Osaka activates.
	cfg.OsakaTime = nil
	cfg.AmsterdamTime = nil

	return newProposingEngineServerWithConfig(cfg, stub)
}

func newProposingEngineServerWithConfig(cfg *chain.Config, stub execmodule.ExecutionModule) *EngineServer {
	return NewEngineServer(
		log.New(),
		cfg,
		stub,
		nil,   // blockDownloader
		false, // caplin
		false, // internalCL
		true,  // proposing
		true,  // consuming
		nil,   // txPool
		nil,   // blobGetter
		0,     // fcuTimeout
		0,     // maxReorgDepth
	)
}

func parisShanghaiChainConfig() *chain.Config {
	cfg := allForksChainConfig()
	cfg.ShanghaiTime = common.NewUint64(100)
	cfg.CancunTime = nil
	cfg.PragueTime = nil
	cfg.OsakaTime = nil
	cfg.AmsterdamTime = nil
	return cfg
}

// minimalPragueBlock builds the smallest possible BlockWithReceipts for Prague
// (timestamp=1, BaseFee set, no transactions) with the given requests slice.
func minimalPragueBlock(timestamp uint64, requests types.FlatRequests) *types.BlockWithReceipts {
	baseFee := uint256.NewInt(1_000_000_000)
	header := &types.Header{
		Number:   *uint256.NewInt(101),
		Time:     timestamp,
		BaseFee:  baseFee,
		GasLimit: 30_000_000,
	}
	block := types.NewBlockWithHeader(header)
	return &types.BlockWithReceipts{
		Block:    block,
		Requests: requests,
	}
}

func payloadIDBytes(payloadID uint64) hexutil.Bytes {
	payloadBytes := make(hexutil.Bytes, 8)
	binary.BigEndian.PutUint64(payloadBytes, payloadID)
	return payloadBytes
}

// getPayloadStubModule is a minimal ExecutionModule stub for GetPayload tests.
// Only GetAssembledBlock and Ready are implemented; all other methods panic.
type getPayloadStubModule struct {
	getAssembledBlockFunc func(ctx context.Context, payloadID uint64) (execmodule.AssembledBlockResult, error)
}

func (s *getPayloadStubModule) GetAssembledBlock(ctx context.Context, payloadID uint64) (execmodule.AssembledBlockResult, error) {
	return s.getAssembledBlockFunc(ctx, payloadID)
}
func (s *getPayloadStubModule) Ready(_ context.Context) (bool, error) { return true, nil }
func (s *getPayloadStubModule) InsertBlocks(_ context.Context, _ []*types.RawBlock) (execmodule.ExecutionStatus, error) {
	panic("not implemented")
}
func (s *getPayloadStubModule) ValidateChain(_ context.Context, _ common.Hash, _ uint64) (execmodule.ValidationResult, error) {
	panic("not implemented")
}
func (s *getPayloadStubModule) UpdateForkChoice(_ context.Context, _, _, _ common.Hash) (execmodule.ForkChoiceResult, error) {
	panic("not implemented")
}
func (s *getPayloadStubModule) GetForkChoice(_ context.Context) (execmodule.ForkChoiceState, error) {
	panic("not implemented")
}
func (s *getPayloadStubModule) AssembleBlock(_ context.Context, _ *builder.Parameters) (execmodule.AssembleBlockResult, error) {
	panic("not implemented")
}
func (s *getPayloadStubModule) CurrentHeader(_ context.Context) (*types.Header, error) {
	panic("not implemented")
}
func (s *getPayloadStubModule) GetHeader(_ context.Context, _ *common.Hash, _ *uint64) (*types.Header, error) {
	panic("not implemented")
}
func (s *getPayloadStubModule) GetBody(_ context.Context, _ *common.Hash, _ *uint64) (*types.RawBody, error) {
	panic("not implemented")
}
func (s *getPayloadStubModule) HasBlock(_ context.Context, _ *common.Hash, _ *uint64) (bool, error) {
	panic("not implemented")
}
func (s *getPayloadStubModule) GetBodiesByRange(_ context.Context, _, _ uint64) ([]*types.RawBody, error) {
	panic("not implemented")
}
func (s *getPayloadStubModule) GetBodiesByHashes(_ context.Context, _ []common.Hash) ([]*types.RawBody, error) {
	panic("not implemented")
}
func (s *getPayloadStubModule) GetPayloadBodiesByHash(_ context.Context, _ []common.Hash) ([]*execmodule.PayloadBody, error) {
	panic("not implemented")
}
func (s *getPayloadStubModule) GetPayloadBodiesByRange(_ context.Context, _, _ uint64) ([]*execmodule.PayloadBody, error) {
	panic("not implemented")
}
func (s *getPayloadStubModule) IsCanonicalHash(_ context.Context, _ common.Hash) (bool, error) {
	panic("not implemented")
}
func (s *getPayloadStubModule) GetHeaderHashNumber(_ context.Context, _ common.Hash) (*uint64, error) {
	panic("not implemented")
}
func (s *getPayloadStubModule) GetTD(_ context.Context, _ *common.Hash, _ *uint64) (*uint256.Int, error) {
	panic("not implemented")
}
func (s *getPayloadStubModule) FrozenBlocks(_ context.Context) (uint64, bool, error) {
	panic("not implemented")
}
