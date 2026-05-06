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
	"math/big"
	"testing"

	"github.com/holiman/uint256"
	"github.com/stretchr/testify/require"

	"github.com/erigontech/erigon/common"
	"github.com/erigontech/erigon/common/hexutil"
	"github.com/erigontech/erigon/common/log/v3"
	"github.com/erigontech/erigon/execution/builder"
	"github.com/erigontech/erigon/execution/execmodule"
	"github.com/erigontech/erigon/execution/types"
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

func newProposingEngineServerForGetPayloadTests(stub execmodule.ExecutionModule) *EngineServer {
	cfg := allForksChainConfig()
	// GetPayloadV4 is valid on Prague but invalid once Osaka activates.
	cfg.OsakaTime = nil
	cfg.AmsterdamTime = nil

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
		0,     // fcuTimeout
		0,     // maxReorgDepth
	)
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
func (s *getPayloadStubModule) GetTD(_ context.Context, _ *common.Hash, _ *uint64) (*big.Int, error) {
	panic("not implemented")
}
func (s *getPayloadStubModule) FrozenBlocks(_ context.Context) (uint64, bool, error) {
	panic("not implemented")
}
