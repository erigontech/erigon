// Copyright 2021 The Erigon Authors
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

package direct

import (
	"context"

	"google.golang.org/grpc"
	"google.golang.org/protobuf/types/known/emptypb"

	"github.com/erigontech/erigon-lib/gointerfaces/executionproto"
	"github.com/erigontech/erigon-lib/gointerfaces/typesproto"
)

type ExecutionClientDirect struct {
	server executionproto.ExecutionServer
}

func NewExecutionClientDirect(server executionproto.ExecutionServer) executionproto.ExecutionClient {
	return &ExecutionClientDirect{server: server}
}

func (s *ExecutionClientDirect) AssembleBlock(ctx context.Context, in *executionproto.AssembleBlockRequest, opts ...grpc.CallOption) (*executionproto.AssembleBlockResponse, error) {
	return s.server.AssembleBlock(ctx, in)
}

func (s *ExecutionClientDirect) GetBodiesByHashes(ctx context.Context, in *executionproto.GetBodiesByHashesRequest, opts ...grpc.CallOption) (*executionproto.GetBodiesBatchResponse, error) {
	return s.server.GetBodiesByHashes(ctx, in)
}

func (s *ExecutionClientDirect) GetBodiesByRange(ctx context.Context, in *executionproto.GetBodiesByRangeRequest, opts ...grpc.CallOption) (*executionproto.GetBodiesBatchResponse, error) {
	return s.server.GetBodiesByRange(ctx, in)
}

func (s *ExecutionClientDirect) HasBlock(ctx context.Context, in *executionproto.GetSegmentRequest, opts ...grpc.CallOption) (*executionproto.HasBlockResponse, error) {
	return s.server.HasBlock(ctx, in)
}

func (s *ExecutionClientDirect) GetAssembledBlock(ctx context.Context, in *executionproto.GetAssembledBlockRequest, opts ...grpc.CallOption) (*executionproto.GetAssembledBlockResponse, error) {
	return s.server.GetAssembledBlock(ctx, in)
}

// Chain Putters.
func (s *ExecutionClientDirect) InsertBlocks(ctx context.Context, in *executionproto.InsertBlocksRequest, opts ...grpc.CallOption) (*executionproto.InsertionResult, error) {
	return s.server.InsertBlocks(ctx, in)
}

// Chain Validation and ForkChoice.
func (s *ExecutionClientDirect) ValidateChain(ctx context.Context, in *executionproto.ValidationRequest, opts ...grpc.CallOption) (*executionproto.ValidationReceipt, error) {
	return s.server.ValidateChain(ctx, in)

}

func (s *ExecutionClientDirect) UpdateForkChoice(ctx context.Context, in *executionproto.ForkChoice, opts ...grpc.CallOption) (*executionproto.ForkChoiceReceipt, error) {
	return s.server.UpdateForkChoice(ctx, in)
}

// Chain Getters.
func (s *ExecutionClientDirect) GetHeader(ctx context.Context, in *executionproto.GetSegmentRequest, opts ...grpc.CallOption) (*executionproto.GetHeaderResponse, error) {
	return s.server.GetHeader(ctx, in)
}

func (s *ExecutionClientDirect) CurrentHeader(ctx context.Context, in *emptypb.Empty, opts ...grpc.CallOption) (*executionproto.GetHeaderResponse, error) {
	return s.server.CurrentHeader(ctx, in)
}

func (s *ExecutionClientDirect) GetTD(ctx context.Context, in *executionproto.GetSegmentRequest, opts ...grpc.CallOption) (*executionproto.GetTDResponse, error) {
	return s.server.GetTD(ctx, in)
}

func (s *ExecutionClientDirect) GetBody(ctx context.Context, in *executionproto.GetSegmentRequest, opts ...grpc.CallOption) (*executionproto.GetBodyResponse, error) {
	return s.server.GetBody(ctx, in)
}

func (s *ExecutionClientDirect) IsCanonicalHash(ctx context.Context, in *typesproto.H256, opts ...grpc.CallOption) (*executionproto.IsCanonicalResponse, error) {
	return s.server.IsCanonicalHash(ctx, in)
}

func (s *ExecutionClientDirect) GetHeaderHashNumber(ctx context.Context, in *typesproto.H256, opts ...grpc.CallOption) (*executionproto.GetHeaderHashNumberResponse, error) {
	return s.server.GetHeaderHashNumber(ctx, in)
}

func (s *ExecutionClientDirect) GetForkChoice(ctx context.Context, in *emptypb.Empty, opts ...grpc.CallOption) (*executionproto.ForkChoice, error) {
	return s.server.GetForkChoice(ctx, in)
}

func (s *ExecutionClientDirect) Ready(ctx context.Context, in *emptypb.Empty, opts ...grpc.CallOption) (*executionproto.ReadyResponse, error) {
	return s.server.Ready(ctx, in)
}

func (s *ExecutionClientDirect) FrozenBlocks(ctx context.Context, in *emptypb.Empty, opts ...grpc.CallOption) (*executionproto.FrozenBlocksResponse, error) {
	return s.server.FrozenBlocks(ctx, in)
}
