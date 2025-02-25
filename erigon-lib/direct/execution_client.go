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

	execution "github.com/erigontech/erigon-lib/gointerfaces/executionproto"
	types "github.com/erigontech/erigon-lib/gointerfaces/typesproto"
	"google.golang.org/grpc"
	"google.golang.org/protobuf/types/known/emptypb"
)

type ExecutionClientDirect struct {
	server execution.ExecutionServer
}

func NewExecutionClientDirect(server execution.ExecutionServer) execution.ExecutionClient {
	return &ExecutionClientDirect{server: server}
}

func (s *ExecutionClientDirect) AssembleBlock(ctx context.Context, in *execution.AssembleBlockRequest, opts ...grpc.CallOption) (*execution.AssembleBlockResponse, error) {
	return s.server.AssembleBlock(ctx, in)
}

func (s *ExecutionClientDirect) GetBodiesByHashes(ctx context.Context, in *execution.GetBodiesByHashesRequest, opts ...grpc.CallOption) (*execution.GetBodiesBatchResponse, error) {
	return s.server.GetBodiesByHashes(ctx, in)
}

func (s *ExecutionClientDirect) GetBodiesByRange(ctx context.Context, in *execution.GetBodiesByRangeRequest, opts ...grpc.CallOption) (*execution.GetBodiesBatchResponse, error) {
	return s.server.GetBodiesByRange(ctx, in)
}

func (s *ExecutionClientDirect) HasBlock(ctx context.Context, in *execution.GetSegmentRequest, opts ...grpc.CallOption) (*execution.HasBlockResponse, error) {
	return s.server.HasBlock(ctx, in)
}

func (s *ExecutionClientDirect) GetAssembledBlock(ctx context.Context, in *execution.GetAssembledBlockRequest, opts ...grpc.CallOption) (*execution.GetAssembledBlockResponse, error) {
	return s.server.GetAssembledBlock(ctx, in)
}

// Chain Putters.
func (s *ExecutionClientDirect) InsertBlocks(ctx context.Context, in *execution.InsertBlocksRequest, opts ...grpc.CallOption) (*execution.InsertionResult, error) {
	return s.server.InsertBlocks(ctx, in)
}

// Chain Validation and ForkChoice.
func (s *ExecutionClientDirect) ValidateChain(ctx context.Context, in *execution.ValidationRequest, opts ...grpc.CallOption) (*execution.ValidationReceipt, error) {
	return s.server.ValidateChain(ctx, in)

}

func (s *ExecutionClientDirect) UpdateForkChoice(ctx context.Context, in *execution.ForkChoice, opts ...grpc.CallOption) (*execution.ForkChoiceReceipt, error) {
	return s.server.UpdateForkChoice(ctx, in)
}

// Chain Getters.
func (s *ExecutionClientDirect) GetHeader(ctx context.Context, in *execution.GetSegmentRequest, opts ...grpc.CallOption) (*execution.GetHeaderResponse, error) {
	return s.server.GetHeader(ctx, in)
}

func (s *ExecutionClientDirect) CurrentHeader(ctx context.Context, in *emptypb.Empty, opts ...grpc.CallOption) (*execution.GetHeaderResponse, error) {
	return s.server.CurrentHeader(ctx, in)
}

func (s *ExecutionClientDirect) GetTD(ctx context.Context, in *execution.GetSegmentRequest, opts ...grpc.CallOption) (*execution.GetTDResponse, error) {
	return s.server.GetTD(ctx, in)
}

func (s *ExecutionClientDirect) GetBody(ctx context.Context, in *execution.GetSegmentRequest, opts ...grpc.CallOption) (*execution.GetBodyResponse, error) {
	return s.server.GetBody(ctx, in)
}

func (s *ExecutionClientDirect) IsCanonicalHash(ctx context.Context, in *types.H256, opts ...grpc.CallOption) (*execution.IsCanonicalResponse, error) {
	return s.server.IsCanonicalHash(ctx, in)
}

func (s *ExecutionClientDirect) GetHeaderHashNumber(ctx context.Context, in *types.H256, opts ...grpc.CallOption) (*execution.GetHeaderHashNumberResponse, error) {
	return s.server.GetHeaderHashNumber(ctx, in)
}

func (s *ExecutionClientDirect) GetForkChoice(ctx context.Context, in *emptypb.Empty, opts ...grpc.CallOption) (*execution.ForkChoice, error) {
	return s.server.GetForkChoice(ctx, in)
}

func (s *ExecutionClientDirect) Ready(ctx context.Context, in *emptypb.Empty, opts ...grpc.CallOption) (*execution.ReadyResponse, error) {
	return s.server.Ready(ctx, in)
}

func (s *ExecutionClientDirect) FrozenBlocks(ctx context.Context, in *emptypb.Empty, opts ...grpc.CallOption) (*execution.FrozenBlocksResponse, error) {
	return s.server.FrozenBlocks(ctx, in)
}
