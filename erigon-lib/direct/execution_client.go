/*
   Copyright 2021 Erigon contributors

   Licensed under the Apache License, Version 2.0 (the "License");
   you may not use this file except in compliance with the License.
   You may obtain a copy of the License at

       http://www.apache.org/licenses/LICENSE-2.0

   Unless required by applicable law or agreed to in writing, software
   distributed under the License is distributed on an "AS IS" BASIS,
   WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
   See the License for the specific language governing permissions and
   limitations under the License.
*/

package direct

import (
	"context"

	"github.com/ledgerwatch/erigon-lib/gointerfaces/execution"
	"github.com/ledgerwatch/erigon-lib/gointerfaces/types"
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
