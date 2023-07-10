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
)

type ExecutionClientDirect struct {
	server execution.ExecutionServer
}

func NewExecutionClientDirect(server execution.ExecutionServer) *ExecutionClientDirect {
	return &ExecutionClientDirect{server: server}
}

func (s *ExecutionClientDirect) AssembleBlock(ctx context.Context, in *execution.EmptyMessage, opts ...grpc.CallOption) (*types.ExecutionPayload, error) {
	return s.server.AssembleBlock(ctx, in)
}

// Chain Putters.
func (s *ExecutionClientDirect) InsertHeaders(ctx context.Context, in *execution.InsertHeadersRequest, opts ...grpc.CallOption) (*execution.EmptyMessage, error) {
	return s.server.InsertHeaders(ctx, in)
}

func (s *ExecutionClientDirect) InsertBodies(ctx context.Context, in *execution.InsertBodiesRequest, opts ...grpc.CallOption) (*execution.EmptyMessage, error) {
	return s.server.InsertBodies(ctx, in)
}

// Chain Validation and ForkChoice.
func (s *ExecutionClientDirect) ValidateChain(ctx context.Context, in *types.H256, opts ...grpc.CallOption) (*execution.ValidationReceipt, error) {
	return s.server.ValidateChain(ctx, in)

}

func (s *ExecutionClientDirect) UpdateForkChoice(ctx context.Context, in *types.H256, opts ...grpc.CallOption) (*execution.ForkChoiceReceipt, error) {
	return s.server.UpdateForkChoice(ctx, in)
}

// Chain Getters.
func (s *ExecutionClientDirect) GetHeader(ctx context.Context, in *execution.GetSegmentRequest, opts ...grpc.CallOption) (*execution.GetHeaderResponse, error) {
	return s.server.GetHeader(ctx, in)
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
