// Copyright 2024 The Erigon Authors
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

package sync

import (
	"context"
	"fmt"
	"time"

	"google.golang.org/protobuf/types/known/emptypb"

	"github.com/erigontech/erigon-lib/gointerfaces"
	"github.com/erigontech/erigon-lib/gointerfaces/executionproto"
	"github.com/erigontech/erigon/core/types"
	eth1utils "github.com/erigontech/erigon/turbo/execution/eth1/eth1_utils"
)

type ExecutionClient interface {
	InsertBlocks(ctx context.Context, blocks []*types.Block) error
	UpdateForkChoice(ctx context.Context, tip *types.Header, finalizedHeader *types.Header) error
	CurrentHeader(ctx context.Context) (*types.Header, error)
	GetHeader(ctx context.Context, blockNum uint64) (*types.Header, error)
}

type executionClient struct {
	client executionproto.ExecutionClient
}

func NewExecutionClient(client executionproto.ExecutionClient) ExecutionClient {
	return &executionClient{client}
}

func (e *executionClient) InsertBlocks(ctx context.Context, blocks []*types.Block) error {
	request := &executionproto.InsertBlocksRequest{
		Blocks: eth1utils.ConvertBlocksToRPC(blocks),
	}

	for {
		response, err := e.client.InsertBlocks(ctx, request)
		if err != nil {
			return err
		}

		status := response.Result
		switch status {
		case executionproto.ExecutionStatus_Success:
			return nil
		case executionproto.ExecutionStatus_Busy:
			// retry after sleep
			func() {
				delayTimer := time.NewTimer(100 * time.Millisecond)
				defer delayTimer.Stop()

				select {
				case <-delayTimer.C:
				case <-ctx.Done():
				}
			}()
		default:
			return fmt.Errorf("executionClient.InsertBlocks failed with response status: %s", status.String())
		}
	}
}

func (e *executionClient) UpdateForkChoice(ctx context.Context, tip *types.Header, finalizedHeader *types.Header) error {
	tipHash := tip.Hash()
	const timeout = 5 * time.Second

	request := executionproto.ForkChoice{
		HeadBlockHash:      gointerfaces.ConvertHashToH256(tipHash),
		SafeBlockHash:      gointerfaces.ConvertHashToH256(tipHash),
		FinalizedBlockHash: gointerfaces.ConvertHashToH256(finalizedHeader.Hash()),
		Timeout:            uint64(timeout.Milliseconds()),
	}

	response, err := e.client.UpdateForkChoice(ctx, &request)
	if err != nil {
		return err
	}

	if len(response.ValidationError) > 0 {
		return fmt.Errorf("executionClient.UpdateForkChoice failed with a validation error: %s", response.ValidationError)
	}
	return nil
}

func (e *executionClient) CurrentHeader(ctx context.Context) (*types.Header, error) {
	response, err := e.client.CurrentHeader(ctx, &emptypb.Empty{})
	if err != nil {
		return nil, err
	}
	if (response == nil) || (response.Header == nil) {
		return nil, nil
	}
	return eth1utils.HeaderRpcToHeader(response.Header)
}

func (e *executionClient) GetHeader(ctx context.Context, blockNum uint64) (*types.Header, error) {
	response, err := e.client.GetHeader(ctx, &executionproto.GetSegmentRequest{
		BlockNumber: &blockNum,
	})
	if err != nil {
		return nil, err
	}

	headerRpc := response.GetHeader()
	if headerRpc == nil {
		return nil, nil
	}

	header, err := eth1utils.HeaderRpcToHeader(headerRpc)
	if err != nil {
		return nil, err
	}

	return header, nil
}
