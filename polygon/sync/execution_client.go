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
	"errors"
	"fmt"
	"math/big"
	"time"

	"google.golang.org/protobuf/types/known/emptypb"

	"github.com/erigontech/erigon-lib/common"
	"github.com/erigontech/erigon-lib/gointerfaces"
	"github.com/erigontech/erigon-lib/gointerfaces/executionproto"
	"github.com/erigontech/erigon/core/types"
	eth1utils "github.com/erigontech/erigon/turbo/execution/eth1/eth1_utils"
	"github.com/erigontech/erigon/turbo/snapshotsync/freezeblocks"
)

var ErrForkChoiceUpdateFailure = errors.New("fork choice update failure")
var ErrForkChoiceUpdateBadBlock = errors.New("fork choice update bad block")

type ExecutionClient interface {
	Prepare(ctx context.Context) error
	InsertBlocks(ctx context.Context, blocks []*types.Block) error
	UpdateForkChoice(ctx context.Context, tip *types.Header, finalizedHeader *types.Header) (common.Hash, error)
	CurrentHeader(ctx context.Context) (*types.Header, error)
	GetHeader(ctx context.Context, blockNum uint64) (*types.Header, error)
	GetTd(ctx context.Context, blockNum uint64, blockHash common.Hash) (*big.Int, error)
}

type executionClient struct {
	client      executionproto.ExecutionClient
	blockReader *freezeblocks.BlockReader
}

func NewExecutionClient(client executionproto.ExecutionClient, blockReader *freezeblocks.BlockReader) ExecutionClient {
	return &executionClient{client, blockReader}
}

func (e *executionClient) Prepare(ctx context.Context) error {
	ready, err := e.client.Ready(ctx, &emptypb.Empty{})

	if err != nil {
		return err
	}

	if !ready.Ready {
		return fmt.Errorf("excecution client no ready")
	}

	return nil
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

func (e *executionClient) UpdateForkChoice(ctx context.Context, tip *types.Header, finalizedHeader *types.Header) (common.Hash, error) {
	tipHash := tip.Hash()

	request := executionproto.ForkChoice{
		HeadBlockHash:      gointerfaces.ConvertHashToH256(tipHash),
		SafeBlockHash:      gointerfaces.ConvertHashToH256(tipHash),
		FinalizedBlockHash: gointerfaces.ConvertHashToH256(finalizedHeader.Hash()),
		Timeout:            0,
	}

	response, err := e.client.UpdateForkChoice(ctx, &request)
	if err != nil {
		return common.Hash{}, err
	}

	var latestValidHash common.Hash
	if response.LatestValidHash != nil {
		latestValidHash = gointerfaces.ConvertH256ToHash(response.LatestValidHash)
	}

	switch response.Status {
	case executionproto.ExecutionStatus_Success:
		return latestValidHash, nil
	case executionproto.ExecutionStatus_BadBlock:
		return latestValidHash, fmt.Errorf(
			"%w: status=%d, validationErr='%s'",
			ErrForkChoiceUpdateBadBlock,
			response.Status,
			response.ValidationError,
		)
	default:
		return latestValidHash, fmt.Errorf(
			"%w: status=%d, validationErr='%s'",
			ErrForkChoiceUpdateFailure,
			response.Status,
			response.ValidationError,
		)
	}
}

func (e *executionClient) CurrentHeader(ctx context.Context) (*types.Header, error) {
	response, err := e.client.CurrentHeader(ctx, &emptypb.Empty{})
	if err != nil {
		return nil, err
	}

	if (response == nil) || (response.Header == nil) || response.Header.BlockNumber == 0 {
		return e.blockReader.HeaderByNumber(ctx, nil, e.blockReader.FrozenBlocks())
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

func (e *executionClient) GetTd(ctx context.Context, blockNum uint64, blockHash common.Hash) (*big.Int, error) {
	response, err := e.client.GetTD(ctx, &executionproto.GetSegmentRequest{
		BlockNumber: &blockNum,
		BlockHash:   gointerfaces.ConvertHashToH256(blockHash),
	})
	if err != nil {
		return nil, err
	}

	return eth1utils.ConvertBigIntFromRpc(response.GetTd()), nil
}
