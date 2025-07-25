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

	"github.com/cenkalti/backoff/v4"
	"google.golang.org/protobuf/types/known/emptypb"

	"github.com/erigontech/erigon-lib/common"
	"github.com/erigontech/erigon-lib/gointerfaces"
	"github.com/erigontech/erigon-lib/gointerfaces/executionproto"
	"github.com/erigontech/erigon-lib/log/v3"
	"github.com/erigontech/erigon-lib/types"
	eth1utils "github.com/erigontech/erigon/execution/eth1/eth1_utils"
)

var (
	ErrForkChoiceUpdateFailure      = errors.New("fork choice update failure")
	ErrForkChoiceUpdateBadBlock     = errors.New("fork choice update bad block")
	ErrForkChoiceUpdateTooFarBehind = errors.New("fork choice update is too far behind")

	ErrExecutionClientBusy = errors.New("execution client busy")
)

type ExecutionClient interface {
	Prepare(ctx context.Context) error
	InsertBlocks(ctx context.Context, blocks []*types.Block) error
	UpdateForkChoice(ctx context.Context, tip *types.Header, finalizedHeader *types.Header) (common.Hash, error)
	CurrentHeader(ctx context.Context) (*types.Header, error)
	GetHeader(ctx context.Context, blockNum uint64) (*types.Header, error)
	GetTd(ctx context.Context, blockNum uint64, blockHash common.Hash) (*big.Int, error)
}

func newExecutionClient(logger log.Logger, client executionproto.ExecutionClient) *executionClient {
	return &executionClient{
		logger: logger,
		client: client,
	}
}

type executionClient struct {
	logger log.Logger
	client executionproto.ExecutionClient
}

func (e *executionClient) Prepare(ctx context.Context) error {
	return e.retryBusy(ctx, "ready", func() error {
		ready, err := e.client.Ready(ctx, &emptypb.Empty{})
		if err != nil {
			return err
		}

		if !ready.Ready {
			return ErrExecutionClientBusy // gets retried
		}

		return nil
	})
}

func (e *executionClient) InsertBlocks(ctx context.Context, blocks []*types.Block) error {
	request := &executionproto.InsertBlocksRequest{
		Blocks: eth1utils.ConvertBlocksToRPC(blocks),
	}

	return e.retryBusy(ctx, "insertBlocks", func() error {
		response, err := e.client.InsertBlocks(ctx, request)
		if err != nil {
			return err
		}

		status := response.Result
		switch status {
		case executionproto.ExecutionStatus_Success:
			return nil
		case executionproto.ExecutionStatus_Busy:
			return ErrExecutionClientBusy // gets retried
		default:
			return fmt.Errorf("executionClient.InsertBlocks failure status: %s", status.String())
		}
	})
}

func (e *executionClient) UpdateForkChoice(ctx context.Context, tip *types.Header, finalizedHeader *types.Header) (common.Hash, error) {
	tipHash := tip.Hash()

	request := executionproto.ForkChoice{
		HeadBlockHash:      gointerfaces.ConvertHashToH256(tipHash),
		SafeBlockHash:      gointerfaces.ConvertHashToH256(tipHash),
		FinalizedBlockHash: gointerfaces.ConvertHashToH256(finalizedHeader.Hash()),
		Timeout:            0,
	}

	var latestValidHash common.Hash
	err := e.retryBusy(ctx, "updateForkChoice", func() error {
		r, err := e.client.UpdateForkChoice(ctx, &request)
		if err != nil {
			return err
		}

		if r.LatestValidHash != nil {
			latestValidHash = gointerfaces.ConvertH256ToHash(r.LatestValidHash)
		}

		switch r.Status {
		case executionproto.ExecutionStatus_Success:
			return nil
		case executionproto.ExecutionStatus_BadBlock:
			return fmt.Errorf("%w: status=%d, validationErr='%s'", ErrForkChoiceUpdateBadBlock, r.Status, r.ValidationError)
		case executionproto.ExecutionStatus_Busy:
			return ErrExecutionClientBusy // gets retried
		case executionproto.ExecutionStatus_TooFarAway:
			return ErrForkChoiceUpdateTooFarBehind
		default:
			return fmt.Errorf("%w: status=%d, validationErr='%s'", ErrForkChoiceUpdateFailure, r.Status, r.ValidationError)
		}
	})

	return latestValidHash, err
}

func (e *executionClient) CurrentHeader(ctx context.Context) (*types.Header, error) {
	response, err := e.client.CurrentHeader(ctx, &emptypb.Empty{})
	if err != nil {
		return nil, err
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

func (e *executionClient) retryBusy(ctx context.Context, label string, f func() error) error {
	backOff := 50 * time.Millisecond
	logEvery := 5 * time.Second
	logEveryXAttempt := int64(logEvery / backOff)
	attempt := int64(1)
	operation := func() error {
		err := f()
		if err == nil {
			return nil
		}

		if errors.Is(err, ErrExecutionClientBusy) {
			if attempt%logEveryXAttempt == 1 {
				e.logger.Debug(syncLogPrefix("execution client busy - retrying"), "in", backOff, "label", label, "attempt", attempt)
			}
			attempt++
			return err
		}

		return backoff.Permanent(err)
	}

	return backoff.Retry(operation, backoff.WithContext(backoff.NewConstantBackOff(backOff), ctx))
}
