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

	"github.com/erigontech/erigon/common"
	"github.com/erigontech/erigon/common/log/v3"
	"github.com/erigontech/erigon/execution/execmodule"
	"github.com/erigontech/erigon/execution/types"
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

func newExecutionClient(logger log.Logger, client execmodule.ExecutionModule) *executionClient {
	return &executionClient{
		logger: logger,
		client: client,
	}
}

type executionClient struct {
	logger log.Logger
	client execmodule.ExecutionModule
}

func (e *executionClient) Prepare(ctx context.Context) error {
	return e.retryBusy(ctx, "ready", func() error {
		ready, err := e.client.Ready(ctx)
		if err != nil {
			return err
		}

		if !ready {
			return ErrExecutionClientBusy // gets retried
		}

		return nil
	})
}

func (e *executionClient) InsertBlocks(ctx context.Context, blocks []*types.Block) error {
	rawBlocks := make([]*types.RawBlock, len(blocks))
	for i, blk := range blocks {
		rawBlocks[i] = &types.RawBlock{
			Header: blk.HeaderNoCopy(),
			Body:   blk.RawBody(),
		}
	}

	return e.retryBusy(ctx, "insertBlocks", func() error {
		status, err := e.client.InsertBlocks(ctx, rawBlocks)
		if err != nil {
			return err
		}

		switch status {
		case execmodule.ExecutionStatusSuccess:
			return nil
		case execmodule.ExecutionStatusBusy:
			return ErrExecutionClientBusy // gets retried
		default:
			return fmt.Errorf("executionClient.InsertBlocks failure status: %s", status.String())
		}
	})
}

func (e *executionClient) UpdateForkChoice(ctx context.Context, tip *types.Header, finalizedHeader *types.Header) (common.Hash, error) {
	tipHash := tip.Hash()
	finalizedHash := finalizedHeader.Hash()

	var latestValidHash common.Hash
	err := e.retryBusy(ctx, "updateForkChoice", func() error {
		r, err := e.client.UpdateForkChoice(ctx, tipHash, tipHash, finalizedHash)
		if err != nil {
			return err
		}

		latestValidHash = r.LatestValidHash

		switch r.Status {
		case execmodule.ExecutionStatusSuccess:
			return nil
		case execmodule.ExecutionStatusBadBlock:
			return fmt.Errorf("%w: status=%s, validationErr='%s'", ErrForkChoiceUpdateBadBlock, r.Status, r.ValidationError)
		case execmodule.ExecutionStatusBusy:
			return ErrExecutionClientBusy // gets retried
		case execmodule.ExecutionStatusTooFarAway:
			return ErrForkChoiceUpdateTooFarBehind
		default:
			return fmt.Errorf("%w: status=%s, validationErr='%s'", ErrForkChoiceUpdateFailure, r.Status, r.ValidationError)
		}
	})

	return latestValidHash, err
}

func (e *executionClient) CurrentHeader(ctx context.Context) (*types.Header, error) {
	return e.client.CurrentHeader(ctx)
}

func (e *executionClient) GetHeader(ctx context.Context, blockNum uint64) (*types.Header, error) {
	return e.client.GetHeader(ctx, nil, &blockNum)
}

func (e *executionClient) GetTd(ctx context.Context, blockNum uint64, blockHash common.Hash) (*big.Int, error) {
	return e.client.GetTD(ctx, &blockHash, &blockNum)
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
