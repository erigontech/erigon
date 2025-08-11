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

package execution_client

import (
	"context"
	"encoding/binary"
	"errors"
	"fmt"
	"math/big"
	"time"

	common "github.com/erigontech/erigon-lib/common"
	"github.com/erigontech/erigon-lib/common/hexutil"
	execution "github.com/erigontech/erigon-lib/gointerfaces/executionproto"
	"github.com/erigontech/erigon-lib/gointerfaces/typesproto"
	"github.com/erigontech/erigon/cl/clparams"
	"github.com/erigontech/erigon/cl/cltypes"
	"github.com/erigontech/erigon/cl/monitor"
	"github.com/erigontech/erigon/execution/engineapi/engine_types"
	"github.com/erigontech/erigon/execution/eth1/eth1_chain_reader"
	"github.com/erigontech/erigon/execution/types"
)

const reorgTooDeepDepth = 3

type ExecutionClientDirect struct {
	chainRW eth1_chain_reader.ChainReaderWriterEth1
}

func NewExecutionClientDirect(chainRW eth1_chain_reader.ChainReaderWriterEth1) (*ExecutionClientDirect, error) {
	return &ExecutionClientDirect{
		chainRW: chainRW,
	}, nil
}

func (cc *ExecutionClientDirect) NewPayload(
	ctx context.Context,
	payload *cltypes.Eth1Block,
	beaconParentRoot *common.Hash,
	versionedHashes []common.Hash,
	executionRequestsList []hexutil.Bytes,
) (PayloadStatus, error) {
	if payload == nil {
		return PayloadStatusValidated, nil
	}

	var requestsHash common.Hash
	if payload.Version() >= clparams.ElectraVersion {
		requestsHash = cltypes.ComputeExecutionRequestHash(executionRequestsList)
	}

	header, err := payload.RlpHeader(beaconParentRoot, requestsHash)
	if err != nil {
		// invalid block
		return PayloadStatusInvalidated, err
	}

	body := payload.Body()
	txs, err := types.DecodeTransactions(body.Transactions)
	if err != nil {
		// invalid block
		return PayloadStatusInvalidated, err
	}

	startInsertBlockAndWait := time.Now()
	if err := cc.chainRW.InsertBlockAndWait(ctx, types.NewBlockFromStorage(payload.BlockHash, header, txs, nil, body.Withdrawals)); err != nil {
		if errors.Is(err, types.ErrBlockExceedsMaxRlpSize) {
			return PayloadStatusInvalidated, err
		}
		return PayloadStatusNone, err
	}
	monitor.ObserveExecutionClientInsertingBlocks(startInsertBlockAndWait)

	headHeader := cc.chainRW.CurrentHeader(ctx)
	if headHeader == nil || header.Number.Uint64() > headHeader.Number.Uint64()+1 {
		// can't validate yet
		return PayloadStatusNotValidated, nil
	}

	// check if the block is too deep in the reorg accounting for underflow
	if headHeader.Number.Uint64() > reorgTooDeepDepth && header.Number.Uint64() < headHeader.Number.Uint64()-reorgTooDeepDepth {
		// reorg too deep
		return PayloadStatusNotValidated, nil
	}

	startValidateChain := time.Now()
	status, _, _, err := cc.chainRW.ValidateChain(ctx, payload.BlockHash, payload.BlockNumber)
	if err != nil {
		return PayloadStatusNone, err
	}
	monitor.ObserveExecutionClientValidateChain(startValidateChain)
	// check status
	switch status {
	case execution.ExecutionStatus_BadBlock, execution.ExecutionStatus_InvalidForkchoice:
		return PayloadStatusInvalidated, errors.New("bad block")
	case execution.ExecutionStatus_Busy, execution.ExecutionStatus_MissingSegment, execution.ExecutionStatus_TooFarAway:
		return PayloadStatusNotValidated, nil
	case execution.ExecutionStatus_Success:
		return PayloadStatusValidated, nil
	}
	return PayloadStatusNone, errors.New("unexpected status")
}

func (cc *ExecutionClientDirect) ForkChoiceUpdate(ctx context.Context, finalized, safe, head common.Hash, attr *engine_types.PayloadAttributes) ([]byte, error) {
	status, _, _, err := cc.chainRW.UpdateForkChoice(ctx, head, safe, finalized)
	if err != nil {
		return nil, fmt.Errorf("execution Client RPC failed to retrieve ForkChoiceUpdate response, err: %w", err)
	}
	if status == execution.ExecutionStatus_InvalidForkchoice {
		return nil, errors.New("forkchoice was invalid")
	}
	if status == execution.ExecutionStatus_BadBlock {
		return nil, errors.New("bad block as forkchoice")
	}
	if attr == nil {
		return nil, nil
	}
	idBytes := make([]byte, 8)
	id, err := cc.chainRW.AssembleBlock(head, attr)
	if err != nil {
		return nil, err
	}
	binary.LittleEndian.PutUint64(idBytes, id)
	return idBytes, nil
}

func (cc *ExecutionClientDirect) SupportInsertion() bool {
	return true
}

func (cc *ExecutionClientDirect) InsertBlocks(ctx context.Context, blocks []*types.Block, wait bool) error {
	if wait {
		return cc.chainRW.InsertBlocksAndWait(ctx, blocks)
	}
	return cc.chainRW.InsertBlocks(ctx, blocks)
}

func (cc *ExecutionClientDirect) InsertBlock(ctx context.Context, blk *types.Block) error {
	return cc.chainRW.InsertBlockAndWait(ctx, blk)
}

func (cc *ExecutionClientDirect) CurrentHeader(ctx context.Context) (*types.Header, error) {
	return cc.chainRW.CurrentHeader(ctx), nil
}

func (cc *ExecutionClientDirect) IsCanonicalHash(ctx context.Context, hash common.Hash) (bool, error) {
	return cc.chainRW.IsCanonicalHash(ctx, hash)
}

func (cc *ExecutionClientDirect) Ready(ctx context.Context) (bool, error) {
	return cc.chainRW.Ready(ctx)
}

// GetBodiesByRange gets block bodies in given block range
func (cc *ExecutionClientDirect) GetBodiesByRange(ctx context.Context, start, count uint64) ([]*types.RawBody, error) {
	return cc.chainRW.GetBodiesByRange(ctx, start, count)
}

// GetBodiesByHashes gets block bodies with given hashes
func (cc *ExecutionClientDirect) GetBodiesByHashes(ctx context.Context, hashes []common.Hash) ([]*types.RawBody, error) {
	return cc.chainRW.GetBodiesByHashes(ctx, hashes)
}

func (cc *ExecutionClientDirect) FrozenBlocks(ctx context.Context) uint64 {
	frozenBlocks, _ := cc.chainRW.FrozenBlocks(ctx)
	return frozenBlocks
}

func (cc *ExecutionClientDirect) HasBlock(ctx context.Context, hash common.Hash) (bool, error) {
	return cc.chainRW.HasBlock(ctx, hash)
}

func (cc *ExecutionClientDirect) GetAssembledBlock(_ context.Context, idBytes []byte) (*cltypes.Eth1Block, *engine_types.BlobsBundleV1, *typesproto.RequestsBundle, *big.Int, error) {
	return cc.chainRW.GetAssembledBlock(binary.LittleEndian.Uint64(idBytes))
}

func (cc *ExecutionClientDirect) HasGapInSnapshots(ctx context.Context) bool {
	_, hasGap := cc.chainRW.FrozenBlocks(ctx)
	return hasGap
}
