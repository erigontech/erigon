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

	libcommon "github.com/erigontech/erigon-lib/common"
	execution "github.com/erigontech/erigon-lib/gointerfaces/executionproto"
	"github.com/erigontech/erigon/v3/cl/cltypes"
	"github.com/erigontech/erigon/v3/core/types"
	"github.com/erigontech/erigon/v3/turbo/engineapi/engine_types"
	"github.com/erigontech/erigon/v3/turbo/execution/eth1/eth1_chain_reader.go"
)

type ExecutionClientDirect struct {
	chainRW eth1_chain_reader.ChainReaderWriterEth1
}

func NewExecutionClientDirect(chainRW eth1_chain_reader.ChainReaderWriterEth1) (*ExecutionClientDirect, error) {
	return &ExecutionClientDirect{
		chainRW: chainRW,
	}, nil
}

func (cc *ExecutionClientDirect) NewPayload(ctx context.Context, payload *cltypes.Eth1Block, beaconParentRoot *libcommon.Hash, versionedHashes []libcommon.Hash) (PayloadStatus, error) {
	if payload == nil {
		return PayloadStatusValidated, nil
	}

	header, err := payload.RlpHeader(beaconParentRoot)
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

	if err := cc.chainRW.InsertBlockAndWait(ctx, types.NewBlockFromStorage(payload.BlockHash, header, txs, nil, body.Withdrawals)); err != nil {
		return PayloadStatusNone, err
	}

	headHeader := cc.chainRW.CurrentHeader(ctx)
	if headHeader == nil || header.Number.Uint64() > headHeader.Number.Uint64()+1 {
		// can't validate yet
		return PayloadStatusNotValidated, nil
	}

	status, _, _, err := cc.chainRW.ValidateChain(ctx, payload.BlockHash, payload.BlockNumber)
	if err != nil {
		return PayloadStatusNone, err
	}
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

func (cc *ExecutionClientDirect) ForkChoiceUpdate(ctx context.Context, finalized libcommon.Hash, head libcommon.Hash, attr *engine_types.PayloadAttributes) ([]byte, error) {
	status, _, _, err := cc.chainRW.UpdateForkChoice(ctx, head, head, finalized)
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

func (cc *ExecutionClientDirect) IsCanonicalHash(ctx context.Context, hash libcommon.Hash) (bool, error) {
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
func (cc *ExecutionClientDirect) GetBodiesByHashes(ctx context.Context, hashes []libcommon.Hash) ([]*types.RawBody, error) {
	return cc.chainRW.GetBodiesByHashes(ctx, hashes)
}

func (cc *ExecutionClientDirect) FrozenBlocks(ctx context.Context) uint64 {
	frozenBlocks, _ := cc.chainRW.FrozenBlocks(ctx)
	return frozenBlocks
}

func (cc *ExecutionClientDirect) HasBlock(ctx context.Context, hash libcommon.Hash) (bool, error) {
	return cc.chainRW.HasBlock(ctx, hash)
}

func (cc *ExecutionClientDirect) GetAssembledBlock(_ context.Context, idBytes []byte) (*cltypes.Eth1Block, *engine_types.BlobsBundleV1, *big.Int, error) {
	return cc.chainRW.GetAssembledBlock(binary.LittleEndian.Uint64(idBytes))
}

func (cc *ExecutionClientDirect) HasGapInSnapshots(ctx context.Context) bool {
	_, hasGap := cc.chainRW.FrozenBlocks(ctx)
	return hasGap
}
