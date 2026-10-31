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

	"github.com/erigontech/erigon/cl/clparams"
	"github.com/erigontech/erigon/cl/cltypes"
	"github.com/erigontech/erigon/cl/monitor"
	"github.com/erigontech/erigon/common"
	"github.com/erigontech/erigon/common/hexutil"
	"github.com/erigontech/erigon/execution/engineapi/engine_types"
	"github.com/erigontech/erigon/execution/execmodule"
	"github.com/erigontech/erigon/execution/execmodule/chainreader"
	"github.com/erigontech/erigon/execution/types"
	"github.com/erigontech/erigon/node/gointerfaces"
	"github.com/erigontech/erigon/node/gointerfaces/txpoolproto"
	"github.com/erigontech/erigon/node/gointerfaces/typesproto"
	"github.com/erigontech/erigon/txnprovider/txpool"
)

const reorgTooDeepDepth = 3

type ExecutionClientDirect struct {
	chainRW chainreader.ChainReaderWriterEth1
	txpool  txpoolproto.TxpoolClient
}

func NewExecutionClientDirect(chainRW chainreader.ChainReaderWriterEth1, txpool txpoolproto.TxpoolClient) (*ExecutionClientDirect, error) {
	return &ExecutionClientDirect{
		chainRW: chainRW,
		txpool:  txpool,
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

	bal, err := DecodeAndValidateBlockAccessList(payload)
	if err != nil {
		return PayloadStatusInvalidated, err
	}

	header, err := payload.RlpHeader(beaconParentRoot, requestsHash, bal)
	if err != nil {
		// invalid block
		return PayloadStatusInvalidated, err
	}

	body := payload.Body()
	txs, err := DecodePayloadTransactions(payload)
	if err != nil {
		// invalid block
		return PayloadStatusInvalidated, err
	}

	startInsertBlock := time.Now()
	if err := cc.chainRW.InsertBlock(ctx, types.NewBlockFromStorageWithBinaryTxs(payload.BlockHash, header, txs, body.Transactions, nil, body.Withdrawals, bal)); err != nil {
		if errors.Is(err, types.ErrBlockExceedsMaxRlpSize) {
			return PayloadStatusInvalidated, err
		}
		return PayloadStatusNone, err
	}
	monitor.ObserveExecutionClientInsertingBlocks(startInsertBlock)

	headHeader := cc.chainRW.CurrentHeader(ctx)
	if headHeader == nil && ctx.Err() != nil {
		return PayloadStatusNone, ctx.Err()
	}
	if headHeader == nil || header.Number.Uint64() > headHeader.Number.Uint64()+1 {
		// can't validate yet
		return PayloadStatusNotValidated, nil
	}

	if header.Number.Uint64() < notValidatedBelow(headHeader) {
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
	case execmodule.ExecutionStatusBadBlock, execmodule.ExecutionStatusInvalidForkchoice:
		return PayloadStatusInvalidated, errors.New("bad block")
	case execmodule.ExecutionStatusBusy, execmodule.ExecutionStatusMissingSegment, execmodule.ExecutionStatusTooFarAway:
		return PayloadStatusNotValidated, nil
	case execmodule.ExecutionStatusSuccess:
		return PayloadStatusValidated, nil
	}
	return PayloadStatusNone, errors.New("unexpected status")
}

func (cc *ExecutionClientDirect) ForkChoiceUpdate(ctx context.Context, finalized, safe, head common.Hash, attr *engine_types.PayloadAttributes, _ clparams.StateVersion) ([]byte, error) {
	result, err := cc.chainRW.UpdateForkChoiceResult(ctx, head, safe, finalized)
	if err != nil {
		return nil, fmt.Errorf("execution Client RPC failed to retrieve ForkChoiceUpdate response, err: %w", err)
	}
	if result.Status == execmodule.ExecutionStatusInvalidForkchoice {
		return nil, errors.New("forkchoice was invalid")
	}
	if result.Status == execmodule.ExecutionStatusBadBlock {
		return nil, errors.New("bad block as forkchoice")
	}
	if attr == nil {
		if result.Status == execmodule.ExecutionStatusBusy && !result.UpdateInProgress {
			return nil, ErrForkChoiceUpdateBusy
		}
		return nil, nil
	}
	// The forkchoice update answers before its commit releases the execution semaphore, so the
	// build that follows it is usually busy for tens of milliseconds.
	id, err := retryAssembleBlock(ctx, 30, 10*time.Millisecond, 200*time.Millisecond, func(ctx context.Context) (uint64, error) {
		return cc.chainRW.AssembleBlock(ctx, head, attr)
	})
	if err != nil {
		return nil, err
	}
	return encodeDirectPayloadID(id), nil
}

func (cc *ExecutionClientDirect) StartPayloadBuild(ctx context.Context, head common.Hash, attributes *engine_types.PayloadAttributes) ([]byte, error) {
	id, err := startPayloadBuild(ctx, cc.chainRW, head, attributes)
	if err != nil {
		return nil, err
	}
	return encodeDirectPayloadID(id), nil
}

// Direct execution-module payload IDs use little-endian uint64 encoding. Engine API payload IDs
// are a separate wire format.
func encodeDirectPayloadID(id uint64) []byte {
	idBytes := make([]byte, 8)
	binary.LittleEndian.PutUint64(idBytes, id)
	return idBytes
}

func decodeDirectPayloadID(id []byte) uint64 {
	return binary.LittleEndian.Uint64(id)
}

// assembleRetryDelay doubles the delay from first on each retry, up to maxDelay.
func assembleRetryDelay(retry int, first, maxDelay time.Duration) time.Duration {
	delay := first
	for range retry {
		if delay >= maxDelay/2 {
			return maxDelay
		}
		delay *= 2
	}
	return min(delay, maxDelay)
}

func retryAssembleBlock(ctx context.Context, attempts int, firstDelay, maxDelay time.Duration, assemble func(context.Context) (uint64, error)) (uint64, error) {
	if attempts <= 0 {
		return 0, errors.New("assemble block requires at least one attempt")
	}
	var (
		id  uint64
		err error
	)
	for attempt := range attempts {
		if ctxErr := ctx.Err(); ctxErr != nil {
			return 0, ctxErr
		}
		if id, err = assemble(ctx); err == nil {
			return id, nil
		}
		if !errors.Is(err, chainreader.ErrExecutionBusy) {
			return 0, err
		}
		if attempt+1 == attempts {
			break
		}
		timer := time.NewTimer(assembleRetryDelay(attempt, firstDelay, maxDelay))
		select {
		case <-ctx.Done():
			if !timer.Stop() {
				select {
				case <-timer.C:
				default:
				}
			}
			return 0, ctx.Err()
		case <-timer.C:
		}
	}
	return 0, err
}

// SkipsValidationBelow returns the block number below which NewPayload inserts a block but answers
// NotValidated without validating it, or 0 when it validates every block. It follows the EL head;
// known is false when the head cannot be read.
func (cc *ExecutionClientDirect) SkipsValidationBelow(ctx context.Context) (below uint64, known bool) {
	head := cc.chainRW.CurrentHeader(ctx)
	return notValidatedBelow(head), head != nil
}

// notValidatedBelow returns the block number below which a block is more than reorgTooDeepDepth
// below head, too deep to validate, or 0.
func notValidatedBelow(head *types.Header) uint64 {
	if head == nil || head.Number.Uint64() <= reorgTooDeepDepth {
		return 0
	}
	return head.Number.Uint64() - reorgTooDeepDepth
}

func (cc *ExecutionClientDirect) SupportInsertion() bool {
	return true
}

func (cc *ExecutionClientDirect) InsertBlocks(ctx context.Context, blocks []*types.Block) error {
	return cc.chainRW.InsertBlocks(ctx, blocks)
}

func (cc *ExecutionClientDirect) InsertBlock(ctx context.Context, block *types.Block) error {
	return cc.chainRW.InsertBlock(ctx, block)
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

func (cc *ExecutionClientDirect) GetAssembledBlock(ctx context.Context, idBytes []byte, _ clparams.StateVersion) (*cltypes.Eth1Block, *engine_types.BlobsBundle, *typesproto.RequestsBundle, *big.Int, error) {
	return cc.chainRW.GetAssembledBlock(ctx, decodeDirectPayloadID(idBytes))
}

func (cc *ExecutionClientDirect) HasGapInSnapshots(ctx context.Context) bool {
	_, hasGap := cc.chainRW.FrozenBlocks(ctx)
	return hasGap
}

func (cc *ExecutionClientDirect) GetBlobs(ctx context.Context, versionedHashes []common.Hash, _ clparams.StateVersion) (blobs [][]byte, proofs [][][]byte, err error) {
	if cc.txpool == nil {
		return nil, nil, nil
	}

	req := &txpoolproto.GetBlobsRequest{BlobHashes: make([]*typesproto.H256, len(versionedHashes))}
	for i, h := range versionedHashes {
		req.BlobHashes[i] = gointerfaces.ConvertHashToH256(h)
	}
	resp, err := cc.txpool.GetBlobs(ctx, req)
	if err != nil {
		if errors.Is(err, txpool.ErrPoolDisabled) {
			return nil, nil, nil
		}
		return nil, nil, fmt.Errorf("txpool GetBlobs: %w", err)
	}
	blobsWithProof := resp.BlobsWithProofs
	blobs = make([][]byte, len(blobsWithProof))
	proofs = make([][][]byte, len(blobsWithProof))
	for i, bwp := range blobsWithProof {
		blobs[i] = bwp.Blob
		proofs[i] = bwp.Proofs
	}
	return blobs, proofs, nil
}

// In direct mode the execution layer is the in-process Erigon node, so report it directly.
func (cc *ExecutionClientDirect) GetClientVersionV1(_ context.Context, _ *engine_types.ClientVersionV1) ([]engine_types.ClientVersionV1, error) {
	return []engine_types.ClientVersionV1{engine_types.LocalClientVersionV1()}, nil
}
