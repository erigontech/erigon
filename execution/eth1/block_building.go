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

package eth1

import (
	"context"
	"fmt"
	"reflect"

	"github.com/holiman/uint256"

	"github.com/erigontech/erigon-lib/common"
	"github.com/erigontech/erigon-lib/gointerfaces"
	execution "github.com/erigontech/erigon-lib/gointerfaces/executionproto"
	types2 "github.com/erigontech/erigon-lib/gointerfaces/typesproto"
	"github.com/erigontech/erigon/core"
	"github.com/erigontech/erigon/execution/builder"
	"github.com/erigontech/erigon/execution/engineapi/engine_helpers"
	"github.com/erigontech/erigon/execution/eth1/eth1_utils"
	"github.com/erigontech/erigon/execution/types"
	"github.com/erigontech/erigon/rpc"
)

func (e *EthereumExecutionModule) checkWithdrawalsPresence(time uint64, withdrawals []*types.Withdrawal) error {
	if !e.config.IsShanghai(time) && withdrawals != nil {
		return &rpc.InvalidParamsError{Message: "withdrawals before shanghai"}
	}
	if e.config.IsShanghai(time) && withdrawals == nil {
		return &rpc.InvalidParamsError{Message: "missing withdrawals list"}
	}
	return nil
}

func (e *EthereumExecutionModule) evictOldBuilders() {
	ids := common.SortedKeys(e.builders)

	// remove old builders so that at most MaxBuilders - 1 remain
	for i := 0; i <= len(e.builders)-engine_helpers.MaxBuilders; i++ {
		delete(e.builders, ids[i])
	}
}

// Missing: NewPayload, AssembleBlock
func (e *EthereumExecutionModule) AssembleBlock(ctx context.Context, req *execution.AssembleBlockRequest) (*execution.AssembleBlockResponse, error) {
	if !e.semaphore.TryAcquire(1) {
		return &execution.AssembleBlockResponse{
			Id:   0,
			Busy: true,
		}, nil
	}
	defer e.semaphore.Release(1)
	param := core.BlockBuilderParameters{
		ParentHash:            gointerfaces.ConvertH256ToHash(req.ParentHash),
		Timestamp:             req.Timestamp,
		PrevRandao:            gointerfaces.ConvertH256ToHash(req.PrevRandao),
		SuggestedFeeRecipient: gointerfaces.ConvertH160toAddress(req.SuggestedFeeRecipient),
		Withdrawals:           eth1_utils.ConvertWithdrawalsFromRpc(req.Withdrawals),
	}

	if err := e.checkWithdrawalsPresence(param.Timestamp, param.Withdrawals); err != nil {
		return nil, err
	}

	if req.ParentBeaconBlockRoot != nil {
		pbbr := common.Hash(gointerfaces.ConvertH256ToHash(req.ParentBeaconBlockRoot))
		param.ParentBeaconBlockRoot = &pbbr
	}

	// First check if we're already building a block with the requested parameters
	if e.lastParameters != nil {
		param.PayloadId = e.lastParameters.PayloadId
		if reflect.DeepEqual(e.lastParameters, &param) {
			e.logger.Info("[ForkChoiceUpdated] duplicate build request")
			return &execution.AssembleBlockResponse{
				Id:   e.lastParameters.PayloadId,
				Busy: false,
			}, nil
		}
	}

	// Initiate payload building
	e.evictOldBuilders()

	e.nextPayloadId++
	param.PayloadId = e.nextPayloadId
	e.lastParameters = &param

	e.builders[e.nextPayloadId] = builder.NewBlockBuilder(e.builderFunc, &param, e.config.SecondsPerSlot())
	e.logger.Info("[ForkChoiceUpdated] BlockBuilder added", "payload", e.nextPayloadId)

	return &execution.AssembleBlockResponse{
		Id:   e.nextPayloadId,
		Busy: false,
	}, nil
}

// The expected value to be received by the feeRecipient in wei
func blockValue(br *types.BlockWithReceipts, baseFee *uint256.Int) *uint256.Int {
	blockValue := uint256.NewInt(0)
	txs := br.Block.Transactions()
	for i := range txs {
		gas := new(uint256.Int).SetUint64(br.Receipts[i].GasUsed)
		effectiveTip := txs[i].GetEffectiveGasTip(baseFee)
		txValue := new(uint256.Int).Mul(gas, effectiveTip)
		blockValue.Add(blockValue, txValue)
	}
	return blockValue
}

func (e *EthereumExecutionModule) GetAssembledBlock(ctx context.Context, req *execution.GetAssembledBlockRequest) (*execution.GetAssembledBlockResponse, error) {
	if !e.semaphore.TryAcquire(1) {
		return &execution.GetAssembledBlockResponse{
			Busy: true,
		}, nil
	}
	defer e.semaphore.Release(1)
	payloadId := req.Id
	builder, ok := e.builders[payloadId]
	if !ok {
		return &execution.GetAssembledBlockResponse{
			Busy: false,
		}, nil
	}

	blockWithReceipts, err := builder.Stop()
	if err != nil {
		e.logger.Error("Failed to build PoS block", "err", err)
		return nil, err
	}
	block := blockWithReceipts.Block
	header := block.Header()

	baseFee := new(uint256.Int)
	baseFee.SetFromBig(header.BaseFee)

	encodedTransactions, err := types.MarshalTransactionsBinary(block.Transactions())
	if err != nil {
		return nil, err
	}

	payload := &types2.ExecutionPayload{
		Version:       1,
		ParentHash:    gointerfaces.ConvertHashToH256(header.ParentHash),
		Coinbase:      gointerfaces.ConvertAddressToH160(header.Coinbase),
		Timestamp:     header.Time,
		PrevRandao:    gointerfaces.ConvertHashToH256(header.MixDigest),
		StateRoot:     gointerfaces.ConvertHashToH256(block.Root()),
		ReceiptRoot:   gointerfaces.ConvertHashToH256(block.ReceiptHash()),
		LogsBloom:     gointerfaces.ConvertBytesToH2048(block.Bloom().Bytes()),
		GasLimit:      block.GasLimit(),
		GasUsed:       block.GasUsed(),
		BlockNumber:   block.NumberU64(),
		ExtraData:     block.Extra(),
		BaseFeePerGas: gointerfaces.ConvertUint256IntToH256(baseFee),
		BlockHash:     gointerfaces.ConvertHashToH256(block.Hash()),
		Transactions:  encodedTransactions,
	}
	if block.Withdrawals() != nil {
		payload.Version = 2
		payload.Withdrawals = eth1_utils.ConvertWithdrawalsToRpc(block.Withdrawals())
	}

	if header.BlobGasUsed != nil && header.ExcessBlobGas != nil {
		payload.Version = 3
		payload.BlobGasUsed = header.BlobGasUsed
		payload.ExcessBlobGas = header.ExcessBlobGas
	}

	blockValue := blockValue(blockWithReceipts, baseFee)

	blobsBundle := &types2.BlobsBundleV1{}
	for i, txn := range block.Transactions() {
		if txn.Type() != types.BlobTxType {
			continue
		}
		blobTx, ok := txn.(*types.BlobTxWrapper)
		if !ok {
			return nil, fmt.Errorf("expected blob transaction to be type BlobTxWrapper, got: %T", blobTx)
		}
		versionedHashes, commitments, proofs, blobs := blobTx.GetBlobHashes(), blobTx.Commitments, blobTx.Proofs, blobTx.Blobs
		lenCheck := len(versionedHashes)
		if lenCheck != len(commitments) || (lenCheck != len(proofs) && blobTx.WrapperVersion == 0) || lenCheck != len(blobs) {
			return nil, fmt.Errorf("tx %d in block %s has inconsistent commitments (%d) / proofs (%d) / blobs (%d) / "+
				"versioned hashes (%d)", i, block.Hash(), len(commitments), len(proofs), len(blobs), lenCheck)
		}
		for _, commitment := range commitments {
			c := types.KZGCommitment{}
			copy(c[:], commitment[:])
			blobsBundle.Commitments = append(blobsBundle.Commitments, c[:])
		}
		for _, proof := range proofs {
			p := types.KZGProof{}
			copy(p[:], proof[:])
			blobsBundle.Proofs = append(blobsBundle.Proofs, p[:])
		}
		for _, blob := range blobs {
			b := types.Blob{}
			copy(b[:], blob[:])
			blobsBundle.Blobs = append(blobsBundle.Blobs, b[:])
		}
	}

	var requestsBundle *types2.RequestsBundle
	if blockWithReceipts.Requests != nil {
		requestsBundle = &types2.RequestsBundle{}
		requests := make([][]byte, 0)
		for _, r := range blockWithReceipts.Requests {
			requests = append(requests, r.Encode())
		}
		requestsBundle.Requests = requests
	}

	return &execution.GetAssembledBlockResponse{
		Data: &execution.AssembledBlockData{
			ExecutionPayload: payload,
			BlockValue:       gointerfaces.ConvertUint256IntToH256(blockValue),
			BlobsBundle:      blobsBundle,
			Requests:         requestsBundle,
		},
		Busy: false,
	}, nil
}
