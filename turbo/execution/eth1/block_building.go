package eth1

import (
	"context"
	"fmt"
	"reflect"

	"github.com/holiman/uint256"
	"github.com/ledgerwatch/erigon-lib/gointerfaces"
	"github.com/ledgerwatch/erigon-lib/gointerfaces/execution"
	types2 "github.com/ledgerwatch/erigon-lib/gointerfaces/types"
	"github.com/ledgerwatch/erigon/common"
	"github.com/ledgerwatch/erigon/core"
	"github.com/ledgerwatch/erigon/core/types"
	"github.com/ledgerwatch/erigon/rpc"
	"github.com/ledgerwatch/erigon/turbo/builder"
	"github.com/ledgerwatch/erigon/turbo/engineapi/engine_helpers"
	"github.com/ledgerwatch/erigon/turbo/execution/eth1/eth1_utils"
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
		PrevRandao:            gointerfaces.ConvertH256ToHash(req.MixDigest),
		SuggestedFeeRecipient: gointerfaces.ConvertH160toAddress(req.SuggestedFeeRecipent),
		Withdrawals:           eth1_utils.ConvertWithdrawalsFromRpc(req.Withdrawals),
	}

	if err := e.checkWithdrawalsPresence(param.Timestamp, param.Withdrawals); err != nil {
		return nil, err
	}

	// First check if we're already building a block with the requested parameters
	if reflect.DeepEqual(e.lastParameters, &param) {
		e.logger.Info("[ForkChoiceUpdated] duplicate build request")
		return &execution.AssembleBlockResponse{
			Id:   e.nextPayloadId,
			Busy: false,
		}, nil
	}

	// Initiate payload building
	e.evictOldBuilders()

	e.nextPayloadId++
	param.PayloadId = e.nextPayloadId
	e.lastParameters = &param

	e.builders[e.nextPayloadId] = builder.NewBlockBuilder(e.builderFunc, &param)
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
	for i, tx := range block.Transactions() {
		if tx.Type() != types.BlobTxType {
			continue
		}
		blobTx, ok := tx.(*types.BlobTxWrapper)
		if !ok {
			return nil, fmt.Errorf("expected blob transaction to be type BlobTxWrapper, got: %T", blobTx)
		}
		versionedHashes, commitments, proofs, blobs := blobTx.GetBlobHashes(), blobTx.Commitments, blobTx.Proofs, blobTx.Blobs
		lenCheck := len(versionedHashes)
		if lenCheck != len(commitments) || lenCheck != len(proofs) || lenCheck != len(blobs) {
			return nil, fmt.Errorf("tx %d in block %s has inconsistent commitments (%d) / proofs (%d) / blobs (%d) / "+
				"versioned hashes (%d)", i, block.Hash(), len(commitments), len(proofs), len(blobs), lenCheck)
		}
		for _, commitment := range commitments {
			blobsBundle.Commitments = append(blobsBundle.Commitments, commitment[:])
		}
		for _, proof := range proofs {
			blobsBundle.Proofs = append(blobsBundle.Proofs, proof[:])
		}
		for _, blob := range blobs {
			blobsBundle.Blobs = append(blobsBundle.Blobs, blob[:])
		}
	}

	return &execution.GetAssembledBlockResponse{
		Data: &execution.AssembledBlockData{
			ExecutionPayload: payload,
			BlockValue:       gointerfaces.ConvertUint256IntToH256(blockValue),
			BlobsBundle:      blobsBundle,
		},
		Busy: false,
	}, nil
}
