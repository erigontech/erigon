package commands

import (
	"bytes"
	"context"
	"encoding/binary"
	"fmt"
	"math/big"

	"github.com/holiman/uint256"
	"github.com/ledgerwatch/erigon-lib/gointerfaces"
	"github.com/ledgerwatch/erigon-lib/gointerfaces/remote"
	types2 "github.com/ledgerwatch/erigon-lib/gointerfaces/types"
	"github.com/ledgerwatch/erigon-lib/kv"
	"github.com/ledgerwatch/erigon/cmd/rpcdaemon/services"
	"github.com/ledgerwatch/erigon/common"
	"github.com/ledgerwatch/erigon/common/hexutil"
	"github.com/ledgerwatch/erigon/core/types"
	"github.com/ledgerwatch/erigon/rlp"
	"github.com/ledgerwatch/erigon/rpc"
	"github.com/ledgerwatch/log/v3"
)

// ExecutionPayload represents an execution payload (aka slot/block)
type ExecutionPayload struct {
	ParentHash    common.Hash     `json:"parentHash"    gencodec:"required"`
	FeeRecipient  common.Address  `json:"feeRecipient"  gencodec:"required"`
	StateRoot     common.Hash     `json:"stateRoot"     gencodec:"required"`
	ReceiptsRoot  common.Hash     `json:"receiptsRoot"  gencodec:"required"`
	LogsBloom     hexutil.Bytes   `json:"logsBloom"     gencodec:"required"`
	Random        common.Hash     `json:"random"        gencodec:"required"`
	BlockNumber   hexutil.Uint64  `json:"blockNumber"   gencodec:"required"`
	GasLimit      hexutil.Uint64  `json:"gasLimit"      gencodec:"required"`
	GasUsed       hexutil.Uint64  `json:"gasUsed"       gencodec:"required"`
	Timestamp     hexutil.Uint64  `json:"timestamp"     gencodec:"required"`
	ExtraData     hexutil.Bytes   `json:"extraData"     gencodec:"required"`
	BaseFeePerGas *hexutil.Big    `json:"baseFeePerGas" gencodec:"required"`
	BlockHash     common.Hash     `json:"blockHash"     gencodec:"required"`
	Transactions  []hexutil.Bytes `json:"transactions"  gencodec:"required"`
}

// PayloadAttributes represent the attributes required to start assembling a payload
type ForkChoiceState struct {
	HeadHash           common.Hash `json:"headBlockHash"             gencodec:"required"`
	SafeBlockHash      common.Hash `json:"safeBlockHash"             gencodec:"required"`
	FinalizedBlockHash common.Hash `json:"finalizedBlockHash"        gencodec:"required"`
}

// PayloadAttributes represent the attributes required to start assembling a payload
type PayloadAttributes struct {
	Timestamp             hexutil.Uint64 `json:"timestamp"             gencodec:"required"`
	Random                common.Hash    `json:"random"                gencodec:"required"`
	SuggestedFeeRecipient common.Address `json:"suggestedFeeRecipient" gencodec:"required"`
}

// EngineAPI Beacon chain communication endpoint
type EngineAPI interface {
	ForkchoiceUpdatedV1(ctx context.Context, forkChoiceState *ForkChoiceState, payloadAttributes *PayloadAttributes) (map[string]interface{}, error)
	NewPayloadV1(context.Context, *ExecutionPayload) (map[string]interface{}, error)
	GetPayloadV1(ctx context.Context, payloadID hexutil.Bytes) (*ExecutionPayload, error)
	GetPayloadBodiesV1(ctx context.Context, blockHashes []rpc.BlockNumberOrHash) (map[common.Hash]ExecutionPayload, error)
}

// EngineImpl is implementation of the EngineAPI interface
type EngineImpl struct {
	*BaseAPI
	db  kv.RoDB
	api services.ApiBackend
}

func convertPayloadStatus(x *remote.EnginePayloadStatus) map[string]interface{} {
	json := map[string]interface{}{
		"status": x.Status.String(),
	}
	if x.LatestValidHash != nil {
		json["latestValidHash"] = gointerfaces.ConvertH256ToHash(x.LatestValidHash)
	}
	if x.ValidationError != "" {
		json["validationError"] = x.ValidationError
	}

	return json
}

func (e *EngineImpl) ForkchoiceUpdatedV1(ctx context.Context, forkChoiceState *ForkChoiceState, payloadAttributes *PayloadAttributes) (map[string]interface{}, error) {
	var prepareParameters *remote.EnginePayloadAttributes
	if payloadAttributes != nil {
		prepareParameters = &remote.EnginePayloadAttributes{
			Timestamp:             uint64(payloadAttributes.Timestamp),
			Random:                gointerfaces.ConvertHashToH256(payloadAttributes.Random),
			SuggestedFeeRecipient: gointerfaces.ConvertAddressToH160(payloadAttributes.SuggestedFeeRecipient),
		}
	}
	reply, err := e.api.EngineForkchoiceUpdatedV1(ctx, &remote.EngineForkChoiceUpdatedRequest{
		ForkchoiceState: &remote.EngineForkChoiceState{
			HeadBlockHash:      gointerfaces.ConvertHashToH256(forkChoiceState.HeadHash),
			FinalizedBlockHash: gointerfaces.ConvertHashToH256(forkChoiceState.FinalizedBlockHash),
			SafeBlockHash:      gointerfaces.ConvertHashToH256(forkChoiceState.SafeBlockHash),
		},
		PayloadAttributes: prepareParameters,
	})
	if err != nil {
		return nil, err
	}

	json := map[string]interface{}{
		"payloadStatus": convertPayloadStatus(reply.PayloadStatus),
	}
	if reply.PayloadId != 0 {
		encodedPayloadId := make([]byte, 8)
		binary.BigEndian.PutUint64(encodedPayloadId, reply.PayloadId)
		json["payloadId"] = hexutil.Bytes(encodedPayloadId)
	}

	return json, nil
}

// NewPayloadV1 processes new payloads (blocks) from the beacon chain.
// See https://github.com/ethereum/execution-apis/blob/main/src/engine/specification.md#engine_newpayloadv1
func (e *EngineImpl) NewPayloadV1(ctx context.Context, payload *ExecutionPayload) (map[string]interface{}, error) {
	var baseFee *uint256.Int
	if payload.BaseFeePerGas != nil {
		var overflow bool
		baseFee, overflow = uint256.FromBig((*big.Int)(payload.BaseFeePerGas))
		if overflow {
			return nil, fmt.Errorf("invalid request")
		}
	}
	log.Info("Received Payload from beacon-chain")

	// Convert slice of hexutil.Bytes to a slice of slice of bytes
	transactions := make([][]byte, len(payload.Transactions))
	for i, transaction := range payload.Transactions {
		transactions[i] = ([]byte)(transaction)
	}
	res, err := e.api.EngineNewPayloadV1(ctx, &types2.ExecutionPayload{
		ParentHash:    gointerfaces.ConvertHashToH256(payload.ParentHash),
		Coinbase:      gointerfaces.ConvertAddressToH160(payload.FeeRecipient),
		StateRoot:     gointerfaces.ConvertHashToH256(payload.StateRoot),
		ReceiptRoot:   gointerfaces.ConvertHashToH256(payload.ReceiptsRoot),
		LogsBloom:     gointerfaces.ConvertBytesToH2048(([]byte)(payload.LogsBloom)),
		Random:        gointerfaces.ConvertHashToH256(payload.Random),
		BlockNumber:   uint64(payload.BlockNumber),
		GasLimit:      uint64(payload.GasLimit),
		GasUsed:       uint64(payload.GasUsed),
		Timestamp:     uint64(payload.Timestamp),
		ExtraData:     payload.ExtraData,
		BaseFeePerGas: gointerfaces.ConvertUint256IntToH256(baseFee),
		BlockHash:     gointerfaces.ConvertHashToH256(payload.BlockHash),
		Transactions:  transactions,
	})
	if err != nil {
		return nil, err
	}

	return convertPayloadStatus(res), nil
}

func (e *EngineImpl) GetPayloadV1(ctx context.Context, payloadID hexutil.Bytes) (*ExecutionPayload, error) {
	decodedPayloadId := binary.BigEndian.Uint64(payloadID)
	payload, err := e.api.EngineGetPayloadV1(ctx, decodedPayloadId)
	if err != nil {
		return nil, err
	}
	var bloom types.Bloom = gointerfaces.ConvertH2048ToBloom(payload.LogsBloom)

	var baseFee *big.Int
	if payload.BaseFeePerGas != nil {
		baseFee = gointerfaces.ConvertH256ToUint256Int(payload.BaseFeePerGas).ToBig()
	}

	// Convert slice of hexutil.Bytes to a slice of slice of bytes
	transactions := make([]hexutil.Bytes, len(payload.Transactions))
	for i, transaction := range payload.Transactions {
		transactions[i] = transaction
	}
	return &ExecutionPayload{
		ParentHash:    gointerfaces.ConvertH256ToHash(payload.ParentHash),
		FeeRecipient:  gointerfaces.ConvertH160toAddress(payload.Coinbase),
		StateRoot:     gointerfaces.ConvertH256ToHash(payload.StateRoot),
		ReceiptsRoot:  gointerfaces.ConvertH256ToHash(payload.ReceiptRoot),
		LogsBloom:     bloom[:],
		Random:        gointerfaces.ConvertH256ToHash(payload.Random),
		BlockNumber:   hexutil.Uint64(payload.BlockNumber),
		GasLimit:      hexutil.Uint64(payload.GasLimit),
		GasUsed:       hexutil.Uint64(payload.GasUsed),
		Timestamp:     hexutil.Uint64(payload.Timestamp),
		ExtraData:     payload.ExtraData,
		BaseFeePerGas: (*hexutil.Big)(baseFee),
		BlockHash:     gointerfaces.ConvertH256ToHash(payload.BlockHash),
		Transactions:  transactions,
	}, nil
}

// GetPayloadBodiesV1 gets a list of blockHashes and returns a map of blockhash => block body
func (e *EngineImpl) GetPayloadBodiesV1(ctx context.Context, blockHashes []rpc.BlockNumberOrHash) (map[common.Hash]ExecutionPayload, error) {
	tx, err := e.db.BeginRo(ctx)
	if err != nil {
		return nil, err
	}
	defer tx.Rollback()

	blockHashToBody := make(map[common.Hash]ExecutionPayload)

	for _, blockHash := range blockHashes {
		hash := *blockHash.BlockHash
		block, err := e.blockByHashWithSenders(tx, hash)
		if err != nil {
			return nil, err
		}
		if block == nil {
			continue
		}

		var bloom types.Bloom = block.Bloom()
		buf := bytes.NewBuffer(nil)
		var encodedTransactions []hexutil.Bytes

		for _, tx := range block.Transactions() {
			buf.Reset()

			err := rlp.Encode(buf, tx)
			if err != nil {
				return nil, fmt.Errorf("broken tx rlp: %w", err)
			}
			encodedTransactions = append(encodedTransactions, common.CopyBytes(buf.Bytes()))
		}

		blockHashToBody[hash] = ExecutionPayload{
			ParentHash:    block.ParentHash(),
			FeeRecipient:  block.Coinbase(),
			StateRoot:     block.Header().Root,
			ReceiptsRoot:  block.ReceiptHash(),
			LogsBloom:     bloom.Bytes(),
			Random:        block.Header().MixDigest,
			BlockNumber:   hexutil.Uint64(block.NumberU64()),
			GasLimit:      hexutil.Uint64(block.GasLimit()),
			GasUsed:       hexutil.Uint64(block.GasUsed()),
			Timestamp:     hexutil.Uint64(block.Header().Time),
			ExtraData:     block.Extra(),
			BaseFeePerGas: (*hexutil.Big)(block.BaseFee()),
			BlockHash:     block.Hash(),
			Transactions:  encodedTransactions,
		}
	}
	return blockHashToBody, nil
}

// NewEngineAPI returns EngineImpl instance
func NewEngineAPI(base *BaseAPI, db kv.RoDB, api services.ApiBackend) *EngineImpl {
	return &EngineImpl{
		BaseAPI: base,
		db:      db,
		api:     api,
	}
}
