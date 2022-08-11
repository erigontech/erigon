package commands

import (
	"context"
	"encoding/binary"
	"fmt"
	"math/big"

	"github.com/holiman/uint256"
	"github.com/ledgerwatch/erigon-lib/gointerfaces"
	"github.com/ledgerwatch/erigon-lib/gointerfaces/remote"
	types2 "github.com/ledgerwatch/erigon-lib/gointerfaces/types"
	"github.com/ledgerwatch/erigon-lib/kv"
	"github.com/ledgerwatch/erigon/common"
	"github.com/ledgerwatch/erigon/common/hexutil"
	"github.com/ledgerwatch/erigon/core/types"
	"github.com/ledgerwatch/erigon/turbo/rpchelper"
	"github.com/ledgerwatch/log/v3"
)

// ExecutionPayload represents an execution payload (aka slot/block)
type ExecutionPayload struct {
	ParentHash    common.Hash     `json:"parentHash"    gencodec:"required"`
	FeeRecipient  common.Address  `json:"feeRecipient"  gencodec:"required"`
	StateRoot     common.Hash     `json:"stateRoot"     gencodec:"required"`
	ReceiptsRoot  common.Hash     `json:"receiptsRoot"  gencodec:"required"`
	LogsBloom     hexutil.Bytes   `json:"logsBloom"     gencodec:"required"`
	PrevRandao    common.Hash     `json:"prevRandao"    gencodec:"required"`
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
	PrevRandao            common.Hash    `json:"prevRandao"            gencodec:"required"`
	SuggestedFeeRecipient common.Address `json:"suggestedFeeRecipient" gencodec:"required"`
}

// TransitionConfiguration represents the correct configurations of the CL and the EL
type TransitionConfiguration struct {
	TerminalTotalDifficulty *hexutil.Big `json:"terminalTotalDifficulty" gencodec:"required"`
	TerminalBlockHash       common.Hash  `json:"terminalBlockHash"       gencodec:"required"`
	TerminalBlockNumber     *hexutil.Big `json:"terminalBlockNumber"     gencodec:"required"`
}

// EngineAPI Beacon chain communication endpoint
type EngineAPI interface {
	ForkchoiceUpdatedV1(ctx context.Context, forkChoiceState *ForkChoiceState, payloadAttributes *PayloadAttributes) (map[string]interface{}, error)
	NewPayloadV1(context.Context, *ExecutionPayload) (map[string]interface{}, error)
	GetPayloadV1(ctx context.Context, payloadID hexutil.Bytes) (*ExecutionPayload, error)
	ExchangeTransitionConfigurationV1(ctx context.Context, transitionConfiguration TransitionConfiguration) (TransitionConfiguration, error)
}

// EngineImpl is implementation of the EngineAPI interface
type EngineImpl struct {
	*BaseAPI
	db  kv.RoDB
	api rpchelper.ApiBackend
}

func convertPayloadStatus(x *remote.EnginePayloadStatus) map[string]interface{} {
	json := map[string]interface{}{
		"status": x.Status.String(),
	}
	if x.LatestValidHash != nil {
		json["latestValidHash"] = common.Hash(gointerfaces.ConvertH256ToHash(x.LatestValidHash))
	}
	if x.ValidationError != "" {
		json["validationError"] = x.ValidationError
	}

	return json
}

func (e *EngineImpl) ForkchoiceUpdatedV1(ctx context.Context, forkChoiceState *ForkChoiceState, payloadAttributes *PayloadAttributes) (map[string]interface{}, error) {
	log.Debug("Received ForkchoiceUpdated", "head", forkChoiceState.HeadHash, "safe", forkChoiceState.HeadHash, "finalized", forkChoiceState.FinalizedBlockHash,
		"build", payloadAttributes != nil)

	var prepareParameters *remote.EnginePayloadAttributes
	if payloadAttributes != nil {
		prepareParameters = &remote.EnginePayloadAttributes{
			Timestamp:             uint64(payloadAttributes.Timestamp),
			PrevRandao:            gointerfaces.ConvertHashToH256(payloadAttributes.PrevRandao),
			SuggestedFeeRecipient: gointerfaces.ConvertAddressToH160(payloadAttributes.SuggestedFeeRecipient),
		}
	}
	reply, err := e.api.EngineForkchoiceUpdatedV1(ctx, &remote.EngineForkChoiceUpdatedRequest{
		ForkchoiceState: &remote.EngineForkChoiceState{
			HeadBlockHash:      gointerfaces.ConvertHashToH256(forkChoiceState.HeadHash),
			SafeBlockHash:      gointerfaces.ConvertHashToH256(forkChoiceState.SafeBlockHash),
			FinalizedBlockHash: gointerfaces.ConvertHashToH256(forkChoiceState.FinalizedBlockHash),
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
	log.Debug("Received NewPayload", "height", uint64(payload.BlockNumber), "hash", payload.BlockHash)

	var baseFee *uint256.Int
	if payload.BaseFeePerGas != nil {
		var overflow bool
		baseFee, overflow = uint256.FromBig((*big.Int)(payload.BaseFeePerGas))
		if overflow {
			log.Warn("NewPayload BaseFeePerGas overflow")
			return nil, fmt.Errorf("invalid request")
		}
	}

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
		PrevRandao:    gointerfaces.ConvertHashToH256(payload.PrevRandao),
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
		log.Warn("NewPayload", "err", err)
		return nil, err
	}

	return convertPayloadStatus(res), nil
}

func (e *EngineImpl) GetPayloadV1(ctx context.Context, payloadID hexutil.Bytes) (*ExecutionPayload, error) {
	decodedPayloadId := binary.BigEndian.Uint64(payloadID)
	log.Info("Received GetPayload", "payloadId", decodedPayloadId)

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
		PrevRandao:    gointerfaces.ConvertH256ToHash(payload.PrevRandao),
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

// Receives consensus layer's transition configuration and checks if the execution layer has the correct configuration.
// Can also be used to ping the execution layer (heartbeats).
// See https://github.com/ethereum/execution-apis/blob/v1.0.0-alpha.7/src/engine/specification.md#engine_exchangetransitionconfigurationv1
func (e *EngineImpl) ExchangeTransitionConfigurationV1(ctx context.Context, beaconConfig TransitionConfiguration) (TransitionConfiguration, error) {
	tx, err := e.db.BeginRo(ctx)

	if err != nil {
		return TransitionConfiguration{}, err
	}

	defer tx.Rollback()

	chainConfig, err := e.BaseAPI.chainConfig(tx)

	if err != nil {
		return TransitionConfiguration{}, err
	}

	terminalTotalDifficulty := chainConfig.TerminalTotalDifficulty
	if terminalTotalDifficulty != nil && terminalTotalDifficulty.Cmp((*big.Int)(beaconConfig.TerminalTotalDifficulty)) != 0 {
		return TransitionConfiguration{}, fmt.Errorf("the execution layer has a wrong terminal total difficulty. expected %v, but instead got: %d", beaconConfig.TerminalTotalDifficulty, terminalTotalDifficulty)
	}

	if chainConfig.TerminalBlockHash != beaconConfig.TerminalBlockHash {
		return TransitionConfiguration{}, fmt.Errorf("the execution layer has a wrong terminal block hash. expected %s, but instead got: %s", beaconConfig.TerminalBlockHash, chainConfig.TerminalBlockHash)
	}

	terminalBlockNumber := chainConfig.TerminalBlockNumber
	if terminalBlockNumber == nil {
		terminalBlockNumber = common.Big0
	}

	if terminalBlockNumber.Cmp((*big.Int)(beaconConfig.TerminalBlockNumber)) != 0 {
		return TransitionConfiguration{}, fmt.Errorf("the execution layer has a wrong terminal block number. expected %v, but instead got: %d", beaconConfig.TerminalBlockNumber, terminalBlockNumber)
	}

	return TransitionConfiguration{
		TerminalTotalDifficulty: (*hexutil.Big)(terminalTotalDifficulty),
		TerminalBlockHash:       chainConfig.TerminalBlockHash,
		TerminalBlockNumber:     (*hexutil.Big)(terminalBlockNumber),
	}, nil
}

// NewEngineAPI returns EngineImpl instance
func NewEngineAPI(base *BaseAPI, db kv.RoDB, api rpchelper.ApiBackend) *EngineImpl {
	return &EngineImpl{
		BaseAPI: base,
		db:      db,
		api:     api,
	}
}
