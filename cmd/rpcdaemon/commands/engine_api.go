package commands

import (
	"context"
	"fmt"
	"math/big"

	"github.com/holiman/uint256"
	"github.com/ledgerwatch/erigon-lib/gointerfaces"
	types2 "github.com/ledgerwatch/erigon-lib/gointerfaces/types"
	"github.com/ledgerwatch/erigon-lib/kv"
	"github.com/ledgerwatch/erigon/cmd/rpcdaemon/services"
	"github.com/ledgerwatch/erigon/common"
	"github.com/ledgerwatch/erigon/common/hexutil"
	"github.com/ledgerwatch/erigon/core/types"
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
type PayloadAttributes struct {
	Timestamp             hexutil.Uint64 `json:"timestamp"             gencodec:"required"`
	Random                common.Hash    `json:"random"                gencodec:"required"`
	SuggestedFeeRecipient common.Address `json:"suggestedFeeRecipient" gencodec:"required"`
}

// EngineAPI Beacon chain communication endpoint
type EngineAPI interface {
	ForkchoiceUpdatedV1(context.Context, struct{}, *PayloadAttributes) (map[string]interface{}, error)
	ExecutePayloadV1(context.Context, *ExecutionPayload) (map[string]interface{}, error)
	GetPayloadV1(ctx context.Context, payloadID hexutil.Uint64) (*ExecutionPayload, error)
}

// EngineImpl is implementation of the EngineAPI interface
type EngineImpl struct {
	*BaseAPI
	db  kv.RoDB
	api services.ApiBackend
}

// ForkchoiceUpdatedV1 is executed only if we are running a beacon validator,
// in erigon we do not use this for reorgs like go-ethereum does since we can do that in engine_executePayloadV1
// if the payloadAttributes is different than null, we return
func (e *EngineImpl) ForkchoiceUpdatedV1(_ context.Context, _ struct{}, payloadAttributes *PayloadAttributes) (map[string]interface{}, error) {
	// Unwinds can be made within engine_excutePayloadV1 so we can return success regardless
	if payloadAttributes == nil {
		return map[string]interface{}{
			"status":    "SUCCESS",
			"payloadId": nil,
		}, nil
	}
	// Request for assembling payload
	return nil, fmt.Errorf("invalid request")
}

// ExecutePayloadV1 takes a block from the beacon chain and do either two of the following things
// - Stageloop the block just received if we have the payload's parent hash already
// - Start the reverse sync process otherwise, and return "Syncing"
func (e *EngineImpl) ExecutePayloadV1(ctx context.Context, payload *ExecutionPayload) (map[string]interface{}, error) {
	var baseFee *uint256.Int
	if payload.BaseFeePerGas != nil {
		var overflow bool
		baseFee, overflow = uint256.FromBig((*big.Int)(payload.BaseFeePerGas))
		if overflow {
			return nil, fmt.Errorf("invalid request")
		}
	}
	// Maximum length of extra is 32 bytes so we can use the hash datatype
	extra := common.BytesToHash(payload.ExtraData)

	log.Info("Received Payload from beacon-chain")

	// Convert slice of hexutil.Bytes to a slice of slice of bytes
	transactions := make([][]byte, len(payload.Transactions))
	for i, transaction := range payload.Transactions {
		transactions[i] = ([]byte)(transaction)
	}
	res, err := e.api.EngineExecutePayloadV1(ctx, &types2.ExecutionPayload{
		ParentHash:    gointerfaces.ConvertHashToH256(payload.ParentHash),
		Coinbase:      gointerfaces.ConvertAddressToH160(payload.FeeRecipient),
		StateRoot:     gointerfaces.ConvertHashToH256(payload.StateRoot),
		ReceiptRoot:   gointerfaces.ConvertHashToH256(payload.ReceiptsRoot),
		LogsBloom:     gointerfaces.ConvertBytesToH2048(([]byte)(payload.LogsBloom)),
		Random:        gointerfaces.ConvertHashToH256(payload.Random),
		BlockNumber:   (uint64)(payload.BlockNumber),
		GasLimit:      (uint64)(payload.GasLimit),
		GasUsed:       (uint64)(payload.GasUsed),
		Timestamp:     (uint64)(payload.Timestamp),
		ExtraData:     gointerfaces.ConvertHashToH256(extra),
		BaseFeePerGas: gointerfaces.ConvertUint256IntToH256(baseFee),
		BlockHash:     gointerfaces.ConvertHashToH256(payload.BlockHash),
		Transactions:  transactions,
	})
	if err != nil {
		return nil, err
	}

	var latestValidHash common.Hash = gointerfaces.ConvertH256ToHash(res.LatestValidHash)
	return map[string]interface{}{
		"status":          res.Status,
		"latestValidHash": common.Bytes2Hex(latestValidHash.Bytes()),
	}, nil
}

func (e *EngineImpl) GetPayloadV1(ctx context.Context, payloadID hexutil.Uint64) (*ExecutionPayload, error) {
	payload, err := e.api.EngineGetPayloadV1(ctx, (uint64)(payloadID))
	if err != nil {
		return nil, err
	}
	var bloom types.Bloom = gointerfaces.ConvertH2048ToBloom(payload.LogsBloom)
	var extra common.Hash = gointerfaces.ConvertH256ToHash(payload.ExtraData)

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
		ExtraData:     extra[:],
		BaseFeePerGas: (*hexutil.Big)(baseFee),
		BlockHash:     gointerfaces.ConvertH256ToHash(payload.BlockHash),
		Transactions:  transactions,
	}, nil
}

// NewEngineAPI returns EngineImpl instance
func NewEngineAPI(base *BaseAPI, db kv.RoDB, api services.ApiBackend) *EngineImpl {
	return &EngineImpl{
		BaseAPI: base,
		db:      db,
		api:     api,
	}
}
