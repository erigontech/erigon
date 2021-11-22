package commands

import (
	"context"
	"math/big"

	"github.com/holiman/uint256"
	"github.com/ledgerwatch/erigon-lib/gointerfaces"
	types2 "github.com/ledgerwatch/erigon-lib/gointerfaces/types"
	"github.com/ledgerwatch/erigon-lib/kv"
	"github.com/ledgerwatch/erigon/cmd/rpcdaemon/services"
	"github.com/ledgerwatch/erigon/common"
	"github.com/ledgerwatch/erigon/common/hexutil"
	"github.com/ledgerwatch/erigon/core/types"
	"github.com/ledgerwatch/erigon/rpc"
	"github.com/ledgerwatch/log/v3"
)

// The following code follows the Proof-of-stake specifications: https://github.com/ethereum/execution-apis/blob/v1.0.0-alpha.2/src/engine/interop/specification.md

// Status codes returned by the engine_ API
// - "SUCCESS": Operation ended successfully
// - "INVALID": Block could not be execute properly (Bad block)
// - "SYNCING": Execution layer is still syncing and trying to catch up to the block head.
const (
	engineSuccessCode = "SUCCESS"
	//	engineInvalidCode = "INVALID"
	engineSyncingCode = "SYNCING"
)

// ExecutionPayload represents an execution payload (aka slot/block)
type ExecutionPayload struct {
	ParentHash    common.Hash     `json:"parentHash"`
	StateRoot     common.Hash     `json:"stateRoot"`
	ReceiptRoot   common.Hash     `json:"receiptRoot"`
	LogsBloom     hexutil.Bytes   `json:"logsBloom"`
	Random        common.Hash     `json:"random"`
	BlockNumber   hexutil.Uint64  `json:"blockNumber"`
	GasLimit      hexutil.Uint64  `json:"gasLimit"`
	GasUsed       hexutil.Uint64  `json:"gasUsed"`
	Extra         hexutil.Bytes   `json:"extraData"`
	BaseFeePerGas *hexutil.Big    `json:"baseFeePerGas"`
	BlockHash     common.Hash     `json:"blockHash"`
	Timestamp     hexutil.Uint64  `json:"timestamp"`
	Coinbase      common.Address  `json:"coinbase"`
	Transactions  []hexutil.Bytes `json:"transactions"`
}

// PreparePayloadArgs represents a request to assemble a payload from the beacon chain
type PreparePayloadArgs struct {
	Random       *common.Hash    `json:"random"`
	Timestamp    *hexutil.Uint64 `json:"timestamp"`
	FeeRecipient *common.Address `json:"feeRecipients"`
}

// PreparePayloadArgs represents a request to execute POS_FORKCHOICE_UPDATED.
// specs: https://github.com/ethereum/EIPs/blob/504954e3bba2b58712d84865966ebc17bd4875f5/EIPS/eip-3675.md
type ForkchoiceArgs struct {
	HeadBlockHash      *rpc.BlockNumberOrHash `json:"headBlockHash"`
	SafeBlockHash      *rpc.BlockNumberOrHash `json:"safeBlockHash"`
	FinalizedBlockHash *rpc.BlockNumberOrHash `json:"finalizedBlockHash"`
}

// EngineAPI Beacon chain communication endpoint
type EngineAPI interface {
	ForkchoiceUpdatedV1(context.Context, ForkchoiceArgs, PreparePayloadArgs) (map[string]interface{}, error)
	ExecutePayloadV1(context.Context, ExecutionPayload) (map[string]interface{}, error)
	GetPayloadV1(context.Context, hexutil.Uint64) (*ExecutionPayload, error)
}

// EngineImpl is implementation of the EngineAPI interface
type EngineImpl struct {
	*BaseAPI
	db  kv.RoDB
	api services.ApiBackend
}

// PreparePayload is executed only if we running a beacon Validator so it
// has relatively low priority
func (e *EngineImpl) ForkchoiceUpdatedV1(_ context.Context, _ ForkchoiceArgs, buildPayloadArgs PreparePayloadArgs) (map[string]interface{}, error) {
	if buildPayloadArgs.Timestamp == nil {
		return map[string]interface{}{
			"status": engineSyncingCode,
		}, nil
	} else {
		return map[string]interface{}{
			"status":    engineSuccessCode,
			"payloadId": 0,
		}, nil
	}
}

// ExecutePayloadV1 takes a block from the beacon chain and do either two of the following things
// - Stageloop the block just received if we have the payload's parent hash already
// - Start the reverse sync process otherwise, and return "Syncing"
func (e *EngineImpl) ExecutePayloadV1(ctx context.Context, payload ExecutionPayload) (map[string]interface{}, error) {
	var baseFee *uint256.Int
	if payload.BaseFeePerGas != nil {
		baseFee, _ = uint256.FromBig((*big.Int)(payload.BaseFeePerGas))
	}
	// Maximum length of extra is 32 bytes so we can use the hash datatype
	extra := common.BytesToHash(payload.Extra)

	log.Info("Received Payload from beacon-chain")

	// Convert slice of hexutil.Bytes to a slice of slice of bytes
	transactions := make([][]byte, len(payload.Transactions))
	for i, transaction := range payload.Transactions {
		transactions[i] = ([]byte)(transaction)
	}
	res, err := e.api.EngineExecutePayloadV1(ctx, &types2.ExecutionPayload{
		ParentHash:  gointerfaces.ConvertHashToH256(payload.ParentHash),
		BlockHash:   gointerfaces.ConvertHashToH256(payload.BlockHash),
		StateRoot:   gointerfaces.ConvertHashToH256(payload.StateRoot),
		Coinbase:    gointerfaces.ConvertAddressToH160(payload.Coinbase),
		ReceiptRoot: gointerfaces.ConvertHashToH256(payload.ReceiptRoot),
		//		LogsBloom:     gointerfaces.ConvertBytesToH2048(([]byte)(payload.LogsBloom)),
		Random:        gointerfaces.ConvertHashToH256(payload.Random),
		BlockNumber:   (uint64)(payload.BlockNumber),
		GasLimit:      (uint64)(payload.GasLimit),
		GasUsed:       (uint64)(payload.GasUsed),
		Timestamp:     (uint64)(payload.Timestamp),
		ExtraData:     gointerfaces.ConvertHashToH256(extra),
		BaseFeePerGas: gointerfaces.ConvertUint256IntToH256(baseFee),
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

func (e *EngineImpl) EngineGetPayloadV1(ctx context.Context, payloadID hexutil.Uint64) (*ExecutionPayload, error) {
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
		BlockHash:     gointerfaces.ConvertH256ToHash(payload.BlockHash),
		ReceiptRoot:   gointerfaces.ConvertH256ToHash(payload.ReceiptRoot),
		StateRoot:     gointerfaces.ConvertH256ToHash(payload.StateRoot),
		Random:        gointerfaces.ConvertH256ToHash(payload.Random),
		LogsBloom:     bloom[:],
		Extra:         extra[:],
		BaseFeePerGas: (*hexutil.Big)(baseFee),
		BlockNumber:   hexutil.Uint64(payload.BlockNumber),
		GasLimit:      hexutil.Uint64(payload.GasLimit),
		GasUsed:       hexutil.Uint64(payload.GasUsed),
		Timestamp:     hexutil.Uint64(payload.Timestamp),
		Coinbase:      gointerfaces.ConvertH160toAddress(payload.Coinbase),
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
