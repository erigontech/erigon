package commands

import (
	"context"

	"github.com/ledgerwatch/erigon-lib/gointerfaces/remote"
	"github.com/ledgerwatch/erigon-lib/kv"
	"github.com/ledgerwatch/erigon/common"
	"github.com/ledgerwatch/erigon/common/hexutil"
	"github.com/ledgerwatch/erigon/rpc"
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
	ParentHash    *rpc.BlockNumberOrHash `json:"parentHash"`
	StateRoot     *rpc.BlockNumberOrHash `json:"stateRoot"`
	ReceiptRoot   *rpc.BlockNumberOrHash `json:"receiptRoot"`
	LogsBloom     *hexutil.Bytes         `json:"logsBloom"`
	Random        *rpc.BlockNumberOrHash `json:"random"`
	BlockNumber   *hexutil.Uint64        `json:"blockNumber"`
	GasLimit      *hexutil.Uint64        `json:"gasLimit"`
	GasUsed       *hexutil.Uint64        `json:"gasUsed"`
	Extra         *hexutil.Bytes         `json:"extraData"`
	BaseFeePerGas *hexutil.Big           `json:"baseFeePerGas"`
	BlockHash     *rpc.BlockNumberOrHash `json:"blockHash"`
	Timestamp     *hexutil.Uint64        `json:"timestamp"`
	Coinbase      *common.Address        `json:"coinbase"`
	Transactions  *[]hexutil.Bytes       `json:"transactions"`
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
	GetPayloadV1(context.Context, hexutil.Uint64) (ExecutionPayload, error)
}

// EngineImpl is implementation of the EngineAPI interface
type EngineImpl struct {
	*BaseAPI
	remoteEthBackend remote.ETHBACKENDClient
	db               kv.RoDB
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
	res, err := e.remoteEthBackend.EngineExecutePayloadV1(ctx, payload)
	if err != nil {
		return nil, err
	}
}

func (e *EngineImpl) GetPayloadV1(context.Context, hexutil.Uint64) (ExecutionPayload, error) {
	return ExecutionPayload{}, nil
}

// NewEngineAPI returns EngineImpl instance
func NewEngineAPI(base *BaseAPI, db kv.RoDB) *EngineImpl {
	return &EngineImpl{
		BaseAPI: base,
		db:      db,
	}
}
