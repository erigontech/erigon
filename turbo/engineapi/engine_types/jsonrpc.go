package engine_types

import (
	"encoding/binary"
	"encoding/json"
	"errors"

	"github.com/ledgerwatch/erigon-lib/common/hexutil"

	"github.com/ledgerwatch/erigon-lib/common"
	"github.com/ledgerwatch/erigon-lib/common/hexutility"
	"github.com/ledgerwatch/erigon-lib/gointerfaces"
	"github.com/ledgerwatch/erigon-lib/gointerfaces/execution"
	types2 "github.com/ledgerwatch/erigon-lib/gointerfaces/types"
	"github.com/ledgerwatch/erigon/core/types"
)

// ExecutionPayload represents an execution payload (aka block)
type ExecutionPayload struct {
	ParentHash    common.Hash         `json:"parentHash"    gencodec:"required"`
	FeeRecipient  common.Address      `json:"feeRecipient"  gencodec:"required"`
	StateRoot     common.Hash         `json:"stateRoot"     gencodec:"required"`
	ReceiptsRoot  common.Hash         `json:"receiptsRoot"  gencodec:"required"`
	LogsBloom     hexutility.Bytes    `json:"logsBloom"     gencodec:"required"`
	PrevRandao    common.Hash         `json:"prevRandao"    gencodec:"required"`
	BlockNumber   hexutil.Uint64      `json:"blockNumber"   gencodec:"required"`
	GasLimit      hexutil.Uint64      `json:"gasLimit"      gencodec:"required"`
	GasUsed       hexutil.Uint64      `json:"gasUsed"       gencodec:"required"`
	Timestamp     hexutil.Uint64      `json:"timestamp"     gencodec:"required"`
	ExtraData     hexutility.Bytes    `json:"extraData"     gencodec:"required"`
	BaseFeePerGas *hexutil.Big        `json:"baseFeePerGas" gencodec:"required"`
	BlockHash     common.Hash         `json:"blockHash"     gencodec:"required"`
	Transactions  []hexutility.Bytes  `json:"transactions"  gencodec:"required"`
	Withdrawals   []*types.Withdrawal `json:"withdrawals"`
	BlobGasUsed   *hexutil.Uint64     `json:"blobGasUsed"`
	ExcessBlobGas *hexutil.Uint64     `json:"excessBlobGas"`
}

// PayloadAttributes represent the attributes required to start assembling a payload
type ForkChoiceState struct {
	HeadHash           common.Hash `json:"headBlockHash"             gencodec:"required"`
	SafeBlockHash      common.Hash `json:"safeBlockHash"             gencodec:"required"`
	FinalizedBlockHash common.Hash `json:"finalizedBlockHash"        gencodec:"required"`
}

// PayloadAttributes represent the attributes required to start assembling a payload
type PayloadAttributes struct {
	Timestamp             hexutil.Uint64      `json:"timestamp"             gencodec:"required"`
	PrevRandao            common.Hash         `json:"prevRandao"            gencodec:"required"`
	SuggestedFeeRecipient common.Address      `json:"suggestedFeeRecipient" gencodec:"required"`
	Withdrawals           []*types.Withdrawal `json:"withdrawals"`
	ParentBeaconBlockRoot *common.Hash        `json:"parentBeaconBlockRoot"`
}

// TransitionConfiguration represents the correct configurations of the CL and the EL
type TransitionConfiguration struct {
	TerminalTotalDifficulty *hexutil.Big `json:"terminalTotalDifficulty" gencodec:"required"`
	TerminalBlockHash       common.Hash  `json:"terminalBlockHash"       gencodec:"required"`
	TerminalBlockNumber     *hexutil.Big `json:"terminalBlockNumber"     gencodec:"required"`
}

// BlobsBundleV1 holds the blobs of an execution payload
type BlobsBundleV1 struct {
	Commitments []hexutility.Bytes `json:"commitments" gencodec:"required"`
	Proofs      []hexutility.Bytes `json:"proofs"      gencodec:"required"`
	Blobs       []hexutility.Bytes `json:"blobs"       gencodec:"required"`
}

type ExecutionPayloadBodyV1 struct {
	Transactions []hexutility.Bytes  `json:"transactions" gencodec:"required"`
	Withdrawals  []*types.Withdrawal `json:"withdrawals"  gencodec:"required"`
}

type PayloadStatus struct {
	Status          EngineStatus      `json:"status" gencodec:"required"`
	ValidationError *StringifiedError `json:"validationError"`
	LatestValidHash *common.Hash      `json:"latestValidHash"`
	CriticalError   error
}

type ForkChoiceUpdatedResponse struct {
	PayloadId     *hexutility.Bytes `json:"payloadId"` // We need to reformat the uint64 so this makes more sense.
	PayloadStatus *PayloadStatus    `json:"payloadStatus"`
}

type GetPayloadResponse struct {
	ExecutionPayload      *ExecutionPayload `json:"executionPayload" gencodec:"required"`
	BlockValue            *hexutil.Big      `json:"blockValue"`
	BlobsBundle           *BlobsBundleV1    `json:"blobsBundle"`
	ShouldOverrideBuilder bool              `json:"shouldOverrideBuilder"`
}

type StringifiedError struct{ err error }

func NewStringifiedError(err error) *StringifiedError {
	return &StringifiedError{err: err}
}

func NewStringifiedErrorFromString(err string) *StringifiedError {
	return &StringifiedError{err: errors.New(err)}
}

func (e StringifiedError) MarshalJSON() ([]byte, error) {
	if e.err == nil {
		return json.Marshal(nil)
	}
	return json.Marshal(e.err.Error())
}

func (e StringifiedError) Error() error {
	return e.err
}

func ConvertRpcBlockToExecutionPayload(payload *execution.Block) *ExecutionPayload {
	header := payload.Header
	body := payload.Body

	var bloom types.Bloom = gointerfaces.ConvertH2048ToBloom(header.LogsBloom)
	baseFee := gointerfaces.ConvertH256ToUint256Int(header.BaseFeePerGas).ToBig()

	// Convert slice of hexutility.Bytes to a slice of slice of bytes
	transactions := make([]hexutility.Bytes, len(body.Transactions))
	for i, transaction := range body.Transactions {
		transactions[i] = transaction
	}

	res := &ExecutionPayload{
		ParentHash:    gointerfaces.ConvertH256ToHash(header.ParentHash),
		FeeRecipient:  gointerfaces.ConvertH160toAddress(header.Coinbase),
		StateRoot:     gointerfaces.ConvertH256ToHash(header.StateRoot),
		ReceiptsRoot:  gointerfaces.ConvertH256ToHash(header.ReceiptRoot),
		LogsBloom:     bloom[:],
		PrevRandao:    gointerfaces.ConvertH256ToHash(header.PrevRandao),
		BlockNumber:   hexutil.Uint64(header.BlockNumber),
		GasLimit:      hexutil.Uint64(header.GasLimit),
		GasUsed:       hexutil.Uint64(header.GasUsed),
		Timestamp:     hexutil.Uint64(header.Timestamp),
		ExtraData:     header.ExtraData,
		BaseFeePerGas: (*hexutil.Big)(baseFee),
		BlockHash:     gointerfaces.ConvertH256ToHash(header.BlockHash),
		Transactions:  transactions,
	}
	if header.WithdrawalHash != nil {
		res.Withdrawals = ConvertWithdrawalsFromRpc(body.Withdrawals)
	}
	if header.BlobGasUsed != nil {
		blobGasUsed := *header.BlobGasUsed
		res.BlobGasUsed = (*hexutil.Uint64)(&blobGasUsed)
		excessBlobGas := *header.ExcessBlobGas
		res.ExcessBlobGas = (*hexutil.Uint64)(&excessBlobGas)
	}
	return res
}

func ConvertPayloadFromRpc(payload *types2.ExecutionPayload) *ExecutionPayload {
	var bloom types.Bloom = gointerfaces.ConvertH2048ToBloom(payload.LogsBloom)
	baseFee := gointerfaces.ConvertH256ToUint256Int(payload.BaseFeePerGas).ToBig()

	// Convert slice of hexutility.Bytes to a slice of slice of bytes
	transactions := make([]hexutility.Bytes, len(payload.Transactions))
	for i, transaction := range payload.Transactions {
		transactions[i] = transaction
	}

	res := &ExecutionPayload{
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
	}
	if payload.Version >= 2 {
		res.Withdrawals = ConvertWithdrawalsFromRpc(payload.Withdrawals)
	}
	if payload.Version >= 3 {
		blobGasUsed := *payload.BlobGasUsed
		res.BlobGasUsed = (*hexutil.Uint64)(&blobGasUsed)
		excessBlobGas := *payload.ExcessBlobGas
		res.ExcessBlobGas = (*hexutil.Uint64)(&excessBlobGas)
	}
	return res
}

func ConvertBlobsFromRpc(bundle *types2.BlobsBundleV1) *BlobsBundleV1 {
	if bundle == nil {
		return nil
	}
	res := &BlobsBundleV1{
		Commitments: make([]hexutility.Bytes, len(bundle.Commitments)),
		Proofs:      make([]hexutility.Bytes, len(bundle.Proofs)),
		Blobs:       make([]hexutility.Bytes, len(bundle.Blobs)),
	}
	for i, commitment := range bundle.Commitments {
		res.Commitments[i] = hexutility.Bytes(commitment)
	}
	for i, proof := range bundle.Proofs {
		res.Proofs[i] = hexutility.Bytes(proof)
	}
	for i, blob := range bundle.Blobs {
		res.Blobs[i] = hexutility.Bytes(blob)
	}
	return res
}

func ConvertWithdrawalsToRpc(in []*types.Withdrawal) []*types2.Withdrawal {
	if in == nil {
		return nil
	}
	out := make([]*types2.Withdrawal, 0, len(in))
	for _, w := range in {
		out = append(out, &types2.Withdrawal{
			Index:          w.Index,
			ValidatorIndex: w.Validator,
			Address:        gointerfaces.ConvertAddressToH160(w.Address),
			Amount:         w.Amount,
		})
	}
	return out
}

func ConvertWithdrawalsFromRpc(in []*types2.Withdrawal) []*types.Withdrawal {
	if in == nil {
		return nil
	}
	out := make([]*types.Withdrawal, 0, len(in))
	for _, w := range in {
		out = append(out, &types.Withdrawal{
			Index:     w.Index,
			Validator: w.ValidatorIndex,
			Address:   gointerfaces.ConvertH160toAddress(w.Address),
			Amount:    w.Amount,
		})
	}
	return out
}

func ConvertPayloadId(payloadId uint64) *hexutility.Bytes {
	encodedPayloadId := make([]byte, 8)
	binary.BigEndian.PutUint64(encodedPayloadId, payloadId)
	ret := hexutility.Bytes(encodedPayloadId)
	return &ret
}
