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

package engine_types

import (
	"encoding/binary"
	"encoding/json"
	"errors"
	"fmt"

	"github.com/erigontech/erigon-lib/common"
	"github.com/erigontech/erigon-lib/common/hexutil"
	"github.com/erigontech/erigon-lib/gointerfaces"
	execution "github.com/erigontech/erigon-lib/gointerfaces/executionproto"
	types2 "github.com/erigontech/erigon-lib/gointerfaces/typesproto"
	"github.com/erigontech/erigon/execution/types"
)

// ExecutionPayload represents an execution payload (aka block)
type ExecutionPayload struct {
	ParentHash    common.Hash         `json:"parentHash"    gencodec:"required"`
	FeeRecipient  common.Address      `json:"feeRecipient"  gencodec:"required"`
	StateRoot     common.Hash         `json:"stateRoot"     gencodec:"required"`
	ReceiptsRoot  common.Hash         `json:"receiptsRoot"  gencodec:"required"`
	LogsBloom     hexutil.Bytes       `json:"logsBloom"     gencodec:"required"`
	PrevRandao    common.Hash         `json:"prevRandao"    gencodec:"required"`
	BlockNumber   hexutil.Uint64      `json:"blockNumber"   gencodec:"required"`
	GasLimit      hexutil.Uint64      `json:"gasLimit"      gencodec:"required"`
	GasUsed       hexutil.Uint64      `json:"gasUsed"       gencodec:"required"`
	Timestamp     hexutil.Uint64      `json:"timestamp"     gencodec:"required"`
	ExtraData     hexutil.Bytes       `json:"extraData"     gencodec:"required"`
	BaseFeePerGas *hexutil.Big        `json:"baseFeePerGas" gencodec:"required"`
	BlockHash     common.Hash         `json:"blockHash"     gencodec:"required"`
	Transactions  []hexutil.Bytes     `json:"transactions"  gencodec:"required"`
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
	Commitments []hexutil.Bytes `json:"commitments" gencodec:"required"`
	Proofs      []hexutil.Bytes `json:"proofs"      gencodec:"required"`
	Blobs       []hexutil.Bytes `json:"blobs"       gencodec:"required"`
}

// BlobAndProofV1 holds one item for engine_getBlobsV1
type BlobAndProofV1 struct {
	Blob  hexutil.Bytes `json:"blob" gencodec:"required"`
	Proof hexutil.Bytes `json:"proof" gencodec:"required"`
}

// BlobAndProofV2 holds one item for engine_getBlobsV2
type BlobAndProofV2 struct {
	Blob       hexutil.Bytes   `json:"blob" gencodec:"required"`
	CellProofs []hexutil.Bytes `json:"proofs" gencodec:"required"`
}

type ExecutionPayloadBody struct {
	Transactions []hexutil.Bytes     `json:"transactions" gencodec:"required"`
	Withdrawals  []*types.Withdrawal `json:"withdrawals"  gencodec:"required"`
}

type PayloadStatus struct {
	Status          EngineStatus      `json:"status" gencodec:"required"`
	ValidationError *StringifiedError `json:"validationError"`
	LatestValidHash *common.Hash      `json:"latestValidHash"`
	CriticalError   error             `json:"-"`
}

type ForkChoiceUpdatedResponse struct {
	PayloadId     *hexutil.Bytes `json:"payloadId"` // We need to reformat the uint64 so this makes more sense.
	PayloadStatus *PayloadStatus `json:"payloadStatus"`
}

type GetPayloadResponse struct {
	ExecutionPayload      *ExecutionPayload `json:"executionPayload" gencodec:"required"`
	BlockValue            *hexutil.Big      `json:"blockValue"`
	BlobsBundle           *BlobsBundleV1    `json:"blobsBundle"`
	ExecutionRequests     []hexutil.Bytes   `json:"executionRequests"`
	ShouldOverrideBuilder bool              `json:"shouldOverrideBuilder"`
}

type ClientVersionV1 struct {
	Code    string `json:"code" gencodec:"required"`
	Name    string `json:"name" gencodec:"required"`
	Version string `json:"version" gencodec:"required"`
	Commit  string `json:"commit" gencodec:"required"`
}

func (c ClientVersionV1) String() string {
	return fmt.Sprintf("ClientCode: %s, %s-%s-%s", c.Code, c.Name, c.Version, c.Commit)
}

type StringifiedError struct{ err error }

func NewStringifiedError(err error) *StringifiedError {
	return &StringifiedError{err: err}
}

func NewStringifiedErrorFromString(err string) *StringifiedError {
	return &StringifiedError{err: errors.New(err)}
}

func (e *StringifiedError) MarshalJSON() ([]byte, error) {
	if e.err == nil {
		return json.Marshal(nil)
	}
	return json.Marshal(e.err.Error())
}

func (e *StringifiedError) UnmarshalJSON(data []byte) error {
	if len(data) == 0 || string(data) == "null" {
		return nil
	}
	var errStr string
	if err := json.Unmarshal(data, &errStr); err != nil {
		return err
	}
	e.err = errors.New(errStr)
	return nil
}

func (e *StringifiedError) Error() error {
	return e.err
}

func ConvertRpcBlockToExecutionPayload(payload *execution.Block) *ExecutionPayload {
	header := payload.Header
	body := payload.Body

	var bloom types.Bloom = gointerfaces.ConvertH2048ToBloom(header.LogsBloom)
	baseFee := gointerfaces.ConvertH256ToUint256Int(header.BaseFeePerGas).ToBig()

	// Convert slice of hexutil.Bytes to a slice of slice of bytes
	transactions := make([]hexutil.Bytes, len(body.Transactions))
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

	// Convert slice of hexutil.Bytes to a slice of slice of bytes
	transactions := make([]hexutil.Bytes, len(payload.Transactions))
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
		Commitments: make([]hexutil.Bytes, len(bundle.Commitments)),
		Proofs:      make([]hexutil.Bytes, len(bundle.Proofs)),
		Blobs:       make([]hexutil.Bytes, len(bundle.Blobs)),
	}
	for i, commitment := range bundle.Commitments {
		res.Commitments[i] = hexutil.Bytes(commitment)
	}
	for i, proof := range bundle.Proofs {
		res.Proofs[i] = hexutil.Bytes(proof)
	}
	for i, blob := range bundle.Blobs {
		res.Blobs[i] = hexutil.Bytes(blob)
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

func ConvertPayloadId(payloadId uint64) *hexutil.Bytes {
	encodedPayloadId := make([]byte, 8)
	binary.BigEndian.PutUint64(encodedPayloadId, payloadId)
	ret := hexutil.Bytes(encodedPayloadId)
	return &ret
}
