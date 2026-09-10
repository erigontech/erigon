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
	"strings"

	"github.com/erigontech/erigon/cl/clparams"
	"github.com/erigontech/erigon/common"
	"github.com/erigontech/erigon/common/hexutil"
	"github.com/erigontech/erigon/db/version"
	"github.com/erigontech/erigon/execution/types"
	"github.com/erigontech/erigon/node/gointerfaces"
	"github.com/erigontech/erigon/node/gointerfaces/executionproto"
	"github.com/erigontech/erigon/node/gointerfaces/typesproto"
)

// ExecutionPayload represents an execution payload (aka block)
type ExecutionPayload struct {
	ParentHash      common.Hash           `json:"parentHash"`
	FeeRecipient    common.Address        `json:"feeRecipient"`
	StateRoot       common.Hash           `json:"stateRoot"`
	ReceiptsRoot    common.Hash           `json:"receiptsRoot"`
	LogsBloom       hexutil.Bytes         `json:"logsBloom"`
	PrevRandao      common.Hash           `json:"prevRandao"`
	BlockNumber     hexutil.Uint64        `json:"blockNumber"`
	GasLimit        hexutil.Uint64        `json:"gasLimit"`
	GasUsed         hexutil.Uint64        `json:"gasUsed"`
	Timestamp       hexutil.Uint64        `json:"timestamp"`
	ExtraData       hexutil.Bytes         `json:"extraData"`
	BaseFeePerGas   *hexutil.Big          `json:"baseFeePerGas"`
	BlockHash       common.Hash           `json:"blockHash"`
	Transactions    []hexutil.Bytes       `json:"transactions"`
	Withdrawals     []*types.Withdrawal   `json:"withdrawals"`
	BlobGasUsed     *hexutil.Uint64       `json:"blobGasUsed"`
	ExcessBlobGas   *hexutil.Uint64       `json:"excessBlobGas"`
	SlotNumber      *hexutil.Uint64       `json:"slotNumber,omitempty"`
	BlockAccessList *hexutil.Bytes        `json:"blockAccessList,omitempty"`
	SSZVersion      clparams.StateVersion `json:"-"`
}

// ForkChoiceState is the head/safe/finalized triple of engine_forkchoiceUpdated.
type ForkChoiceState struct {
	HeadHash           common.Hash `json:"headBlockHash"`
	SafeBlockHash      common.Hash `json:"safeBlockHash"`
	FinalizedBlockHash common.Hash `json:"finalizedBlockHash"`
}

// PayloadAttributes represent the attributes required to start assembling a payload
type PayloadAttributes struct {
	Timestamp             hexutil.Uint64        `json:"timestamp"`
	PrevRandao            common.Hash           `json:"prevRandao"`
	SuggestedFeeRecipient common.Address        `json:"suggestedFeeRecipient"`
	Withdrawals           []*types.Withdrawal   `json:"withdrawals"`
	ParentBeaconBlockRoot *common.Hash          `json:"parentBeaconBlockRoot"`
	SlotNumber            *hexutil.Uint64       `json:"slotNumber"`
	TargetGasLimit        *hexutil.Uint64       `json:"targetGasLimit"`
	SSZVersion            clparams.StateVersion `json:"-"`
}

// TransitionConfiguration represents the correct configurations of the CL and the EL
type TransitionConfiguration struct {
	TerminalTotalDifficulty *hexutil.Big `json:"terminalTotalDifficulty"`
	TerminalBlockHash       common.Hash  `json:"terminalBlockHash"`
	TerminalBlockNumber     *hexutil.Big `json:"terminalBlockNumber"`
}

// BlobsBundle holds the blobs of an execution payload.
// It covers both BlobsBundleV1 (https://github.com/ethereum/execution-apis/blob/main/src/engine/cancun.md#blobsbundlev1)
// and BlobsBundleV2 (https://github.com/ethereum/execution-apis/blob/main/src/engine/osaka.md#blobsbundlev2)
type BlobsBundle struct {
	Commitments []hexutil.Bytes       `json:"commitments"`
	Proofs      []hexutil.Bytes       `json:"proofs"`
	Blobs       []hexutil.Bytes       `json:"blobs"`
	SSZVersion  clparams.StateVersion `json:"-"`
}

// BlobsBundleFromTransactions builds a BlobsBundle by extracting blobs,
// commitments, and proofs from blob transactions in the given list.
func BlobsBundleFromTransactions(txs types.Transactions) (*BlobsBundle, error) {
	bundle := &BlobsBundle{
		Commitments: make([]hexutil.Bytes, 0),
		Proofs:      make([]hexutil.Bytes, 0),
		Blobs:       make([]hexutil.Bytes, 0),
	}
	for i, txn := range txs {
		if txn.Type() != types.BlobTxType {
			continue
		}
		blobTx, ok := txn.(*types.BlobTxWrapper)
		if !ok {
			return nil, fmt.Errorf("expected BlobTxWrapper for tx %d, got %T", i, txn)
		}
		for _, c := range blobTx.Commitments {
			cp := make([]byte, len(c))
			copy(cp, c[:])
			bundle.Commitments = append(bundle.Commitments, cp)
		}
		for _, p := range blobTx.Proofs {
			pp := make([]byte, len(p))
			copy(pp, p[:])
			bundle.Proofs = append(bundle.Proofs, pp)
		}
		//nolint:gocritic // rangeValCopy: iterating over 128KB blob byte array directly
		for _, b := range blobTx.Blobs {
			bp := make([]byte, len(b))
			copy(bp, b[:])
			bundle.Blobs = append(bundle.Blobs, bp)
		}
	}
	return bundle, nil
}

// BlobAndProofV1 holds one item for engine_getBlobsV1
type BlobAndProofV1 struct {
	Blob  hexutil.Bytes `json:"blob"`
	Proof hexutil.Bytes `json:"proof"`
}

// BlobAndProofV2 holds one item for engine_getBlobsV2/engine_getBlobsV3
type BlobAndProofV2 struct {
	Blob       hexutil.Bytes   `json:"blob"`
	CellProofs []hexutil.Bytes `json:"proofs"`
}

type ExecutionPayloadBody struct {
	Transactions []hexutil.Bytes     `json:"transactions"`
	Withdrawals  []*types.Withdrawal `json:"withdrawals"`
}

type ExecutionPayloadBodyV2 struct {
	Transactions    []hexutil.Bytes     `json:"transactions"`
	Withdrawals     []*types.Withdrawal `json:"withdrawals"`
	BlockAccessList *hexutil.Bytes      `json:"blockAccessList"`
}

type PayloadStatus struct {
	Status          EngineStatus      `json:"status"`
	ValidationError *StringifiedError `json:"validationError"`
	LatestValidHash *common.Hash      `json:"latestValidHash"`
	CriticalError   error             `json:"-"`
}

type ForkChoiceUpdatedResponse struct {
	PayloadId     *hexutil.Bytes `json:"payloadId"` // We need to reformat the uint64 so this makes more sense.
	PayloadStatus *PayloadStatus `json:"payloadStatus"`
}

type GetPayloadResponse struct {
	ExecutionPayload      *ExecutionPayload `json:"executionPayload"`
	BlockValue            *hexutil.Big      `json:"blockValue"`
	BlobsBundle           *BlobsBundle      `json:"blobsBundle"`
	ExecutionRequests     []hexutil.Bytes   `json:"executionRequests"`
	ShouldOverrideBuilder bool              `json:"shouldOverrideBuilder"`
}

type ClientVersionV1 struct {
	Code    string `json:"code"`
	Name    string `json:"name"`
	Version string `json:"version"`
	Commit  string `json:"commit"`
}

func (c ClientVersionV1) String() string {
	return fmt.Sprintf("ClientCode: %s, %s-%s-%s", c.Code, c.Name, c.Version, c.Commit)
}

// NewClientVersionV1 builds a ClientVersionV1 from a git commit hash, using its leading
// 4 bytes as required by the standard, or all-zero bytes when the hash is missing or too
// short. See https://github.com/ethereum/execution-apis/blob/main/src/engine/identification.md
func NewClientVersionV1(code, name, versionStr, gitCommit string) ClientVersionV1 {
	commit := strings.TrimPrefix(gitCommit, "0x")
	if len(commit) >= 8 {
		commit = commit[:8]
	} else {
		commit = "00000000"
	}
	return ClientVersionV1{
		Code:    code,
		Name:    name,
		Version: versionStr,
		Commit:  "0x" + commit,
	}
}

// LocalClientVersionV1 returns the ClientVersionV1 describing this node.
func LocalClientVersionV1() ClientVersionV1 {
	return NewClientVersionV1(version.ClientCode, version.ClientName, version.VersionWithCommit(version.GitCommit), version.GitCommit)
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

func ConvertRpcBlockToExecutionPayload(payload *executionproto.Block) *ExecutionPayload {
	header := payload.Header
	body := payload.Body

	var bloom types.Bloom = gointerfaces.ConvertH2048ToBloom(header.LogsBloom)
	baseFee := gointerfaces.ConvertH256ToUint256Int(header.BaseFeePerGas)

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
		BaseFeePerGas: (*hexutil.Big)(baseFee.ToBig()),
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
	if header.SlotNumber != nil {
		slotNumber := *header.SlotNumber
		res.SlotNumber = (*hexutil.Uint64)(&slotNumber)
	}
	return res
}

func ConvertPayloadFromRpc(payload *typesproto.ExecutionPayload) *ExecutionPayload {
	var bloom types.Bloom = gointerfaces.ConvertH2048ToBloom(payload.LogsBloom)
	baseFee := gointerfaces.ConvertH256ToUint256Int(payload.BaseFeePerGas)

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
		BaseFeePerGas: (*hexutil.Big)(baseFee.ToBig()),
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
	if payload.Version >= 4 {
		if payload.SlotNumber != nil {
			slotNumber := *payload.SlotNumber
			res.SlotNumber = (*hexutil.Uint64)(&slotNumber)
		}
		bal := types.ConvertBlockAccessListFromTypesProto(payload.BlockAccessList)
		if bal == nil {
			bal = hexutil.Bytes{}
		}
		res.BlockAccessList = &bal
	}
	return res
}

func ConvertBlobsFromRpc(bundle *typesproto.BlobsBundle) *BlobsBundle {
	if bundle == nil {
		return nil
	}
	res := &BlobsBundle{
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

func ConvertWithdrawalsToRpc(in []*types.Withdrawal) []*typesproto.Withdrawal {
	if in == nil {
		return nil
	}
	out := make([]*typesproto.Withdrawal, 0, len(in))
	for _, w := range in {
		out = append(out, &typesproto.Withdrawal{
			Index:          uint64(w.Index),
			ValidatorIndex: uint64(w.Validator),
			Address:        gointerfaces.ConvertAddressToH160(w.Address),
			Amount:         uint64(w.Amount),
		})
	}
	return out
}

func ConvertWithdrawalsFromRpc(in []*typesproto.Withdrawal) []*types.Withdrawal {
	if in == nil {
		return nil
	}
	out := make([]*types.Withdrawal, 0, len(in))
	for _, w := range in {
		out = append(out, &types.Withdrawal{
			Index:     hexutil.Uint64(w.Index),
			Validator: hexutil.Uint64(w.ValidatorIndex),
			Address:   gointerfaces.ConvertH160toAddress(w.Address),
			Amount:    hexutil.Uint64(w.Amount),
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
