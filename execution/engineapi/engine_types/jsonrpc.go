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
	BaseFeePerGas   *hexutil.U256         `json:"baseFeePerGas"`
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

//go:generate go run github.com/erigontech/erigon/cmd/tools/jsongen -type BlobsBundle

// BlobsBundle holds the blobs of an execution payload.
// It covers both BlobsBundleV1 (https://github.com/ethereum/execution-apis/blob/main/src/engine/cancun.md#blobsbundlev1)
// and BlobsBundleV2 (https://github.com/ethereum/execution-apis/blob/main/src/engine/osaka.md#blobsbundlev2)
type BlobsBundle struct {
	Commitments []hexutil.Bytes       `json:"commitments" ethjson:"datalist"`
	Proofs      []hexutil.Bytes       `json:"proofs" ethjson:"datalist"`
	Blobs       []hexutil.Bytes       `json:"blobs" ethjson:"datalist"`
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
		if txn.Type() != types.BlobTxType && txn.GetBlobGas() == 0 {
			continue
		}
		var blobs types.Blobs
		var commitments types.BlobKzgs
		var proofs types.KZGProofs
		switch tx := txn.(type) {
		case *types.BlobTxWrapper:
			blobs, commitments, proofs = tx.Blobs, tx.Commitments, tx.Proofs
		case *types.FrameTxWrapper:
			blobs, commitments, proofs = tx.Blobs, tx.Commitments, tx.Proofs
		default:
			return nil, fmt.Errorf("missing blob sidecar for tx %d (%T)", i, txn)
		}
		for _, c := range commitments {
			cp := make([]byte, len(c))
			copy(cp, c[:])
			bundle.Commitments = append(bundle.Commitments, cp)
		}
		for _, p := range proofs {
			pp := make([]byte, len(p))
			copy(pp, p[:])
			bundle.Proofs = append(bundle.Proofs, pp)
		}
		//nolint:gocritic // rangeValCopy: iterating over 128KB blob byte array directly
		for _, b := range blobs {
			bp := make([]byte, len(b))
			copy(bp, b[:])
			bundle.Blobs = append(bundle.Blobs, bp)
		}
	}
	return bundle, nil
}

// BlobAndProofV1 holds one item for engine_getBlobsV1
//
//go:generate go run github.com/erigontech/erigon/cmd/tools/jsongen -type BlobAndProofV1
type BlobAndProofV1 struct {
	Blob  hexutil.Bytes `json:"blob" ethjson:"data"`
	Proof hexutil.Bytes `json:"proof" ethjson:"data"`
}

// BlobAndProofV2 holds one item for engine_getBlobsV2/engine_getBlobsV3
//
//go:generate go run github.com/erigontech/erigon/cmd/tools/jsongen -type BlobAndProofV2
type BlobAndProofV2 struct {
	Blob       hexutil.Bytes   `json:"blob" ethjson:"data"`
	CellProofs []hexutil.Bytes `json:"proofs" ethjson:"datalist"`
}

type BlobCellsAndProofsV1 struct {
	BlobCells []*hexutil.Bytes `json:"blob_cells"`
	Proofs    []*hexutil.Bytes `json:"proofs"`
}

//go:generate go run github.com/erigontech/erigon/cmd/tools/jsongen -type ExecutionPayloadBody

type ExecutionPayloadBody struct {
	Transactions []hexutil.Bytes   `json:"transactions" ethjson:"datalist"`
	Withdrawals  types.Withdrawals `json:"withdrawals" ethjson:"objects"`
}

//go:generate go run github.com/erigontech/erigon/cmd/tools/jsongen -type ExecutionPayloadBodyV2

type ExecutionPayloadBodyV2 struct {
	Transactions    []hexutil.Bytes   `json:"transactions" ethjson:"datalist"`
	Withdrawals     types.Withdrawals `json:"withdrawals" ethjson:"objects"`
	BlockAccessList *hexutil.Bytes    `json:"blockAccessList" ethjson:"data"`
}

type PayloadStatus struct {
	Status                 EngineStatus      `json:"status"`
	ValidationError        *StringifiedError `json:"validationError"`
	LatestValidHash        *common.Hash      `json:"latestValidHash"`
	CriticalError          error             `json:"-"`
	InclusionListSatisfied *bool             `json:"inclusionListSatisfied,omitempty"`
	// SSZVersion selects the JSON shape too: from HezeVersion on, the response
	// is PayloadStatusV2 and always carries inclusionListSatisfied, even as null.
	SSZVersion clparams.StateVersion `json:"-"`
}

func (s PayloadStatus) MarshalJSON() ([]byte, error) {
	type alias PayloadStatus
	if s.SSZVersion.Before(clparams.HezeVersion) {
		return json.Marshal(alias(s))
	}
	return json.Marshal(struct {
		alias
		InclusionListSatisfied *bool `json:"inclusionListSatisfied"`
	}{alias(s), s.InclusionListSatisfied})
}

type ForkChoiceUpdatedResponse struct {
	PayloadId     *hexutil.Bytes `json:"payloadId"` // We need to reformat the uint64 so this makes more sense.
	PayloadStatus *PayloadStatus `json:"payloadStatus"`
}

type GetPayloadResponse struct {
	ExecutionPayload      *ExecutionPayload `json:"executionPayload"`
	BlockValue            *hexutil.U256     `json:"blockValue"`
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

func (v ClientVersionV1) String() string {
	return fmt.Sprintf("ClientCode: %s, %s-%s-%s", v.Code, v.Name, v.Version, v.Commit)
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

func ConvertPayloadId(payloadId uint64) *hexutil.Bytes {
	encodedPayloadId := make([]byte, 8)
	binary.BigEndian.PutUint64(encodedPayloadId, payloadId)
	ret := hexutil.Bytes(encodedPayloadId)
	return &ret
}

func DecodePayloadID(payloadID []byte) (uint64, error) {
	if len(payloadID) != 8 {
		return 0, errors.New("invalid payload id: expected 8 bytes")
	}
	return binary.BigEndian.Uint64(payloadID), nil
}
