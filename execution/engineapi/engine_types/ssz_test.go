// Copyright 2025 The Erigon Authors
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
	"math/big"
	"testing"

	"github.com/erigontech/erigon/common"
	"github.com/erigontech/erigon/common/hexutil"
	"github.com/erigontech/erigon/execution/types"
	"github.com/stretchr/testify/require"
)

func TestPayloadStatusSSZRoundTrip(t *testing.T) {
	req := require.New(t)

	// Test with all fields set
	hash := common.HexToHash("0x1234567890abcdef1234567890abcdef1234567890abcdef1234567890abcdef")
	ps := &PayloadStatusSSZ{
		Status:          SSZStatusValid,
		LatestValidHash: &hash,
		ValidationError: "test error",
	}

	encoded, err := ps.EncodeSSZ(nil)
	req.NoError(err)
	decoded, err := DecodePayloadStatusSSZ(encoded)
	req.NoError(err)
	req.Equal(ps.Status, decoded.Status)
	req.NotNil(decoded.LatestValidHash)
	req.Equal(*ps.LatestValidHash, *decoded.LatestValidHash)
	req.Equal(ps.ValidationError, decoded.ValidationError)

	// Test with nil LatestValidHash
	ps2 := &PayloadStatusSSZ{
		Status:          SSZStatusSyncing,
		LatestValidHash: nil,
		ValidationError: "",
	}

	encoded2, err := ps2.EncodeSSZ(nil)
	req.NoError(err)
	decoded2, err := DecodePayloadStatusSSZ(encoded2)
	req.NoError(err)
	req.Equal(SSZStatusSyncing, decoded2.Status)
	req.Nil(decoded2.LatestValidHash)
	req.Empty(decoded2.ValidationError)
}

func TestPayloadStatusConversion(t *testing.T) {
	req := require.New(t)

	hash := common.HexToHash("0xabcdef")
	ps := &PayloadStatus{
		Status:          ValidStatus,
		LatestValidHash: &hash,
		ValidationError: NewStringifiedErrorFromString("block invalid"),
	}

	ssz := PayloadStatusToSSZ(ps)
	req.Equal(SSZStatusValid, ssz.Status)
	req.Equal(hash, *ssz.LatestValidHash)
	req.Equal("block invalid", ssz.ValidationError)

	back := ssz.ToPayloadStatus()
	req.Equal(ValidStatus, back.Status)
	req.Equal(hash, *back.LatestValidHash)
	req.NotNil(back.ValidationError)
	req.Equal("block invalid", back.ValidationError.Error().Error())
}

func TestEngineStatusSSZConversion(t *testing.T) {
	req := require.New(t)

	tests := []struct {
		status   EngineStatus
		sszValue uint8
	}{
		{ValidStatus, SSZStatusValid},
		{InvalidStatus, SSZStatusInvalid},
		{SyncingStatus, SSZStatusSyncing},
		{AcceptedStatus, SSZStatusAccepted},
		{InvalidBlockHashStatus, SSZStatusInvalidBlockHash},
	}

	for _, tt := range tests {
		req.Equal(tt.sszValue, EngineStatusToSSZ(tt.status), "EngineStatusToSSZ(%s)", tt.status)
		req.Equal(tt.status, SSZToEngineStatus(tt.sszValue), "SSZToEngineStatus(%d)", tt.sszValue)
	}
}

func TestForkchoiceStateRoundTrip(t *testing.T) {
	req := require.New(t)

	fcs := &ForkChoiceState{
		HeadHash:           common.HexToHash("0x1111111111111111111111111111111111111111111111111111111111111111"),
		SafeBlockHash:      common.HexToHash("0x2222222222222222222222222222222222222222222222222222222222222222"),
		FinalizedBlockHash: common.HexToHash("0x3333333333333333333333333333333333333333333333333333333333333333"),
	}

	encoded := EncodeForkchoiceState(fcs)
	req.Len(encoded, 96)

	decoded, err := DecodeForkchoiceState(encoded)
	req.NoError(err)
	req.Equal(fcs.HeadHash, decoded.HeadHash)
	req.Equal(fcs.SafeBlockHash, decoded.SafeBlockHash)
	req.Equal(fcs.FinalizedBlockHash, decoded.FinalizedBlockHash)
}

func TestForkchoiceStateDecodeShortBuffer(t *testing.T) {
	req := require.New(t)

	_, err := DecodeForkchoiceState(make([]byte, 50))
	req.Error(err)
}

func TestCapabilitiesRoundTrip(t *testing.T) {
	req := require.New(t)

	caps := []string{
		"engine_newPayloadV4",
		"engine_forkchoiceUpdatedV3",
		"engine_getPayloadV4",
	}

	encoded := EncodeCapabilities(caps)
	decoded, err := DecodeCapabilities(encoded)
	req.NoError(err)
	req.Equal(caps, decoded)
}

func TestClientVersionRoundTrip(t *testing.T) {
	req := require.New(t)

	cv := &ClientVersionV1{
		Code:    "EG",
		Name:    "Erigon",
		Version: "3.0.0",
		Commit:  "0xdeadbeef",
	}

	encoded := EncodeClientVersion(cv)
	decoded, err := DecodeClientVersion(encoded)
	req.NoError(err)
	req.Equal(cv.Code, decoded.Code)
	req.Equal(cv.Name, decoded.Name)
	req.Equal(cv.Version, decoded.Version)
	req.Equal(cv.Commit, decoded.Commit)
}

func TestClientVersionsRoundTrip(t *testing.T) {
	req := require.New(t)

	versions := []ClientVersionV1{
		{Code: "EG", Name: "Erigon", Version: "3.0.0", Commit: "0xdeadbeef"},
		{Code: "GE", Name: "Geth", Version: "1.14.0", Commit: "0xabcdef01"},
	}

	encoded := EncodeClientVersions(versions)
	decoded, err := DecodeClientVersions(encoded)
	req.NoError(err)
	req.Len(decoded, 2)
	req.Equal(versions[0].Code, decoded[0].Code)
	req.Equal(versions[0].Name, decoded[0].Name)
	req.Equal(versions[0].Commit, decoded[0].Commit)
	req.Equal(versions[1].Code, decoded[1].Code)
	req.Equal(versions[1].Version, decoded[1].Version)
	req.Equal(versions[1].Commit, decoded[1].Commit)
}

func TestGetBlobsRequestRoundTrip(t *testing.T) {
	req := require.New(t)

	hashes := []common.Hash{
		common.HexToHash("0x1111111111111111111111111111111111111111111111111111111111111111"),
		common.HexToHash("0x2222222222222222222222222222222222222222222222222222222222222222"),
		common.HexToHash("0x3333333333333333333333333333333333333333333333333333333333333333"),
	}

	encoded := EncodeGetBlobsRequest(hashes)
	decoded, err := DecodeGetBlobsRequest(encoded)
	req.NoError(err)
	req.Len(decoded, 3)
	for i := range hashes {
		req.Equal(hashes[i], decoded[i])
	}
}

func TestGetBlobsRequestEmpty(t *testing.T) {
	req := require.New(t)

	encoded := EncodeGetBlobsRequest(nil)
	decoded, err := DecodeGetBlobsRequest(encoded)
	req.NoError(err)
	req.Empty(decoded)
}

func TestPayloadStatusSSZDecodeShortBuffer(t *testing.T) {
	req := require.New(t)

	_, err := DecodePayloadStatusSSZ(make([]byte, 5))
	req.Error(err)
}

func TestCapabilitiesDecodeShortBuffer(t *testing.T) {
	req := require.New(t)

	_, err := DecodeCapabilities(make([]byte, 2))
	req.Error(err)
	req.Contains(err.Error(), "buffer too short")
}

func TestClientVersionDecodeShortBuffer(t *testing.T) {
	req := require.New(t)

	_, err := DecodeClientVersion(make([]byte, 4))
	req.Error(err)
}

func TestGetBlobsRequestDecodeShortBuffer(t *testing.T) {
	req := require.New(t)

	_, err := DecodeGetBlobsRequest(make([]byte, 2))
	req.Error(err)
}

// --- ForkchoiceUpdatedResponse round-trip tests ---

func TestForkchoiceUpdatedResponseRoundTrip(t *testing.T) {
	req := require.New(t)

	hash := common.HexToHash("0xabcdef")
	ps := &PayloadStatus{
		Status:          ValidStatus,
		LatestValidHash: &hash,
	}
	resp := &ForkChoiceUpdatedResponse{
		PayloadStatus: ps,
		PayloadId:     nil,
	}

	encoded := EncodeForkchoiceUpdatedResponse(resp)
	decoded, err := DecodeForkchoiceUpdatedResponse(encoded)
	req.NoError(err)
	req.Equal(SSZStatusValid, decoded.PayloadStatus.Status)
	req.Equal(hash, *decoded.PayloadStatus.LatestValidHash)
	req.Empty(decoded.PayloadStatus.ValidationError)
	req.Nil(decoded.PayloadId)
}

func TestForkchoiceUpdatedResponseWithPayloadId(t *testing.T) {
	req := require.New(t)

	hash := common.HexToHash("0x1234")
	pidBytes := make(hexutil.Bytes, 8)
	pidBytes[0] = 0x00
	pidBytes[1] = 0x00
	pidBytes[2] = 0x00
	pidBytes[3] = 0x00
	pidBytes[4] = 0x00
	pidBytes[5] = 0x00
	pidBytes[6] = 0x00
	pidBytes[7] = 0x42
	ps := &PayloadStatus{
		Status:          SyncingStatus,
		LatestValidHash: &hash,
	}
	resp := &ForkChoiceUpdatedResponse{
		PayloadStatus: ps,
		PayloadId:     &pidBytes,
	}

	encoded := EncodeForkchoiceUpdatedResponse(resp)
	decoded, err := DecodeForkchoiceUpdatedResponse(encoded)
	req.NoError(err)
	req.Equal(SSZStatusSyncing, decoded.PayloadStatus.Status)
	req.NotNil(decoded.PayloadId)
	req.Equal([]byte(pidBytes), decoded.PayloadId)
}

func TestForkchoiceUpdatedResponseWithValidationError(t *testing.T) {
	req := require.New(t)

	hash := common.HexToHash("0xdeadbeef")
	pidBytes := make(hexutil.Bytes, 8)
	pidBytes[7] = 0xFF
	ps := &PayloadStatus{
		Status:          InvalidStatus,
		LatestValidHash: &hash,
		ValidationError: NewStringifiedErrorFromString("block gas limit exceeded by a very long error message that makes the buffer larger"),
	}
	resp := &ForkChoiceUpdatedResponse{
		PayloadStatus: ps,
		PayloadId:     &pidBytes,
	}

	encoded := EncodeForkchoiceUpdatedResponse(resp)
	decoded, err := DecodeForkchoiceUpdatedResponse(encoded)
	req.NoError(err)
	req.Equal(SSZStatusInvalid, decoded.PayloadStatus.Status)
	req.Equal(hash, *decoded.PayloadStatus.LatestValidHash)
	req.Equal("block gas limit exceeded by a very long error message that makes the buffer larger", decoded.PayloadStatus.ValidationError)
	req.NotNil(decoded.PayloadId)
	req.Equal([]byte(pidBytes), decoded.PayloadId)
}

func TestForkchoiceUpdatedResponseShortBuffer(t *testing.T) {
	req := require.New(t)

	_, err := DecodeForkchoiceUpdatedResponse(make([]byte, 4))
	req.Error(err)
}

// --- ExecutionPayload SSZ round-trip tests ---

func makeTestExecutionPayloadV1() *ExecutionPayload {
	baseFee := big.NewInt(1000000000) // 1 gwei
	return &ExecutionPayload{
		ParentHash:    common.HexToHash("0x1111111111111111111111111111111111111111111111111111111111111111"),
		FeeRecipient:  common.HexToAddress("0x2222222222222222222222222222222222222222"),
		StateRoot:     common.HexToHash("0x3333333333333333333333333333333333333333333333333333333333333333"),
		ReceiptsRoot:  common.HexToHash("0x4444444444444444444444444444444444444444444444444444444444444444"),
		LogsBloom:     make(hexutil.Bytes, 256),
		PrevRandao:    common.HexToHash("0x5555555555555555555555555555555555555555555555555555555555555555"),
		BlockNumber:   hexutil.Uint64(100),
		GasLimit:      hexutil.Uint64(30000000),
		GasUsed:       hexutil.Uint64(21000),
		Timestamp:     hexutil.Uint64(1700000000),
		ExtraData:     hexutil.Bytes{0x01, 0x02, 0x03},
		BaseFeePerGas: (*hexutil.Big)(baseFee),
		BlockHash:     common.HexToHash("0x6666666666666666666666666666666666666666666666666666666666666666"),
		Transactions: []hexutil.Bytes{
			{0xf8, 0x50, 0x80, 0x01, 0x82, 0x52, 0x08},
			{0xf8, 0x60, 0x80, 0x02, 0x83, 0x01, 0x00, 0x00},
		},
	}
}

func TestExecutionPayloadV1RoundTrip(t *testing.T) {
	req := require.New(t)

	ep := makeTestExecutionPayloadV1()

	encoded := EncodeExecutionPayloadSSZ(ep, 1)
	decoded, err := DecodeExecutionPayloadSSZ(encoded, 1)
	req.NoError(err)

	req.Equal(ep.ParentHash, decoded.ParentHash)
	req.Equal(ep.FeeRecipient, decoded.FeeRecipient)
	req.Equal(ep.StateRoot, decoded.StateRoot)
	req.Equal(ep.ReceiptsRoot, decoded.ReceiptsRoot)
	req.Equal(ep.PrevRandao, decoded.PrevRandao)
	req.Equal(ep.BlockNumber, decoded.BlockNumber)
	req.Equal(ep.GasLimit, decoded.GasLimit)
	req.Equal(ep.GasUsed, decoded.GasUsed)
	req.Equal(ep.Timestamp, decoded.Timestamp)
	req.Equal([]byte(ep.ExtraData), []byte(decoded.ExtraData))
	req.Equal(ep.BaseFeePerGas.ToInt().String(), decoded.BaseFeePerGas.ToInt().String())
	req.Equal(ep.BlockHash, decoded.BlockHash)
	req.Len(decoded.Transactions, 2)
	req.Equal([]byte(ep.Transactions[0]), []byte(decoded.Transactions[0]))
	req.Equal([]byte(ep.Transactions[1]), []byte(decoded.Transactions[1]))
}

func TestExecutionPayloadV2RoundTrip(t *testing.T) {
	req := require.New(t)

	ep := makeTestExecutionPayloadV1()
	ep.Withdrawals = []*types.Withdrawal{
		{Index: 1, Validator: 100, Address: common.HexToAddress("0xaaaa"), Amount: 32000000000},
		{Index: 2, Validator: 200, Address: common.HexToAddress("0xbbbb"), Amount: 64000000000},
	}

	encoded := EncodeExecutionPayloadSSZ(ep, 2)
	decoded, err := DecodeExecutionPayloadSSZ(encoded, 2)
	req.NoError(err)

	req.Equal(ep.ParentHash, decoded.ParentHash)
	req.Equal(ep.BlockHash, decoded.BlockHash)
	req.Len(decoded.Transactions, 2)
	req.Len(decoded.Withdrawals, 2)
	req.Equal(ep.Withdrawals[0].Index, decoded.Withdrawals[0].Index)
	req.Equal(ep.Withdrawals[0].Validator, decoded.Withdrawals[0].Validator)
	req.Equal(ep.Withdrawals[0].Address, decoded.Withdrawals[0].Address)
	req.Equal(ep.Withdrawals[0].Amount, decoded.Withdrawals[0].Amount)
	req.Equal(ep.Withdrawals[1].Index, decoded.Withdrawals[1].Index)
}

func TestExecutionPayloadV3RoundTrip(t *testing.T) {
	req := require.New(t)

	ep := makeTestExecutionPayloadV1()
	ep.Withdrawals = []*types.Withdrawal{}
	blobGasUsed := hexutil.Uint64(131072)
	excessBlobGas := hexutil.Uint64(262144)
	ep.BlobGasUsed = &blobGasUsed
	ep.ExcessBlobGas = &excessBlobGas

	encoded := EncodeExecutionPayloadSSZ(ep, 3)
	decoded, err := DecodeExecutionPayloadSSZ(encoded, 3)
	req.NoError(err)

	req.Equal(ep.ParentHash, decoded.ParentHash)
	req.NotNil(decoded.BlobGasUsed)
	req.Equal(uint64(131072), uint64(*decoded.BlobGasUsed))
	req.NotNil(decoded.ExcessBlobGas)
	req.Equal(uint64(262144), uint64(*decoded.ExcessBlobGas))
}

func TestExecutionPayloadV3EmptyTransactions(t *testing.T) {
	req := require.New(t)

	ep := makeTestExecutionPayloadV1()
	ep.Transactions = []hexutil.Bytes{}
	ep.Withdrawals = []*types.Withdrawal{}
	blobGasUsed := hexutil.Uint64(0)
	excessBlobGas := hexutil.Uint64(0)
	ep.BlobGasUsed = &blobGasUsed
	ep.ExcessBlobGas = &excessBlobGas

	encoded := EncodeExecutionPayloadSSZ(ep, 3)
	decoded, err := DecodeExecutionPayloadSSZ(encoded, 3)
	req.NoError(err)
	req.Empty(decoded.Transactions)
}

func TestExecutionPayloadSSZDecodeShortBuffer(t *testing.T) {
	req := require.New(t)

	_, err := DecodeExecutionPayloadSSZ(make([]byte, 100), 1)
	req.Error(err)
}

// --- NewPayload request SSZ round-trip tests ---

func TestNewPayloadRequestV1RoundTrip(t *testing.T) {
	req := require.New(t)

	ep := makeTestExecutionPayloadV1()
	encoded := EncodeNewPayloadRequestSSZ(ep, nil, nil, nil, 1)
	decodedEp, blobHashes, parentRoot, execReqs, err := DecodeNewPayloadRequestSSZ(encoded, 1)
	req.NoError(err)
	req.Nil(blobHashes)
	req.Nil(parentRoot)
	req.Nil(execReqs)
	req.Equal(ep.BlockHash, decodedEp.BlockHash)
	req.Len(decodedEp.Transactions, 2)
}

func TestNewPayloadRequestV3RoundTrip(t *testing.T) {
	req := require.New(t)

	ep := makeTestExecutionPayloadV1()
	ep.Withdrawals = []*types.Withdrawal{}
	blobGasUsed := hexutil.Uint64(0)
	excessBlobGas := hexutil.Uint64(0)
	ep.BlobGasUsed = &blobGasUsed
	ep.ExcessBlobGas = &excessBlobGas

	hashes := []common.Hash{
		common.HexToHash("0xaaaa"),
		common.HexToHash("0xbbbb"),
	}
	root := common.HexToHash("0xcccc")

	encoded := EncodeNewPayloadRequestSSZ(ep, hashes, &root, nil, 3)
	decodedEp, decodedHashes, decodedRoot, _, err := DecodeNewPayloadRequestSSZ(encoded, 3)
	req.NoError(err)
	req.Equal(ep.BlockHash, decodedEp.BlockHash)
	req.Len(decodedHashes, 2)
	req.Equal(hashes[0], decodedHashes[0])
	req.Equal(hashes[1], decodedHashes[1])
	req.Equal(root, *decodedRoot)
}

func TestNewPayloadRequestV4RoundTrip(t *testing.T) {
	req := require.New(t)

	ep := makeTestExecutionPayloadV1()
	ep.Withdrawals = []*types.Withdrawal{}
	blobGasUsed := hexutil.Uint64(0)
	excessBlobGas := hexutil.Uint64(0)
	ep.BlobGasUsed = &blobGasUsed
	ep.ExcessBlobGas = &excessBlobGas

	hashes := []common.Hash{common.HexToHash("0xdddd")}
	root := common.HexToHash("0xeeee")
	execReqs := []hexutil.Bytes{
		{0x00, 0x01, 0x02, 0x03},
		{0x01, 0x04, 0x05},
	}

	encoded := EncodeNewPayloadRequestSSZ(ep, hashes, &root, execReqs, 4)
	decodedEp, decodedHashes, decodedRoot, decodedReqs, err := DecodeNewPayloadRequestSSZ(encoded, 4)
	req.NoError(err)
	req.Equal(ep.BlockHash, decodedEp.BlockHash)
	req.Len(decodedHashes, 1)
	req.Equal(hashes[0], decodedHashes[0])
	req.Equal(root, *decodedRoot)
	req.Len(decodedReqs, 2)
	req.Equal([]byte(execReqs[0]), []byte(decodedReqs[0]))
	req.Equal([]byte(execReqs[1]), []byte(decodedReqs[1]))
}

// --- GetPayload response SSZ round-trip tests ---

func TestGetPayloadResponseV1RoundTrip(t *testing.T) {
	req := require.New(t)

	ep := makeTestExecutionPayloadV1()
	resp := &GetPayloadResponse{ExecutionPayload: ep}

	encoded := EncodeGetPayloadResponseSSZ(resp, 1)
	decoded, err := DecodeGetPayloadResponseSSZ(encoded, 1)
	req.NoError(err)
	req.Equal(ep.BlockHash, decoded.ExecutionPayload.BlockHash)
	req.Len(decoded.ExecutionPayload.Transactions, 2)
}

func TestGetPayloadResponseV3RoundTrip(t *testing.T) {
	req := require.New(t)

	ep := makeTestExecutionPayloadV1()
	ep.Withdrawals = []*types.Withdrawal{}
	blobGasUsed := hexutil.Uint64(131072)
	excessBlobGas := hexutil.Uint64(0)
	ep.BlobGasUsed = &blobGasUsed
	ep.ExcessBlobGas = &excessBlobGas

	blockValue := big.NewInt(1234567890)
	resp := &GetPayloadResponse{
		ExecutionPayload:      ep,
		BlockValue:            (*hexutil.Big)(blockValue),
		BlobsBundle:           &BlobsBundle{},
		ShouldOverrideBuilder: true,
	}

	encoded := EncodeGetPayloadResponseSSZ(resp, 3)
	decoded, err := DecodeGetPayloadResponseSSZ(encoded, 3)
	req.NoError(err)
	req.Equal(ep.BlockHash, decoded.ExecutionPayload.BlockHash)
	req.Equal(blockValue.String(), decoded.BlockValue.ToInt().String())
	req.True(decoded.ShouldOverrideBuilder)
}

func TestGetPayloadResponseShortBuffer(t *testing.T) {
	req := require.New(t)

	_, err := DecodeGetPayloadResponseSSZ(make([]byte, 10), 2)
	req.Error(err)
	req.Contains(err.Error(), "buffer too short")
}

// --- uint256 SSZ conversion tests ---

func TestUint256SSZRoundTrip(t *testing.T) {
	req := require.New(t)

	tests := []*big.Int{
		big.NewInt(0),
		big.NewInt(1),
		big.NewInt(1000000000),
		new(big.Int).SetBytes(common.Hex2Bytes("ffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffff")),
	}

	for _, val := range tests {
		encoded := uint256ToSSZBytes(val)
		req.Len(encoded, 32)
		decoded := sszBytesToUint256(encoded[:])
		req.Equal(val.String(), decoded.String(), "round-trip failed for %s", val.String())
	}

	// Test nil
	encoded := uint256ToSSZBytes(nil)
	req.Len(encoded, 32)
	decoded := sszBytesToUint256(encoded[:])
	req.Equal("0", decoded.String())
}
