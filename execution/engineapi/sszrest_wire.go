// Copyright 2026 The Erigon Authors
// This file is part of Erigon.
//
// Erigon is free software: you can redistribute it and/or modify
// it under the terms of the GNU Lesser General Public License as published by
// the Free Software Foundation, either version 3 of the License, or
// (at your option) any later version.

package engineapi

import (
	"encoding/binary"
	"fmt"
	"math/big"

	"github.com/holiman/uint256"

	"github.com/erigontech/erigon/cl/clparams"
	"github.com/erigontech/erigon/cl/cltypes"
	"github.com/erigontech/erigon/cl/cltypes/solid"
	ssz2 "github.com/erigontech/erigon/cl/ssz"
	"github.com/erigontech/erigon/common"
	"github.com/erigontech/erigon/common/hexutil"
	"github.com/erigontech/erigon/execution/engineapi/engine_types"
	"github.com/erigontech/erigon/execution/types"
)

const (
	sszMaxBlobHashes          = 4096
	sszMaxGetBlobHashes       = 128
	sszMaxCapabilityNameBytes = 64
	sszMaxCapabilities        = 64
	sszBlobBytes              = 0x20000
	sszKZGBytes               = 48
	sszCellsPerExtBlob        = 128
)

var mainnetBeaconCfg = &clparams.MainnetBeaconConfig

func hashListValues(l solid.HashListSSZ) []common.Hash {
	if l == nil {
		return nil
	}
	out := make([]common.Hash, 0, l.Length())
	l.Range(func(_ int, hash common.Hash, _ int) bool {
		out = append(out, hash)
		return true
	})
	return out
}

func transactionsBytes(txs *solid.TransactionsSSZ) []hexutil.Bytes {
	if txs == nil {
		return nil
	}
	out := make([]hexutil.Bytes, 0)
	txs.ForEach(func(tx []byte, _ int, _ int) bool {
		out = append(out, tx)
		return true
	})
	return out
}

func validatePayloadIDList(id *solid.ByteListSSZ) error {
	if id == nil {
		return nil
	}
	if l := id.Len(); l != 0 && l != 8 {
		return fmt.Errorf("payload ID length %d, want 0 or 8", l)
	}
	return nil
}

func newPayloadRequestSchema(version clparams.StateVersion, payload *engine_types.ExecutionPayload, blobHashes solid.HashListSSZ, parentRoot *common.Hash, requests *solid.TransactionsSSZ) []any {
	switch version {
	case clparams.BellatrixVersion, clparams.CapellaVersion:
		return []any{payload}
	case clparams.DenebVersion:
		return []any{payload, blobHashes, parentRoot[:]}
	default:
		return []any{payload, blobHashes, parentRoot[:], requests}
	}
}

func decodeNewPayloadRequest(buf []byte, version clparams.StateVersion) (*engine_types.ExecutionPayload, solid.HashListSSZ, common.Hash, *solid.TransactionsSSZ, error) {
	payload := engine_types.NewExecutionPayloadSSZ(version)
	blobHashes := solid.NewHashList(sszMaxBlobHashes)
	parentRoot := common.Hash{}
	requests := &solid.TransactionsSSZ{}
	err := ssz2.UnmarshalSSZ(buf, int(version), newPayloadRequestSchema(version, payload, blobHashes, &parentRoot, requests)...)
	return payload, blobHashes, parentRoot, requests, err
}

func encodeNewPayloadRequest(version clparams.StateVersion, payload *engine_types.ExecutionPayload, blobHashes solid.HashListSSZ, parentRoot common.Hash, requests *solid.TransactionsSSZ) ([]byte, error) {
	return ssz2.MarshalSSZ(nil, newPayloadRequestSchema(version, payload, blobHashes, &parentRoot, requests)...)
}

func decodeForkchoiceRequest(buf []byte, version clparams.StateVersion) (engine_types.ForkChoiceState, *engine_types.PayloadAttributes, error) {
	state := engine_types.ForkChoiceState{}
	attrsList := solid.NewDynamicListSSZ[*engine_types.PayloadAttributes](1)
	if err := ssz2.UnmarshalSSZ(buf, int(version), &state, attrsList); err != nil {
		return state, nil, err
	}
	if attrsList.Len() == 0 {
		return state, nil, nil
	}
	attrs := attrsList.Get(0)
	attrs.SSZVersion = version
	return state, attrs, nil
}

func encodeForkchoiceRequest(version clparams.StateVersion, state *engine_types.ForkChoiceState, attrs *engine_types.PayloadAttributes) ([]byte, error) {
	attrsList := solid.NewDynamicListSSZ[*engine_types.PayloadAttributes](1)
	if attrs != nil {
		attrs.SSZVersion = version
		attrsList.Append(attrs)
	}
	return ssz2.MarshalSSZ(nil, state, attrsList)
}

func encodeForkchoiceResponse(resp *engine_types.ForkChoiceUpdatedResponse) ([]byte, error) {
	payloadID := solid.NewByteListSSZ(8)
	if resp.PayloadId != nil && len(*resp.PayloadId) == 8 {
		if err := payloadID.SetBytes(*resp.PayloadId); err != nil {
			return nil, err
		}
	}
	if err := validatePayloadIDList(payloadID); err != nil {
		return nil, err
	}
	return ssz2.MarshalSSZ(nil, resp.PayloadStatus, payloadID)
}

func encodeCapabilities(names []string) ([]byte, error) {
	capabilities := make([]*solid.ByteListSSZ, 0, len(names))
	for _, name := range names {
		capabilities = append(capabilities, capabilityName(name))
	}
	if len(capabilities) > sszMaxCapabilities {
		return nil, fmt.Errorf("too many capabilities: %d", len(capabilities))
	}
	out := make([]byte, 0)
	offset := len(capabilities) * 4
	for _, capability := range capabilities {
		if capability == nil {
			capability = solid.NewByteListSSZ(sszMaxCapabilityNameBytes)
		}
		out = append(out, 0, 0, 0, 0)
		binary.LittleEndian.PutUint32(out[len(out)-4:], uint32(offset))
		offset += capability.EncodingSizeSSZ()
	}
	for _, capability := range capabilities {
		if capability == nil {
			capability = solid.NewByteListSSZ(sszMaxCapabilityNameBytes)
		}
		var err error
		out, err = capability.EncodeSSZ(out)
		if err != nil {
			return nil, err
		}
	}
	return out, nil
}

func decodeCapabilities(buf []byte, version int) ([]string, error) {
	if len(buf) == 0 {
		return nil, nil
	}
	if len(buf) < 4 {
		return nil, fmt.Errorf("capabilities: short offset table")
	}
	firstOffset := binary.LittleEndian.Uint32(buf[:4])
	if firstOffset%4 != 0 || firstOffset > uint32(len(buf)) {
		return nil, fmt.Errorf("capabilities: bad first offset %d", firstOffset)
	}
	count := int(firstOffset / 4)
	if count > sszMaxCapabilities {
		return nil, fmt.Errorf("too many capabilities: %d", count)
	}
	offsets := make([]uint32, count+1)
	offsets[count] = uint32(len(buf))
	for i := 0; i < count; i++ {
		offsets[i] = binary.LittleEndian.Uint32(buf[i*4:])
		if i > 0 && offsets[i] < offsets[i-1] {
			return nil, fmt.Errorf("capabilities: non-monotonic offset %d", i)
		}
		if offsets[i] > uint32(len(buf)) {
			return nil, fmt.Errorf("capabilities: offset %d out of bounds", i)
		}
	}
	out := make([]string, 0, count)
	for i := 0; i < count; i++ {
		capability := solid.NewByteListSSZ(sszMaxCapabilityNameBytes)
		if err := capability.DecodeSSZ(buf[offsets[i]:offsets[i+1]], version); err != nil {
			return nil, err
		}
		out = append(out, string(capability.Bytes()))
	}
	return out, nil
}

func capabilityName(s string) *solid.ByteListSSZ {
	b := solid.NewByteListSSZ(sszMaxCapabilityNameBytes)
	_ = b.SetBytes([]byte(s))
	return b
}

func encodeClientVersionResponse(versions []engine_types.ClientVersionV1) ([]byte, error) {
	list := solid.NewDynamicListSSZ[*engine_types.ClientVersionV1](4)
	for i := range versions {
		list.Append(&versions[i])
	}
	return ssz2.MarshalSSZ(nil, list)
}

func decodeClientVersionRequest(buf []byte) (*engine_types.ClientVersionV1, error) {
	version := &engine_types.ClientVersionV1{}
	if err := ssz2.UnmarshalSSZ(buf, 0, version); err != nil {
		return nil, err
	}
	return version, nil
}

func blockValueHash(v *hexutil.Big) common.Hash {
	var out common.Hash
	if v == nil {
		return out
	}
	u := uint256.MustFromBig((*big.Int)(v))
	b := u.Bytes32()
	for i := range b {
		out[i] = b[31-i]
	}
	return out
}

func newBlobsBundleSSZ(b *engine_types.BlobsBundle, version clparams.StateVersion) *engine_types.BlobsBundle {
	if b == nil {
		return engine_types.NewBlobsBundleSSZ(version)
	}
	b.SSZVersion = version
	return b
}

func encodeGetPayloadResponse(resp *engine_types.GetPayloadResponse, version clparams.StateVersion) ([]byte, error) {
	payload := resp.ExecutionPayload
	payload.SSZVersion = version
	executionRequests, err := executionRequestsFromList(resp.ExecutionRequests, version)
	if err != nil {
		return nil, err
	}
	blockValue := blockValueHash(resp.BlockValue)
	blobsBundle := newBlobsBundleSSZ(resp.BlobsBundle, version)
	switch version {
	case clparams.CapellaVersion:
		return ssz2.MarshalSSZ(nil, payload, blockValue[:])
	case clparams.DenebVersion:
		return ssz2.MarshalSSZ(nil, payload, blockValue[:], blobsBundle, resp.ShouldOverrideBuilder)
	default:
		return ssz2.MarshalSSZ(nil, payload, blockValue[:], blobsBundle, resp.ShouldOverrideBuilder, executionRequests)
	}
}

func executionRequestsFromList(requests []hexutil.Bytes, version clparams.StateVersion) (*cltypes.ExecutionRequests, error) {
	out := cltypes.NewExecutionRequests(mainnetBeaconCfg)
	for _, request := range requests {
		if len(request) == 0 {
			continue
		}
		data := request[1:]
		switch request[0] {
		case types.DepositRequestType:
			if err := out.Deposits.DecodeSSZ(data, int(version)); err != nil {
				return nil, err
			}
		case types.WithdrawalRequestType:
			if err := out.Withdrawals.DecodeSSZ(data, int(version)); err != nil {
				return nil, err
			}
		case types.ConsolidationRequestType:
			if err := out.Consolidations.DecodeSSZ(data, int(version)); err != nil {
				return nil, err
			}
		default:
			return nil, fmt.Errorf("unknown execution request type %d", request[0])
		}
	}
	return out, nil
}

func encodeGetBlobsV1Response(blobs []*engine_types.BlobAndProofV1) ([]byte, error) {
	list := solid.NewStaticListSSZ[*engine_types.BlobAndProofV1](sszMaxGetBlobHashes, sszBlobBytes+sszKZGBytes)
	for _, blob := range blobs {
		if blob != nil {
			list.Append(blob)
		}
	}
	return ssz2.MarshalSSZ(nil, list)
}

func encodeGetBlobsV2Response(blobs []*engine_types.BlobAndProofV2) ([]byte, error) {
	list := solid.NewDynamicListSSZ[*engine_types.BlobAndProofV2](sszMaxGetBlobHashes)
	for _, blob := range blobs {
		if blob != nil {
			list.Append(blob)
		}
	}
	return ssz2.MarshalSSZ(nil, list)
}

func encodeGetBlobsV3Response(blobs []*engine_types.BlobAndProofV2) ([]byte, error) {
	list := solid.NewDynamicListSSZ[*engine_types.NullableBlobAndProofV2](sszMaxGetBlobHashes)
	for _, blob := range blobs {
		list.Append(engine_types.NewNullableBlobAndProofV2(blob))
	}
	return ssz2.MarshalSSZ(nil, list)
}
