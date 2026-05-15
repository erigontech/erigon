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
	"github.com/erigontech/erigon/common/clonable"
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

type sszRESTMessageKind uint8

const (
	sszMessageForkchoiceRequest sszRESTMessageKind = iota
	sszMessageForkchoiceResponse
	sszMessageNewPayloadRequest
	sszMessageCapabilities
	sszMessageClientVersionRequest
	sszMessageClientVersionResponse
	sszMessageGetPayloadResponse
	sszMessageGetBlobsV1Response
	sszMessageGetBlobsV2Response
	sszMessageGetBlobsV3Response
)

type sszRESTMessage struct {
	kind    sszRESTMessageKind
	version clparams.StateVersion

	State     engine_types.ForkChoiceState
	AttrsList *solid.ListSSZ[*engine_types.PayloadAttributes]
	Status    *engine_types.PayloadStatus
	PayloadID *solid.ByteListSSZ

	Payload               *engine_types.ExecutionPayload
	ExpectedBlobHashes    solid.HashListSSZ
	ParentBeaconBlockRoot common.Hash
	ExecutionRequests     *solid.TransactionsSSZ

	Capabilities    []*solid.ByteListSSZ
	ClientVersion   *engine_types.ClientVersionV1
	ClientVersions  *solid.ListSSZ[*engine_types.ClientVersionV1]
	BlockValue      common.Hash
	BlobsBundle     *engine_types.BlobsBundle
	OverrideBuilder bool
	PayloadRequests *cltypes.ExecutionRequests

	BlobsAndProofsV1 *solid.ListSSZ[*engine_types.BlobAndProofV1]
	BlobsAndProofsV2 *solid.ListSSZ[*engine_types.BlobAndProofV2]
	BlobsAndProofsV3 *solid.ListSSZ[*engine_types.NullableBlobAndProofV2]
}

func newSSZRESTMessage(kind sszRESTMessageKind, version clparams.StateVersion) *sszRESTMessage {
	m := &sszRESTMessage{kind: kind, version: version}
	m.init()
	return m
}

func (m *sszRESTMessage) init() {
	switch m.kind {
	case sszMessageForkchoiceRequest:
		m.AttrsList = solid.NewDynamicListSSZ[*engine_types.PayloadAttributes](1)
	case sszMessageForkchoiceResponse:
		m.Status = &engine_types.PayloadStatus{}
		m.PayloadID = solid.NewByteListSSZ(8)
	case sszMessageNewPayloadRequest:
		m.Payload = engine_types.NewExecutionPayloadSSZ(m.version)
		m.ExpectedBlobHashes = solid.NewHashList(sszMaxBlobHashes)
		m.ExecutionRequests = &solid.TransactionsSSZ{}
	case sszMessageClientVersionRequest:
		m.ClientVersion = &engine_types.ClientVersionV1{}
	case sszMessageClientVersionResponse:
		m.ClientVersions = solid.NewDynamicListSSZ[*engine_types.ClientVersionV1](4)
	case sszMessageGetPayloadResponse:
		m.Payload = engine_types.NewExecutionPayloadSSZ(m.version)
		m.BlobsBundle = engine_types.NewBlobsBundleSSZ(m.version)
		m.PayloadRequests = cltypes.NewExecutionRequests(mainnetBeaconCfg)
	case sszMessageGetBlobsV1Response:
		m.BlobsAndProofsV1 = solid.NewStaticListSSZ[*engine_types.BlobAndProofV1](sszMaxGetBlobHashes, sszBlobBytes+sszKZGBytes)
	case sszMessageGetBlobsV2Response:
		m.BlobsAndProofsV2 = solid.NewDynamicListSSZ[*engine_types.BlobAndProofV2](sszMaxGetBlobHashes)
	case sszMessageGetBlobsV3Response:
		m.BlobsAndProofsV3 = solid.NewDynamicListSSZ[*engine_types.NullableBlobAndProofV2](sszMaxGetBlobHashes)
	}
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

func (m *sszRESTMessage) Static() bool { return false }
func (m *sszRESTMessage) EncodeSSZ(dst []byte) ([]byte, error) {
	switch m.kind {
	case sszMessageForkchoiceRequest:
		if m.AttrsList == nil {
			m.AttrsList = solid.NewDynamicListSSZ[*engine_types.PayloadAttributes](1)
		}
		return ssz2.MarshalSSZ(dst, &m.State, m.AttrsList)
	case sszMessageForkchoiceResponse:
		if m.PayloadID == nil {
			m.PayloadID = solid.NewByteListSSZ(8)
		}
		if err := validatePayloadIDList(m.PayloadID); err != nil {
			return nil, err
		}
		return ssz2.MarshalSSZ(dst, m.Status, m.PayloadID)
	case sszMessageNewPayloadRequest:
		switch m.version {
		case clparams.BellatrixVersion, clparams.CapellaVersion:
			return ssz2.MarshalSSZ(dst, m.Payload)
		case clparams.DenebVersion:
			return ssz2.MarshalSSZ(dst, m.Payload, m.ExpectedBlobHashes, m.ParentBeaconBlockRoot[:])
		default:
			return ssz2.MarshalSSZ(dst, m.Payload, m.ExpectedBlobHashes, m.ParentBeaconBlockRoot[:], m.ExecutionRequests)
		}
	case sszMessageCapabilities:
		return m.encodeCapabilities(dst)
	case sszMessageClientVersionRequest:
		return ssz2.MarshalSSZ(dst, m.ClientVersion)
	case sszMessageClientVersionResponse:
		return ssz2.MarshalSSZ(dst, m.ClientVersions)
	case sszMessageGetPayloadResponse:
		switch m.version {
		case clparams.CapellaVersion:
			return ssz2.MarshalSSZ(dst, m.Payload, m.BlockValue[:])
		case clparams.DenebVersion:
			return ssz2.MarshalSSZ(dst, m.Payload, m.BlockValue[:], m.BlobsBundle, m.OverrideBuilder)
		default:
			return ssz2.MarshalSSZ(dst, m.Payload, m.BlockValue[:], m.BlobsBundle, m.OverrideBuilder, m.PayloadRequests)
		}
	case sszMessageGetBlobsV1Response:
		if m.BlobsAndProofsV1 == nil {
			m.BlobsAndProofsV1 = solid.NewStaticListSSZ[*engine_types.BlobAndProofV1](sszMaxGetBlobHashes, sszBlobBytes+sszKZGBytes)
		}
		return ssz2.MarshalSSZ(dst, m.BlobsAndProofsV1)
	case sszMessageGetBlobsV2Response:
		if m.BlobsAndProofsV2 == nil {
			m.BlobsAndProofsV2 = solid.NewDynamicListSSZ[*engine_types.BlobAndProofV2](sszMaxGetBlobHashes)
		}
		return ssz2.MarshalSSZ(dst, m.BlobsAndProofsV2)
	case sszMessageGetBlobsV3Response:
		if m.BlobsAndProofsV3 == nil {
			m.BlobsAndProofsV3 = solid.NewDynamicListSSZ[*engine_types.NullableBlobAndProofV2](sszMaxGetBlobHashes)
		}
		return ssz2.MarshalSSZ(dst, m.BlobsAndProofsV3)
	default:
		return nil, fmt.Errorf("unknown SSZ REST message kind %d", m.kind)
	}
}

func (m *sszRESTMessage) DecodeSSZ(buf []byte, version int) error {
	m.version = clparams.StateVersion(version)
	m.init()
	switch m.kind {
	case sszMessageForkchoiceRequest:
		return ssz2.UnmarshalSSZ(buf, version, &m.State, m.AttrsList)
	case sszMessageForkchoiceResponse:
		if err := ssz2.UnmarshalSSZ(buf, version, m.Status, m.PayloadID); err != nil {
			return err
		}
		return validatePayloadIDList(m.PayloadID)
	case sszMessageNewPayloadRequest:
		switch m.version {
		case clparams.BellatrixVersion, clparams.CapellaVersion:
			return ssz2.UnmarshalSSZ(buf, version, m.Payload)
		case clparams.DenebVersion:
			return ssz2.UnmarshalSSZ(buf, version, m.Payload, m.ExpectedBlobHashes, m.ParentBeaconBlockRoot[:])
		default:
			return ssz2.UnmarshalSSZ(buf, version, m.Payload, m.ExpectedBlobHashes, m.ParentBeaconBlockRoot[:], m.ExecutionRequests)
		}
	case sszMessageCapabilities:
		return m.decodeCapabilities(buf, version)
	case sszMessageClientVersionRequest:
		return ssz2.UnmarshalSSZ(buf, version, m.ClientVersion)
	case sszMessageClientVersionResponse:
		return ssz2.UnmarshalSSZ(buf, version, m.ClientVersions)
	case sszMessageGetPayloadResponse:
		switch m.version {
		case clparams.CapellaVersion:
			return ssz2.UnmarshalSSZ(buf, version, m.Payload, m.BlockValue[:])
		case clparams.DenebVersion:
			return ssz2.UnmarshalSSZ(buf, version, m.Payload, m.BlockValue[:], m.BlobsBundle, &m.OverrideBuilder)
		default:
			return ssz2.UnmarshalSSZ(buf, version, m.Payload, m.BlockValue[:], m.BlobsBundle, &m.OverrideBuilder, m.PayloadRequests)
		}
	case sszMessageGetBlobsV1Response:
		return ssz2.UnmarshalSSZ(buf, version, m.BlobsAndProofsV1)
	case sszMessageGetBlobsV2Response:
		return ssz2.UnmarshalSSZ(buf, version, m.BlobsAndProofsV2)
	case sszMessageGetBlobsV3Response:
		return ssz2.UnmarshalSSZ(buf, version, m.BlobsAndProofsV3)
	default:
		return fmt.Errorf("unknown SSZ REST message kind %d", m.kind)
	}
}

func (m *sszRESTMessage) EncodingSizeSSZ() int { b, _ := m.EncodeSSZ(nil); return len(b) }
func (m *sszRESTMessage) Clone() clonable.Clonable {
	return newSSZRESTMessage(m.kind, m.version)
}

func (m *sszRESTMessage) payloadAttributes() *engine_types.PayloadAttributes {
	if m.AttrsList == nil || m.AttrsList.Len() == 0 {
		return nil
	}
	a := m.AttrsList.Get(0)
	a.SSZVersion = m.version
	return a
}

func (m *sszRESTMessage) encodeCapabilities(dst []byte) ([]byte, error) {
	if len(m.Capabilities) > sszMaxCapabilities {
		return nil, fmt.Errorf("too many capabilities: %d", len(m.Capabilities))
	}
	offset := len(m.Capabilities) * 4
	for _, capability := range m.Capabilities {
		if capability == nil {
			capability = solid.NewByteListSSZ(sszMaxCapabilityNameBytes)
		}
		dst = append(dst, 0, 0, 0, 0)
		binary.LittleEndian.PutUint32(dst[len(dst)-4:], uint32(offset))
		offset += capability.EncodingSizeSSZ()
	}
	for _, capability := range m.Capabilities {
		if capability == nil {
			capability = solid.NewByteListSSZ(sszMaxCapabilityNameBytes)
		}
		var err error
		dst, err = capability.EncodeSSZ(dst)
		if err != nil {
			return nil, err
		}
	}
	return dst, nil
}

func (m *sszRESTMessage) decodeCapabilities(buf []byte, version int) error {
	m.Capabilities = nil
	if len(buf) == 0 {
		return nil
	}
	if len(buf) < 4 {
		return fmt.Errorf("capabilities: short offset table")
	}
	firstOffset := binary.LittleEndian.Uint32(buf[:4])
	if firstOffset%4 != 0 || firstOffset > uint32(len(buf)) {
		return fmt.Errorf("capabilities: bad first offset %d", firstOffset)
	}
	count := int(firstOffset / 4)
	if count > sszMaxCapabilities {
		return fmt.Errorf("too many capabilities: %d", count)
	}
	offsets := make([]uint32, count+1)
	offsets[count] = uint32(len(buf))
	for i := 0; i < count; i++ {
		offsets[i] = binary.LittleEndian.Uint32(buf[i*4:])
		if i > 0 && offsets[i] < offsets[i-1] {
			return fmt.Errorf("capabilities: non-monotonic offset %d", i)
		}
		if offsets[i] > uint32(len(buf)) {
			return fmt.Errorf("capabilities: offset %d out of bounds", i)
		}
	}
	m.Capabilities = make([]*solid.ByteListSSZ, 0, count)
	for i := 0; i < count; i++ {
		capability := solid.NewByteListSSZ(sszMaxCapabilityNameBytes)
		if err := capability.DecodeSSZ(buf[offsets[i]:offsets[i+1]], version); err != nil {
			return err
		}
		m.Capabilities = append(m.Capabilities, capability)
	}
	return nil
}

func (m *sszRESTMessage) capabilityNames() []string {
	if m.Capabilities == nil {
		return nil
	}
	out := make([]string, 0, len(m.Capabilities))
	for _, v := range m.Capabilities {
		out = append(out, string(v.Bytes()))
	}
	return out
}

func capabilityName(s string) *solid.ByteListSSZ {
	b := solid.NewByteListSSZ(sszMaxCapabilityNameBytes)
	_ = b.SetBytes([]byte(s))
	return b
}

func forkchoiceResponseToSSZ(resp *engine_types.ForkChoiceUpdatedResponse) (*sszRESTMessage, error) {
	msg := newSSZRESTMessage(sszMessageForkchoiceResponse, 0)
	msg.Status = resp.PayloadStatus
	if resp.PayloadId != nil && len(*resp.PayloadId) == 8 {
		if err := msg.PayloadID.SetBytes(*resp.PayloadId); err != nil {
			return nil, err
		}
	}
	return msg, nil
}

func newSSZNewPayloadRequest(version clparams.StateVersion) *sszRESTMessage {
	return newSSZRESTMessage(sszMessageNewPayloadRequest, version)
}

func newSSZCapabilities(names []string) *sszRESTMessage {
	msg := newSSZRESTMessage(sszMessageCapabilities, 0)
	msg.Capabilities = make([]*solid.ByteListSSZ, 0, len(names))
	for _, name := range names {
		msg.Capabilities = append(msg.Capabilities, capabilityName(name))
	}
	return msg
}

func newSSZClientVersionResponse(versions []engine_types.ClientVersionV1) *sszRESTMessage {
	msg := newSSZRESTMessage(sszMessageClientVersionResponse, 0)
	for i := range versions {
		msg.ClientVersions.Append(&versions[i])
	}
	return msg
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

func newSSZGetPayloadResponse(resp *engine_types.GetPayloadResponse, version clparams.StateVersion) (*sszRESTMessage, error) {
	payload := resp.ExecutionPayload
	payload.SSZVersion = version
	executionRequests, err := executionRequestsFromList(resp.ExecutionRequests, version)
	if err != nil {
		return nil, err
	}
	msg := newSSZRESTMessage(sszMessageGetPayloadResponse, version)
	msg.Payload = payload
	msg.BlockValue = blockValueHash(resp.BlockValue)
	msg.BlobsBundle = newBlobsBundleSSZ(resp.BlobsBundle, version)
	msg.OverrideBuilder = resp.ShouldOverrideBuilder
	msg.PayloadRequests = executionRequests
	return msg, nil
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

func newSSZGetBlobsV1Response(blobs []*engine_types.BlobAndProofV1) *sszRESTMessage {
	msg := newSSZRESTMessage(sszMessageGetBlobsV1Response, 0)
	for _, blob := range blobs {
		if blob != nil {
			msg.BlobsAndProofsV1.Append(blob)
		}
	}
	return msg
}

func newSSZGetBlobsV2Response(blobs []*engine_types.BlobAndProofV2) *sszRESTMessage {
	msg := newSSZRESTMessage(sszMessageGetBlobsV2Response, 0)
	for _, blob := range blobs {
		if blob != nil {
			msg.BlobsAndProofsV2.Append(blob)
		}
	}
	return msg
}

func newSSZGetBlobsV3Response(blobs []*engine_types.BlobAndProofV2) *sszRESTMessage {
	msg := newSSZRESTMessage(sszMessageGetBlobsV3Response, 0)
	for _, blob := range blobs {
		msg.BlobsAndProofsV3.Append(engine_types.NewNullableBlobAndProofV2(blob))
	}
	return msg
}
