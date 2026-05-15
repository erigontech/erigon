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

type forkchoiceRequest struct {
	State     engine_types.ForkChoiceState
	AttrsList *solid.ListSSZ[*engine_types.PayloadAttributes]
	version   clparams.StateVersion
}

func (r *forkchoiceRequest) Static() bool { return false }
func (r *forkchoiceRequest) EncodeSSZ(dst []byte) ([]byte, error) {
	if r.AttrsList == nil {
		r.AttrsList = solid.NewDynamicListSSZ[*engine_types.PayloadAttributes](1)
	}
	return ssz2.MarshalSSZ(dst, &r.State, r.AttrsList)
}
func (r *forkchoiceRequest) DecodeSSZ(buf []byte, version int) error {
	r.version = clparams.StateVersion(version)
	r.AttrsList = solid.NewDynamicListSSZ[*engine_types.PayloadAttributes](1)
	return ssz2.UnmarshalSSZ(buf, version, &r.State, r.AttrsList)
}
func (r *forkchoiceRequest) EncodingSizeSSZ() int { b, _ := r.EncodeSSZ(nil); return len(b) }
func (r *forkchoiceRequest) Clone() clonable.Clonable {
	return &forkchoiceRequest{version: r.version}
}
func (r *forkchoiceRequest) payloadAttributes() *engine_types.PayloadAttributes {
	if r.AttrsList == nil || r.AttrsList.Len() == 0 {
		return nil
	}
	a := r.AttrsList.Get(0)
	a.SSZVersion = r.version
	return a
}

type forkchoiceResponse struct {
	Status    *engine_types.PayloadStatus
	PayloadID *solid.ByteListSSZ
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

func (r *forkchoiceResponse) Static() bool { return false }
func (r *forkchoiceResponse) EncodeSSZ(dst []byte) ([]byte, error) {
	if r.PayloadID == nil {
		r.PayloadID = solid.NewByteListSSZ(8)
	}
	if err := validatePayloadIDList(r.PayloadID); err != nil {
		return nil, err
	}
	return ssz2.MarshalSSZ(dst, r.Status, r.PayloadID)
}
func (r *forkchoiceResponse) DecodeSSZ(buf []byte, version int) error {
	r.Status = &engine_types.PayloadStatus{}
	r.PayloadID = solid.NewByteListSSZ(8)
	if err := ssz2.UnmarshalSSZ(buf, version, r.Status, r.PayloadID); err != nil {
		return err
	}
	return validatePayloadIDList(r.PayloadID)
}
func (r *forkchoiceResponse) EncodingSizeSSZ() int   { b, _ := r.EncodeSSZ(nil); return len(b) }
func (*forkchoiceResponse) Clone() clonable.Clonable { return &forkchoiceResponse{} }

func forkchoiceResponseToSSZ(resp *engine_types.ForkChoiceUpdatedResponse) (*forkchoiceResponse, error) {
	pid := solid.NewByteListSSZ(8)
	if resp.PayloadId != nil && len(*resp.PayloadId) == 8 {
		if err := pid.SetBytes(*resp.PayloadId); err != nil {
			return nil, err
		}
	}
	return &forkchoiceResponse{Status: resp.PayloadStatus, PayloadID: pid}, nil
}

type newPayloadRequest struct {
	Payload               *engine_types.ExecutionPayload
	ExpectedBlobHashes    solid.HashListSSZ
	ParentBeaconBlockRoot common.Hash
	ExecutionRequests     *solid.TransactionsSSZ
	version               clparams.StateVersion
}

func newSSZNewPayloadRequest(version clparams.StateVersion) *newPayloadRequest {
	return &newPayloadRequest{Payload: engine_types.NewExecutionPayloadSSZ(version), ExpectedBlobHashes: solid.NewHashList(sszMaxBlobHashes), ExecutionRequests: &solid.TransactionsSSZ{}, version: version}
}
func (r *newPayloadRequest) Static() bool { return false }
func (r *newPayloadRequest) EncodeSSZ(dst []byte) ([]byte, error) {
	switch r.version {
	case clparams.BellatrixVersion, clparams.CapellaVersion:
		return ssz2.MarshalSSZ(dst, r.Payload)
	case clparams.DenebVersion:
		return ssz2.MarshalSSZ(dst, r.Payload, r.ExpectedBlobHashes, r.ParentBeaconBlockRoot[:])
	default:
		return ssz2.MarshalSSZ(dst, r.Payload, r.ExpectedBlobHashes, r.ParentBeaconBlockRoot[:], r.ExecutionRequests)
	}
}
func (r *newPayloadRequest) DecodeSSZ(buf []byte, version int) error {
	r.version = clparams.StateVersion(version)
	r.Payload = engine_types.NewExecutionPayloadSSZ(r.version)
	r.ExpectedBlobHashes = solid.NewHashList(sszMaxBlobHashes)
	r.ExecutionRequests = &solid.TransactionsSSZ{}
	switch r.version {
	case clparams.BellatrixVersion, clparams.CapellaVersion:
		return ssz2.UnmarshalSSZ(buf, version, r.Payload)
	case clparams.DenebVersion:
		return ssz2.UnmarshalSSZ(buf, version, r.Payload, r.ExpectedBlobHashes, r.ParentBeaconBlockRoot[:])
	default:
		return ssz2.UnmarshalSSZ(buf, version, r.Payload, r.ExpectedBlobHashes, r.ParentBeaconBlockRoot[:], r.ExecutionRequests)
	}
}
func (r *newPayloadRequest) EncodingSizeSSZ() int     { b, _ := r.EncodeSSZ(nil); return len(b) }
func (r *newPayloadRequest) Clone() clonable.Clonable { return newSSZNewPayloadRequest(r.version) }

func capabilityName(s string) *solid.ByteListSSZ {
	b := solid.NewByteListSSZ(sszMaxCapabilityNameBytes)
	_ = b.SetBytes([]byte(s))
	return b
}

type capabilities struct {
	List []*solid.ByteListSSZ
}

func newSSZCapabilities(names []string) *capabilities {
	l := make([]*solid.ByteListSSZ, 0, len(names))
	for _, name := range names {
		l = append(l, capabilityName(name))
	}
	return &capabilities{List: l}
}
func (c *capabilities) Static() bool { return false }
func (c *capabilities) EncodeSSZ(dst []byte) ([]byte, error) {
	if len(c.List) > sszMaxCapabilities {
		return nil, fmt.Errorf("too many capabilities: %d", len(c.List))
	}
	offset := len(c.List) * 4
	for _, capability := range c.List {
		if capability == nil {
			capability = solid.NewByteListSSZ(sszMaxCapabilityNameBytes)
		}
		dst = append(dst, 0, 0, 0, 0)
		binary.LittleEndian.PutUint32(dst[len(dst)-4:], uint32(offset))
		offset += capability.EncodingSizeSSZ()
	}
	for _, capability := range c.List {
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
func (c *capabilities) DecodeSSZ(buf []byte, version int) error {
	c.List = nil
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
	c.List = make([]*solid.ByteListSSZ, 0, count)
	for i := 0; i < count; i++ {
		capability := solid.NewByteListSSZ(sszMaxCapabilityNameBytes)
		if err := capability.DecodeSSZ(buf[offsets[i]:offsets[i+1]], version); err != nil {
			return err
		}
		c.List = append(c.List, capability)
	}
	return nil
}
func (c *capabilities) EncodingSizeSSZ() int   { b, _ := c.EncodeSSZ(nil); return len(b) }
func (*capabilities) Clone() clonable.Clonable { return &capabilities{} }
func (c *capabilities) names() []string {
	if c.List == nil {
		return nil
	}
	out := make([]string, 0, len(c.List))
	for _, v := range c.List {
		out = append(out, string(v.Bytes()))
	}
	return out
}

type clientVersionRequest struct{ ClientVersion *engine_types.ClientVersionV1 }

func (r *clientVersionRequest) Static() bool { return false }
func (r *clientVersionRequest) EncodeSSZ(dst []byte) ([]byte, error) {
	return ssz2.MarshalSSZ(dst, r.ClientVersion)
}
func (r *clientVersionRequest) DecodeSSZ(buf []byte, version int) error {
	r.ClientVersion = &engine_types.ClientVersionV1{}
	return ssz2.UnmarshalSSZ(buf, version, r.ClientVersion)
}
func (r *clientVersionRequest) EncodingSizeSSZ() int   { b, _ := r.EncodeSSZ(nil); return len(b) }
func (*clientVersionRequest) Clone() clonable.Clonable { return &clientVersionRequest{} }

type clientVersionResponse struct {
	Versions *solid.ListSSZ[*engine_types.ClientVersionV1]
}

func newSSZClientVersionResponse(versions []engine_types.ClientVersionV1) *clientVersionResponse {
	l := solid.NewDynamicListSSZ[*engine_types.ClientVersionV1](4)
	for i := range versions {
		l.Append(&versions[i])
	}
	return &clientVersionResponse{Versions: l}
}
func (r *clientVersionResponse) Static() bool { return false }
func (r *clientVersionResponse) EncodeSSZ(dst []byte) ([]byte, error) {
	return ssz2.MarshalSSZ(dst, r.Versions)
}
func (r *clientVersionResponse) DecodeSSZ(buf []byte, version int) error {
	r.Versions = solid.NewDynamicListSSZ[*engine_types.ClientVersionV1](4)
	return ssz2.UnmarshalSSZ(buf, version, r.Versions)
}
func (r *clientVersionResponse) EncodingSizeSSZ() int   { b, _ := r.EncodeSSZ(nil); return len(b) }
func (*clientVersionResponse) Clone() clonable.Clonable { return &clientVersionResponse{} }

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

type getPayloadResponse struct {
	Payload               *engine_types.ExecutionPayload
	BlockValue            common.Hash
	BlobsBundle           *engine_types.BlobsBundle
	ShouldOverrideBuilder bool
	ExecutionRequests     *cltypes.ExecutionRequests
	version               clparams.StateVersion
}

func newSSZGetPayloadResponse(resp *engine_types.GetPayloadResponse, version clparams.StateVersion) (*getPayloadResponse, error) {
	payload := resp.ExecutionPayload
	payload.SSZVersion = version
	executionRequests, err := executionRequestsFromList(resp.ExecutionRequests, version)
	if err != nil {
		return nil, err
	}
	return &getPayloadResponse{
		Payload:               payload,
		BlockValue:            blockValueHash(resp.BlockValue),
		BlobsBundle:           newBlobsBundleSSZ(resp.BlobsBundle, version),
		ShouldOverrideBuilder: resp.ShouldOverrideBuilder,
		ExecutionRequests:     executionRequests,
		version:               version,
	}, nil
}
func (r *getPayloadResponse) Static() bool { return false }
func (r *getPayloadResponse) EncodeSSZ(dst []byte) ([]byte, error) {
	switch r.version {
	case clparams.CapellaVersion:
		return ssz2.MarshalSSZ(dst, r.Payload, r.BlockValue[:])
	case clparams.DenebVersion:
		return ssz2.MarshalSSZ(dst, r.Payload, r.BlockValue[:], r.BlobsBundle, r.ShouldOverrideBuilder)
	default:
		return ssz2.MarshalSSZ(dst, r.Payload, r.BlockValue[:], r.BlobsBundle, r.ShouldOverrideBuilder, r.ExecutionRequests)
	}
}
func (r *getPayloadResponse) DecodeSSZ(buf []byte, version int) error {
	r.version = clparams.StateVersion(version)
	r.Payload = engine_types.NewExecutionPayloadSSZ(r.version)
	r.BlobsBundle = engine_types.NewBlobsBundleSSZ(r.version)
	r.ExecutionRequests = cltypes.NewExecutionRequests(mainnetBeaconCfg)
	switch r.version {
	case clparams.CapellaVersion:
		return ssz2.UnmarshalSSZ(buf, version, r.Payload, r.BlockValue[:])
	case clparams.DenebVersion:
		return ssz2.UnmarshalSSZ(buf, version, r.Payload, r.BlockValue[:], r.BlobsBundle, &r.ShouldOverrideBuilder)
	default:
		return ssz2.UnmarshalSSZ(buf, version, r.Payload, r.BlockValue[:], r.BlobsBundle, &r.ShouldOverrideBuilder, r.ExecutionRequests)
	}
}
func (r *getPayloadResponse) EncodingSizeSSZ() int { out, _ := r.EncodeSSZ(nil); return len(out) }
func (r *getPayloadResponse) Clone() clonable.Clonable {
	return &getPayloadResponse{version: r.version}
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

type getBlobsV1Response struct {
	BlobsAndProofs *solid.ListSSZ[*engine_types.BlobAndProofV1]
}

func newSSZGetBlobsV1Response(blobs []*engine_types.BlobAndProofV1) *getBlobsV1Response {
	l := solid.NewStaticListSSZ[*engine_types.BlobAndProofV1](sszMaxGetBlobHashes, sszBlobBytes+sszKZGBytes)
	for _, blob := range blobs {
		if blob != nil {
			l.Append(blob)
		}
	}
	return &getBlobsV1Response{BlobsAndProofs: l}
}
func (r *getBlobsV1Response) Static() bool { return false }
func (r *getBlobsV1Response) EncodeSSZ(dst []byte) ([]byte, error) {
	if r.BlobsAndProofs == nil {
		r.BlobsAndProofs = solid.NewStaticListSSZ[*engine_types.BlobAndProofV1](sszMaxGetBlobHashes, sszBlobBytes+sszKZGBytes)
	}
	return ssz2.MarshalSSZ(dst, r.BlobsAndProofs)
}
func (r *getBlobsV1Response) DecodeSSZ(buf []byte, version int) error {
	r.BlobsAndProofs = solid.NewStaticListSSZ[*engine_types.BlobAndProofV1](sszMaxGetBlobHashes, sszBlobBytes+sszKZGBytes)
	return ssz2.UnmarshalSSZ(buf, version, r.BlobsAndProofs)
}
func (r *getBlobsV1Response) EncodingSizeSSZ() int   { b, _ := r.EncodeSSZ(nil); return len(b) }
func (*getBlobsV1Response) Clone() clonable.Clonable { return &getBlobsV1Response{} }

type getBlobsV2Response struct {
	BlobsAndProofs *solid.ListSSZ[*engine_types.BlobAndProofV2]
}

func newSSZGetBlobsV2Response(blobs []*engine_types.BlobAndProofV2) *getBlobsV2Response {
	l := solid.NewDynamicListSSZ[*engine_types.BlobAndProofV2](sszMaxGetBlobHashes)
	for _, blob := range blobs {
		if blob != nil {
			l.Append(blob)
		}
	}
	return &getBlobsV2Response{BlobsAndProofs: l}
}
func (r *getBlobsV2Response) Static() bool { return false }
func (r *getBlobsV2Response) EncodeSSZ(dst []byte) ([]byte, error) {
	if r.BlobsAndProofs == nil {
		r.BlobsAndProofs = solid.NewDynamicListSSZ[*engine_types.BlobAndProofV2](sszMaxGetBlobHashes)
	}
	return ssz2.MarshalSSZ(dst, r.BlobsAndProofs)
}
func (r *getBlobsV2Response) DecodeSSZ(buf []byte, version int) error {
	r.BlobsAndProofs = solid.NewDynamicListSSZ[*engine_types.BlobAndProofV2](sszMaxGetBlobHashes)
	return ssz2.UnmarshalSSZ(buf, version, r.BlobsAndProofs)
}
func (r *getBlobsV2Response) EncodingSizeSSZ() int   { b, _ := r.EncodeSSZ(nil); return len(b) }
func (*getBlobsV2Response) Clone() clonable.Clonable { return &getBlobsV2Response{} }

type getBlobsV3Response struct {
	BlobsAndProofs *solid.ListSSZ[*engine_types.NullableBlobAndProofV2]
}

func newSSZGetBlobsV3Response(blobs []*engine_types.BlobAndProofV2) *getBlobsV3Response {
	l := solid.NewDynamicListSSZ[*engine_types.NullableBlobAndProofV2](sszMaxGetBlobHashes)
	for _, blob := range blobs {
		l.Append(engine_types.NewNullableBlobAndProofV2(blob))
	}
	return &getBlobsV3Response{BlobsAndProofs: l}
}
func (r *getBlobsV3Response) Static() bool { return false }
func (r *getBlobsV3Response) EncodeSSZ(dst []byte) ([]byte, error) {
	if r.BlobsAndProofs == nil {
		r.BlobsAndProofs = solid.NewDynamicListSSZ[*engine_types.NullableBlobAndProofV2](sszMaxGetBlobHashes)
	}
	return ssz2.MarshalSSZ(dst, r.BlobsAndProofs)
}
func (r *getBlobsV3Response) DecodeSSZ(buf []byte, version int) error {
	r.BlobsAndProofs = solid.NewDynamicListSSZ[*engine_types.NullableBlobAndProofV2](sszMaxGetBlobHashes)
	return ssz2.UnmarshalSSZ(buf, version, r.BlobsAndProofs)
}
func (r *getBlobsV3Response) EncodingSizeSSZ() int   { b, _ := r.EncodeSSZ(nil); return len(b) }
func (*getBlobsV3Response) Clone() clonable.Clonable { return &getBlobsV3Response{} }
