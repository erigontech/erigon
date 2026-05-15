// Copyright 2026 The Erigon Authors
// This file is part of Erigon.
//
// Erigon is free software: you can redistribute it and/or modify
// it under the terms of the GNU Lesser General Public License as published by
// the Free Software Foundation, either version 3 of the License, or
// (at your option) any later version.

package engineapi

import (
	"errors"
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

type sszForkchoiceRequest struct {
	State     engine_types.ForkChoiceState
	AttrsList *solid.ListSSZ[*engine_types.PayloadAttributes]
	version   clparams.StateVersion
}

func (r *sszForkchoiceRequest) Static() bool { return false }
func (r *sszForkchoiceRequest) EncodeSSZ(dst []byte) ([]byte, error) {
	if r.AttrsList == nil {
		r.AttrsList = solid.NewDynamicListSSZ[*engine_types.PayloadAttributes](1)
	}
	return ssz2.MarshalSSZ(dst, &r.State, r.AttrsList)
}
func (r *sszForkchoiceRequest) DecodeSSZ(buf []byte, version int) error {
	r.version = clparams.StateVersion(version)
	r.AttrsList = solid.NewDynamicListSSZ[*engine_types.PayloadAttributes](1)
	return ssz2.UnmarshalSSZ(buf, version, &r.State, r.AttrsList)
}
func (r *sszForkchoiceRequest) EncodingSizeSSZ() int { b, _ := r.EncodeSSZ(nil); return len(b) }
func (r *sszForkchoiceRequest) Clone() clonable.Clonable {
	return &sszForkchoiceRequest{version: r.version}
}
func (r *sszForkchoiceRequest) payloadAttributes() *engine_types.PayloadAttributes {
	if r.AttrsList == nil || r.AttrsList.Len() == 0 {
		return nil
	}
	a := r.AttrsList.Get(0)
	a.SSZVersion = r.version
	return a
}

type sszForkchoiceResponse struct {
	Status    *engine_types.PayloadStatus
	PayloadID *solid.ListSSZ[*sszBytes8]
}

type sszBytes8 struct{ Bytes [8]byte }

func (*sszBytes8) Static() bool                           { return true }
func (*sszBytes8) EncodingSizeSSZ() int                   { return 8 }
func (b *sszBytes8) EncodeSSZ(dst []byte) ([]byte, error) { return append(dst, b.Bytes[:]...), nil }
func (b *sszBytes8) DecodeSSZ(buf []byte, _ int) error {
	if len(buf) < 8 {
		return errors.New("short bytes8")
	}
	copy(b.Bytes[:], buf[:8])
	return nil
}
func (b *sszBytes8) HashSSZ() ([32]byte, error) {
	var h [32]byte
	copy(h[:], b.Bytes[:])
	return h, nil
}
func (*sszBytes8) Clone() clonable.Clonable { return &sszBytes8{} }

func (r *sszForkchoiceResponse) Static() bool { return false }
func (r *sszForkchoiceResponse) EncodeSSZ(dst []byte) ([]byte, error) {
	return ssz2.MarshalSSZ(dst, r.Status, r.PayloadID)
}
func (r *sszForkchoiceResponse) DecodeSSZ(buf []byte, version int) error {
	r.Status = &engine_types.PayloadStatus{}
	r.PayloadID = solid.NewStaticListSSZ[*sszBytes8](1, 8)
	return ssz2.UnmarshalSSZ(buf, version, r.Status, r.PayloadID)
}
func (r *sszForkchoiceResponse) EncodingSizeSSZ() int   { b, _ := r.EncodeSSZ(nil); return len(b) }
func (*sszForkchoiceResponse) Clone() clonable.Clonable { return &sszForkchoiceResponse{} }

func forkchoiceResponseToSSZ(resp *engine_types.ForkChoiceUpdatedResponse) (*sszForkchoiceResponse, error) {
	pids := solid.NewStaticListSSZ[*sszBytes8](1, 8)
	if resp.PayloadId != nil && len(*resp.PayloadId) == 8 {
		var id [8]byte
		copy(id[:], *resp.PayloadId)
		pids.Append(&sszBytes8{Bytes: id})
	}
	return &sszForkchoiceResponse{Status: resp.PayloadStatus, PayloadID: pids}, nil
}

type sszNewPayloadRequest struct {
	Payload               *engine_types.ExecutionPayload
	ExpectedBlobHashes    solid.HashListSSZ
	ParentBeaconBlockRoot common.Hash
	ExecutionRequests     *solid.TransactionsSSZ
	version               clparams.StateVersion
}

func newSSZNewPayloadRequest(version clparams.StateVersion) *sszNewPayloadRequest {
	return &sszNewPayloadRequest{Payload: engine_types.NewExecutionPayloadSSZ(version), ExpectedBlobHashes: solid.NewHashList(sszMaxBlobHashes), ExecutionRequests: &solid.TransactionsSSZ{}, version: version}
}
func (r *sszNewPayloadRequest) Static() bool { return false }
func (r *sszNewPayloadRequest) EncodeSSZ(dst []byte) ([]byte, error) {
	switch r.version {
	case clparams.BellatrixVersion, clparams.CapellaVersion:
		return ssz2.MarshalSSZ(dst, r.Payload)
	case clparams.DenebVersion:
		return ssz2.MarshalSSZ(dst, r.Payload, r.ExpectedBlobHashes, r.ParentBeaconBlockRoot[:])
	default:
		return ssz2.MarshalSSZ(dst, r.Payload, r.ExpectedBlobHashes, r.ParentBeaconBlockRoot[:], r.ExecutionRequests)
	}
}
func (r *sszNewPayloadRequest) DecodeSSZ(buf []byte, version int) error {
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
func (r *sszNewPayloadRequest) EncodingSizeSSZ() int     { b, _ := r.EncodeSSZ(nil); return len(b) }
func (r *sszNewPayloadRequest) Clone() clonable.Clonable { return newSSZNewPayloadRequest(r.version) }

type sszCapability struct{ Name *solid.ByteListSSZ }

func newSSZCapability(s string) *sszCapability {
	b := solid.NewByteListSSZ(sszMaxCapabilityNameBytes)
	_ = b.SetBytes([]byte(s))
	return &sszCapability{Name: b}
}
func (c *sszCapability) Static() bool { return false }
func (c *sszCapability) EncodeSSZ(dst []byte) ([]byte, error) {
	if c.Name == nil {
		c.Name = solid.NewByteListSSZ(sszMaxCapabilityNameBytes)
	}
	return ssz2.MarshalSSZ(dst, c.Name)
}
func (c *sszCapability) DecodeSSZ(buf []byte, version int) error {
	c.Name = solid.NewByteListSSZ(sszMaxCapabilityNameBytes)
	return ssz2.UnmarshalSSZ(buf, version, c.Name)
}
func (c *sszCapability) EncodingSizeSSZ() int       { return 4 + c.Name.EncodingSizeSSZ() }
func (c *sszCapability) HashSSZ() ([32]byte, error) { return c.Name.HashSSZ() }
func (*sszCapability) Clone() clonable.Clonable {
	return &sszCapability{Name: solid.NewByteListSSZ(sszMaxCapabilityNameBytes)}
}

type sszCapabilities struct {
	List *solid.ListSSZ[*sszCapability]
}

func newSSZCapabilities(names []string) *sszCapabilities {
	l := solid.NewDynamicListSSZ[*sszCapability](sszMaxCapabilities)
	for _, name := range names {
		l.Append(newSSZCapability(name))
	}
	return &sszCapabilities{List: l}
}
func (c *sszCapabilities) Static() bool { return false }
func (c *sszCapabilities) EncodeSSZ(dst []byte) ([]byte, error) {
	if c.List == nil {
		c.List = solid.NewDynamicListSSZ[*sszCapability](sszMaxCapabilities)
	}
	return ssz2.MarshalSSZ(dst, c.List)
}
func (c *sszCapabilities) DecodeSSZ(buf []byte, version int) error {
	c.List = solid.NewDynamicListSSZ[*sszCapability](sszMaxCapabilities)
	return ssz2.UnmarshalSSZ(buf, version, c.List)
}
func (c *sszCapabilities) EncodingSizeSSZ() int   { b, _ := c.EncodeSSZ(nil); return len(b) }
func (*sszCapabilities) Clone() clonable.Clonable { return &sszCapabilities{} }
func (c *sszCapabilities) names() []string {
	if c.List == nil {
		return nil
	}
	out := make([]string, 0, c.List.Len())
	c.List.Range(func(_ int, v *sszCapability, _ int) bool { out = append(out, string(v.Name.Bytes())); return true })
	return out
}

type sszClientVersionRequest struct{ ClientVersion *engine_types.ClientVersionV1 }

func (r *sszClientVersionRequest) Static() bool { return false }
func (r *sszClientVersionRequest) EncodeSSZ(dst []byte) ([]byte, error) {
	return ssz2.MarshalSSZ(dst, r.ClientVersion)
}
func (r *sszClientVersionRequest) DecodeSSZ(buf []byte, version int) error {
	r.ClientVersion = &engine_types.ClientVersionV1{}
	return ssz2.UnmarshalSSZ(buf, version, r.ClientVersion)
}
func (r *sszClientVersionRequest) EncodingSizeSSZ() int   { b, _ := r.EncodeSSZ(nil); return len(b) }
func (*sszClientVersionRequest) Clone() clonable.Clonable { return &sszClientVersionRequest{} }

type sszClientVersionResponse struct {
	Versions *solid.ListSSZ[*engine_types.ClientVersionV1]
}

func newSSZClientVersionResponse(versions []engine_types.ClientVersionV1) *sszClientVersionResponse {
	l := solid.NewDynamicListSSZ[*engine_types.ClientVersionV1](4)
	for i := range versions {
		l.Append(&versions[i])
	}
	return &sszClientVersionResponse{Versions: l}
}
func (r *sszClientVersionResponse) Static() bool { return false }
func (r *sszClientVersionResponse) EncodeSSZ(dst []byte) ([]byte, error) {
	return ssz2.MarshalSSZ(dst, r.Versions)
}
func (r *sszClientVersionResponse) DecodeSSZ(buf []byte, version int) error {
	r.Versions = solid.NewDynamicListSSZ[*engine_types.ClientVersionV1](4)
	return ssz2.UnmarshalSSZ(buf, version, r.Versions)
}
func (r *sszClientVersionResponse) EncodingSizeSSZ() int   { b, _ := r.EncodeSSZ(nil); return len(b) }
func (*sszClientVersionResponse) Clone() clonable.Clonable { return &sszClientVersionResponse{} }

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

type sszGetPayloadResponse struct {
	Payload               *engine_types.ExecutionPayload
	BlockValue            common.Hash
	BlobsBundle           *engine_types.BlobsBundle
	ShouldOverrideBuilder bool
	ExecutionRequests     *cltypes.ExecutionRequests
	version               clparams.StateVersion
}

func newSSZGetPayloadResponse(resp *engine_types.GetPayloadResponse, version clparams.StateVersion) (*sszGetPayloadResponse, error) {
	payload := resp.ExecutionPayload
	payload.SSZVersion = version
	executionRequests, err := executionRequestsFromList(resp.ExecutionRequests, version)
	if err != nil {
		return nil, err
	}
	return &sszGetPayloadResponse{
		Payload:               payload,
		BlockValue:            blockValueHash(resp.BlockValue),
		BlobsBundle:           newBlobsBundleSSZ(resp.BlobsBundle, version),
		ShouldOverrideBuilder: resp.ShouldOverrideBuilder,
		ExecutionRequests:     executionRequests,
		version:               version,
	}, nil
}
func (r *sszGetPayloadResponse) Static() bool { return false }
func (r *sszGetPayloadResponse) EncodeSSZ(dst []byte) ([]byte, error) {
	switch r.version {
	case clparams.CapellaVersion:
		return ssz2.MarshalSSZ(dst, r.Payload, r.BlockValue[:])
	case clparams.DenebVersion:
		return ssz2.MarshalSSZ(dst, r.Payload, r.BlockValue[:], r.BlobsBundle, r.ShouldOverrideBuilder)
	default:
		return ssz2.MarshalSSZ(dst, r.Payload, r.BlockValue[:], r.BlobsBundle, r.ShouldOverrideBuilder, r.ExecutionRequests)
	}
}
func (r *sszGetPayloadResponse) DecodeSSZ(buf []byte, version int) error {
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
func (r *sszGetPayloadResponse) EncodingSizeSSZ() int { out, _ := r.EncodeSSZ(nil); return len(out) }
func (r *sszGetPayloadResponse) Clone() clonable.Clonable {
	return &sszGetPayloadResponse{version: r.version}
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

type sszGetBlobsV1Response struct {
	BlobsAndProofs *solid.ListSSZ[*engine_types.BlobAndProofV1]
}

func newSSZGetBlobsV1Response(blobs []*engine_types.BlobAndProofV1) *sszGetBlobsV1Response {
	l := solid.NewStaticListSSZ[*engine_types.BlobAndProofV1](sszMaxGetBlobHashes, sszBlobBytes+sszKZGBytes)
	for _, blob := range blobs {
		if blob != nil {
			l.Append(blob)
		}
	}
	return &sszGetBlobsV1Response{BlobsAndProofs: l}
}
func (r *sszGetBlobsV1Response) Static() bool { return false }
func (r *sszGetBlobsV1Response) EncodeSSZ(dst []byte) ([]byte, error) {
	if r.BlobsAndProofs == nil {
		r.BlobsAndProofs = solid.NewStaticListSSZ[*engine_types.BlobAndProofV1](sszMaxGetBlobHashes, sszBlobBytes+sszKZGBytes)
	}
	return ssz2.MarshalSSZ(dst, r.BlobsAndProofs)
}
func (r *sszGetBlobsV1Response) DecodeSSZ(buf []byte, version int) error {
	r.BlobsAndProofs = solid.NewStaticListSSZ[*engine_types.BlobAndProofV1](sszMaxGetBlobHashes, sszBlobBytes+sszKZGBytes)
	return ssz2.UnmarshalSSZ(buf, version, r.BlobsAndProofs)
}
func (r *sszGetBlobsV1Response) EncodingSizeSSZ() int   { b, _ := r.EncodeSSZ(nil); return len(b) }
func (*sszGetBlobsV1Response) Clone() clonable.Clonable { return &sszGetBlobsV1Response{} }

type sszGetBlobsV2Response struct {
	BlobsAndProofs *solid.ListSSZ[*engine_types.BlobAndProofV2]
}

func newSSZGetBlobsV2Response(blobs []*engine_types.BlobAndProofV2) *sszGetBlobsV2Response {
	l := solid.NewDynamicListSSZ[*engine_types.BlobAndProofV2](sszMaxGetBlobHashes)
	for _, blob := range blobs {
		if blob != nil {
			l.Append(blob)
		}
	}
	return &sszGetBlobsV2Response{BlobsAndProofs: l}
}
func (r *sszGetBlobsV2Response) Static() bool { return false }
func (r *sszGetBlobsV2Response) EncodeSSZ(dst []byte) ([]byte, error) {
	if r.BlobsAndProofs == nil {
		r.BlobsAndProofs = solid.NewDynamicListSSZ[*engine_types.BlobAndProofV2](sszMaxGetBlobHashes)
	}
	return ssz2.MarshalSSZ(dst, r.BlobsAndProofs)
}
func (r *sszGetBlobsV2Response) DecodeSSZ(buf []byte, version int) error {
	r.BlobsAndProofs = solid.NewDynamicListSSZ[*engine_types.BlobAndProofV2](sszMaxGetBlobHashes)
	return ssz2.UnmarshalSSZ(buf, version, r.BlobsAndProofs)
}
func (r *sszGetBlobsV2Response) EncodingSizeSSZ() int   { b, _ := r.EncodeSSZ(nil); return len(b) }
func (*sszGetBlobsV2Response) Clone() clonable.Clonable { return &sszGetBlobsV2Response{} }

type sszGetBlobsV3Response struct {
	BlobsAndProofs *solid.ListSSZ[*engine_types.NullableBlobAndProofV2]
}

func newSSZGetBlobsV3Response(blobs []*engine_types.BlobAndProofV2) *sszGetBlobsV3Response {
	l := solid.NewDynamicListSSZ[*engine_types.NullableBlobAndProofV2](sszMaxGetBlobHashes)
	for _, blob := range blobs {
		l.Append(engine_types.NewNullableBlobAndProofV2(blob))
	}
	return &sszGetBlobsV3Response{BlobsAndProofs: l}
}
func (r *sszGetBlobsV3Response) Static() bool { return false }
func (r *sszGetBlobsV3Response) EncodeSSZ(dst []byte) ([]byte, error) {
	if r.BlobsAndProofs == nil {
		r.BlobsAndProofs = solid.NewDynamicListSSZ[*engine_types.NullableBlobAndProofV2](sszMaxGetBlobHashes)
	}
	return ssz2.MarshalSSZ(dst, r.BlobsAndProofs)
}
func (r *sszGetBlobsV3Response) DecodeSSZ(buf []byte, version int) error {
	r.BlobsAndProofs = solid.NewDynamicListSSZ[*engine_types.NullableBlobAndProofV2](sszMaxGetBlobHashes)
	return ssz2.UnmarshalSSZ(buf, version, r.BlobsAndProofs)
}
func (r *sszGetBlobsV3Response) EncodingSizeSSZ() int   { b, _ := r.EncodeSSZ(nil); return len(b) }
func (*sszGetBlobsV3Response) Clone() clonable.Clonable { return &sszGetBlobsV3Response{} }
