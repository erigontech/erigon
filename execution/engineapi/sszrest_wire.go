// Copyright 2026 The Erigon Authors
// This file is part of Erigon.
//
// Erigon is free software: you can redistribute it and/or modify
// it under the terms of the GNU Lesser General Public License as published by
// the Free Software Foundation, either version 3 of the License, or
// (at your option) any later version.

package engineapi

import (
	"encoding/hex"
	"errors"
	"fmt"
	"math/big"
	"strings"

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
	sszMaxExecutionRequests   = 256
	sszMaxCapabilityNameBytes = 64
	sszMaxCapabilities        = 64
	sszMaxValidationError     = 1024
	sszMaxBytesPerTransaction = 0x40000000
	sszBlobBytes              = 0x20000
	sszKZGBytes               = 48
	sszCellsPerExtBlob        = 128
	sszMaxCellProofs          = 33554432
)

var mainnetBeaconCfg = &clparams.MainnetBeaconConfig

type sszPayload struct {
	Block   *cltypes.Eth1Block
	version clparams.StateVersion
}

func newSSZPayload(version clparams.StateVersion) *sszPayload {
	return &sszPayload{Block: cltypes.NewEth1Block(version, mainnetBeaconCfg), version: version}
}

func (p *sszPayload) Static() bool { return false }

func (p *sszPayload) EncodeSSZ(dst []byte) ([]byte, error) {
	if p.Block == nil {
		p.Block = cltypes.NewEth1Block(p.version, mainnetBeaconCfg)
	}
	return p.Block.EncodeSSZ(dst)
}

func (p *sszPayload) DecodeSSZ(buf []byte, version int) error {
	p.version = clparams.StateVersion(version)
	p.Block = cltypes.NewEth1Block(p.version, mainnetBeaconCfg)
	return p.Block.DecodeSSZ(buf, version)
}

func (p *sszPayload) EncodingSizeSSZ() int {
	if p.Block == nil {
		return cltypes.NewEth1Block(p.version, mainnetBeaconCfg).EncodingSizeSSZ()
	}
	return p.Block.EncodingSizeSSZ()
}
func (p *sszPayload) Clone() clonable.Clonable { return newSSZPayload(p.version) }

func executionPayloadToSSZ(ep *engine_types.ExecutionPayload, version clparams.StateVersion) (*sszPayload, error) {
	block := cltypes.NewEth1Block(version, mainnetBeaconCfg)
	block.ParentHash = ep.ParentHash
	block.FeeRecipient = ep.FeeRecipient
	block.StateRoot = ep.StateRoot
	block.ReceiptsRoot = ep.ReceiptsRoot
	if len(ep.LogsBloom) != len(block.LogsBloom) {
		return nil, fmt.Errorf("invalid logsBloom length %d", len(ep.LogsBloom))
	}
	copy(block.LogsBloom[:], ep.LogsBloom)
	block.PrevRandao = ep.PrevRandao
	block.BlockNumber = uint64(ep.BlockNumber)
	block.GasLimit = uint64(ep.GasLimit)
	block.GasUsed = uint64(ep.GasUsed)
	block.Time = uint64(ep.Timestamp)
	block.Extra = solid.NewExtraData()
	block.Extra.SetBytes(ep.ExtraData)
	if ep.BaseFeePerGas != nil {
		baseFee := uint256.MustFromBig(ep.BaseFeePerGas.ToInt())
		baseFeeBytes := baseFee.Bytes32()
		for i, j := 0, len(baseFeeBytes)-1; i < j; i, j = i+1, j-1 {
			baseFeeBytes[i], baseFeeBytes[j] = baseFeeBytes[j], baseFeeBytes[i]
		}
		copy(block.BaseFeePerGas[:], baseFeeBytes[:])
	}
	block.BlockHash = ep.BlockHash
	txs := make([][]byte, len(ep.Transactions))
	for i, tx := range ep.Transactions {
		txs[i] = tx
	}
	block.Transactions = solid.NewTransactionsSSZFromTransactions(txs)
	if version >= clparams.CapellaVersion {
		block.Withdrawals = solid.NewStaticListSSZ[*cltypes.Withdrawal](int(mainnetBeaconCfg.MaxWithdrawalsPerPayload), 44)
		for _, w := range ep.Withdrawals {
			block.Withdrawals.Append(&cltypes.Withdrawal{Index: w.Index, Validator: w.Validator, Address: w.Address, Amount: w.Amount})
		}
	}
	if ep.BlobGasUsed != nil {
		block.BlobGasUsed = uint64(*ep.BlobGasUsed)
	}
	if ep.ExcessBlobGas != nil {
		block.ExcessBlobGas = uint64(*ep.ExcessBlobGas)
	}
	if version >= clparams.GloasVersion {
		block.BlockAccessList = solid.NewByteListSSZ(sszMaxBytesPerTransaction)
		if err := block.BlockAccessList.SetBytes(ep.BlockAccessList); err != nil {
			return nil, err
		}
		if ep.SlotNumber != nil {
			block.SlotNumber = uint64(*ep.SlotNumber)
		}
	}
	return &sszPayload{Block: block, version: version}, nil
}

func sszToExecutionPayload(p *sszPayload) *engine_types.ExecutionPayload {
	block := p.Block
	baseFeeBytes := common.Copy(block.BaseFeePerGas[:])
	for i, j := 0, len(baseFeeBytes)-1; i < j; i, j = i+1, j-1 {
		baseFeeBytes[i], baseFeeBytes[j] = baseFeeBytes[j], baseFeeBytes[i]
	}
	baseFee := new(uint256.Int).SetBytes(baseFeeBytes)
	body := block.Body()
	ep := &engine_types.ExecutionPayload{
		ParentHash:    block.ParentHash,
		FeeRecipient:  block.FeeRecipient,
		StateRoot:     block.StateRoot,
		ReceiptsRoot:  block.ReceiptsRoot,
		LogsBloom:     block.LogsBloom[:],
		PrevRandao:    block.PrevRandao,
		BlockNumber:   hexutil.Uint64(block.BlockNumber),
		GasLimit:      hexutil.Uint64(block.GasLimit),
		GasUsed:       hexutil.Uint64(block.GasUsed),
		Timestamp:     hexutil.Uint64(block.Time),
		ExtraData:     block.Extra.Bytes(),
		BaseFeePerGas: (*hexutil.Big)(baseFee.ToBig()),
		BlockHash:     block.BlockHash,
		Withdrawals:   body.Withdrawals,
	}
	for _, tx := range body.Transactions {
		ep.Transactions = append(ep.Transactions, tx)
	}
	if p.version >= clparams.DenebVersion {
		bg, ebg := hexutil.Uint64(block.BlobGasUsed), hexutil.Uint64(block.ExcessBlobGas)
		ep.BlobGasUsed, ep.ExcessBlobGas = &bg, &ebg
	}
	if p.version >= clparams.GloasVersion {
		if block.BlockAccessList != nil {
			ep.BlockAccessList = block.BlockAccessList.Bytes()
		}
		slot := hexutil.Uint64(block.SlotNumber)
		ep.SlotNumber = &slot
	}
	return ep
}

type sszHashList struct {
	List  *solid.ListSSZ[*sszHash]
	Limit int
}

type sszHash struct{ Hash common.Hash }

func (*sszHash) Static() bool                           { return true }
func (*sszHash) EncodingSizeSSZ() int                   { return 32 }
func (h *sszHash) EncodeSSZ(dst []byte) ([]byte, error) { return append(dst, h.Hash[:]...), nil }
func (h *sszHash) DecodeSSZ(buf []byte, _ int) error {
	if len(buf) < 32 {
		return errors.New("short hash")
	}
	copy(h.Hash[:], buf[:32])
	return nil
}
func (h *sszHash) HashSSZ() ([32]byte, error) { return h.Hash, nil }
func (*sszHash) Clone() clonable.Clonable     { return &sszHash{} }

func newSSZHashList(limit int, hashes []common.Hash) *sszHashList {
	l := solid.NewStaticListSSZ[*sszHash](limit, 32)
	for _, h := range hashes {
		l.Append(&sszHash{Hash: h})
	}
	return &sszHashList{List: l, Limit: limit}
}
func (l *sszHashList) Static() bool { return false }
func (l *sszHashList) EncodeSSZ(dst []byte) ([]byte, error) {
	if l.List == nil {
		l.List = solid.NewStaticListSSZ[*sszHash](l.Limit, 32)
	}
	return l.List.EncodeSSZ(dst)
}
func (l *sszHashList) DecodeSSZ(buf []byte, version int) error {
	l.List = solid.NewStaticListSSZ[*sszHash](l.Limit, 32)
	return l.List.DecodeSSZ(buf, version)
}
func (l *sszHashList) EncodingSizeSSZ() int {
	if l.List == nil {
		return 0
	}
	return l.List.EncodingSizeSSZ()
}
func (l *sszHashList) Clone() clonable.Clonable { return &sszHashList{Limit: l.Limit} }
func (l *sszHashList) hashes() []common.Hash {
	if l.List == nil {
		return nil
	}
	out := make([]common.Hash, 0, l.List.Len())
	l.List.Range(func(_ int, v *sszHash, _ int) bool { out = append(out, v.Hash); return true })
	return out
}

type sszByteListList struct {
	List  *solid.ListSSZ[*solid.ByteListSSZ]
	Limit int
}

func newSSZByteListList(limit int, items []hexutil.Bytes) *sszByteListList {
	l := solid.NewDynamicListSSZ[*solid.ByteListSSZ](limit)
	for _, item := range items {
		b := solid.NewByteListSSZ(sszMaxBytesPerTransaction)
		_ = b.SetBytes(item)
		l.Append(b)
	}
	return &sszByteListList{List: l, Limit: limit}
}
func (l *sszByteListList) Static() bool { return false }
func (l *sszByteListList) EncodeSSZ(dst []byte) ([]byte, error) {
	if l.List == nil {
		l.List = solid.NewDynamicListSSZ[*solid.ByteListSSZ](l.Limit)
	}
	return l.List.EncodeSSZ(dst)
}
func (l *sszByteListList) DecodeSSZ(buf []byte, version int) error {
	l.List = solid.NewDynamicListSSZ[*solid.ByteListSSZ](l.Limit)
	return l.List.DecodeSSZ(buf, version)
}
func (l *sszByteListList) EncodingSizeSSZ() int {
	if l.List == nil {
		return 0
	}
	return l.List.EncodingSizeSSZ()
}
func (l *sszByteListList) Clone() clonable.Clonable { return &sszByteListList{Limit: l.Limit} }
func (l *sszByteListList) bytes() []hexutil.Bytes {
	if l.List == nil {
		return nil
	}
	out := make([]hexutil.Bytes, 0, l.List.Len())
	l.List.Range(func(_ int, v *solid.ByteListSSZ, _ int) bool { out = append(out, v.Bytes()); return true })
	return out
}

type sszWithdrawalList struct {
	List *solid.ListSSZ[*cltypes.Withdrawal]
}

func newSSZWithdrawalList(ws []*types.Withdrawal) *sszWithdrawalList {
	l := solid.NewStaticListSSZ[*cltypes.Withdrawal](int(mainnetBeaconCfg.MaxWithdrawalsPerPayload), 44)
	for _, w := range ws {
		l.Append(&cltypes.Withdrawal{Index: w.Index, Validator: w.Validator, Address: w.Address, Amount: w.Amount})
	}
	return &sszWithdrawalList{List: l}
}
func (l *sszWithdrawalList) Static() bool { return false }
func (l *sszWithdrawalList) EncodeSSZ(dst []byte) ([]byte, error) {
	if l.List == nil {
		l.List = solid.NewStaticListSSZ[*cltypes.Withdrawal](int(mainnetBeaconCfg.MaxWithdrawalsPerPayload), 44)
	}
	return l.List.EncodeSSZ(dst)
}
func (l *sszWithdrawalList) DecodeSSZ(buf []byte, version int) error {
	l.List = solid.NewStaticListSSZ[*cltypes.Withdrawal](int(mainnetBeaconCfg.MaxWithdrawalsPerPayload), 44)
	return l.List.DecodeSSZ(buf, version)
}
func (l *sszWithdrawalList) EncodingSizeSSZ() int {
	if l.List == nil {
		return 0
	}
	return l.List.EncodingSizeSSZ()
}
func (*sszWithdrawalList) Clone() clonable.Clonable { return &sszWithdrawalList{} }
func (l *sszWithdrawalList) withdrawals() []*types.Withdrawal {
	if l.List == nil {
		return nil
	}
	out := make([]*types.Withdrawal, 0, l.List.Len())
	l.List.Range(func(_ int, w *cltypes.Withdrawal, _ int) bool {
		out = append(out, &types.Withdrawal{Index: w.Index, Validator: w.Validator, Address: w.Address, Amount: w.Amount})
		return true
	})
	return out
}

type sszPayloadStatus struct {
	Status          uint8
	LatestValidHash *sszHashList
	ValidationError *solid.ByteListSSZ
}

func (s *sszPayloadStatus) Static() bool { return false }
func (s *sszPayloadStatus) EncodeSSZ(dst []byte) ([]byte, error) {
	if s.LatestValidHash == nil {
		s.LatestValidHash = newSSZHashList(1, nil)
	}
	if s.ValidationError == nil {
		s.ValidationError = solid.NewByteListSSZ(sszMaxValidationError)
	}
	return ssz2.MarshalSSZ(dst, []byte{s.Status}, s.LatestValidHash, s.ValidationError)
}
func (s *sszPayloadStatus) DecodeSSZ(buf []byte, version int) error {
	s.LatestValidHash = &sszHashList{Limit: 1}
	s.ValidationError = solid.NewByteListSSZ(sszMaxValidationError)
	status := []byte{0}
	if err := ssz2.UnmarshalSSZ(buf, version, status, s.LatestValidHash, s.ValidationError); err != nil {
		return err
	}
	s.Status = status[0]
	return nil
}
func (s *sszPayloadStatus) EncodingSizeSSZ() int {
	return 1 + 4 + 4 + s.LatestValidHash.EncodingSizeSSZ() + s.ValidationError.EncodingSizeSSZ()
}
func (*sszPayloadStatus) Clone() clonable.Clonable { return &sszPayloadStatus{} }

func payloadStatusToSSZ(ps *engine_types.PayloadStatus) (*sszPayloadStatus, error) {
	status := uint8(0)
	switch ps.Status {
	case engine_types.ValidStatus:
		status = 0
	case engine_types.InvalidStatus:
		status = 1
	case engine_types.SyncingStatus:
		status = 2
	case engine_types.AcceptedStatus:
		status = 3
	default:
		return nil, fmt.Errorf("unknown payload status %q", ps.Status)
	}
	var latest []common.Hash
	if ps.LatestValidHash != nil {
		latest = []common.Hash{*ps.LatestValidHash}
	}
	errBytes := solid.NewByteListSSZ(sszMaxValidationError)
	if ps.ValidationError != nil && ps.ValidationError.Error() != nil {
		msg := []byte(ps.ValidationError.Error().Error())
		if len(msg) > sszMaxValidationError {
			msg = msg[:sszMaxValidationError]
		}
		_ = errBytes.SetBytes(msg)
	}
	return &sszPayloadStatus{Status: status, LatestValidHash: newSSZHashList(1, latest), ValidationError: errBytes}, nil
}

type sszForkchoiceState struct {
	HeadHash           common.Hash
	SafeBlockHash      common.Hash
	FinalizedBlockHash common.Hash
}

func (*sszForkchoiceState) Static() bool         { return true }
func (*sszForkchoiceState) EncodingSizeSSZ() int { return 96 }
func (s *sszForkchoiceState) EncodeSSZ(dst []byte) ([]byte, error) {
	return ssz2.MarshalSSZ(dst, s.HeadHash[:], s.SafeBlockHash[:], s.FinalizedBlockHash[:])
}
func (s *sszForkchoiceState) DecodeSSZ(buf []byte, version int) error {
	return ssz2.UnmarshalSSZ(buf, version, s.HeadHash[:], s.SafeBlockHash[:], s.FinalizedBlockHash[:])
}
func (*sszForkchoiceState) Clone() clonable.Clonable { return &sszForkchoiceState{} }

func forkchoiceStateFromEngine(s *engine_types.ForkChoiceState) sszForkchoiceState {
	return sszForkchoiceState{HeadHash: s.HeadHash, SafeBlockHash: s.SafeBlockHash, FinalizedBlockHash: s.FinalizedBlockHash}
}
func (s sszForkchoiceState) engine() *engine_types.ForkChoiceState {
	return &engine_types.ForkChoiceState{HeadHash: s.HeadHash, SafeBlockHash: s.SafeBlockHash, FinalizedBlockHash: s.FinalizedBlockHash}
}

type sszPayloadAttributes struct {
	Timestamp             uint64
	PrevRandao            common.Hash
	SuggestedFeeRecipient common.Address
	Withdrawals           *sszWithdrawalList
	ParentBeaconBlockRoot common.Hash
	SlotNumber            uint64
	version               clparams.StateVersion
}

func (a *sszPayloadAttributes) Static() bool { return false }
func (a *sszPayloadAttributes) EncodeSSZ(dst []byte) ([]byte, error) {
	switch a.version {
	case clparams.BellatrixVersion:
		return ssz2.MarshalSSZ(dst, a.Timestamp, a.PrevRandao[:], a.SuggestedFeeRecipient[:])
	case clparams.CapellaVersion:
		return ssz2.MarshalSSZ(dst, a.Timestamp, a.PrevRandao[:], a.SuggestedFeeRecipient[:], a.Withdrawals)
	case clparams.DenebVersion:
		return ssz2.MarshalSSZ(dst, a.Timestamp, a.PrevRandao[:], a.SuggestedFeeRecipient[:], a.Withdrawals, a.ParentBeaconBlockRoot[:])
	default:
		return ssz2.MarshalSSZ(dst, a.Timestamp, a.PrevRandao[:], a.SuggestedFeeRecipient[:], a.Withdrawals, a.ParentBeaconBlockRoot[:], a.SlotNumber)
	}
}
func (a *sszPayloadAttributes) DecodeSSZ(buf []byte, version int) error {
	a.version = clparams.StateVersion(version)
	a.Withdrawals = &sszWithdrawalList{}
	switch a.version {
	case clparams.BellatrixVersion:
		return ssz2.UnmarshalSSZ(buf, version, &a.Timestamp, a.PrevRandao[:], a.SuggestedFeeRecipient[:])
	case clparams.CapellaVersion:
		return ssz2.UnmarshalSSZ(buf, version, &a.Timestamp, a.PrevRandao[:], a.SuggestedFeeRecipient[:], a.Withdrawals)
	case clparams.DenebVersion:
		return ssz2.UnmarshalSSZ(buf, version, &a.Timestamp, a.PrevRandao[:], a.SuggestedFeeRecipient[:], a.Withdrawals, a.ParentBeaconBlockRoot[:])
	default:
		return ssz2.UnmarshalSSZ(buf, version, &a.Timestamp, a.PrevRandao[:], a.SuggestedFeeRecipient[:], a.Withdrawals, a.ParentBeaconBlockRoot[:], &a.SlotNumber)
	}
}
func (a *sszPayloadAttributes) EncodingSizeSSZ() int { b, _ := a.EncodeSSZ(nil); return len(b) }
func (*sszPayloadAttributes) Clone() clonable.Clonable {
	return &sszPayloadAttributes{}
}
func (a *sszPayloadAttributes) HashSSZ() ([32]byte, error) { return [32]byte{}, nil }
func (a *sszPayloadAttributes) engine() *engine_types.PayloadAttributes {
	pa := &engine_types.PayloadAttributes{Timestamp: hexutil.Uint64(a.Timestamp), PrevRandao: a.PrevRandao, SuggestedFeeRecipient: a.SuggestedFeeRecipient}
	if a.version >= clparams.CapellaVersion {
		pa.Withdrawals = a.Withdrawals.withdrawals()
	}
	if a.version >= clparams.DenebVersion {
		root := a.ParentBeaconBlockRoot
		pa.ParentBeaconBlockRoot = &root
	}
	if a.version >= clparams.GloasVersion {
		slot := hexutil.Uint64(a.SlotNumber)
		pa.SlotNumber = &slot
	}
	return pa
}

type sszForkchoiceRequest struct {
	State     sszForkchoiceState
	AttrsList *solid.ListSSZ[*sszPayloadAttributes]
	version   clparams.StateVersion
}

func (r *sszForkchoiceRequest) Static() bool { return false }
func (r *sszForkchoiceRequest) EncodeSSZ(dst []byte) ([]byte, error) {
	if r.AttrsList == nil {
		r.AttrsList = solid.NewDynamicListSSZ[*sszPayloadAttributes](1)
	}
	return ssz2.MarshalSSZ(dst, &r.State, r.AttrsList)
}
func (r *sszForkchoiceRequest) DecodeSSZ(buf []byte, version int) error {
	r.version = clparams.StateVersion(version)
	r.AttrsList = solid.NewDynamicListSSZ[*sszPayloadAttributes](1)
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
	a.version = r.version
	return a.engine()
}

type sszForkchoiceResponse struct {
	Status    *sszPayloadStatus
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
	r.Status = &sszPayloadStatus{}
	r.PayloadID = solid.NewStaticListSSZ[*sszBytes8](1, 8)
	return ssz2.UnmarshalSSZ(buf, version, r.Status, r.PayloadID)
}
func (r *sszForkchoiceResponse) EncodingSizeSSZ() int   { b, _ := r.EncodeSSZ(nil); return len(b) }
func (*sszForkchoiceResponse) Clone() clonable.Clonable { return &sszForkchoiceResponse{} }

func forkchoiceResponseToSSZ(resp *engine_types.ForkChoiceUpdatedResponse) (*sszForkchoiceResponse, error) {
	status, err := payloadStatusToSSZ(resp.PayloadStatus)
	if err != nil {
		return nil, err
	}
	pids := solid.NewStaticListSSZ[*sszBytes8](1, 8)
	if resp.PayloadId != nil && len(*resp.PayloadId) == 8 {
		var id [8]byte
		copy(id[:], *resp.PayloadId)
		pids.Append(&sszBytes8{Bytes: id})
	}
	return &sszForkchoiceResponse{Status: status, PayloadID: pids}, nil
}

type sszNewPayloadRequest struct {
	Payload               *sszPayload
	ExpectedBlobHashes    *sszHashList
	ParentBeaconBlockRoot common.Hash
	ExecutionRequests     *sszByteListList
	version               clparams.StateVersion
}

func newSSZNewPayloadRequest(version clparams.StateVersion) *sszNewPayloadRequest {
	return &sszNewPayloadRequest{Payload: newSSZPayload(version), ExpectedBlobHashes: &sszHashList{Limit: sszMaxBlobHashes}, ExecutionRequests: &sszByteListList{Limit: sszMaxExecutionRequests}, version: version}
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
	r.Payload = newSSZPayload(r.version)
	r.ExpectedBlobHashes = &sszHashList{Limit: sszMaxBlobHashes}
	r.ExecutionRequests = &sszByteListList{Limit: sszMaxExecutionRequests}
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

type sszClientVersion struct {
	Code    *solid.ByteListSSZ
	Name    *solid.ByteListSSZ
	Version *solid.ByteListSSZ
	Commit  [4]byte
}

func newSSZClientVersion(v engine_types.ClientVersionV1) *sszClientVersion {
	code, name, ver := solid.NewByteListSSZ(2), solid.NewByteListSSZ(64), solid.NewByteListSSZ(64)
	_ = code.SetBytes([]byte(v.Code))
	_ = name.SetBytes([]byte(v.Name))
	_ = ver.SetBytes([]byte(v.Version))
	var commit [4]byte
	ch := strings.TrimPrefix(v.Commit, "0x")
	if b, err := hex.DecodeString(ch); err == nil && len(b) >= 4 {
		copy(commit[:], b[:4])
	}
	return &sszClientVersion{Code: code, Name: name, Version: ver, Commit: commit}
}
func (v *sszClientVersion) Static() bool { return false }
func (v *sszClientVersion) EncodeSSZ(dst []byte) ([]byte, error) {
	return ssz2.MarshalSSZ(dst, v.Code, v.Name, v.Version, v.Commit[:])
}
func (v *sszClientVersion) DecodeSSZ(buf []byte, version int) error {
	v.Code, v.Name, v.Version = solid.NewByteListSSZ(2), solid.NewByteListSSZ(64), solid.NewByteListSSZ(64)
	return ssz2.UnmarshalSSZ(buf, version, v.Code, v.Name, v.Version, v.Commit[:])
}
func (v *sszClientVersion) EncodingSizeSSZ() int       { b, _ := v.EncodeSSZ(nil); return len(b) }
func (v *sszClientVersion) HashSSZ() ([32]byte, error) { return [32]byte{}, nil }
func (*sszClientVersion) Clone() clonable.Clonable     { return &sszClientVersion{} }
func (v *sszClientVersion) engine() engine_types.ClientVersionV1 {
	return engine_types.ClientVersionV1{Code: string(v.Code.Bytes()), Name: string(v.Name.Bytes()), Version: string(v.Version.Bytes()), Commit: "0x" + hex.EncodeToString(v.Commit[:])}
}

type sszClientVersionRequest struct{ ClientVersion *sszClientVersion }

func (r *sszClientVersionRequest) Static() bool { return false }
func (r *sszClientVersionRequest) EncodeSSZ(dst []byte) ([]byte, error) {
	return ssz2.MarshalSSZ(dst, r.ClientVersion)
}
func (r *sszClientVersionRequest) DecodeSSZ(buf []byte, version int) error {
	r.ClientVersion = &sszClientVersion{}
	return ssz2.UnmarshalSSZ(buf, version, r.ClientVersion)
}
func (r *sszClientVersionRequest) EncodingSizeSSZ() int   { b, _ := r.EncodeSSZ(nil); return len(b) }
func (*sszClientVersionRequest) Clone() clonable.Clonable { return &sszClientVersionRequest{} }

type sszClientVersionResponse struct {
	Versions *solid.ListSSZ[*sszClientVersion]
}

func newSSZClientVersionResponse(versions []engine_types.ClientVersionV1) *sszClientVersionResponse {
	l := solid.NewDynamicListSSZ[*sszClientVersion](4)
	for _, v := range versions {
		l.Append(newSSZClientVersion(v))
	}
	return &sszClientVersionResponse{Versions: l}
}
func (r *sszClientVersionResponse) Static() bool { return false }
func (r *sszClientVersionResponse) EncodeSSZ(dst []byte) ([]byte, error) {
	return ssz2.MarshalSSZ(dst, r.Versions)
}
func (r *sszClientVersionResponse) DecodeSSZ(buf []byte, version int) error {
	r.Versions = solid.NewDynamicListSSZ[*sszClientVersion](4)
	return ssz2.UnmarshalSSZ(buf, version, r.Versions)
}
func (r *sszClientVersionResponse) EncodingSizeSSZ() int   { b, _ := r.EncodeSSZ(nil); return len(b) }
func (*sszClientVersionResponse) Clone() clonable.Clonable { return &sszClientVersionResponse{} }

func blockValueBytes(v *hexutil.Big) []byte {
	out := make([]byte, 32)
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

type sszBool struct{ Value bool }

func (*sszBool) Static() bool         { return true }
func (*sszBool) EncodingSizeSSZ() int { return 1 }
func (b *sszBool) EncodeSSZ(dst []byte) ([]byte, error) {
	if b.Value {
		return append(dst, 1), nil
	}
	return append(dst, 0), nil
}
func (b *sszBool) DecodeSSZ(buf []byte, _ int) error {
	if len(buf) < 1 {
		return errors.New("short bool")
	}
	b.Value = buf[0] != 0
	return nil
}
func (*sszBool) Clone() clonable.Clonable { return &sszBool{} }

type sszBlobsBundle struct {
	Commitments *solid.ListSSZ[*sszKZGVector]
	Proofs      *solid.ListSSZ[*sszKZGVector]
	Blobs       *solid.ListSSZ[*sszBlobVector]
	ProofsLimit int
}

func newSSZBlobsBundle(b *engine_types.BlobsBundle, version clparams.StateVersion) *sszBlobsBundle {
	proofsLimit := sszMaxBlobHashes
	if version >= clparams.FuluVersion {
		proofsLimit = sszMaxCellProofs
	}
	bundle := &sszBlobsBundle{
		Commitments: solid.NewStaticListSSZ[*sszKZGVector](sszMaxBlobHashes, sszKZGBytes),
		Proofs:      solid.NewStaticListSSZ[*sszKZGVector](proofsLimit, sszKZGBytes),
		Blobs:       solid.NewStaticListSSZ[*sszBlobVector](sszMaxBlobHashes, sszBlobBytes),
		ProofsLimit: proofsLimit,
	}
	var commitments, proofs, blobs []hexutil.Bytes
	if b != nil {
		commitments, proofs, blobs = b.Commitments, b.Proofs, b.Blobs
	}
	for _, commitment := range commitments {
		bundle.Commitments.Append(newSSZKZGVector(commitment))
	}
	for _, proof := range proofs {
		bundle.Proofs.Append(newSSZKZGVector(proof))
	}
	for _, blob := range blobs {
		bundle.Blobs.Append(newSSZBlobVector(blob))
	}
	return bundle
}
func (b *sszBlobsBundle) Static() bool { return false }
func (b *sszBlobsBundle) EncodeSSZ(dst []byte) ([]byte, error) {
	if b.Commitments == nil {
		if b.ProofsLimit == 0 {
			b.ProofsLimit = sszMaxBlobHashes
		}
		b.Commitments = solid.NewStaticListSSZ[*sszKZGVector](sszMaxBlobHashes, sszKZGBytes)
		b.Proofs = solid.NewStaticListSSZ[*sszKZGVector](b.ProofsLimit, sszKZGBytes)
		b.Blobs = solid.NewStaticListSSZ[*sszBlobVector](sszMaxBlobHashes, sszBlobBytes)
	}
	return ssz2.MarshalSSZ(dst, b.Commitments, b.Proofs, b.Blobs)
}
func (b *sszBlobsBundle) DecodeSSZ(buf []byte, version int) error {
	if b.ProofsLimit == 0 {
		b.ProofsLimit = sszMaxBlobHashes
	}
	b.Commitments = solid.NewStaticListSSZ[*sszKZGVector](sszMaxBlobHashes, sszKZGBytes)
	b.Proofs = solid.NewStaticListSSZ[*sszKZGVector](b.ProofsLimit, sszKZGBytes)
	b.Blobs = solid.NewStaticListSSZ[*sszBlobVector](sszMaxBlobHashes, sszBlobBytes)
	return ssz2.UnmarshalSSZ(buf, version, b.Commitments, b.Proofs, b.Blobs)
}
func (b *sszBlobsBundle) EncodingSizeSSZ() int   { out, _ := b.EncodeSSZ(nil); return len(out) }
func (*sszBlobsBundle) Clone() clonable.Clonable { return &sszBlobsBundle{} }

type sszGetPayloadResponse struct {
	Payload               *sszPayload
	BlockValue            *sszFixedBytes
	BlobsBundle           *sszBlobsBundle
	ShouldOverrideBuilder *sszBool
	ExecutionRequests     *cltypes.ExecutionRequests
	version               clparams.StateVersion
}

func newSSZGetPayloadResponse(resp *engine_types.GetPayloadResponse, version clparams.StateVersion) (*sszGetPayloadResponse, error) {
	payload, err := executionPayloadToSSZ(resp.ExecutionPayload, version)
	if err != nil {
		return nil, err
	}
	executionRequests, err := executionRequestsFromList(resp.ExecutionRequests, version)
	if err != nil {
		return nil, err
	}
	return &sszGetPayloadResponse{
		Payload:               payload,
		BlockValue:            newSSZFixedBytes(32, blockValueBytes(resp.BlockValue)),
		BlobsBundle:           newSSZBlobsBundle(resp.BlobsBundle, version),
		ShouldOverrideBuilder: &sszBool{Value: resp.ShouldOverrideBuilder},
		ExecutionRequests:     executionRequests,
		version:               version,
	}, nil
}
func (r *sszGetPayloadResponse) Static() bool { return false }
func (r *sszGetPayloadResponse) EncodeSSZ(dst []byte) ([]byte, error) {
	switch r.version {
	case clparams.CapellaVersion:
		return ssz2.MarshalSSZ(dst, r.Payload, r.BlockValue)
	case clparams.DenebVersion:
		return ssz2.MarshalSSZ(dst, r.Payload, r.BlockValue, r.BlobsBundle, r.ShouldOverrideBuilder)
	default:
		return ssz2.MarshalSSZ(dst, r.Payload, r.BlockValue, r.BlobsBundle, r.ShouldOverrideBuilder, r.ExecutionRequests)
	}
}
func (r *sszGetPayloadResponse) DecodeSSZ(buf []byte, version int) error {
	r.version = clparams.StateVersion(version)
	r.Payload = newSSZPayload(r.version)
	r.BlockValue = newSSZFixedBytes(32, nil)
	proofsLimit := sszMaxBlobHashes
	if r.version >= clparams.FuluVersion {
		proofsLimit = sszMaxCellProofs
	}
	r.BlobsBundle = &sszBlobsBundle{ProofsLimit: proofsLimit}
	r.ShouldOverrideBuilder = &sszBool{}
	r.ExecutionRequests = cltypes.NewExecutionRequests(mainnetBeaconCfg)
	switch r.version {
	case clparams.CapellaVersion:
		return ssz2.UnmarshalSSZ(buf, version, r.Payload, r.BlockValue)
	case clparams.DenebVersion:
		return ssz2.UnmarshalSSZ(buf, version, r.Payload, r.BlockValue, r.BlobsBundle, r.ShouldOverrideBuilder)
	default:
		return ssz2.UnmarshalSSZ(buf, version, r.Payload, r.BlockValue, r.BlobsBundle, r.ShouldOverrideBuilder, r.ExecutionRequests)
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

type sszFixedBytes struct {
	Bytes []byte
	Size  int
}

func newSSZFixedBytes(size int, in []byte) *sszFixedBytes {
	out := &sszFixedBytes{Bytes: make([]byte, size), Size: size}
	copy(out.Bytes, in)
	return out
}
func (b *sszFixedBytes) Static() bool { return true }
func (b *sszFixedBytes) EncodingSizeSSZ() int {
	if b.Size == 0 {
		return len(b.Bytes)
	}
	return b.Size
}
func (b *sszFixedBytes) EncodeSSZ(dst []byte) ([]byte, error) {
	size := b.EncodingSizeSSZ()
	if len(b.Bytes) != size {
		return nil, fmt.Errorf("fixed bytes length %d, want %d", len(b.Bytes), size)
	}
	return append(dst, b.Bytes...), nil
}
func (b *sszFixedBytes) DecodeSSZ(buf []byte, _ int) error {
	size := b.EncodingSizeSSZ()
	if len(buf) < size {
		return errors.New("short fixed bytes")
	}
	b.Bytes = common.Copy(buf[:size])
	return nil
}
func (b *sszFixedBytes) HashSSZ() ([32]byte, error) {
	var h common.Hash
	copy(h[:], b.Bytes)
	return h, nil
}
func (b *sszFixedBytes) Clone() clonable.Clonable {
	if b == nil {
		return &sszFixedBytes{}
	}
	return &sszFixedBytes{Size: b.Size}
}

type sszKZGVector struct{ Bytes [sszKZGBytes]byte }

func newSSZKZGVector(in []byte) *sszKZGVector {
	var out sszKZGVector
	copy(out.Bytes[:], in)
	return &out
}
func (*sszKZGVector) Static() bool         { return true }
func (*sszKZGVector) EncodingSizeSSZ() int { return sszKZGBytes }
func (b *sszKZGVector) EncodeSSZ(dst []byte) ([]byte, error) {
	return append(dst, b.Bytes[:]...), nil
}
func (b *sszKZGVector) DecodeSSZ(buf []byte, _ int) error {
	if len(buf) < sszKZGBytes {
		return errors.New("short kzg vector")
	}
	copy(b.Bytes[:], buf[:sszKZGBytes])
	return nil
}
func (b *sszKZGVector) HashSSZ() ([32]byte, error) {
	var h common.Hash
	copy(h[:], b.Bytes[:])
	return h, nil
}
func (*sszKZGVector) Clone() clonable.Clonable { return &sszKZGVector{} }

type sszBlobVector struct{ Bytes [sszBlobBytes]byte }

func newSSZBlobVector(in []byte) *sszBlobVector {
	var out sszBlobVector
	copy(out.Bytes[:], in)
	return &out
}
func (*sszBlobVector) Static() bool         { return true }
func (*sszBlobVector) EncodingSizeSSZ() int { return sszBlobBytes }
func (b *sszBlobVector) EncodeSSZ(dst []byte) ([]byte, error) {
	return append(dst, b.Bytes[:]...), nil
}
func (b *sszBlobVector) DecodeSSZ(buf []byte, _ int) error {
	if len(buf) < sszBlobBytes {
		return errors.New("short blob vector")
	}
	copy(b.Bytes[:], buf[:sszBlobBytes])
	return nil
}
func (b *sszBlobVector) HashSSZ() ([32]byte, error) {
	return common.Hash{}, nil
}
func (*sszBlobVector) Clone() clonable.Clonable { return &sszBlobVector{} }

type sszBlobAndProofV1 struct {
	Blob  *sszFixedBytes
	Proof *sszFixedBytes
}

func newSSZBlobAndProofV1(in *engine_types.BlobAndProofV1) *sszBlobAndProofV1 {
	return &sszBlobAndProofV1{
		Blob:  newSSZFixedBytes(sszBlobBytes, in.Blob),
		Proof: newSSZFixedBytes(sszKZGBytes, in.Proof),
	}
}
func (*sszBlobAndProofV1) Static() bool         { return true }
func (*sszBlobAndProofV1) EncodingSizeSSZ() int { return sszBlobBytes + sszKZGBytes }
func (b *sszBlobAndProofV1) EncodeSSZ(dst []byte) ([]byte, error) {
	return ssz2.MarshalSSZ(dst, b.Blob, b.Proof)
}
func (b *sszBlobAndProofV1) DecodeSSZ(buf []byte, version int) error {
	b.Blob = &sszFixedBytes{Size: sszBlobBytes}
	b.Proof = &sszFixedBytes{Size: sszKZGBytes}
	return ssz2.UnmarshalSSZ(buf, version, b.Blob, b.Proof)
}
func (b *sszBlobAndProofV1) HashSSZ() ([32]byte, error) { return [32]byte{}, nil }
func (*sszBlobAndProofV1) Clone() clonable.Clonable     { return &sszBlobAndProofV1{} }

type sszGetBlobsV1Response struct {
	BlobsAndProofs *solid.ListSSZ[*sszBlobAndProofV1]
}

func newSSZGetBlobsV1Response(blobs []*engine_types.BlobAndProofV1) *sszGetBlobsV1Response {
	l := solid.NewStaticListSSZ[*sszBlobAndProofV1](sszMaxGetBlobHashes, sszBlobBytes+sszKZGBytes)
	for _, blob := range blobs {
		if blob != nil {
			l.Append(newSSZBlobAndProofV1(blob))
		}
	}
	return &sszGetBlobsV1Response{BlobsAndProofs: l}
}
func (r *sszGetBlobsV1Response) Static() bool { return false }
func (r *sszGetBlobsV1Response) EncodeSSZ(dst []byte) ([]byte, error) {
	if r.BlobsAndProofs == nil {
		r.BlobsAndProofs = solid.NewStaticListSSZ[*sszBlobAndProofV1](sszMaxGetBlobHashes, sszBlobBytes+sszKZGBytes)
	}
	return ssz2.MarshalSSZ(dst, r.BlobsAndProofs)
}
func (r *sszGetBlobsV1Response) DecodeSSZ(buf []byte, version int) error {
	r.BlobsAndProofs = solid.NewStaticListSSZ[*sszBlobAndProofV1](sszMaxGetBlobHashes, sszBlobBytes+sszKZGBytes)
	return ssz2.UnmarshalSSZ(buf, version, r.BlobsAndProofs)
}
func (r *sszGetBlobsV1Response) EncodingSizeSSZ() int   { b, _ := r.EncodeSSZ(nil); return len(b) }
func (*sszGetBlobsV1Response) Clone() clonable.Clonable { return &sszGetBlobsV1Response{} }

type sszBlobAndProofV2 struct {
	Blob   *sszFixedBytes
	Proofs *solid.ListSSZ[*sszKZGVector]
}

func newSSZBlobAndProofV2(in *engine_types.BlobAndProofV2) *sszBlobAndProofV2 {
	proofs := solid.NewStaticListSSZ[*sszKZGVector](sszCellsPerExtBlob, sszKZGBytes)
	for _, proof := range in.CellProofs {
		proofs.Append(newSSZKZGVector(proof))
	}
	return &sszBlobAndProofV2{Blob: newSSZFixedBytes(sszBlobBytes, in.Blob), Proofs: proofs}
}
func (*sszBlobAndProofV2) Static() bool { return false }
func (b *sszBlobAndProofV2) EncodeSSZ(dst []byte) ([]byte, error) {
	if b.Proofs == nil {
		b.Proofs = solid.NewStaticListSSZ[*sszKZGVector](sszCellsPerExtBlob, sszKZGBytes)
	}
	return ssz2.MarshalSSZ(dst, b.Blob, b.Proofs)
}
func (b *sszBlobAndProofV2) DecodeSSZ(buf []byte, version int) error {
	b.Blob = &sszFixedBytes{Size: sszBlobBytes}
	b.Proofs = solid.NewStaticListSSZ[*sszKZGVector](sszCellsPerExtBlob, sszKZGBytes)
	return ssz2.UnmarshalSSZ(buf, version, b.Blob, b.Proofs)
}
func (b *sszBlobAndProofV2) EncodingSizeSSZ() int       { out, _ := b.EncodeSSZ(nil); return len(out) }
func (b *sszBlobAndProofV2) HashSSZ() ([32]byte, error) { return [32]byte{}, nil }
func (*sszBlobAndProofV2) Clone() clonable.Clonable {
	return &sszBlobAndProofV2{Blob: &sszFixedBytes{Size: sszBlobBytes}, Proofs: solid.NewStaticListSSZ[*sszKZGVector](sszCellsPerExtBlob, sszKZGBytes)}
}

type sszGetBlobsV2Response struct {
	BlobsAndProofs *solid.ListSSZ[*sszBlobAndProofV2]
}

func newSSZGetBlobsV2Response(blobs []*engine_types.BlobAndProofV2) *sszGetBlobsV2Response {
	l := solid.NewDynamicListSSZ[*sszBlobAndProofV2](sszMaxGetBlobHashes)
	for _, blob := range blobs {
		if blob != nil {
			l.Append(newSSZBlobAndProofV2(blob))
		}
	}
	return &sszGetBlobsV2Response{BlobsAndProofs: l}
}
func (r *sszGetBlobsV2Response) Static() bool { return false }
func (r *sszGetBlobsV2Response) EncodeSSZ(dst []byte) ([]byte, error) {
	if r.BlobsAndProofs == nil {
		r.BlobsAndProofs = solid.NewDynamicListSSZ[*sszBlobAndProofV2](sszMaxGetBlobHashes)
	}
	return ssz2.MarshalSSZ(dst, r.BlobsAndProofs)
}
func (r *sszGetBlobsV2Response) DecodeSSZ(buf []byte, version int) error {
	r.BlobsAndProofs = solid.NewDynamicListSSZ[*sszBlobAndProofV2](sszMaxGetBlobHashes)
	return ssz2.UnmarshalSSZ(buf, version, r.BlobsAndProofs)
}
func (r *sszGetBlobsV2Response) EncodingSizeSSZ() int   { b, _ := r.EncodeSSZ(nil); return len(b) }
func (*sszGetBlobsV2Response) Clone() clonable.Clonable { return &sszGetBlobsV2Response{} }

type sszNullableBlobAndProofV2 struct {
	BlobAndProof *solid.ListSSZ[*sszBlobAndProofV2]
}

func newSSZNullableBlobAndProofV2(blob *engine_types.BlobAndProofV2) *sszNullableBlobAndProofV2 {
	l := solid.NewDynamicListSSZ[*sszBlobAndProofV2](1)
	if blob != nil {
		l.Append(newSSZBlobAndProofV2(blob))
	}
	return &sszNullableBlobAndProofV2{BlobAndProof: l}
}
func (n *sszNullableBlobAndProofV2) Static() bool { return false }
func (n *sszNullableBlobAndProofV2) EncodeSSZ(dst []byte) ([]byte, error) {
	if n.BlobAndProof == nil {
		n.BlobAndProof = solid.NewDynamicListSSZ[*sszBlobAndProofV2](1)
	}
	return ssz2.MarshalSSZ(dst, n.BlobAndProof)
}
func (n *sszNullableBlobAndProofV2) DecodeSSZ(buf []byte, version int) error {
	n.BlobAndProof = solid.NewDynamicListSSZ[*sszBlobAndProofV2](1)
	return ssz2.UnmarshalSSZ(buf, version, n.BlobAndProof)
}
func (n *sszNullableBlobAndProofV2) EncodingSizeSSZ() int { b, _ := n.EncodeSSZ(nil); return len(b) }
func (n *sszNullableBlobAndProofV2) HashSSZ() ([32]byte, error) {
	return [32]byte{}, nil
}
func (*sszNullableBlobAndProofV2) Clone() clonable.Clonable {
	return &sszNullableBlobAndProofV2{}
}

type sszGetBlobsV3Response struct {
	BlobsAndProofs *solid.ListSSZ[*sszNullableBlobAndProofV2]
}

func newSSZGetBlobsV3Response(blobs []*engine_types.BlobAndProofV2) *sszGetBlobsV3Response {
	l := solid.NewDynamicListSSZ[*sszNullableBlobAndProofV2](sszMaxGetBlobHashes)
	for _, blob := range blobs {
		l.Append(newSSZNullableBlobAndProofV2(blob))
	}
	return &sszGetBlobsV3Response{BlobsAndProofs: l}
}
func (r *sszGetBlobsV3Response) Static() bool { return false }
func (r *sszGetBlobsV3Response) EncodeSSZ(dst []byte) ([]byte, error) {
	if r.BlobsAndProofs == nil {
		r.BlobsAndProofs = solid.NewDynamicListSSZ[*sszNullableBlobAndProofV2](sszMaxGetBlobHashes)
	}
	return ssz2.MarshalSSZ(dst, r.BlobsAndProofs)
}
func (r *sszGetBlobsV3Response) DecodeSSZ(buf []byte, version int) error {
	r.BlobsAndProofs = solid.NewDynamicListSSZ[*sszNullableBlobAndProofV2](sszMaxGetBlobHashes)
	return ssz2.UnmarshalSSZ(buf, version, r.BlobsAndProofs)
}
func (r *sszGetBlobsV3Response) EncodingSizeSSZ() int   { b, _ := r.EncodeSSZ(nil); return len(b) }
func (*sszGetBlobsV3Response) Clone() clonable.Clonable { return &sszGetBlobsV3Response{} }
