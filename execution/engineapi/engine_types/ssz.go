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
	"fmt"
	"math/big"

	"github.com/erigontech/erigon/cl/cltypes"
	"github.com/erigontech/erigon/cl/cltypes/solid"
	"github.com/erigontech/erigon/cl/merkle_tree"
	ssz2 "github.com/erigontech/erigon/cl/ssz"
	"github.com/erigontech/erigon/common"
	"github.com/erigontech/erigon/common/clonable"
	"github.com/erigontech/erigon/common/hexutil"
	"github.com/erigontech/erigon/execution/types"
)

// SSZ status codes for PayloadStatus (EIP-8161)
const (
	SSZStatusValid            uint8 = 0
	SSZStatusInvalid          uint8 = 1
	SSZStatusSyncing          uint8 = 2
	SSZStatusAccepted         uint8 = 3
	SSZStatusInvalidBlockHash uint8 = 4
)

// EngineStatusToSSZ converts a string EngineStatus to the SSZ uint8 representation.
func EngineStatusToSSZ(status EngineStatus) uint8 {
	switch status {
	case ValidStatus:
		return SSZStatusValid
	case InvalidStatus:
		return SSZStatusInvalid
	case SyncingStatus:
		return SSZStatusSyncing
	case AcceptedStatus:
		return SSZStatusAccepted
	case InvalidBlockHashStatus:
		return SSZStatusInvalidBlockHash
	default:
		return SSZStatusInvalid
	}
}

// SSZToEngineStatus converts an SSZ uint8 status to the string EngineStatus.
func SSZToEngineStatus(status uint8) EngineStatus {
	switch status {
	case SSZStatusValid:
		return ValidStatus
	case SSZStatusInvalid:
		return InvalidStatus
	case SSZStatusSyncing:
		return SyncingStatus
	case SSZStatusAccepted:
		return AcceptedStatus
	case SSZStatusInvalidBlockHash:
		return InvalidBlockHashStatus
	default:
		return InvalidStatus
	}
}

const payloadStatusFixedSize = 9 // status(1) + hash_offset(4) + err_offset(4)

func (p *PayloadStatus) EncodeSSZ(buf []byte) (dst []byte, err error) {
	status := []byte{EngineStatusToSSZ(p.Status)}
	hashes := solid.NewHashList(1)
	if p.LatestValidHash != nil {
		hashes.Append(*p.LatestValidHash)
	}
	var errBytes []byte
	if p.ValidationError != nil && p.ValidationError.Error() != nil {
		errBytes = []byte(p.ValidationError.Error().Error())
	}
	return ssz2.MarshalSSZ(buf, status, hashes, &ByteListSSZ{data: errBytes})
}

func (p *PayloadStatus) DecodeSSZ(buf []byte, _ int) error {
	status := []byte{0}
	hashes := solid.NewHashList(1)
	validationError := &ByteListSSZ{}
	if err := ssz2.UnmarshalSSZ(buf, 0, status, hashes, validationError); err != nil {
		return fmt.Errorf("PayloadStatus: %w", err)
	}
	p.Status = SSZToEngineStatus(status[0])
	switch hashes.Length() {
	case 0:
		p.LatestValidHash = nil
	case 1:
		hash := hashes.Get(0)
		p.LatestValidHash = &hash
	default:
		return fmt.Errorf("PayloadStatus: invalid latest valid hash count %d", hashes.Length())
	}
	if len(validationError.data) > 1024 {
		return fmt.Errorf("PayloadStatus: validation error too long (%d > 1024)", len(validationError.data))
	}
	if len(validationError.data) > 0 {
		p.ValidationError = NewStringifiedErrorFromString(string(validationError.data))
	} else {
		p.ValidationError = nil
	}
	return nil
}

func (p *PayloadStatus) EncodingSizeSSZ() int {
	size := payloadStatusFixedSize
	if p.LatestValidHash != nil {
		size += 32
	}
	if p.ValidationError != nil && p.ValidationError.Error() != nil {
		size += len(p.ValidationError.Error().Error())
	}
	return size
}

func (p *PayloadStatus) Static() bool             { return false }
func (p *PayloadStatus) Clone() clonable.Clonable { return &PayloadStatus{} }

func DecodePayloadStatus(buf []byte) (*PayloadStatus, error) {
	p := &PayloadStatus{}
	if err := p.DecodeSSZ(buf, 0); err != nil {
		return nil, err
	}
	return p, nil
}

func (f *ForkChoiceState) EncodeSSZ(buf []byte) ([]byte, error) {
	return ssz2.MarshalSSZ(buf, f.HeadHash[:], f.SafeBlockHash[:], f.FinalizedBlockHash[:])
}

func (f *ForkChoiceState) DecodeSSZ(buf []byte, version int) error {
	return ssz2.UnmarshalSSZ(buf, version, f.HeadHash[:], f.SafeBlockHash[:], f.FinalizedBlockHash[:])
}

func (f *ForkChoiceState) EncodingSizeSSZ() int     { return 96 }
func (f *ForkChoiceState) Static() bool             { return true }
func (f *ForkChoiceState) Clone() clonable.Clonable { return &ForkChoiceState{} }

func EncodeForkchoiceState(fcs *ForkChoiceState) []byte {
	buf, _ := fcs.EncodeSSZ(nil)
	return buf
}

func DecodeForkchoiceState(buf []byte) (*ForkChoiceState, error) {
	s := &ForkChoiceState{}
	if err := s.DecodeSSZ(buf, 0); err != nil {
		return nil, fmt.Errorf("ForkchoiceState: %w", err)
	}
	return s, nil
}

func (p *PayloadAttributes) EncodeSSZ(buf []byte) ([]byte, error) {
	version := payloadAttributesVersionFromFields(p)
	timestamp := uint64(p.Timestamp)
	schema := []any{&timestamp, p.PrevRandao[:], p.SuggestedFeeRecipient[:]}
	if version >= 2 {
		schema = append(schema, executionWithdrawalsToSolid(p.Withdrawals))
	}
	if version >= 3 {
		root := common.Hash{}
		if p.ParentBeaconBlockRoot != nil {
			root = *p.ParentBeaconBlockRoot
		}
		schema = append(schema, root[:])
	}
	return ssz2.MarshalSSZ(buf, schema...)
}

func (p *PayloadAttributes) DecodeSSZ(buf []byte, version int) error {
	var timestamp uint64
	schema := []any{&timestamp, p.PrevRandao[:], p.SuggestedFeeRecipient[:]}
	withdrawals := solid.NewStaticListSSZ[*cltypes.Withdrawal](1_048_576, (&cltypes.Withdrawal{}).EncodingSizeSSZ())
	if version >= 2 {
		schema = append(schema, withdrawals)
	}
	var parentBeaconBlockRoot common.Hash
	if version >= 3 {
		schema = append(schema, parentBeaconBlockRoot[:])
	}
	if err := ssz2.UnmarshalSSZ(buf, version, schema...); err != nil {
		return fmt.Errorf("PayloadAttributes: %w", err)
	}
	p.Timestamp = hexutil.Uint64(timestamp)
	if version >= 2 {
		p.Withdrawals = solidWithdrawalsToExecution(withdrawals)
	}
	if version >= 3 {
		p.ParentBeaconBlockRoot = &parentBeaconBlockRoot
	}
	return nil
}

func (p *PayloadAttributes) EncodingSizeSSZ() int {
	version := payloadAttributesVersionFromFields(p)
	size := 60
	if version >= 2 {
		size += 4 + len(p.Withdrawals)*(&cltypes.Withdrawal{}).EncodingSizeSSZ()
	}
	if version >= 3 {
		size += 32
	}
	return size
}

func (p *PayloadAttributes) Static() bool             { return false }
func (p *PayloadAttributes) Clone() clonable.Clonable { return &PayloadAttributes{} }
func (p *PayloadAttributes) HashSSZ() ([32]byte, error) {
	version := payloadAttributesVersionFromFields(p)
	timestamp := uint64(p.Timestamp)
	schema := []any{&timestamp, p.PrevRandao[:], p.SuggestedFeeRecipient[:]}
	if version >= 2 {
		schema = append(schema, executionWithdrawalsToSolid(p.Withdrawals))
	}
	if version >= 3 {
		root := common.Hash{}
		if p.ParentBeaconBlockRoot != nil {
			root = *p.ParentBeaconBlockRoot
		}
		schema = append(schema, root[:])
	}
	return merkle_tree.HashTreeRoot(schema...)
}

func payloadAttributesVersionFromFields(p *PayloadAttributes) int {
	if p.ParentBeaconBlockRoot != nil {
		return 3
	}
	if p.Withdrawals != nil {
		return 2
	}
	return 1
}

const forkchoiceUpdatedResponseFixedSize = 8

func (r *ForkChoiceUpdatedResponse) EncodeSSZ(buf []byte) (dst []byte, err error) {
	var payloadID []byte
	if r.PayloadId != nil {
		payloadID = []byte(*r.PayloadId)
	}
	return ssz2.MarshalSSZ(buf, r.PayloadStatus, &ByteListSSZ{data: payloadID})
}

func (r *ForkChoiceUpdatedResponse) DecodeSSZ(buf []byte, _ int) error {
	r.PayloadStatus = &PayloadStatus{}
	payloadIDBytes := &ByteListSSZ{}
	if err := ssz2.UnmarshalSSZ(buf, 0, r.PayloadStatus, payloadIDBytes); err != nil {
		return fmt.Errorf("ForkChoiceUpdatedResponse: %w", err)
	}

	if len(payloadIDBytes.data) == 8 {
		payloadID := make(hexutil.Bytes, 8)
		copy(payloadID, payloadIDBytes.data)
		r.PayloadId = &payloadID
	} else if len(payloadIDBytes.data) == 0 {
		r.PayloadId = nil
	} else {
		return fmt.Errorf("ForkChoiceUpdatedResponse: invalid payload ID length %d", len(payloadIDBytes.data))
	}
	return nil
}

func (r *ForkChoiceUpdatedResponse) EncodingSizeSSZ() int {
	size := forkchoiceUpdatedResponseFixedSize
	if r.PayloadStatus != nil {
		size += r.PayloadStatus.EncodingSizeSSZ()
	}
	if r.PayloadId != nil {
		size += len(*r.PayloadId)
	}
	return size
}

func (r *ForkChoiceUpdatedResponse) Static() bool             { return false }
func (r *ForkChoiceUpdatedResponse) Clone() clonable.Clonable { return &ForkChoiceUpdatedResponse{} }

func EncodeForkchoiceUpdatedResponse(resp *ForkChoiceUpdatedResponse) []byte {
	buf, _ := resp.EncodeSSZ(nil)
	return buf
}

func DecodeForkchoiceUpdatedResponse(buf []byte) (*ForkChoiceUpdatedResponse, error) {
	r := &ForkChoiceUpdatedResponse{}
	if err := r.DecodeSSZ(buf, 0); err != nil {
		return nil, err
	}
	return r, nil
}

// ---------------------------------------------------------------
// SSZ Helper Types (SizedObjectSSZ implementations)
// ---------------------------------------------------------------

// ByteListSSZ wraps a byte slice for use in SSZ schemas as a variable-length field.
type ByteListSSZ struct{ data []byte }

func (b *ByteListSSZ) EncodeSSZ(buf []byte) ([]byte, error) { return append(buf, b.data...), nil }
func (b *ByteListSSZ) DecodeSSZ(buf []byte, _ int) error {
	b.data = append([]byte(nil), buf...)
	return nil
}
func (b *ByteListSSZ) EncodingSizeSSZ() int     { return len(b.data) }
func (b *ByteListSSZ) Static() bool             { return false }
func (b *ByteListSSZ) Clone() clonable.Clonable { return &ByteListSSZ{} }

// ConcatBytesListSSZ wraps a list of fixed-size byte slices (commitments, proofs, blobs).
type ConcatBytesListSSZ struct {
	items    [][]byte
	itemSize int
}

func (c *ConcatBytesListSSZ) EncodeSSZ(buf []byte) ([]byte, error) {
	for _, item := range c.items {
		buf = append(buf, item...)
	}
	return buf, nil
}

func (c *ConcatBytesListSSZ) DecodeSSZ(buf []byte, _ int) error {
	if len(buf) == 0 {
		c.items = nil
		return nil
	}
	if c.itemSize > 0 && len(buf)%c.itemSize != 0 {
		return fmt.Errorf("ConcatBytesListSSZ: length %d not aligned to %d", len(buf), c.itemSize)
	}
	if c.itemSize == 0 {
		c.items = [][]byte{append([]byte(nil), buf...)}
		return nil
	}
	count := len(buf) / c.itemSize
	c.items = make([][]byte, count)
	for i := range count {
		c.items[i] = append([]byte(nil), buf[i*c.itemSize:(i+1)*c.itemSize]...)
	}
	return nil
}

func (c *ConcatBytesListSSZ) EncodingSizeSSZ() int {
	size := 0
	for _, item := range c.items {
		size += len(item)
	}
	return size
}

func (c *ConcatBytesListSSZ) Static() bool { return false }
func (c *ConcatBytesListSSZ) Clone() clonable.Clonable {
	return &ConcatBytesListSSZ{itemSize: c.itemSize}
}

// ---------------------------------------------------------------
// ExchangeCapabilities SSZ
// ---------------------------------------------------------------

type stringSSZ struct {
	data string
}

func (s *stringSSZ) EncodeSSZ(buf []byte) ([]byte, error) {
	return append(buf, s.data...), nil
}

func (s *stringSSZ) DecodeSSZ(buf []byte, _ int) error {
	s.data = string(buf)
	return nil
}

func (s *stringSSZ) EncodingSizeSSZ() int {
	return len(s.data)
}

func (s *stringSSZ) Static() bool             { return false }
func (s *stringSSZ) Clone() clonable.Clonable { return &stringSSZ{} }
func (s *stringSSZ) HashSSZ() ([32]byte, error) {
	return merkle_tree.HashTreeRoot([]byte(s.data))
}

// Convenience wrappers (backward-compatible API).
func EncodeCapabilities(capabilities []string) []byte {
	caps := make([]*stringSSZ, len(capabilities))
	for i, cap := range capabilities {
		caps[i] = &stringSSZ{data: cap}
	}
	list := solid.NewDynamicListSSZFromList[*stringSSZ](caps, 128)
	buf, _ := ssz2.MarshalSSZ(nil, list)
	return buf
}

func DecodeCapabilities(buf []byte) ([]string, error) {
	list := solid.NewDynamicListSSZ[*stringSSZ](128)
	if err := ssz2.UnmarshalSSZ(buf, 0, list); err != nil {
		return nil, err
	}
	capabilities := make([]string, 0, list.Len())
	list.Range(func(_ int, value *stringSSZ, _ int) bool {
		capabilities = append(capabilities, value.data)
		return true
	})
	return capabilities, nil
}

func (cv *ClientVersionV1) EncodeSSZ(buf []byte) (dst []byte, err error) {
	commit := clientVersionCommitBytes(cv.Commit)
	return ssz2.MarshalSSZ(buf,
		&ByteListSSZ{data: []byte(cv.Code)},
		&ByteListSSZ{data: []byte(cv.Name)},
		&ByteListSSZ{data: []byte(cv.Version)},
		commit[:],
	)
}

func (cv *ClientVersionV1) DecodeSSZ(buf []byte, _ int) error {
	code := &ByteListSSZ{}
	name := &ByteListSSZ{}
	version := &ByteListSSZ{}
	var commit [4]byte
	if err := ssz2.UnmarshalSSZ(buf, 0, code, name, version, commit[:]); err != nil {
		return fmt.Errorf("ClientVersion: %w", err)
	}
	cv.Code = string(code.data)
	cv.Name = string(name.data)
	cv.Version = string(version.data)
	cv.Commit = hexutil.Encode(commit[:])
	return nil
}

func (cv *ClientVersionV1) EncodingSizeSSZ() int {
	return 16 + len(cv.Code) + len(cv.Name) + len(cv.Version)
}
func (cv *ClientVersionV1) Static() bool             { return false }
func (cv *ClientVersionV1) Clone() clonable.Clonable { return &ClientVersionV1{} }
func (cv *ClientVersionV1) HashSSZ() ([32]byte, error) {
	commit := clientVersionCommitBytes(cv.Commit)
	return merkle_tree.HashTreeRoot([]byte(cv.Code), []byte(cv.Name), []byte(cv.Version), commit[:])
}

func clientVersionCommitBytes(commit string) [4]byte {
	var out [4]byte
	if commitRaw, err := hexutil.Decode(commit); err == nil {
		copy(out[:], commitRaw)
	}
	return out
}

func EncodeClientVersion(cv *ClientVersionV1) []byte {
	buf, _ := cv.EncodeSSZ(nil)
	return buf
}

func DecodeClientVersion(buf []byte) (*ClientVersionV1, error) {
	cv := &ClientVersionV1{}
	if err := cv.DecodeSSZ(buf, 0); err != nil {
		return nil, err
	}
	return cv, nil
}

func EncodeClientVersions(versions []ClientVersionV1) []byte {
	items := make([]*ClientVersionV1, len(versions))
	for i := range versions {
		items[i] = &versions[i]
	}
	l := solid.NewDynamicListSSZFromList[*ClientVersionV1](items, 16)
	buf, _ := ssz2.MarshalSSZ(nil, l)
	return buf
}

func DecodeClientVersions(buf []byte) ([]ClientVersionV1, error) {
	l := solid.NewDynamicListSSZ[*ClientVersionV1](16)
	if err := ssz2.UnmarshalSSZ(buf, 0, l); err != nil {
		return nil, err
	}
	result := make([]ClientVersionV1, 0, l.Len())
	l.Range(func(_ int, value *ClientVersionV1, _ int) bool {
		result = append(result, *value)
		return true
	})
	return result, nil
}

func EncodeGetBlobsRequest(hashes []common.Hash) []byte {
	versionedHashes := solid.NewHashList(max(len(hashes), 4096))
	for _, hash := range hashes {
		versionedHashes.Append(hash)
	}
	buf, _ := ssz2.MarshalSSZ(nil, versionedHashes)
	return buf
}

func DecodeGetBlobsRequest(buf []byte) ([]common.Hash, error) {
	versionedHashes := solid.NewHashList(4096)
	if err := ssz2.UnmarshalSSZ(buf, 0, versionedHashes); err != nil {
		return nil, err
	}
	return hashListToSlice(versionedHashes), nil
}

const (
	blobAndProofV1BlobSize  = 131072
	blobAndProofV1ProofSize = 48
	blobAndProofV1Size      = blobAndProofV1BlobSize + blobAndProofV1ProofSize
)

func (b *BlobAndProofV1) EncodeSSZ(buf []byte) ([]byte, error) {
	start := len(buf)
	buf = append(buf, make([]byte, blobAndProofV1Size)...)
	if b == nil {
		return buf, nil
	}
	copy(buf[start:start+blobAndProofV1BlobSize], b.Blob)
	copy(buf[start+blobAndProofV1BlobSize:start+blobAndProofV1Size], b.Proof)
	return buf, nil
}

func (b *BlobAndProofV1) DecodeSSZ(buf []byte, version int) error {
	blob := make([]byte, blobAndProofV1BlobSize)
	proof := make([]byte, blobAndProofV1ProofSize)
	if err := ssz2.UnmarshalSSZ(buf, version, blob, proof); err != nil {
		return fmt.Errorf("BlobAndProofV1: %w", err)
	}
	b.Blob = blob
	b.Proof = proof
	return nil
}

func (b *BlobAndProofV1) EncodingSizeSSZ() int     { return blobAndProofV1Size }
func (b *BlobAndProofV1) Static() bool             { return true }
func (b *BlobAndProofV1) Clone() clonable.Clonable { return &BlobAndProofV1{} }
func (b *BlobAndProofV1) HashSSZ() ([32]byte, error) {
	if b == nil {
		return merkle_tree.HashTreeRoot(make([]byte, blobAndProofV1BlobSize), make([]byte, blobAndProofV1ProofSize))
	}
	return merkle_tree.HashTreeRoot(fixedBytes(b.Blob, blobAndProofV1BlobSize), fixedBytes(b.Proof, blobAndProofV1ProofSize))
}

func EncodeGetBlobsV1Response(blobs []*BlobAndProofV1) []byte {
	items := make([]*BlobAndProofV1, 0, len(blobs))
	for _, blob := range blobs {
		if blob != nil {
			items = append(items, blob)
		}
	}
	list := solid.NewStaticListSSZFromList[*BlobAndProofV1](items, 4096, blobAndProofV1Size)
	buf, _ := ssz2.MarshalSSZ(nil, list)
	return buf
}

func fixedBytes(src []byte, size int) []byte {
	dst := make([]byte, size)
	copy(dst, src)
	return dst
}

func hashListFromSlice(hashes []common.Hash) solid.HashListSSZ {
	versionedHashes := solid.NewHashList(max(len(hashes), 4096))
	for _, hash := range hashes {
		versionedHashes.Append(hash)
	}
	return versionedHashes
}

func hashListToSlice(versionedHashes solid.HashListSSZ) []common.Hash {
	hashes := make([]common.Hash, 0, versionedHashes.Length())
	versionedHashes.Range(func(_ int, hash common.Hash, _ int) bool {
		hashes = append(hashes, hash)
		return true
	})
	return hashes
}

// engineVersionToPayloadVersion maps Engine API versions to ExecutionPayload SSZ versions.
func engineVersionToPayloadVersion(engineVersion int) int {
	if engineVersion == 4 {
		return 3
	}
	if engineVersion >= 5 {
		return 4
	}
	return engineVersion
}

func (e *ExecutionPayload) EncodeSSZ(buf []byte) ([]byte, error) {
	version := e.sszVersion
	if version == 0 {
		version = executionPayloadVersionFromFields(e)
	}
	logsBloom := executionPayloadLogsBloom(e)
	extraData := solid.NewExtraData()
	extraData.SetBytes(e.ExtraData)
	baseFee := [32]byte{}
	if e.BaseFeePerGas != nil {
		baseFee = uint256ToSSZBytes(e.BaseFeePerGas.ToInt())
	}
	txs := make([][]byte, len(e.Transactions))
	for i, tx := range e.Transactions {
		txs[i] = []byte(tx)
	}
	blockNumber := uint64(e.BlockNumber)
	gasLimit := uint64(e.GasLimit)
	gasUsed := uint64(e.GasUsed)
	timestamp := uint64(e.Timestamp)
	schema := []any{
		e.ParentHash[:], e.FeeRecipient[:], e.StateRoot[:], e.ReceiptsRoot[:],
		logsBloom[:], e.PrevRandao[:],
		&blockNumber, &gasLimit, &gasUsed, &timestamp,
		extraData, baseFee[:], e.BlockHash[:], solid.NewTransactionsSSZFromTransactions(txs),
	}
	if version >= 2 {
		schema = append(schema, executionWithdrawalsToSolid(e.Withdrawals))
	}
	if version >= 3 {
		blobGasUsed, excessBlobGas := uint64(0), uint64(0)
		if e.BlobGasUsed != nil {
			blobGasUsed = uint64(*e.BlobGasUsed)
		}
		if e.ExcessBlobGas != nil {
			excessBlobGas = uint64(*e.ExcessBlobGas)
		}
		schema = append(schema, &blobGasUsed, &excessBlobGas)
	}
	if version >= 4 {
		slotNumber := uint64(0)
		if e.SlotNumber != nil {
			slotNumber = uint64(*e.SlotNumber)
		}
		schema = append(schema, &slotNumber, &ByteListSSZ{data: []byte(e.BlockAccessList)})
	}
	return ssz2.MarshalSSZ(buf, schema...)
}

func (e *ExecutionPayload) DecodeSSZ(buf []byte, version int) error {
	payloadVersion := engineVersionToPayloadVersion(version)
	var (
		logsBloom     [256]byte
		blockNumber   uint64
		gasLimit      uint64
		gasUsed       uint64
		timestamp     uint64
		baseFee       [32]byte
		extraData     = solid.NewExtraData()
		transactions  = &solid.TransactionsSSZ{}
		withdrawals   = solid.NewStaticListSSZ[*cltypes.Withdrawal](1_048_576, (&cltypes.Withdrawal{}).EncodingSizeSSZ())
		blobGasUsed   uint64
		excessBlobGas uint64
		slotNumber    uint64
		blockAccess   = &ByteListSSZ{}
	)
	schema := []any{
		e.ParentHash[:], e.FeeRecipient[:], e.StateRoot[:], e.ReceiptsRoot[:],
		logsBloom[:], e.PrevRandao[:],
		&blockNumber, &gasLimit, &gasUsed, &timestamp,
		extraData, baseFee[:], e.BlockHash[:], transactions,
	}
	if payloadVersion >= 2 {
		schema = append(schema, withdrawals)
	}
	if payloadVersion >= 3 {
		schema = append(schema, &blobGasUsed, &excessBlobGas)
	}
	if payloadVersion >= 4 {
		schema = append(schema, &slotNumber, blockAccess)
	}
	if err := ssz2.UnmarshalSSZ(buf, payloadVersion, schema...); err != nil {
		return err
	}
	e.sszVersion = payloadVersion
	e.LogsBloom = make(hexutil.Bytes, 256)
	copy(e.LogsBloom, logsBloom[:])
	e.BlockNumber = hexutil.Uint64(blockNumber)
	e.GasLimit = hexutil.Uint64(gasLimit)
	e.GasUsed = hexutil.Uint64(gasUsed)
	e.Timestamp = hexutil.Uint64(timestamp)
	e.ExtraData = extraData.Bytes()
	baseFeeInt := sszBytesToUint256(baseFee[:])
	e.BaseFeePerGas = (*hexutil.Big)(baseFeeInt)
	e.Transactions = transactionsToHex(transactions)
	if payloadVersion >= 2 {
		e.Withdrawals = solidWithdrawalsToExecution(withdrawals)
	}
	if payloadVersion >= 3 {
		bgu := hexutil.Uint64(blobGasUsed)
		e.BlobGasUsed = &bgu
		ebg := hexutil.Uint64(excessBlobGas)
		e.ExcessBlobGas = &ebg
	}
	if payloadVersion >= 4 {
		sn := hexutil.Uint64(slotNumber)
		e.SlotNumber = &sn
		e.BlockAccessList = make(hexutil.Bytes, len(blockAccess.data))
		copy(e.BlockAccessList, blockAccess.data)
	}
	return nil
}

func (e *ExecutionPayload) EncodingSizeSSZ() int {
	version := e.sszVersion
	if version == 0 {
		version = executionPayloadVersionFromFields(e)
	}
	size := 508 // fixed part for v1 (includes ExtraData and Transactions offset slots)
	size += len(e.ExtraData)
	for _, tx := range e.Transactions {
		size += len(tx) + 4
	}
	if version >= 2 {
		size += 4 + len(e.Withdrawals)*(&cltypes.Withdrawal{}).EncodingSizeSSZ()
	}
	if version >= 3 {
		size += 16
	}
	if version >= 4 {
		size += 12 + len(e.BlockAccessList)
	}
	return size
}

func (e *ExecutionPayload) Static() bool             { return false }
func (e *ExecutionPayload) Clone() clonable.Clonable { return &ExecutionPayload{} }

func executionPayloadVersionFromFields(ep *ExecutionPayload) int {
	if ep.SlotNumber != nil || ep.BlockAccessList != nil {
		return 4
	}
	if ep.BlobGasUsed != nil || ep.ExcessBlobGas != nil {
		return 3
	}
	if ep.Withdrawals != nil {
		return 2
	}
	return 1
}

// uint256ToSSZBytes converts a big.Int to 32-byte little-endian SSZ representation.
func uint256ToSSZBytes(val *big.Int) [32]byte {
	var buf [32]byte
	if val == nil {
		return buf
	}
	b := val.Bytes()
	for i, v := range b {
		buf[len(b)-1-i] = v
	}
	return buf
}

// sszBytesToUint256 converts 32-byte little-endian SSZ bytes to a big.Int.
func sszBytesToUint256(buf []byte) *big.Int {
	be := make([]byte, 32)
	for i := 0; i < 32; i++ {
		be[31-i] = buf[i]
	}
	return new(big.Int).SetBytes(be)
}

func executionPayloadLogsBloom(ep *ExecutionPayload) [256]byte {
	var logsBloom [256]byte
	if len(ep.LogsBloom) >= 256 {
		copy(logsBloom[:], ep.LogsBloom[:256])
	}
	return logsBloom
}

func executionWithdrawalsToSolid(withdrawals []*types.Withdrawal) *solid.ListSSZ[*cltypes.Withdrawal] {
	wds := make([]*cltypes.Withdrawal, len(withdrawals))
	for i, w := range withdrawals {
		wds[i] = &cltypes.Withdrawal{Index: w.Index, Validator: w.Validator, Address: w.Address, Amount: w.Amount}
	}
	return solid.NewStaticListSSZFromList[*cltypes.Withdrawal](wds, 1_048_576, (&cltypes.Withdrawal{}).EncodingSizeSSZ())
}

func solidWithdrawalsToExecution(withdrawals *solid.ListSSZ[*cltypes.Withdrawal]) []*types.Withdrawal {
	out := make([]*types.Withdrawal, 0, withdrawals.Len())
	withdrawals.Range(func(_ int, w *cltypes.Withdrawal, _ int) bool {
		out = append(out, &types.Withdrawal{Index: w.Index, Validator: w.Validator, Address: w.Address, Amount: w.Amount})
		return true
	})
	return out
}

func transactionsToHex(transactions *solid.TransactionsSSZ) []hexutil.Bytes {
	txs := transactions.UnderlyngReference()
	if len(txs) == 0 {
		return []hexutil.Bytes{}
	}
	out := make([]hexutil.Bytes, len(txs))
	for i, tx := range txs {
		out[i] = make(hexutil.Bytes, len(tx))
		copy(out[i], tx)
	}
	return out
}

// Convenience wrappers (backward-compatible API).
func EncodeExecutionPayloadSSZ(ep *ExecutionPayload, version int) []byte {
	ep.sszVersion = version
	buf, _ := ep.EncodeSSZ(nil)
	return buf
}

func DecodeExecutionPayloadSSZ(buf []byte, version int) (*ExecutionPayload, error) {
	ep := &ExecutionPayload{sszVersion: version}
	if err := ep.DecodeSSZ(buf, version); err != nil {
		return nil, fmt.Errorf("ExecutionPayload SSZ: %w", err)
	}
	return ep, nil
}

// ---------------------------------------------------------------
// StructuredExecutionRequests SSZ
// ---------------------------------------------------------------

// StructuredRequestsSSZ is the SSZ container for execution requests
// (deposits, withdrawals, consolidations) as 3 dynamic byte fields.
type StructuredRequestsSSZ struct {
	Deposits       *ByteListSSZ
	Withdrawals    *ByteListSSZ
	Consolidations *ByteListSSZ
}

func (r *StructuredRequestsSSZ) EncodeSSZ(buf []byte) ([]byte, error) {
	return ssz2.MarshalSSZ(buf, r.Deposits, r.Withdrawals, r.Consolidations)
}

func (r *StructuredRequestsSSZ) DecodeSSZ(buf []byte, version int) error {
	if r.Deposits == nil {
		r.Deposits = &ByteListSSZ{}
	}
	if r.Withdrawals == nil {
		r.Withdrawals = &ByteListSSZ{}
	}
	if r.Consolidations == nil {
		r.Consolidations = &ByteListSSZ{}
	}
	return ssz2.UnmarshalSSZ(buf, version, r.Deposits, r.Withdrawals, r.Consolidations)
}

func (r *StructuredRequestsSSZ) EncodingSizeSSZ() int {
	return 12 + r.Deposits.EncodingSizeSSZ() + r.Withdrawals.EncodingSizeSSZ() + r.Consolidations.EncodingSizeSSZ()
}

func (r *StructuredRequestsSSZ) Static() bool             { return false }
func (r *StructuredRequestsSSZ) Clone() clonable.Clonable { return &StructuredRequestsSSZ{} }

func structuredRequestsFromSlice(reqs []hexutil.Bytes) *StructuredRequestsSSZ {
	s := &StructuredRequestsSSZ{
		Deposits: &ByteListSSZ{}, Withdrawals: &ByteListSSZ{}, Consolidations: &ByteListSSZ{},
	}
	for _, r := range reqs {
		if len(r) < 1 {
			continue
		}
		switch r[0] {
		case 0x00:
			s.Deposits.data = append(s.Deposits.data, r[1:]...)
		case 0x01:
			s.Withdrawals.data = append(s.Withdrawals.data, r[1:]...)
		case 0x02:
			s.Consolidations.data = append(s.Consolidations.data, r[1:]...)
		}
	}
	return s
}

func (r *StructuredRequestsSSZ) toSlice() []hexutil.Bytes {
	reqs := make([]hexutil.Bytes, 0, 3)
	if len(r.Deposits.data) > 0 {
		req := make(hexutil.Bytes, 1+len(r.Deposits.data))
		req[0] = 0x00
		copy(req[1:], r.Deposits.data)
		reqs = append(reqs, req)
	}
	if len(r.Withdrawals.data) > 0 {
		req := make(hexutil.Bytes, 1+len(r.Withdrawals.data))
		req[0] = 0x01
		copy(req[1:], r.Withdrawals.data)
		reqs = append(reqs, req)
	}
	if len(r.Consolidations.data) > 0 {
		req := make(hexutil.Bytes, 1+len(r.Consolidations.data))
		req[0] = 0x02
		copy(req[1:], r.Consolidations.data)
		reqs = append(reqs, req)
	}
	return reqs
}

func (n *NewPayloadRequest) getSchema() []any {
	hashes := hashListFromSlice(n.BlobVersionedHashes)
	if n.sszVersion == 3 {
		return []any{n.Payload, hashes, n.ParentBeaconBlockRoot[:]}
	}
	// V4+
	return []any{n.Payload, hashes, n.ParentBeaconBlockRoot[:], structuredRequestsFromSlice(n.ExecutionRequests)}
}

func (n *NewPayloadRequest) EncodeSSZ(buf []byte) ([]byte, error) {
	if n.sszVersion <= 2 {
		return n.Payload.EncodeSSZ(buf)
	}
	return ssz2.MarshalSSZ(buf, n.getSchema()...)
}

func (n *NewPayloadRequest) DecodeSSZ(buf []byte, version int) error {
	n.sszVersion = version
	payloadVersion := engineVersionToPayloadVersion(version)
	if n.Payload == nil {
		n.Payload = &ExecutionPayload{sszVersion: payloadVersion}
	}
	n.Payload.sszVersion = payloadVersion
	if version <= 2 {
		return n.Payload.DecodeSSZ(buf, payloadVersion)
	}
	hashes := solid.NewHashList(4096)
	executionRequests := &StructuredRequestsSSZ{
		Deposits: &ByteListSSZ{}, Withdrawals: &ByteListSSZ{}, Consolidations: &ByteListSSZ{},
	}
	schema := []any{n.Payload, hashes, n.ParentBeaconBlockRoot[:]}
	if version >= 4 {
		schema = append(schema, executionRequests)
	}
	if err := ssz2.UnmarshalSSZ(buf, version, schema...); err != nil {
		return err
	}
	n.BlobVersionedHashes = hashListToSlice(hashes)
	if version >= 4 {
		n.ExecutionRequests = executionRequests.toSlice()
	}
	return nil
}

func (n *NewPayloadRequest) EncodingSizeSSZ() int {
	if n.sszVersion <= 2 {
		return n.Payload.EncodingSizeSSZ()
	}
	size := 4 + 4 + 32 // payload offset + hashes offset + parent root
	size += n.Payload.EncodingSizeSSZ()
	size += len(n.BlobVersionedHashes) * 32
	if n.sszVersion >= 4 {
		size += 4 // requests offset
		size += structuredRequestsFromSlice(n.ExecutionRequests).EncodingSizeSSZ()
	}
	return size
}

func (n *NewPayloadRequest) Static() bool             { return false }
func (n *NewPayloadRequest) Clone() clonable.Clonable { return &NewPayloadRequest{} }

// Convenience wrappers (backward-compatible API).
func EncodeNewPayloadRequest(
	ep *ExecutionPayload,
	blobHashes []common.Hash,
	parentBeaconBlockRoot *common.Hash,
	executionRequests []hexutil.Bytes,
	version int,
) []byte {
	payloadVersion := engineVersionToPayloadVersion(version)
	n := &NewPayloadRequest{
		Payload:               ep,
		BlobVersionedHashes:   blobHashes,
		ExecutionRequests:     executionRequests,
		ParentBeaconBlockRoot: common.Hash{},
		sszVersion:            version,
	}
	n.Payload.sszVersion = payloadVersion
	if parentBeaconBlockRoot != nil {
		n.ParentBeaconBlockRoot = *parentBeaconBlockRoot
	}
	buf, _ := n.EncodeSSZ(nil)
	return buf
}

func DecodeNewPayloadRequest(buf []byte, version int) (
	ep *ExecutionPayload,
	blobHashes []common.Hash,
	parentBeaconBlockRoot *common.Hash,
	executionRequests []hexutil.Bytes,
	err error,
) {
	n := &NewPayloadRequest{sszVersion: version}
	if err = n.DecodeSSZ(buf, version); err != nil {
		return
	}
	ep = n.Payload
	if version >= 3 {
		blobHashes = n.BlobVersionedHashes
		root := n.ParentBeaconBlockRoot
		parentBeaconBlockRoot = &root
	}
	if version >= 4 {
		executionRequests = n.ExecutionRequests
	}
	return
}

func (b *BlobsBundle) EncodeSSZ(buf []byte) ([]byte, error) {
	commitments, proofs, blobs := b.sszLists()
	return ssz2.MarshalSSZ(buf, commitments, proofs, blobs)
}

func (b *BlobsBundle) DecodeSSZ(buf []byte, version int) error {
	commitments := &ConcatBytesListSSZ{itemSize: 48}
	proofs := &ConcatBytesListSSZ{itemSize: 48}
	blobs := &ConcatBytesListSSZ{itemSize: 131072}
	if err := ssz2.UnmarshalSSZ(buf, version, commitments, proofs, blobs); err != nil {
		return err
	}
	b.Commitments = bytesToHex(commitments.items)
	b.Proofs = bytesToHex(proofs.items)
	b.Blobs = bytesToHex(blobs.items)
	return nil
}

func (b *BlobsBundle) EncodingSizeSSZ() int {
	commitments, proofs, blobs := b.sszLists()
	return 12 + commitments.EncodingSizeSSZ() + proofs.EncodingSizeSSZ() + blobs.EncodingSizeSSZ()
}

func (b *BlobsBundle) Static() bool             { return false }
func (b *BlobsBundle) Clone() clonable.Clonable { return &BlobsBundle{} }

func (b *BlobsBundle) sszLists() (*ConcatBytesListSSZ, *ConcatBytesListSSZ, *ConcatBytesListSSZ) {
	if b == nil {
		return &ConcatBytesListSSZ{itemSize: 48}, &ConcatBytesListSSZ{itemSize: 48}, &ConcatBytesListSSZ{itemSize: 131072}
	}
	toBytes := func(items []hexutil.Bytes) [][]byte {
		result := make([][]byte, len(items))
		for i, item := range items {
			result[i] = []byte(item)
		}
		return result
	}
	return &ConcatBytesListSSZ{items: toBytes(b.Commitments), itemSize: 48},
		&ConcatBytesListSSZ{items: toBytes(b.Proofs), itemSize: 48},
		&ConcatBytesListSSZ{items: toBytes(b.Blobs), itemSize: 131072}
}

func bytesToHex(items [][]byte) []hexutil.Bytes {
	result := make([]hexutil.Bytes, len(items))
	for i, item := range items {
		result[i] = make(hexutil.Bytes, len(item))
		copy(result[i], item)
	}
	return result
}

// ---------------------------------------------------------------
// GetPayload response SSZ
// ---------------------------------------------------------------

// GetPayloadResponse uses the JSON-RPC response type as the SSZ container.
func (g *GetPayloadResponse) EncodeSSZ(buf []byte) ([]byte, error) {
	version := g.sszVersion
	if version == 0 {
		version = 1
	}
	payloadVersion := engineVersionToPayloadVersion(version)
	g.ExecutionPayload.sszVersion = payloadVersion
	if version == 1 {
		return g.ExecutionPayload.EncodeSSZ(buf)
	}
	var overrideByte byte
	if g.ShouldOverrideBuilder {
		overrideByte = 1
	}
	var blockValue [32]byte
	if g.BlockValue != nil {
		blockValue = uint256ToSSZBytes(g.BlockValue.ToInt())
	}
	return ssz2.MarshalSSZ(buf,
		g.ExecutionPayload, blockValue[:], g.BlobsBundle, []byte{overrideByte}, structuredRequestsFromSlice(g.ExecutionRequests),
	)
}

func (g *GetPayloadResponse) DecodeSSZ(buf []byte, version int) error {
	g.sszVersion = version
	payloadVersion := engineVersionToPayloadVersion(version)
	if g.ExecutionPayload == nil {
		g.ExecutionPayload = &ExecutionPayload{sszVersion: payloadVersion}
	}
	g.ExecutionPayload.sszVersion = payloadVersion
	if version == 1 {
		return g.ExecutionPayload.DecodeSSZ(buf, payloadVersion)
	}
	blobsBundle := &BlobsBundle{}
	executionRequests := &StructuredRequestsSSZ{
		Deposits: &ByteListSSZ{}, Withdrawals: &ByteListSSZ{}, Consolidations: &ByteListSSZ{},
	}
	var blockValue [32]byte
	overrideByte := []byte{0}
	if err := ssz2.UnmarshalSSZ(buf, version, g.ExecutionPayload, blockValue[:], blobsBundle, overrideByte, executionRequests); err != nil {
		return fmt.Errorf("GetPayloadResponse SSZ: %w", err)
	}
	blockValueInt := sszBytesToUint256(blockValue[:])
	g.BlockValue = (*hexutil.Big)(blockValueInt)
	g.ShouldOverrideBuilder = overrideByte[0] != 0
	g.BlobsBundle = blobsBundle
	g.ExecutionRequests = executionRequests.toSlice()
	return nil
}

func (g *GetPayloadResponse) EncodingSizeSSZ() int {
	version := g.sszVersion
	if version == 0 {
		version = 1
	}
	payloadVersion := engineVersionToPayloadVersion(version)
	g.ExecutionPayload.sszVersion = payloadVersion
	if version == 1 {
		return g.ExecutionPayload.EncodingSizeSSZ()
	}
	return 45 + g.ExecutionPayload.EncodingSizeSSZ() + g.BlobsBundle.EncodingSizeSSZ() + structuredRequestsFromSlice(g.ExecutionRequests).EncodingSizeSSZ()
}

func (g *GetPayloadResponse) Static() bool             { return false }
func (g *GetPayloadResponse) Clone() clonable.Clonable { return &GetPayloadResponse{} }

// Convenience wrappers (backward-compatible API).
func EncodeGetPayloadResponseSSZ(resp *GetPayloadResponse, version int) []byte {
	resp.sszVersion = version
	buf, _ := resp.EncodeSSZ(nil)
	return buf
}

func DecodeGetPayloadResponseSSZ(buf []byte, version int) (*GetPayloadResponse, error) {
	resp := &GetPayloadResponse{sszVersion: version}
	if err := resp.DecodeSSZ(buf, version); err != nil {
		return nil, err
	}
	return resp, nil
}
