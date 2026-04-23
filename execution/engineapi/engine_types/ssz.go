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
	commonssz "github.com/erigontech/erigon/common/ssz"
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
	dst = buf
	var hashData []byte
	if p.LatestValidHash != nil {
		hashData = p.LatestValidHash[:]
	}
	var errBytes []byte
	if p.ValidationError != nil && p.ValidationError.Error() != nil {
		errBytes = []byte(p.ValidationError.Error().Error())
	}

	dst = append(dst, EngineStatusToSSZ(p.Status))
	dst = append(dst, commonssz.OffsetSSZ(uint32(payloadStatusFixedSize))...)
	dst = append(dst, commonssz.OffsetSSZ(uint32(payloadStatusFixedSize+len(hashData)))...)
	dst = append(dst, hashData...)
	dst = append(dst, errBytes...)
	return dst, nil
}

func (p *PayloadStatus) DecodeSSZ(buf []byte, _ int) error {
	if len(buf) < payloadStatusFixedSize {
		return fmt.Errorf("PayloadStatus: %w (need %d, got %d)", commonssz.ErrLowBufferSize, payloadStatusFixedSize, len(buf))
	}
	p.Status = SSZToEngineStatus(buf[0])
	hashOffset := commonssz.DecodeOffset(buf[1:])
	errOffset := commonssz.DecodeOffset(buf[5:])

	if hashOffset > uint32(len(buf)) || errOffset > uint32(len(buf)) || hashOffset > errOffset {
		return fmt.Errorf("PayloadStatus: %w", commonssz.ErrBadOffset)
	}

	hashData := buf[hashOffset:errOffset]
	switch len(hashData) {
	case 32:
		hash := common.BytesToHash(hashData)
		p.LatestValidHash = &hash
	case 0:
		p.LatestValidHash = nil
	default:
		return fmt.Errorf("PayloadStatus: invalid hash list length %d", len(hashData))
	}

	errData := buf[errOffset:]
	if len(errData) > 1024 {
		return fmt.Errorf("PayloadStatus: validation error too long (%d > 1024)", len(errData))
	}
	if len(errData) > 0 {
		p.ValidationError = NewStringifiedErrorFromString(string(errData))
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

func DecodePayloadStatusSSZ(buf []byte) (*PayloadStatus, error) {
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

const forkchoiceUpdatedResponseFixedSize = 8

func (r *ForkChoiceUpdatedResponse) EncodeSSZ(buf []byte) (dst []byte, err error) {
	dst = buf
	psBytes, err := r.PayloadStatus.EncodeSSZ(nil)
	if err != nil {
		return nil, err
	}
	var payloadID []byte
	if r.PayloadId != nil {
		payloadID = []byte(*r.PayloadId)
	}

	dst = append(dst, commonssz.OffsetSSZ(uint32(forkchoiceUpdatedResponseFixedSize))...)
	dst = append(dst, commonssz.OffsetSSZ(uint32(forkchoiceUpdatedResponseFixedSize+len(psBytes)))...)
	dst = append(dst, psBytes...)
	dst = append(dst, payloadID...)
	return dst, nil
}

func (r *ForkChoiceUpdatedResponse) DecodeSSZ(buf []byte, _ int) error {
	if len(buf) < forkchoiceUpdatedResponseFixedSize {
		return fmt.Errorf("ForkChoiceUpdatedResponse: %w", commonssz.ErrLowBufferSize)
	}
	psOffset := commonssz.DecodeOffset(buf[0:])
	pidOffset := commonssz.DecodeOffset(buf[4:])

	if psOffset > uint32(len(buf)) || pidOffset > uint32(len(buf)) || psOffset > pidOffset {
		return fmt.Errorf("ForkChoiceUpdatedResponse: %w", commonssz.ErrBadOffset)
	}

	r.PayloadStatus = &PayloadStatus{}
	if err := r.PayloadStatus.DecodeSSZ(buf[psOffset:pidOffset], 0); err != nil {
		return err
	}

	pidData := buf[pidOffset:]
	if len(pidData) == 8 {
		payloadID := make(hexutil.Bytes, 8)
		copy(payloadID, pidData)
		r.PayloadId = &payloadID
	} else if len(pidData) == 0 {
		r.PayloadId = nil
	} else {
		return fmt.Errorf("ForkChoiceUpdatedResponse: invalid payload ID length %d", len(pidData))
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

// CapabilitiesSSZ is the SSZ container for ExchangeCapabilities requests/responses.
type CapabilitiesSSZ struct {
	Capabilities []string
}

func (c *CapabilitiesSSZ) EncodeSSZ(buf []byte) (dst []byte, err error) {
	dst = buf
	// Container: offset(4) → list data
	// List data: N item offsets(4 each) + concatenated UTF-8 strings
	n := len(c.Capabilities)
	offsetsSize := n * 4
	totalStrBytes := 0
	for _, cap := range c.Capabilities {
		totalStrBytes += len(cap)
	}

	dst = append(dst, commonssz.OffsetSSZ(4)...)
	itemOffset := uint32(offsetsSize)
	for _, cap := range c.Capabilities {
		dst = append(dst, commonssz.OffsetSSZ(itemOffset)...)
		itemOffset += uint32(len(cap))
	}
	for _, cap := range c.Capabilities {
		dst = append(dst, []byte(cap)...)
	}
	return dst, nil
}

func (c *CapabilitiesSSZ) DecodeSSZ(buf []byte, _ int) error {
	if len(buf) < 4 {
		return fmt.Errorf("Capabilities: buffer too short")
	}
	listOffset := commonssz.DecodeOffset(buf[0:])
	if listOffset > uint32(len(buf)) {
		return fmt.Errorf("Capabilities: list offset out of bounds")
	}
	listData := buf[listOffset:]
	if len(listData) == 0 {
		c.Capabilities = []string{}
		return nil
	}
	if len(listData) < 4 {
		return fmt.Errorf("Capabilities: list data too short")
	}
	firstOffset := commonssz.DecodeOffset(listData[0:])
	if firstOffset%4 != 0 || firstOffset == 0 {
		return fmt.Errorf("Capabilities: invalid first offset %d", firstOffset)
	}
	count := firstOffset / 4
	if count > 128 {
		return fmt.Errorf("Capabilities: too many capabilities (%d > 128)", count)
	}
	if uint32(len(listData)) < count*4 {
		return fmt.Errorf("Capabilities: truncated offset table")
	}
	offsets := make([]uint32, count)
	for i := uint32(0); i < count; i++ {
		offsets[i] = commonssz.DecodeOffset(listData[i*4:])
	}
	c.Capabilities = make([]string, count)
	for i := uint32(0); i < count; i++ {
		start := offsets[i]
		end := uint32(len(listData))
		if i+1 < count {
			end = offsets[i+1]
		}
		if start > uint32(len(listData)) || end > uint32(len(listData)) || start > end {
			return fmt.Errorf("Capabilities: offset out of bounds")
		}
		if end-start > 64 {
			return fmt.Errorf("Capabilities: capability too long (%d > 64)", end-start)
		}
		c.Capabilities[i] = string(listData[start:end])
	}
	return nil
}

func (c *CapabilitiesSSZ) EncodingSizeSSZ() int {
	size := 4 // container offset
	size += len(c.Capabilities) * 4
	for _, cap := range c.Capabilities {
		size += len(cap)
	}
	return size
}

func (c *CapabilitiesSSZ) Static() bool             { return false }
func (c *CapabilitiesSSZ) Clone() clonable.Clonable { return &CapabilitiesSSZ{} }

// Convenience wrappers (backward-compatible API).
func EncodeCapabilities(capabilities []string) []byte {
	c := &CapabilitiesSSZ{Capabilities: capabilities}
	buf, _ := c.EncodeSSZ(nil)
	return buf
}

func DecodeCapabilities(buf []byte) ([]string, error) {
	c := &CapabilitiesSSZ{}
	if err := c.DecodeSSZ(buf, 0); err != nil {
		return nil, err
	}
	return c.Capabilities, nil
}

func (cv *ClientVersionV1) EncodeSSZ(buf []byte) (dst []byte, err error) {
	dst = buf
	const fixedSize = 16
	code := []byte(cv.Code)
	name := []byte(cv.Name)
	version := []byte(cv.Version)
	commit := clientVersionCommitBytes(cv.Commit)
	nameOff := uint32(fixedSize + len(code))
	versionOff := nameOff + uint32(len(name))
	dst = append(dst, commonssz.OffsetSSZ(uint32(fixedSize))...)
	dst = append(dst, commonssz.OffsetSSZ(nameOff)...)
	dst = append(dst, commonssz.OffsetSSZ(versionOff)...)
	dst = append(dst, commit[:]...)
	dst = append(dst, code...)
	dst = append(dst, name...)
	dst = append(dst, version...)
	return dst, nil
}

func (cv *ClientVersionV1) DecodeSSZ(buf []byte, _ int) error {
	const fixedSize = 16
	if len(buf) < fixedSize {
		return fmt.Errorf("ClientVersion: buffer too short (%d < %d)", len(buf), fixedSize)
	}
	codeOff := commonssz.DecodeOffset(buf[0:])
	nameOff := commonssz.DecodeOffset(buf[4:])
	versionOff := commonssz.DecodeOffset(buf[8:])
	var commit [4]byte
	copy(commit[:], buf[12:16])
	bufLen := uint32(len(buf))
	if codeOff > bufLen || nameOff > bufLen || versionOff > bufLen || codeOff > nameOff || nameOff > versionOff {
		return fmt.Errorf("ClientVersion: invalid offsets")
	}
	cv.Code = string(buf[codeOff:nameOff])
	cv.Name = string(buf[nameOff:versionOff])
	cv.Version = string(buf[versionOff:])
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

// ---------------------------------------------------------------
// GetBlobs request SSZ
// ---------------------------------------------------------------

// GetBlobsRequestSSZ is the SSZ container for GetBlobs requests.
type GetBlobsRequestSSZ struct {
	VersionedHashes solid.HashListSSZ
}

func (g *GetBlobsRequestSSZ) EncodeSSZ(buf []byte) ([]byte, error) {
	return ssz2.MarshalSSZ(buf, g.VersionedHashes)
}

func (g *GetBlobsRequestSSZ) DecodeSSZ(buf []byte, version int) error {
	if g.VersionedHashes == nil {
		g.VersionedHashes = solid.NewHashList(4096)
	}
	return ssz2.UnmarshalSSZ(buf, version, g.VersionedHashes)
}

func (g *GetBlobsRequestSSZ) EncodingSizeSSZ() int     { return 4 + g.VersionedHashes.EncodingSizeSSZ() }
func (g *GetBlobsRequestSSZ) Static() bool             { return false }
func (g *GetBlobsRequestSSZ) Clone() clonable.Clonable { return &GetBlobsRequestSSZ{} }

func EncodeGetBlobsRequest(hashes []common.Hash) []byte {
	versionedHashes := solid.NewHashList(max(len(hashes), 4096))
	for _, hash := range hashes {
		versionedHashes.Append(hash)
	}
	g := &GetBlobsRequestSSZ{VersionedHashes: versionedHashes}
	buf, _ := g.EncodeSSZ(nil)
	return buf
}

func DecodeGetBlobsRequest(buf []byte) ([]common.Hash, error) {
	g := &GetBlobsRequestSSZ{}
	if err := g.DecodeSSZ(buf, 0); err != nil {
		return nil, err
	}
	hashes := make([]common.Hash, 0, g.VersionedHashes.Length())
	g.VersionedHashes.Range(func(_ int, hash common.Hash, _ int) bool {
		hashes = append(hashes, hash)
		return true
	})
	return hashes, nil
}

// ---------------------------------------------------------------
// ExecutionPayload SSZ
// ---------------------------------------------------------------

// executionPayloadSSZ adapts the JSON-RPC ExecutionPayload to the CL SSZ schema.
type executionPayloadSSZ struct {
	ParentHash      common.Hash
	FeeRecipient    common.Address
	StateRoot       common.Hash
	ReceiptsRoot    common.Hash
	LogsBloom       [256]byte
	PrevRandao      common.Hash
	BlockNumber     uint64
	GasLimit        uint64
	GasUsed         uint64
	Timestamp       uint64
	ExtraData       *solid.ExtraData
	BaseFeePerGas   [32]byte // uint256 LE
	BlockHash       common.Hash
	Transactions    *solid.TransactionsSSZ
	Withdrawals     *solid.ListSSZ[*cltypes.Withdrawal] // v2+
	BlobGasUsed     uint64                              // v3+
	ExcessBlobGas   uint64                              // v3+
	SlotNumber      uint64                              // v4+
	BlockAccessList *ByteListSSZ                        // v4+
	version         int
}

func (e *executionPayloadSSZ) getSchema() []any {
	s := []any{
		e.ParentHash[:], e.FeeRecipient[:], e.StateRoot[:], e.ReceiptsRoot[:],
		e.LogsBloom[:], e.PrevRandao[:],
		&e.BlockNumber, &e.GasLimit, &e.GasUsed, &e.Timestamp,
		e.ExtraData, e.BaseFeePerGas[:], e.BlockHash[:], e.Transactions,
	}
	if e.version >= 2 {
		s = append(s, e.Withdrawals)
	}
	if e.version >= 3 {
		s = append(s, &e.BlobGasUsed, &e.ExcessBlobGas)
	}
	if e.version >= 4 {
		s = append(s, &e.SlotNumber, e.BlockAccessList)
	}
	return s
}

func (e *executionPayloadSSZ) EncodeSSZ(buf []byte) ([]byte, error) {
	return ssz2.MarshalSSZ(buf, e.getSchema()...)
}

func (e *executionPayloadSSZ) DecodeSSZ(buf []byte, version int) error {
	e.version = engineVersionToPayloadVersion(version)
	if e.ExtraData == nil {
		e.ExtraData = solid.NewExtraData()
	}
	if e.Transactions == nil {
		e.Transactions = &solid.TransactionsSSZ{}
	}
	if version >= 2 && e.Withdrawals == nil {
		e.Withdrawals = solid.NewStaticListSSZ[*cltypes.Withdrawal](1_048_576, (&cltypes.Withdrawal{}).EncodingSizeSSZ())
	}
	if version >= 4 && e.BlockAccessList == nil {
		e.BlockAccessList = &ByteListSSZ{}
	}
	return ssz2.UnmarshalSSZ(buf, version, e.getSchema()...)
}

func (e *executionPayloadSSZ) EncodingSizeSSZ() int {
	size := 508 // fixed part for v1 (includes ExtraData and Transactions offset slots)
	if e.ExtraData != nil {
		size += e.ExtraData.EncodingSizeSSZ()
	}
	if e.Transactions != nil {
		size += e.Transactions.EncodingSizeSSZ()
	}
	if e.version >= 2 {
		size += 4 // withdrawals offset
		if e.Withdrawals != nil {
			size += e.Withdrawals.EncodingSizeSSZ()
		}
	}
	if e.version >= 3 {
		size += 16 // BlobGasUsed + ExcessBlobGas
	}
	if e.version >= 4 {
		size += 12 // SlotNumber + BlockAccessList offset
		if e.BlockAccessList != nil {
			size += e.BlockAccessList.EncodingSizeSSZ()
		}
	}
	return size
}

func (e *executionPayloadSSZ) Static() bool             { return false }
func (e *executionPayloadSSZ) Clone() clonable.Clonable { return &executionPayloadSSZ{} }

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
	return executionPayloadToSSZ(e, version).EncodeSSZ(buf)
}

func (e *ExecutionPayload) DecodeSSZ(buf []byte, version int) error {
	adapter := &executionPayloadSSZ{version: version}
	if err := adapter.DecodeSSZ(buf, version); err != nil {
		return err
	}
	decoded := adapter.ToExecutionPayload()
	decoded.sszVersion = version
	*e = *decoded
	return nil
}

func (e *ExecutionPayload) EncodingSizeSSZ() int {
	version := e.sszVersion
	if version == 0 {
		version = executionPayloadVersionFromFields(e)
	}
	return executionPayloadToSSZ(e, version).EncodingSizeSSZ()
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

// executionPayloadToSSZ adapts a JSON-RPC ExecutionPayload to the CL SSZ schema.
func executionPayloadToSSZ(ep *ExecutionPayload, version int) *executionPayloadSSZ {
	extraData := solid.NewExtraData()
	extraData.SetBytes(ep.ExtraData)
	txs := make([][]byte, len(ep.Transactions))
	for i, tx := range ep.Transactions {
		txs[i] = []byte(tx)
	}
	s := &executionPayloadSSZ{
		ParentHash:   ep.ParentHash,
		FeeRecipient: ep.FeeRecipient,
		StateRoot:    ep.StateRoot,
		ReceiptsRoot: ep.ReceiptsRoot,
		PrevRandao:   ep.PrevRandao,
		BlockNumber:  uint64(ep.BlockNumber),
		GasLimit:     uint64(ep.GasLimit),
		GasUsed:      uint64(ep.GasUsed),
		Timestamp:    uint64(ep.Timestamp),
		ExtraData:    extraData,
		BlockHash:    ep.BlockHash,
		Transactions: solid.NewTransactionsSSZFromTransactions(txs),
		version:      version,
	}
	if len(ep.LogsBloom) >= 256 {
		copy(s.LogsBloom[:], ep.LogsBloom[:256])
	}
	if ep.BaseFeePerGas != nil {
		s.BaseFeePerGas = uint256ToSSZBytes(ep.BaseFeePerGas.ToInt())
	}
	if version >= 2 {
		wds := make([]*cltypes.Withdrawal, len(ep.Withdrawals))
		for i, w := range ep.Withdrawals {
			wds[i] = &cltypes.Withdrawal{Index: w.Index, Validator: w.Validator, Address: w.Address, Amount: w.Amount}
		}
		s.Withdrawals = solid.NewStaticListSSZFromList[*cltypes.Withdrawal](wds, 1_048_576, (&cltypes.Withdrawal{}).EncodingSizeSSZ())
	}
	if version >= 3 {
		if ep.BlobGasUsed != nil {
			s.BlobGasUsed = uint64(*ep.BlobGasUsed)
		}
		if ep.ExcessBlobGas != nil {
			s.ExcessBlobGas = uint64(*ep.ExcessBlobGas)
		}
	}
	if version >= 4 {
		if ep.SlotNumber != nil {
			s.SlotNumber = uint64(*ep.SlotNumber)
		}
		s.BlockAccessList = &ByteListSSZ{data: []byte(ep.BlockAccessList)}
	}
	return s
}

// ToExecutionPayload converts SSZ format back to JSON-RPC ExecutionPayload.
func (e *executionPayloadSSZ) ToExecutionPayload() *ExecutionPayload {
	ep := &ExecutionPayload{
		ParentHash:   e.ParentHash,
		FeeRecipient: e.FeeRecipient,
		StateRoot:    e.StateRoot,
		ReceiptsRoot: e.ReceiptsRoot,
		PrevRandao:   e.PrevRandao,
		BlockNumber:  hexutil.Uint64(e.BlockNumber),
		GasLimit:     hexutil.Uint64(e.GasLimit),
		GasUsed:      hexutil.Uint64(e.GasUsed),
		Timestamp:    hexutil.Uint64(e.Timestamp),
		BlockHash:    e.BlockHash,
	}
	ep.LogsBloom = make(hexutil.Bytes, 256)
	copy(ep.LogsBloom, e.LogsBloom[:])
	baseFee := sszBytesToUint256(e.BaseFeePerGas[:])
	ep.BaseFeePerGas = (*hexutil.Big)(baseFee)
	if e.ExtraData != nil {
		ep.ExtraData = e.ExtraData.Bytes()
	}
	if e.Transactions != nil {
		txs := e.Transactions.UnderlyngReference()
		ep.Transactions = make([]hexutil.Bytes, len(txs))
		for i, tx := range txs {
			ep.Transactions[i] = make(hexutil.Bytes, len(tx))
			copy(ep.Transactions[i], tx)
		}
	}
	if ep.Transactions == nil {
		ep.Transactions = []hexutil.Bytes{}
	}
	if e.version >= 2 && e.Withdrawals != nil {
		ep.Withdrawals = make([]*types.Withdrawal, 0, e.Withdrawals.Len())
		e.Withdrawals.Range(func(_ int, w *cltypes.Withdrawal, _ int) bool {
			ep.Withdrawals = append(ep.Withdrawals, &types.Withdrawal{Index: w.Index, Validator: w.Validator, Address: w.Address, Amount: w.Amount})
			return true
		})
	}
	if e.version >= 3 {
		bgu := hexutil.Uint64(e.BlobGasUsed)
		ep.BlobGasUsed = &bgu
		ebg := hexutil.Uint64(e.ExcessBlobGas)
		ep.ExcessBlobGas = &ebg
	}
	if e.version >= 4 {
		sn := hexutil.Uint64(e.SlotNumber)
		ep.SlotNumber = &sn
		if e.BlockAccessList != nil {
			ep.BlockAccessList = make(hexutil.Bytes, len(e.BlockAccessList.data))
			copy(ep.BlockAccessList, e.BlockAccessList.data)
		}
	}
	return ep
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

// ---------------------------------------------------------------
// NewPayload request SSZ
// ---------------------------------------------------------------

// NewPayloadRequestSSZ is the version-dependent SSZ container for newPayload requests.
type NewPayloadRequestSSZ struct {
	Payload               *ExecutionPayload
	BlobVersionedHashes   solid.HashListSSZ
	ParentBeaconBlockRoot common.Hash
	ExecutionRequests     *StructuredRequestsSSZ
	version               int
}

func (n *NewPayloadRequestSSZ) getSchema() []any {
	if n.version == 3 {
		return []any{n.Payload, n.BlobVersionedHashes, n.ParentBeaconBlockRoot[:]}
	}
	// V4+
	return []any{n.Payload, n.BlobVersionedHashes, n.ParentBeaconBlockRoot[:], n.ExecutionRequests}
}

func (n *NewPayloadRequestSSZ) EncodeSSZ(buf []byte) ([]byte, error) {
	if n.version <= 2 {
		return n.Payload.EncodeSSZ(buf)
	}
	return ssz2.MarshalSSZ(buf, n.getSchema()...)
}

func (n *NewPayloadRequestSSZ) DecodeSSZ(buf []byte, version int) error {
	n.version = version
	payloadVersion := engineVersionToPayloadVersion(version)
	if n.Payload == nil {
		n.Payload = &ExecutionPayload{sszVersion: payloadVersion}
	}
	n.Payload.sszVersion = payloadVersion
	if version <= 2 {
		return n.Payload.DecodeSSZ(buf, payloadVersion)
	}
	if n.BlobVersionedHashes == nil {
		n.BlobVersionedHashes = solid.NewHashList(4096)
	}
	if version >= 4 && n.ExecutionRequests == nil {
		n.ExecutionRequests = &StructuredRequestsSSZ{
			Deposits: &ByteListSSZ{}, Withdrawals: &ByteListSSZ{}, Consolidations: &ByteListSSZ{},
		}
	}
	return ssz2.UnmarshalSSZ(buf, version, n.getSchema()...)
}

func (n *NewPayloadRequestSSZ) EncodingSizeSSZ() int {
	if n.version <= 2 {
		return n.Payload.EncodingSizeSSZ()
	}
	size := 4 + 4 + 32 // payload offset + hashes offset + parent root
	size += n.Payload.EncodingSizeSSZ()
	size += n.BlobVersionedHashes.EncodingSizeSSZ()
	if n.version >= 4 {
		size += 4 // requests offset
		size += n.ExecutionRequests.EncodingSizeSSZ()
	}
	return size
}

func (n *NewPayloadRequestSSZ) Static() bool             { return false }
func (n *NewPayloadRequestSSZ) Clone() clonable.Clonable { return &NewPayloadRequestSSZ{} }

// Convenience wrappers (backward-compatible API).
func EncodeNewPayloadRequestSSZ(
	ep *ExecutionPayload,
	blobHashes []common.Hash,
	parentBeaconBlockRoot *common.Hash,
	executionRequests []hexutil.Bytes,
	version int,
) []byte {
	payloadVersion := engineVersionToPayloadVersion(version)
	n := &NewPayloadRequestSSZ{
		Payload:             ep,
		BlobVersionedHashes: solid.NewHashList(max(len(blobHashes), 4096)),
		version:             version,
	}
	n.Payload.sszVersion = payloadVersion
	for _, hash := range blobHashes {
		n.BlobVersionedHashes.Append(hash)
	}
	if parentBeaconBlockRoot != nil {
		n.ParentBeaconBlockRoot = *parentBeaconBlockRoot
	}
	if version >= 4 {
		n.ExecutionRequests = structuredRequestsFromSlice(executionRequests)
	}
	buf, _ := n.EncodeSSZ(nil)
	return buf
}

func DecodeNewPayloadRequestSSZ(buf []byte, version int) (
	ep *ExecutionPayload,
	blobHashes []common.Hash,
	parentBeaconBlockRoot *common.Hash,
	executionRequests []hexutil.Bytes,
	err error,
) {
	n := &NewPayloadRequestSSZ{version: version}
	if err = n.DecodeSSZ(buf, version); err != nil {
		return
	}
	ep = n.Payload
	if version >= 3 {
		blobHashes = make([]common.Hash, 0, n.BlobVersionedHashes.Length())
		n.BlobVersionedHashes.Range(func(_ int, hash common.Hash, _ int) bool {
			blobHashes = append(blobHashes, hash)
			return true
		})
		root := n.ParentBeaconBlockRoot
		parentBeaconBlockRoot = &root
	}
	if version >= 4 && n.ExecutionRequests != nil {
		executionRequests = n.ExecutionRequests.toSlice()
	}
	return
}

// ---------------------------------------------------------------
// BlobsBundle SSZ
// ---------------------------------------------------------------

// BlobsBundleSSZ is the SSZ container for BlobsBundle.
type BlobsBundleSSZ struct {
	Commitments *ConcatBytesListSSZ
	Proofs      *ConcatBytesListSSZ
	Blobs       *ConcatBytesListSSZ
}

func (b *BlobsBundleSSZ) EncodeSSZ(buf []byte) ([]byte, error) {
	return ssz2.MarshalSSZ(buf, b.Commitments, b.Proofs, b.Blobs)
}

func (b *BlobsBundleSSZ) DecodeSSZ(buf []byte, version int) error {
	if b.Commitments == nil {
		b.Commitments = &ConcatBytesListSSZ{itemSize: 48}
	}
	if b.Proofs == nil {
		b.Proofs = &ConcatBytesListSSZ{itemSize: 48}
	}
	if b.Blobs == nil {
		b.Blobs = &ConcatBytesListSSZ{itemSize: 131072}
	}
	return ssz2.UnmarshalSSZ(buf, version, b.Commitments, b.Proofs, b.Blobs)
}

func (b *BlobsBundleSSZ) EncodingSizeSSZ() int {
	return 12 + b.Commitments.EncodingSizeSSZ() + b.Proofs.EncodingSizeSSZ() + b.Blobs.EncodingSizeSSZ()
}

func (b *BlobsBundleSSZ) Static() bool             { return false }
func (b *BlobsBundleSSZ) Clone() clonable.Clonable { return &BlobsBundleSSZ{} }

func blobsBundleToSSZ(bundle *BlobsBundle) *BlobsBundleSSZ {
	if bundle == nil {
		return &BlobsBundleSSZ{
			Commitments: &ConcatBytesListSSZ{itemSize: 48},
			Proofs:      &ConcatBytesListSSZ{itemSize: 48},
			Blobs:       &ConcatBytesListSSZ{itemSize: 131072},
		}
	}
	toBytes := func(items []hexutil.Bytes) [][]byte {
		result := make([][]byte, len(items))
		for i, item := range items {
			result[i] = []byte(item)
		}
		return result
	}
	return &BlobsBundleSSZ{
		Commitments: &ConcatBytesListSSZ{items: toBytes(bundle.Commitments), itemSize: 48},
		Proofs:      &ConcatBytesListSSZ{items: toBytes(bundle.Proofs), itemSize: 48},
		Blobs:       &ConcatBytesListSSZ{items: toBytes(bundle.Blobs), itemSize: 131072},
	}
}

func (b *BlobsBundleSSZ) toBlobsBundle() *BlobsBundle {
	toHex := func(items [][]byte) []hexutil.Bytes {
		result := make([]hexutil.Bytes, len(items))
		for i, item := range items {
			result[i] = make(hexutil.Bytes, len(item))
			copy(result[i], item)
		}
		return result
	}
	return &BlobsBundle{
		Commitments: toHex(b.Commitments.items),
		Proofs:      toHex(b.Proofs.items),
		Blobs:       toHex(b.Blobs.items),
	}
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
		g.ExecutionPayload, blockValue[:], blobsBundleToSSZ(g.BlobsBundle), []byte{overrideByte}, structuredRequestsFromSlice(g.ExecutionRequests),
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
	blobsBundle := &BlobsBundleSSZ{
		Commitments: &ConcatBytesListSSZ{itemSize: 48},
		Proofs:      &ConcatBytesListSSZ{itemSize: 48},
		Blobs:       &ConcatBytesListSSZ{itemSize: 131072},
	}
	executionRequests := &StructuredRequestsSSZ{
		Deposits: &ByteListSSZ{}, Withdrawals: &ByteListSSZ{}, Consolidations: &ByteListSSZ{},
	}
	// Manual decode: fixed part is ep_offset(4) + block_value(32) + blobs_offset(4) + override(1) + requests_offset(4) = 45
	const fixedSize = 45
	if len(buf) < fixedSize {
		return fmt.Errorf("GetPayloadResponse SSZ: buffer too short (%d < %d)", len(buf), fixedSize)
	}
	epOffset := commonssz.DecodeOffset(buf[0:])
	blockValue := sszBytesToUint256(buf[4:36])
	g.BlockValue = (*hexutil.Big)(blockValue)
	blobsOffset := commonssz.DecodeOffset(buf[36:])
	g.ShouldOverrideBuilder = buf[40] != 0
	reqOffset := commonssz.DecodeOffset(buf[41:])

	bufLen := uint32(len(buf))
	if epOffset > bufLen || blobsOffset > bufLen || reqOffset > bufLen {
		return fmt.Errorf("GetPayloadResponse SSZ: offsets out of bounds")
	}
	if epOffset > blobsOffset || blobsOffset > reqOffset {
		return fmt.Errorf("GetPayloadResponse SSZ: offsets not in order")
	}
	if err := g.ExecutionPayload.DecodeSSZ(buf[epOffset:blobsOffset], payloadVersion); err != nil {
		return err
	}
	if err := blobsBundle.DecodeSSZ(buf[blobsOffset:reqOffset], 0); err != nil {
		return err
	}
	g.BlobsBundle = blobsBundle.toBlobsBundle()
	if reqOffset < bufLen {
		if err := executionRequests.DecodeSSZ(buf[reqOffset:], 0); err != nil {
			return err
		}
		g.ExecutionRequests = executionRequests.toSlice()
	}
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
	return 45 + g.ExecutionPayload.EncodingSizeSSZ() + blobsBundleToSSZ(g.BlobsBundle).EncodingSizeSSZ() + structuredRequestsFromSlice(g.ExecutionRequests).EncodingSizeSSZ()
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
