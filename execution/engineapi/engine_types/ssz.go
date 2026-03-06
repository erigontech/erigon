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

	ssz2 "github.com/erigontech/erigon/cl/ssz"
	"github.com/erigontech/erigon/common"
	"github.com/erigontech/erigon/common/clonable"
	"github.com/erigontech/erigon/common/hexutil"
	commonssz "github.com/erigontech/erigon/common/ssz"
	"github.com/erigontech/erigon/execution/types"
)

// SSZ status codes for PayloadStatusSSZ (EIP-8161)
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

// ---------------------------------------------------------------
// PayloadStatusSSZ
// ---------------------------------------------------------------

// PayloadStatusSSZ is the SSZ-encoded version of PayloadStatus for EIP-8161.
//
// SSZ Container layout:
//
//	Fixed: status(1) + latest_valid_hash_offset(4) + validation_error_offset(4) = 9 bytes
//	Variable: List[Hash32, 1] (0 or 32 bytes) + List[uint8, 1024]
type PayloadStatusSSZ struct {
	Status          uint8
	LatestValidHash *common.Hash
	ValidationError string
}

const payloadStatusFixedSize = 9 // status(1) + hash_offset(4) + err_offset(4)

func (p *PayloadStatusSSZ) EncodeSSZ(buf []byte) (dst []byte, err error) {
	dst = buf
	var hashData []byte
	if p.LatestValidHash != nil {
		hashData = p.LatestValidHash[:]
	}
	errBytes := []byte(p.ValidationError)

	// Fixed part
	dst = append(dst, p.Status)
	dst = append(dst, commonssz.OffsetSSZ(uint32(payloadStatusFixedSize))...)
	dst = append(dst, commonssz.OffsetSSZ(uint32(payloadStatusFixedSize+len(hashData)))...)

	// Variable part
	dst = append(dst, hashData...)
	dst = append(dst, errBytes...)
	return dst, nil
}

func (p *PayloadStatusSSZ) DecodeSSZ(buf []byte, _ int) error {
	if len(buf) < payloadStatusFixedSize {
		return fmt.Errorf("PayloadStatusSSZ: %w (need %d, got %d)", commonssz.ErrLowBufferSize, payloadStatusFixedSize, len(buf))
	}
	p.Status = buf[0]
	hashOffset := commonssz.DecodeOffset(buf[1:])
	errOffset := commonssz.DecodeOffset(buf[5:])

	if hashOffset > uint32(len(buf)) || errOffset > uint32(len(buf)) || hashOffset > errOffset {
		return fmt.Errorf("PayloadStatusSSZ: %w", commonssz.ErrBadOffset)
	}

	hashData := buf[hashOffset:errOffset]
	switch len(hashData) {
	case 32:
		hash := common.BytesToHash(hashData)
		p.LatestValidHash = &hash
	case 0:
		p.LatestValidHash = nil
	default:
		return fmt.Errorf("PayloadStatusSSZ: invalid hash list length %d", len(hashData))
	}

	errData := buf[errOffset:]
	if len(errData) > 1024 {
		return fmt.Errorf("PayloadStatusSSZ: validation error too long (%d > 1024)", len(errData))
	}
	p.ValidationError = string(errData)
	return nil
}

func (p *PayloadStatusSSZ) EncodingSizeSSZ() int {
	size := payloadStatusFixedSize
	if p.LatestValidHash != nil {
		size += 32
	}
	size += len(p.ValidationError)
	return size
}

func (p *PayloadStatusSSZ) Static() bool          { return false }
func (p *PayloadStatusSSZ) Clone() clonable.Clonable { return &PayloadStatusSSZ{} }

// ToPayloadStatus converts SSZ format to the standard JSON-RPC PayloadStatus.
func (p *PayloadStatusSSZ) ToPayloadStatus() *PayloadStatus {
	ps := &PayloadStatus{
		Status:          SSZToEngineStatus(p.Status),
		LatestValidHash: p.LatestValidHash,
	}
	if p.ValidationError != "" {
		ps.ValidationError = NewStringifiedErrorFromString(p.ValidationError)
	}
	return ps
}

// PayloadStatusToSSZ converts a JSON-RPC PayloadStatus to the SSZ format.
func PayloadStatusToSSZ(ps *PayloadStatus) *PayloadStatusSSZ {
	s := &PayloadStatusSSZ{
		Status:          EngineStatusToSSZ(ps.Status),
		LatestValidHash: ps.LatestValidHash,
	}
	if ps.ValidationError != nil && ps.ValidationError.Error() != nil {
		s.ValidationError = ps.ValidationError.Error().Error()
	}
	return s
}

// DecodePayloadStatusSSZ decodes SSZ bytes into a PayloadStatusSSZ.
func DecodePayloadStatusSSZ(buf []byte) (*PayloadStatusSSZ, error) {
	p := &PayloadStatusSSZ{}
	if err := p.DecodeSSZ(buf, 0); err != nil {
		return nil, err
	}
	return p, nil
}

// ---------------------------------------------------------------
// ForkchoiceStateSSZ
// ---------------------------------------------------------------

// ForkchoiceStateSSZ is the SSZ encoding of ForkchoiceState.
// Fixed layout: head_block_hash(32) + safe_block_hash(32) + finalized_block_hash(32) = 96 bytes
type ForkchoiceStateSSZ struct {
	HeadBlockHash      common.Hash
	SafeBlockHash      common.Hash
	FinalizedBlockHash common.Hash
}

func (f *ForkchoiceStateSSZ) EncodeSSZ(buf []byte) ([]byte, error) {
	return ssz2.MarshalSSZ(buf, f.HeadBlockHash[:], f.SafeBlockHash[:], f.FinalizedBlockHash[:])
}

func (f *ForkchoiceStateSSZ) DecodeSSZ(buf []byte, version int) error {
	return ssz2.UnmarshalSSZ(buf, version, f.HeadBlockHash[:], f.SafeBlockHash[:], f.FinalizedBlockHash[:])
}

func (f *ForkchoiceStateSSZ) EncodingSizeSSZ() int   { return 96 }
func (f *ForkchoiceStateSSZ) Static() bool            { return true }
func (f *ForkchoiceStateSSZ) Clone() clonable.Clonable { return &ForkchoiceStateSSZ{} }

func EncodeForkchoiceState(fcs *ForkChoiceState) []byte {
	s := &ForkchoiceStateSSZ{
		HeadBlockHash:      fcs.HeadHash,
		SafeBlockHash:      fcs.SafeBlockHash,
		FinalizedBlockHash: fcs.FinalizedBlockHash,
	}
	buf, _ := s.EncodeSSZ(nil)
	return buf
}

func DecodeForkchoiceState(buf []byte) (*ForkChoiceState, error) {
	s := &ForkchoiceStateSSZ{}
	if err := s.DecodeSSZ(buf, 0); err != nil {
		return nil, fmt.Errorf("ForkchoiceState: %w", err)
	}
	return &ForkChoiceState{
		HeadHash:           s.HeadBlockHash,
		SafeBlockHash:      s.SafeBlockHash,
		FinalizedBlockHash: s.FinalizedBlockHash,
	}, nil
}

// ---------------------------------------------------------------
// ForkchoiceUpdatedResponseSSZ
// ---------------------------------------------------------------

// ForkchoiceUpdatedResponseSSZ is the SSZ-encoded forkchoice updated response.
//
// SSZ Container layout:
//
//	Fixed: payload_status_offset(4) + payload_id_offset(4) = 8 bytes
//	Variable: PayloadStatusSSZ data + List[Bytes8, 1] (0 or 8 bytes)
type ForkchoiceUpdatedResponseSSZ struct {
	PayloadStatus *PayloadStatusSSZ
	PayloadId     []byte // raw Bytes8 (nil=absent, 8 bytes=present)
}

const forkchoiceUpdatedResponseFixedSize = 8

func (r *ForkchoiceUpdatedResponseSSZ) EncodeSSZ(buf []byte) (dst []byte, err error) {
	dst = buf
	psBytes, err := r.PayloadStatus.EncodeSSZ(nil)
	if err != nil {
		return nil, err
	}

	// Fixed part
	dst = append(dst, commonssz.OffsetSSZ(uint32(forkchoiceUpdatedResponseFixedSize))...)
	dst = append(dst, commonssz.OffsetSSZ(uint32(forkchoiceUpdatedResponseFixedSize+len(psBytes)))...)

	// Variable part
	dst = append(dst, psBytes...)
	dst = append(dst, r.PayloadId...) // 0 or 8 bytes
	return dst, nil
}

func (r *ForkchoiceUpdatedResponseSSZ) DecodeSSZ(buf []byte, _ int) error {
	if len(buf) < forkchoiceUpdatedResponseFixedSize {
		return fmt.Errorf("ForkchoiceUpdatedResponseSSZ: %w", commonssz.ErrLowBufferSize)
	}
	psOffset := commonssz.DecodeOffset(buf[0:])
	pidOffset := commonssz.DecodeOffset(buf[4:])

	if psOffset > uint32(len(buf)) || pidOffset > uint32(len(buf)) || psOffset > pidOffset {
		return fmt.Errorf("ForkchoiceUpdatedResponseSSZ: %w", commonssz.ErrBadOffset)
	}

	r.PayloadStatus = &PayloadStatusSSZ{}
	if err := r.PayloadStatus.DecodeSSZ(buf[psOffset:pidOffset], 0); err != nil {
		return err
	}

	pidData := buf[pidOffset:]
	if len(pidData) == 8 {
		r.PayloadId = make([]byte, 8)
		copy(r.PayloadId, pidData)
	}
	return nil
}

func (r *ForkchoiceUpdatedResponseSSZ) EncodingSizeSSZ() int {
	size := forkchoiceUpdatedResponseFixedSize
	if r.PayloadStatus != nil {
		size += r.PayloadStatus.EncodingSizeSSZ()
	}
	size += len(r.PayloadId)
	return size
}

func (r *ForkchoiceUpdatedResponseSSZ) Static() bool            { return false }
func (r *ForkchoiceUpdatedResponseSSZ) Clone() clonable.Clonable { return &ForkchoiceUpdatedResponseSSZ{PayloadStatus: &PayloadStatusSSZ{}} }

func EncodeForkchoiceUpdatedResponse(resp *ForkChoiceUpdatedResponse) []byte {
	ps := PayloadStatusToSSZ(resp.PayloadStatus)
	r := &ForkchoiceUpdatedResponseSSZ{PayloadStatus: ps}
	if resp.PayloadId != nil {
		pidBytes := []byte(*resp.PayloadId)
		if len(pidBytes) == 8 {
			r.PayloadId = pidBytes
		}
	}
	buf, _ := r.EncodeSSZ(nil)
	return buf
}

func DecodeForkchoiceUpdatedResponse(buf []byte) (*ForkchoiceUpdatedResponseSSZ, error) {
	r := &ForkchoiceUpdatedResponseSSZ{}
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

func (b *ByteListSSZ) EncodeSSZ(buf []byte) ([]byte, error)  { return append(buf, b.data...), nil }
func (b *ByteListSSZ) DecodeSSZ(buf []byte, _ int) error      { b.data = append([]byte(nil), buf...); return nil }
func (b *ByteListSSZ) EncodingSizeSSZ() int                    { return len(b.data) }
func (b *ByteListSSZ) Static() bool                            { return false }
func (b *ByteListSSZ) Clone() clonable.Clonable                { return &ByteListSSZ{} }

// TransactionListSSZ wraps a list of variable-length transactions for SSZ schemas.
type TransactionListSSZ struct{ txs [][]byte }

func (t *TransactionListSSZ) EncodeSSZ(buf []byte) ([]byte, error) {
	if len(t.txs) == 0 {
		return buf, nil
	}
	offsetsSize := len(t.txs) * 4
	dataSize := 0
	for _, tx := range t.txs {
		dataSize += len(tx)
	}
	start := len(buf)
	buf = append(buf, make([]byte, offsetsSize+dataSize)...)
	dataStart := uint32(offsetsSize)
	for i, tx := range t.txs {
		commonssz.EncodeOffset(buf[start+i*4:], dataStart)
		dataStart += uint32(len(tx))
	}
	pos := start + offsetsSize
	for _, tx := range t.txs {
		copy(buf[pos:], tx)
		pos += len(tx)
	}
	return buf, nil
}

func (t *TransactionListSSZ) DecodeSSZ(buf []byte, _ int) error {
	if len(buf) == 0 {
		t.txs = nil
		return nil
	}
	if len(buf) < 4 {
		return fmt.Errorf("transactions SSZ: buffer too short")
	}
	firstOffset := commonssz.DecodeOffset(buf[0:])
	if firstOffset%4 != 0 || firstOffset > uint32(len(buf)) {
		return fmt.Errorf("transactions SSZ: invalid first offset (%d)", firstOffset)
	}
	count := firstOffset / 4
	if count == 0 {
		t.txs = nil
		return nil
	}
	offsets := make([]uint32, count)
	for i := uint32(0); i < count; i++ {
		offsets[i] = commonssz.DecodeOffset(buf[i*4:])
	}
	t.txs = make([][]byte, count)
	for i := uint32(0); i < count; i++ {
		start := offsets[i]
		end := uint32(len(buf))
		if i+1 < count {
			end = offsets[i+1]
		}
		if start > uint32(len(buf)) || end > uint32(len(buf)) || start > end {
			return fmt.Errorf("transactions SSZ: invalid offset at index %d", i)
		}
		t.txs[i] = append([]byte(nil), buf[start:end]...)
	}
	return nil
}

func (t *TransactionListSSZ) EncodingSizeSSZ() int {
	size := len(t.txs) * 4
	for _, tx := range t.txs {
		size += len(tx)
	}
	return size
}

func (t *TransactionListSSZ) Static() bool             { return false }
func (t *TransactionListSSZ) Clone() clonable.Clonable { return &TransactionListSSZ{} }

// WithdrawalSSZ is a single execution-layer withdrawal (44 bytes fixed).
type WithdrawalSSZ struct {
	Index     uint64
	Validator uint64
	Address   common.Address
	Amount    uint64
}

func (w *WithdrawalSSZ) EncodeSSZ(buf []byte) ([]byte, error) {
	buf = append(buf, commonssz.Uint64SSZ(w.Index)...)
	buf = append(buf, commonssz.Uint64SSZ(w.Validator)...)
	buf = append(buf, w.Address[:]...)
	buf = append(buf, commonssz.Uint64SSZ(w.Amount)...)
	return buf, nil
}

func (w *WithdrawalSSZ) DecodeSSZ(buf []byte, _ int) error {
	if len(buf) < 44 {
		return fmt.Errorf("WithdrawalSSZ: %w (need 44, got %d)", commonssz.ErrLowBufferSize, len(buf))
	}
	w.Index = commonssz.UnmarshalUint64SSZ(buf[0:])
	w.Validator = commonssz.UnmarshalUint64SSZ(buf[8:])
	copy(w.Address[:], buf[16:36])
	w.Amount = commonssz.UnmarshalUint64SSZ(buf[36:])
	return nil
}

func (w *WithdrawalSSZ) EncodingSizeSSZ() int   { return 44 }
func (w *WithdrawalSSZ) Static() bool            { return true }
func (w *WithdrawalSSZ) Clone() clonable.Clonable { return &WithdrawalSSZ{} }

func (w *WithdrawalSSZ) ToExecution() *types.Withdrawal {
	return &types.Withdrawal{Index: w.Index, Validator: w.Validator, Address: w.Address, Amount: w.Amount}
}

func WithdrawalFromExecution(ew *types.Withdrawal) *WithdrawalSSZ {
	return &WithdrawalSSZ{Index: ew.Index, Validator: ew.Validator, Address: ew.Address, Amount: ew.Amount}
}

// WithdrawalListSSZ is a list of fixed-size withdrawals for SSZ schemas.
type WithdrawalListSSZ struct{ withdrawals []*WithdrawalSSZ }

func (l *WithdrawalListSSZ) EncodeSSZ(buf []byte) ([]byte, error) {
	var err error
	for _, w := range l.withdrawals {
		if buf, err = w.EncodeSSZ(buf); err != nil {
			return nil, err
		}
	}
	return buf, nil
}

func (l *WithdrawalListSSZ) DecodeSSZ(buf []byte, _ int) error {
	if len(buf) == 0 {
		l.withdrawals = nil
		return nil
	}
	if len(buf)%44 != 0 {
		return fmt.Errorf("WithdrawalListSSZ: length %d not divisible by 44", len(buf))
	}
	count := len(buf) / 44
	l.withdrawals = make([]*WithdrawalSSZ, count)
	for i := range count {
		w := &WithdrawalSSZ{}
		if err := w.DecodeSSZ(buf[i*44:(i+1)*44], 0); err != nil {
			return err
		}
		l.withdrawals[i] = w
	}
	return nil
}

func (l *WithdrawalListSSZ) EncodingSizeSSZ() int   { return len(l.withdrawals) * 44 }
func (l *WithdrawalListSSZ) Static() bool            { return false }
func (l *WithdrawalListSSZ) Clone() clonable.Clonable { return &WithdrawalListSSZ{} }

// HashListSSZ is a list of 32-byte hashes for SSZ schemas.
type HashListSSZ struct{ hashes []common.Hash }

func (h *HashListSSZ) EncodeSSZ(buf []byte) ([]byte, error) {
	for _, hash := range h.hashes {
		buf = append(buf, hash[:]...)
	}
	return buf, nil
}

func (h *HashListSSZ) DecodeSSZ(buf []byte, _ int) error {
	if len(buf)%32 != 0 {
		return fmt.Errorf("HashListSSZ: length %d not aligned to 32", len(buf))
	}
	count := len(buf) / 32
	h.hashes = make([]common.Hash, count)
	for i := range count {
		copy(h.hashes[i][:], buf[i*32:(i+1)*32])
	}
	return nil
}

func (h *HashListSSZ) EncodingSizeSSZ() int   { return len(h.hashes) * 32 }
func (h *HashListSSZ) Static() bool            { return false }
func (h *HashListSSZ) Clone() clonable.Clonable { return &HashListSSZ{} }

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

func (c *ConcatBytesListSSZ) Static() bool             { return false }
func (c *ConcatBytesListSSZ) Clone() clonable.Clonable { return &ConcatBytesListSSZ{itemSize: c.itemSize} }

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

// ---------------------------------------------------------------
// ClientVersion SSZ
// ---------------------------------------------------------------

// ClientVersionSSZ is the SSZ container for a single ClientVersionV1.
type ClientVersionSSZ struct {
	Code    []byte
	Name    []byte
	Version []byte
	Commit  [4]byte
}

func (cv *ClientVersionSSZ) EncodeSSZ(buf []byte) (dst []byte, err error) {
	dst = buf
	const fixedSize = 16
	nameOff := uint32(fixedSize + len(cv.Code))
	versionOff := nameOff + uint32(len(cv.Name))
	dst = append(dst, commonssz.OffsetSSZ(uint32(fixedSize))...)
	dst = append(dst, commonssz.OffsetSSZ(nameOff)...)
	dst = append(dst, commonssz.OffsetSSZ(versionOff)...)
	dst = append(dst, cv.Commit[:]...)
	dst = append(dst, cv.Code...)
	dst = append(dst, cv.Name...)
	dst = append(dst, cv.Version...)
	return dst, nil
}

func (cv *ClientVersionSSZ) DecodeSSZ(buf []byte, _ int) error {
	const fixedSize = 16
	if len(buf) < fixedSize {
		return fmt.Errorf("ClientVersion: buffer too short (%d < %d)", len(buf), fixedSize)
	}
	codeOff := commonssz.DecodeOffset(buf[0:])
	nameOff := commonssz.DecodeOffset(buf[4:])
	versionOff := commonssz.DecodeOffset(buf[8:])
	copy(cv.Commit[:], buf[12:16])
	bufLen := uint32(len(buf))
	if codeOff > bufLen || nameOff > bufLen || versionOff > bufLen || codeOff > nameOff || nameOff > versionOff {
		return fmt.Errorf("ClientVersion: invalid offsets")
	}
	cv.Code = append([]byte(nil), buf[codeOff:nameOff]...)
	cv.Name = append([]byte(nil), buf[nameOff:versionOff]...)
	cv.Version = append([]byte(nil), buf[versionOff:]...)
	return nil
}

func (cv *ClientVersionSSZ) EncodingSizeSSZ() int { return 16 + len(cv.Code) + len(cv.Name) + len(cv.Version) }
func (cv *ClientVersionSSZ) Static() bool             { return false }
func (cv *ClientVersionSSZ) Clone() clonable.Clonable { return &ClientVersionSSZ{} }

// ClientVersionListSSZ is the SSZ container wrapping a list of ClientVersionSSZ.
type ClientVersionListSSZ struct {
	Versions []*ClientVersionSSZ
}

func (l *ClientVersionListSSZ) EncodeSSZ(buf []byte) (dst []byte, err error) {
	dst = buf
	// Container: offset(4) → list data
	dst = append(dst, commonssz.OffsetSSZ(4)...)
	// List data: item offsets + concatenated items
	var itemParts [][]byte
	for _, v := range l.Versions {
		part, err := v.EncodeSSZ(nil)
		if err != nil {
			return nil, err
		}
		itemParts = append(itemParts, part)
	}
	itemOffsetsSize := uint32(len(l.Versions) * 4)
	itemOffset := itemOffsetsSize
	for _, part := range itemParts {
		dst = append(dst, commonssz.OffsetSSZ(itemOffset)...)
		itemOffset += uint32(len(part))
	}
	for _, part := range itemParts {
		dst = append(dst, part...)
	}
	return dst, nil
}

func (l *ClientVersionListSSZ) DecodeSSZ(buf []byte, _ int) error {
	if len(buf) < 4 {
		return fmt.Errorf("ClientVersions: buffer too short")
	}
	listOffset := commonssz.DecodeOffset(buf[0:])
	if listOffset > uint32(len(buf)) {
		return fmt.Errorf("ClientVersions: list offset out of bounds")
	}
	listData := buf[listOffset:]
	if len(listData) == 0 {
		l.Versions = nil
		return nil
	}
	if len(listData) < 4 {
		return fmt.Errorf("ClientVersions: list data too short")
	}
	firstOffset := commonssz.DecodeOffset(listData[0:])
	if firstOffset%4 != 0 || firstOffset == 0 {
		return fmt.Errorf("ClientVersions: invalid first offset %d", firstOffset)
	}
	count := firstOffset / 4
	if count > 16 {
		return fmt.Errorf("ClientVersions: too many versions (%d > 16)", count)
	}
	offsets := make([]uint32, count)
	for i := uint32(0); i < count; i++ {
		offsets[i] = commonssz.DecodeOffset(listData[i*4:])
	}
	l.Versions = make([]*ClientVersionSSZ, count)
	for i := uint32(0); i < count; i++ {
		start := offsets[i]
		end := uint32(len(listData))
		if i+1 < count {
			end = offsets[i+1]
		}
		if start > uint32(len(listData)) || end > uint32(len(listData)) || start > end {
			return fmt.Errorf("ClientVersions: offset out of bounds at %d", i)
		}
		cv := &ClientVersionSSZ{}
		if err := cv.DecodeSSZ(listData[start:end], 0); err != nil {
			return err
		}
		l.Versions[i] = cv
	}
	return nil
}

func (l *ClientVersionListSSZ) EncodingSizeSSZ() int {
	size := 4 // container offset
	size += len(l.Versions) * 4
	for _, v := range l.Versions {
		size += v.EncodingSizeSSZ()
	}
	return size
}

func (l *ClientVersionListSSZ) Static() bool             { return false }
func (l *ClientVersionListSSZ) Clone() clonable.Clonable { return &ClientVersionListSSZ{} }

// Convenience wrappers (backward-compatible API).
func EncodeClientVersion(cv *ClientVersionV1) []byte {
	s := &ClientVersionSSZ{Code: []byte(cv.Code), Name: []byte(cv.Name), Version: []byte(cv.Version)}
	if commitRaw, err := hexutil.Decode(cv.Commit); err == nil {
		copy(s.Commit[:], commitRaw)
	}
	buf, _ := s.EncodeSSZ(nil)
	return buf
}

func DecodeClientVersion(buf []byte) (*ClientVersionV1, error) {
	s := &ClientVersionSSZ{}
	if err := s.DecodeSSZ(buf, 0); err != nil {
		return nil, err
	}
	return &ClientVersionV1{
		Code: string(s.Code), Name: string(s.Name),
		Version: string(s.Version), Commit: hexutil.Encode(s.Commit[:]),
	}, nil
}

func EncodeClientVersions(versions []ClientVersionV1) []byte {
	l := &ClientVersionListSSZ{Versions: make([]*ClientVersionSSZ, len(versions))}
	for i := range versions {
		s := &ClientVersionSSZ{Code: []byte(versions[i].Code), Name: []byte(versions[i].Name), Version: []byte(versions[i].Version)}
		if commitRaw, err := hexutil.Decode(versions[i].Commit); err == nil {
			copy(s.Commit[:], commitRaw)
		}
		l.Versions[i] = s
	}
	buf, _ := l.EncodeSSZ(nil)
	return buf
}

func DecodeClientVersions(buf []byte) ([]ClientVersionV1, error) {
	l := &ClientVersionListSSZ{}
	if err := l.DecodeSSZ(buf, 0); err != nil {
		return nil, err
	}
	result := make([]ClientVersionV1, len(l.Versions))
	for i, v := range l.Versions {
		result[i] = ClientVersionV1{
			Code: string(v.Code), Name: string(v.Name),
			Version: string(v.Version), Commit: hexutil.Encode(v.Commit[:]),
		}
	}
	return result, nil
}

// ---------------------------------------------------------------
// GetBlobs request SSZ
// ---------------------------------------------------------------

// GetBlobsRequestSSZ is the SSZ container for GetBlobs requests.
type GetBlobsRequestSSZ struct {
	VersionedHashes *HashListSSZ
}

func (g *GetBlobsRequestSSZ) EncodeSSZ(buf []byte) ([]byte, error) {
	return ssz2.MarshalSSZ(buf, g.VersionedHashes)
}

func (g *GetBlobsRequestSSZ) DecodeSSZ(buf []byte, version int) error {
	if g.VersionedHashes == nil {
		g.VersionedHashes = &HashListSSZ{}
	}
	return ssz2.UnmarshalSSZ(buf, version, g.VersionedHashes)
}

func (g *GetBlobsRequestSSZ) EncodingSizeSSZ() int { return 4 + g.VersionedHashes.EncodingSizeSSZ() }
func (g *GetBlobsRequestSSZ) Static() bool             { return false }
func (g *GetBlobsRequestSSZ) Clone() clonable.Clonable { return &GetBlobsRequestSSZ{} }

func EncodeGetBlobsRequest(hashes []common.Hash) []byte {
	g := &GetBlobsRequestSSZ{VersionedHashes: &HashListSSZ{hashes: hashes}}
	buf, _ := g.EncodeSSZ(nil)
	return buf
}

func DecodeGetBlobsRequest(buf []byte) ([]common.Hash, error) {
	g := &GetBlobsRequestSSZ{}
	if err := g.DecodeSSZ(buf, 0); err != nil {
		return nil, err
	}
	return g.VersionedHashes.hashes, nil
}

// ---------------------------------------------------------------
// ExecutionPayload SSZ
// ---------------------------------------------------------------

// ExecutionPayloadSSZ is a version-dependent SSZ container for execution payloads.
// Follows the Eth1Block pattern from cl/cltypes using getSchema().
type ExecutionPayloadSSZ struct {
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
	ExtraData       *ByteListSSZ
	BaseFeePerGas   [32]byte // uint256 LE
	BlockHash       common.Hash
	Transactions    *TransactionListSSZ
	Withdrawals     *WithdrawalListSSZ  // v2+
	BlobGasUsed     uint64              // v3+
	ExcessBlobGas   uint64              // v3+
	SlotNumber      uint64              // v4+
	BlockAccessList *ByteListSSZ        // v4+
	version         int
}

func (e *ExecutionPayloadSSZ) getSchema() []any {
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

func (e *ExecutionPayloadSSZ) EncodeSSZ(buf []byte) ([]byte, error) {
	return ssz2.MarshalSSZ(buf, e.getSchema()...)
}

func (e *ExecutionPayloadSSZ) DecodeSSZ(buf []byte, version int) error {
	e.version = engineVersionToPayloadVersion(version)
	if e.ExtraData == nil {
		e.ExtraData = &ByteListSSZ{}
	}
	if e.Transactions == nil {
		e.Transactions = &TransactionListSSZ{}
	}
	if version >= 2 && e.Withdrawals == nil {
		e.Withdrawals = &WithdrawalListSSZ{}
	}
	if version >= 4 && e.BlockAccessList == nil {
		e.BlockAccessList = &ByteListSSZ{}
	}
	return ssz2.UnmarshalSSZ(buf, version, e.getSchema()...)
}

func (e *ExecutionPayloadSSZ) EncodingSizeSSZ() int {
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

func (e *ExecutionPayloadSSZ) Static() bool             { return false }
func (e *ExecutionPayloadSSZ) Clone() clonable.Clonable { return &ExecutionPayloadSSZ{} }

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

// ExecutionPayloadToSSZ converts a JSON-RPC ExecutionPayload to SSZ format.
func ExecutionPayloadToSSZ(ep *ExecutionPayload, version int) *ExecutionPayloadSSZ {
	s := &ExecutionPayloadSSZ{
		ParentHash:   ep.ParentHash,
		FeeRecipient: ep.FeeRecipient,
		StateRoot:    ep.StateRoot,
		ReceiptsRoot: ep.ReceiptsRoot,
		PrevRandao:   ep.PrevRandao,
		BlockNumber:  uint64(ep.BlockNumber),
		GasLimit:     uint64(ep.GasLimit),
		GasUsed:      uint64(ep.GasUsed),
		Timestamp:    uint64(ep.Timestamp),
		ExtraData:    &ByteListSSZ{data: []byte(ep.ExtraData)},
		BlockHash:    ep.BlockHash,
		Transactions: &TransactionListSSZ{},
		version:      version,
	}
	if len(ep.LogsBloom) >= 256 {
		copy(s.LogsBloom[:], ep.LogsBloom[:256])
	}
	if ep.BaseFeePerGas != nil {
		s.BaseFeePerGas = uint256ToSSZBytes(ep.BaseFeePerGas.ToInt())
	}
	txs := make([][]byte, len(ep.Transactions))
	for i, tx := range ep.Transactions {
		txs[i] = []byte(tx)
	}
	s.Transactions = &TransactionListSSZ{txs: txs}
	if version >= 2 {
		wds := make([]*WithdrawalSSZ, len(ep.Withdrawals))
		for i, w := range ep.Withdrawals {
			wds[i] = WithdrawalFromExecution(w)
		}
		s.Withdrawals = &WithdrawalListSSZ{withdrawals: wds}
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
func (e *ExecutionPayloadSSZ) ToExecutionPayload() *ExecutionPayload {
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
		ep.ExtraData = make(hexutil.Bytes, len(e.ExtraData.data))
		copy(ep.ExtraData, e.ExtraData.data)
	}
	if e.Transactions != nil {
		ep.Transactions = make([]hexutil.Bytes, len(e.Transactions.txs))
		for i, tx := range e.Transactions.txs {
			ep.Transactions[i] = make(hexutil.Bytes, len(tx))
			copy(ep.Transactions[i], tx)
		}
	}
	if ep.Transactions == nil {
		ep.Transactions = []hexutil.Bytes{}
	}
	if e.version >= 2 && e.Withdrawals != nil {
		ep.Withdrawals = make([]*types.Withdrawal, len(e.Withdrawals.withdrawals))
		for i, w := range e.Withdrawals.withdrawals {
			ep.Withdrawals[i] = w.ToExecution()
		}
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
	s := ExecutionPayloadToSSZ(ep, version)
	buf, _ := s.EncodeSSZ(nil)
	return buf
}

func DecodeExecutionPayloadSSZ(buf []byte, version int) (*ExecutionPayload, error) {
	s := &ExecutionPayloadSSZ{version: version}
	if err := s.DecodeSSZ(buf, version); err != nil {
		return nil, fmt.Errorf("ExecutionPayload SSZ: %w", err)
	}
	return s.ToExecutionPayload(), nil
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
	Payload               *ExecutionPayloadSSZ
	BlobVersionedHashes   *HashListSSZ
	ParentBeaconBlockRoot common.Hash
	ExecutionRequests     *StructuredRequestsSSZ
	version               int
}

func (n *NewPayloadRequestSSZ) getSchema() []any {
	if n.version <= 2 {
		return n.Payload.getSchema()
	}
	if n.version == 3 {
		return []any{n.Payload, n.BlobVersionedHashes, n.ParentBeaconBlockRoot[:]}
	}
	// V4+
	return []any{n.Payload, n.BlobVersionedHashes, n.ParentBeaconBlockRoot[:], n.ExecutionRequests}
}

func (n *NewPayloadRequestSSZ) EncodeSSZ(buf []byte) ([]byte, error) {
	return ssz2.MarshalSSZ(buf, n.getSchema()...)
}

func (n *NewPayloadRequestSSZ) DecodeSSZ(buf []byte, version int) error {
	n.version = version
	payloadVersion := engineVersionToPayloadVersion(version)
	if n.Payload == nil {
		n.Payload = &ExecutionPayloadSSZ{version: payloadVersion}
	}
	n.Payload.version = payloadVersion
	if version <= 2 {
		return n.Payload.DecodeSSZ(buf, payloadVersion)
	}
	if n.BlobVersionedHashes == nil {
		n.BlobVersionedHashes = &HashListSSZ{}
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
		Payload:             ExecutionPayloadToSSZ(ep, payloadVersion),
		BlobVersionedHashes: &HashListSSZ{hashes: blobHashes},
		version:             version,
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
	ep = n.Payload.ToExecutionPayload()
	if version >= 3 {
		blobHashes = n.BlobVersionedHashes.hashes
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

// GetPayloadResponseSSZType is the SSZ container for GetPayloadResponse.
type GetPayloadResponseSSZType struct {
	Payload               *ExecutionPayloadSSZ
	BlockValue            [32]byte // uint256 LE
	BlobsBundle           *BlobsBundleSSZ
	ShouldOverrideBuilder bool
	ExecutionRequests     *StructuredRequestsSSZ
	version               int
}

func (g *GetPayloadResponseSSZType) EncodeSSZ(buf []byte) ([]byte, error) {
	if g.version == 1 {
		return g.Payload.EncodeSSZ(buf)
	}
	overrideByte := commonssz.BoolSSZ(g.ShouldOverrideBuilder)
	return ssz2.MarshalSSZ(buf,
		g.Payload, g.BlockValue[:], g.BlobsBundle, []byte{overrideByte}, g.ExecutionRequests,
	)
}

func (g *GetPayloadResponseSSZType) DecodeSSZ(buf []byte, version int) error {
	g.version = version
	payloadVersion := engineVersionToPayloadVersion(version)
	if g.Payload == nil {
		g.Payload = &ExecutionPayloadSSZ{version: payloadVersion}
	}
	g.Payload.version = payloadVersion
	if version == 1 {
		return g.Payload.DecodeSSZ(buf, payloadVersion)
	}
	if g.BlobsBundle == nil {
		g.BlobsBundle = &BlobsBundleSSZ{
			Commitments: &ConcatBytesListSSZ{itemSize: 48},
			Proofs:      &ConcatBytesListSSZ{itemSize: 48},
			Blobs:       &ConcatBytesListSSZ{itemSize: 131072},
		}
	}
	if g.ExecutionRequests == nil {
		g.ExecutionRequests = &StructuredRequestsSSZ{
			Deposits: &ByteListSSZ{}, Withdrawals: &ByteListSSZ{}, Consolidations: &ByteListSSZ{},
		}
	}
	// Manual decode: fixed part is ep_offset(4) + block_value(32) + blobs_offset(4) + override(1) + requests_offset(4) = 45
	const fixedSize = 45
	if len(buf) < fixedSize {
		return fmt.Errorf("GetPayloadResponse SSZ: buffer too short (%d < %d)", len(buf), fixedSize)
	}
	epOffset := commonssz.DecodeOffset(buf[0:])
	copy(g.BlockValue[:], buf[4:36])
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
	if err := g.Payload.DecodeSSZ(buf[epOffset:blobsOffset], payloadVersion); err != nil {
		return err
	}
	if err := g.BlobsBundle.DecodeSSZ(buf[blobsOffset:reqOffset], 0); err != nil {
		return err
	}
	if reqOffset < bufLen {
		if err := g.ExecutionRequests.DecodeSSZ(buf[reqOffset:], 0); err != nil {
			return err
		}
	}
	return nil
}

func (g *GetPayloadResponseSSZType) EncodingSizeSSZ() int {
	if g.version == 1 {
		return g.Payload.EncodingSizeSSZ()
	}
	return 45 + g.Payload.EncodingSizeSSZ() + g.BlobsBundle.EncodingSizeSSZ() + g.ExecutionRequests.EncodingSizeSSZ()
}

func (g *GetPayloadResponseSSZType) Static() bool             { return false }
func (g *GetPayloadResponseSSZType) Clone() clonable.Clonable { return &GetPayloadResponseSSZType{} }

// Convenience wrappers (backward-compatible API).
func EncodeGetPayloadResponseSSZ(resp *GetPayloadResponse, version int) []byte {
	payloadVersion := engineVersionToPayloadVersion(version)
	g := &GetPayloadResponseSSZType{
		Payload: ExecutionPayloadToSSZ(resp.ExecutionPayload, payloadVersion),
		version: version,
	}
	if version > 1 {
		if resp.BlockValue != nil {
			g.BlockValue = uint256ToSSZBytes(resp.BlockValue.ToInt())
		}
		g.BlobsBundle = blobsBundleToSSZ(resp.BlobsBundle)
		g.ShouldOverrideBuilder = resp.ShouldOverrideBuilder
		g.ExecutionRequests = structuredRequestsFromSlice(resp.ExecutionRequests)
	}
	buf, _ := g.EncodeSSZ(nil)
	return buf
}

func DecodeGetPayloadResponseSSZ(buf []byte, version int) (*GetPayloadResponse, error) {
	g := &GetPayloadResponseSSZType{version: version}
	if err := g.DecodeSSZ(buf, version); err != nil {
		return nil, err
	}
	resp := &GetPayloadResponse{
		ExecutionPayload:      g.Payload.ToExecutionPayload(),
		ShouldOverrideBuilder: g.ShouldOverrideBuilder,
	}
	if version > 1 {
		blockValue := sszBytesToUint256(g.BlockValue[:])
		resp.BlockValue = (*hexutil.Big)(blockValue)
		if g.BlobsBundle != nil {
			resp.BlobsBundle = g.BlobsBundle.toBlobsBundle()
		}
		if g.ExecutionRequests != nil {
			resp.ExecutionRequests = g.ExecutionRequests.toSlice()
		}
	}
	return resp, nil
}
