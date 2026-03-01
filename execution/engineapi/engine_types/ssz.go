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
	"encoding/binary"
	"fmt"
	"math/big"

	"github.com/erigontech/erigon/common"
	"github.com/erigontech/erigon/common/hexutil"
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

// PayloadStatusSSZ is the SSZ-encoded version of PayloadStatus for EIP-8161.
//
// SSZ layout (fixed part = 9 bytes):
//   - status:                    1 byte  (uint8)
//   - latest_valid_hash_offset:  4 bytes (offset to Union[None, Hash32])
//   - validation_error_offset:   4 bytes (offset to List[uint8, 1024])
//
// SSZ variable part:
//   - Union[None, Hash32]: selector(1) + hash(32) if selector==1; selector(1) if selector==0
//   - validation_error: List[uint8, 1024] — UTF-8 bytes
type PayloadStatusSSZ struct {
	Status          uint8
	LatestValidHash *common.Hash
	ValidationError string
}

const payloadStatusFixedSize = 9 // status(1) + hash_offset(4) + err_offset(4)

// EncodeSSZ encodes the PayloadStatusSSZ to SSZ bytes per EIP-8161.
func (p *PayloadStatusSSZ) EncodeSSZ() []byte {
	// Build Union[None, Hash32] variable data
	var hashUnion []byte
	if p.LatestValidHash != nil {
		hashUnion = make([]byte, 33) // selector(1) + hash(32)
		hashUnion[0] = 1
		copy(hashUnion[1:33], p.LatestValidHash[:])
	} else {
		hashUnion = []byte{0} // selector(0) = None
	}

	errorBytes := []byte(p.ValidationError)

	buf := make([]byte, payloadStatusFixedSize+len(hashUnion)+len(errorBytes))

	buf[0] = p.Status

	// Offset to Union[None, Hash32] (starts after fixed part)
	binary.LittleEndian.PutUint32(buf[1:5], uint32(payloadStatusFixedSize))
	// Offset to validation_error
	binary.LittleEndian.PutUint32(buf[5:9], uint32(payloadStatusFixedSize+len(hashUnion)))

	copy(buf[payloadStatusFixedSize:], hashUnion)
	copy(buf[payloadStatusFixedSize+len(hashUnion):], errorBytes)
	return buf
}

// DecodePayloadStatusSSZ decodes SSZ bytes into a PayloadStatusSSZ.
func DecodePayloadStatusSSZ(buf []byte) (*PayloadStatusSSZ, error) {
	if len(buf) < payloadStatusFixedSize {
		return nil, fmt.Errorf("PayloadStatusSSZ: buffer too short (%d < %d)", len(buf), payloadStatusFixedSize)
	}

	p := &PayloadStatusSSZ{
		Status: buf[0],
	}

	hashOffset := binary.LittleEndian.Uint32(buf[1:5])
	errOffset := binary.LittleEndian.Uint32(buf[5:9])

	if hashOffset > uint32(len(buf)) || errOffset > uint32(len(buf)) || hashOffset > errOffset {
		return nil, fmt.Errorf("PayloadStatusSSZ: offsets out of bounds")
	}

	// Decode Union[None, Hash32]
	unionData := buf[hashOffset:errOffset]
	if len(unionData) > 0 {
		selector := unionData[0]
		if selector == 1 {
			if len(unionData) < 33 {
				return nil, fmt.Errorf("PayloadStatusSSZ: Union hash data too short")
			}
			hash := common.BytesToHash(unionData[1:33])
			p.LatestValidHash = &hash
		}
		// selector == 0 means None, LatestValidHash stays nil
	}

	// Decode validation_error
	if errOffset < uint32(len(buf)) {
		errLen := uint32(len(buf)) - errOffset
		if errLen > 1024 {
			return nil, fmt.Errorf("PayloadStatusSSZ: validation error too long (%d > 1024)", errLen)
		}
		p.ValidationError = string(buf[errOffset:])
	}

	return p, nil
}

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

// ForkchoiceStateSSZ is the SSZ encoding of ForkchoiceState.
// Fixed layout: head_block_hash(32) + safe_block_hash(32) + finalized_block_hash(32) = 96 bytes
type ForkchoiceStateSSZ struct {
	HeadBlockHash      common.Hash
	SafeBlockHash      common.Hash
	FinalizedBlockHash common.Hash
}

func EncodeForkchoiceState(fcs *ForkChoiceState) []byte {
	buf := make([]byte, 96)
	copy(buf[0:32], fcs.HeadHash[:])
	copy(buf[32:64], fcs.SafeBlockHash[:])
	copy(buf[64:96], fcs.FinalizedBlockHash[:])
	return buf
}

func DecodeForkchoiceState(buf []byte) (*ForkChoiceState, error) {
	if len(buf) < 96 {
		return nil, fmt.Errorf("ForkchoiceState: buffer too short (%d < 96)", len(buf))
	}
	fcs := &ForkChoiceState{}
	copy(fcs.HeadHash[:], buf[0:32])
	copy(fcs.SafeBlockHash[:], buf[32:64])
	copy(fcs.FinalizedBlockHash[:], buf[64:96])
	return fcs, nil
}

// ForkchoiceUpdatedResponseSSZ is the SSZ-encoded forkchoice updated response.
//
// SSZ layout (fixed part = 8 bytes):
//   - payload_status_offset: 4 bytes (uint32 LE, points to variable PayloadStatusSSZ data)
//   - payload_id_offset:     4 bytes (uint32 LE, points to Union[None, uint64])
//
// Variable part:
//   - PayloadStatusSSZ data (variable length due to validation_error)
//   - Union[None, uint64]: selector(1) + uint64(8) if selector==1; selector(1) if selector==0
type ForkchoiceUpdatedResponseSSZ struct {
	PayloadStatus *PayloadStatusSSZ
	PayloadId     *uint64
}

const forkchoiceUpdatedResponseFixedSize = 8 // 4 + 4

func EncodeForkchoiceUpdatedResponse(resp *ForkChoiceUpdatedResponse) []byte {
	ps := PayloadStatusToSSZ(resp.PayloadStatus)
	psBytes := ps.EncodeSSZ()

	// Build Union[None, uint64] for payload ID
	var pidUnion []byte
	if resp.PayloadId != nil {
		pidUnion = make([]byte, 9) // selector(1) + uint64(8)
		pidUnion[0] = 1
		payloadIdBytes := []byte(*resp.PayloadId)
		if len(payloadIdBytes) == 8 {
			copy(pidUnion[1:9], payloadIdBytes)
		}
	} else {
		pidUnion = []byte{0} // selector(0) = None
	}

	buf := make([]byte, forkchoiceUpdatedResponseFixedSize+len(psBytes)+len(pidUnion))

	// Offset to PayloadStatus variable data (starts after fixed part)
	binary.LittleEndian.PutUint32(buf[0:4], uint32(forkchoiceUpdatedResponseFixedSize))
	// Offset to Union[None, uint64] (after PayloadStatus data)
	binary.LittleEndian.PutUint32(buf[4:8], uint32(forkchoiceUpdatedResponseFixedSize+len(psBytes)))

	// Variable part
	copy(buf[forkchoiceUpdatedResponseFixedSize:], psBytes)
	copy(buf[forkchoiceUpdatedResponseFixedSize+len(psBytes):], pidUnion)

	return buf
}

func DecodeForkchoiceUpdatedResponse(buf []byte) (*ForkchoiceUpdatedResponseSSZ, error) {
	if len(buf) < forkchoiceUpdatedResponseFixedSize {
		return nil, fmt.Errorf("ForkchoiceUpdatedResponseSSZ: buffer too short (%d < %d)", len(buf), forkchoiceUpdatedResponseFixedSize)
	}

	psOffset := binary.LittleEndian.Uint32(buf[0:4])
	pidOffset := binary.LittleEndian.Uint32(buf[4:8])

	if psOffset > uint32(len(buf)) || pidOffset > uint32(len(buf)) || psOffset > pidOffset {
		return nil, fmt.Errorf("ForkchoiceUpdatedResponseSSZ: offsets out of bounds")
	}

	resp := &ForkchoiceUpdatedResponseSSZ{}

	// Decode PayloadStatus from psOffset to pidOffset
	ps, err := DecodePayloadStatusSSZ(buf[psOffset:pidOffset])
	if err != nil {
		return nil, err
	}
	resp.PayloadStatus = ps

	// Decode Union[None, uint64] from pidOffset to end
	pidData := buf[pidOffset:]
	if len(pidData) > 0 {
		selector := pidData[0]
		if selector == 1 {
			if len(pidData) < 9 {
				return nil, fmt.Errorf("ForkchoiceUpdatedResponseSSZ: Union payload_id data too short")
			}
			pid := binary.BigEndian.Uint64(pidData[1:9])
			resp.PayloadId = &pid
		}
		// selector == 0 means None
	}

	return resp, nil
}

// CommunicationChannelSSZ is the SSZ container for a communication channel.
// Used by get_client_communication_channels.
//
// SSZ layout (fixed part):
//   - protocol_offset: 4 bytes
//   - url_offset:      4 bytes
//
// Variable part:
//   - protocol: List[uint8, 32]
//   - url:      List[uint8, 256]
type CommunicationChannelSSZ struct {
	Protocol string
	URL      string
}

func EncodeCommunicationChannels(channels []CommunicationChannel) []byte {
	if len(channels) == 0 {
		return []byte{}
	}

	// Encode as a simple length-prefixed list of channels
	// Each channel: protocol_len(4) + protocol + url_len(4) + url
	var totalSize int
	for _, ch := range channels {
		totalSize += 4 + len(ch.Protocol) + 4 + len(ch.URL)
	}

	buf := make([]byte, 4+totalSize) // count(4) + channels
	binary.LittleEndian.PutUint32(buf[0:4], uint32(len(channels)))

	offset := 4
	for _, ch := range channels {
		protBytes := []byte(ch.Protocol)
		urlBytes := []byte(ch.URL)

		binary.LittleEndian.PutUint32(buf[offset:offset+4], uint32(len(protBytes)))
		offset += 4
		copy(buf[offset:], protBytes)
		offset += len(protBytes)

		binary.LittleEndian.PutUint32(buf[offset:offset+4], uint32(len(urlBytes)))
		offset += 4
		copy(buf[offset:], urlBytes)
		offset += len(urlBytes)
	}

	return buf
}

func DecodeCommunicationChannels(buf []byte) ([]CommunicationChannel, error) {
	if len(buf) < 4 {
		return nil, fmt.Errorf("CommunicationChannels: buffer too short")
	}

	count := binary.LittleEndian.Uint32(buf[0:4])
	if count > 16 {
		return nil, fmt.Errorf("CommunicationChannels: too many channels (%d > 16)", count)
	}

	channels := make([]CommunicationChannel, 0, count)
	offset := uint32(4)

	for i := uint32(0); i < count; i++ {
		if offset+4 > uint32(len(buf)) {
			return nil, fmt.Errorf("CommunicationChannels: unexpected end of buffer")
		}
		protLen := binary.LittleEndian.Uint32(buf[offset : offset+4])
		offset += 4
		if protLen > 32 || offset+protLen > uint32(len(buf)) {
			return nil, fmt.Errorf("CommunicationChannels: protocol too long or truncated")
		}
		protocol := string(buf[offset : offset+protLen])
		offset += protLen

		if offset+4 > uint32(len(buf)) {
			return nil, fmt.Errorf("CommunicationChannels: unexpected end of buffer")
		}
		urlLen := binary.LittleEndian.Uint32(buf[offset : offset+4])
		offset += 4
		if urlLen > 256 || offset+urlLen > uint32(len(buf)) {
			return nil, fmt.Errorf("CommunicationChannels: URL too long or truncated")
		}
		url := string(buf[offset : offset+urlLen])
		offset += urlLen

		channels = append(channels, CommunicationChannel{
			Protocol: protocol,
			URL:      url,
		})
	}

	return channels, nil
}

// ExchangeCapabilitiesSSZ encodes/decodes a list of capability strings for SSZ transport.
func EncodeCapabilities(capabilities []string) []byte {
	// count(4) + for each: len(4) + bytes
	var totalSize int
	for _, cap := range capabilities {
		totalSize += 4 + len(cap)
	}

	buf := make([]byte, 4+totalSize)
	binary.LittleEndian.PutUint32(buf[0:4], uint32(len(capabilities)))

	offset := 4
	for _, cap := range capabilities {
		capBytes := []byte(cap)
		binary.LittleEndian.PutUint32(buf[offset:offset+4], uint32(len(capBytes)))
		offset += 4
		copy(buf[offset:], capBytes)
		offset += len(capBytes)
	}

	return buf
}

func DecodeCapabilities(buf []byte) ([]string, error) {
	if len(buf) < 4 {
		return nil, fmt.Errorf("Capabilities: buffer too short")
	}

	count := binary.LittleEndian.Uint32(buf[0:4])
	if count > 128 {
		return nil, fmt.Errorf("Capabilities: too many capabilities (%d > 128)", count)
	}

	capabilities := make([]string, 0, count)
	offset := uint32(4)

	for i := uint32(0); i < count; i++ {
		if offset+4 > uint32(len(buf)) {
			return nil, fmt.Errorf("Capabilities: unexpected end of buffer")
		}
		capLen := binary.LittleEndian.Uint32(buf[offset : offset+4])
		offset += 4
		if capLen > 64 || offset+capLen > uint32(len(buf)) {
			return nil, fmt.Errorf("Capabilities: capability too long or truncated")
		}
		capabilities = append(capabilities, string(buf[offset:offset+capLen]))
		offset += capLen
	}

	return capabilities, nil
}

// ClientVersionSSZ encodes/decodes a ClientVersionV1 for SSZ transport.
func EncodeClientVersion(cv *ClientVersionV1) []byte {
	codeBytes := []byte(cv.Code)
	nameBytes := []byte(cv.Name)
	versionBytes := []byte(cv.Version)
	commitBytes := []byte(cv.Commit)

	// code_len(4) + code + name_len(4) + name + version_len(4) + version + commit_len(4) + commit
	totalLen := 4 + len(codeBytes) + 4 + len(nameBytes) + 4 + len(versionBytes) + 4 + len(commitBytes)
	buf := make([]byte, totalLen)

	offset := 0
	binary.LittleEndian.PutUint32(buf[offset:offset+4], uint32(len(codeBytes)))
	offset += 4
	copy(buf[offset:], codeBytes)
	offset += len(codeBytes)

	binary.LittleEndian.PutUint32(buf[offset:offset+4], uint32(len(nameBytes)))
	offset += 4
	copy(buf[offset:], nameBytes)
	offset += len(nameBytes)

	binary.LittleEndian.PutUint32(buf[offset:offset+4], uint32(len(versionBytes)))
	offset += 4
	copy(buf[offset:], versionBytes)
	offset += len(versionBytes)

	binary.LittleEndian.PutUint32(buf[offset:offset+4], uint32(len(commitBytes)))
	offset += 4
	copy(buf[offset:], commitBytes)

	return buf
}

func DecodeClientVersion(buf []byte) (*ClientVersionV1, error) {
	if len(buf) < 16 { // minimum: 4 length fields
		return nil, fmt.Errorf("ClientVersion: buffer too short")
	}

	cv := &ClientVersionV1{}
	offset := uint32(0)

	readString := func(maxLen uint32) (string, error) {
		if offset+4 > uint32(len(buf)) {
			return "", fmt.Errorf("ClientVersion: unexpected end of buffer")
		}
		strLen := binary.LittleEndian.Uint32(buf[offset : offset+4])
		offset += 4
		if strLen > maxLen || offset+strLen > uint32(len(buf)) {
			return "", fmt.Errorf("ClientVersion: string too long or truncated")
		}
		s := string(buf[offset : offset+strLen])
		offset += strLen
		return s, nil
	}

	var err error
	if cv.Code, err = readString(8); err != nil {
		return nil, err
	}
	if cv.Name, err = readString(64); err != nil {
		return nil, err
	}
	if cv.Version, err = readString(64); err != nil {
		return nil, err
	}
	if cv.Commit, err = readString(64); err != nil {
		return nil, err
	}

	return cv, nil
}

// EncodeClientVersions encodes a list of ClientVersionV1 for SSZ transport.
func EncodeClientVersions(versions []ClientVersionV1) []byte {
	var parts [][]byte
	for i := range versions {
		parts = append(parts, EncodeClientVersion(&versions[i]))
	}

	// count(4) + for each: len(4) + encoded
	totalLen := 4
	for _, p := range parts {
		totalLen += 4 + len(p)
	}

	buf := make([]byte, totalLen)
	binary.LittleEndian.PutUint32(buf[0:4], uint32(len(versions)))

	offset := 4
	for _, p := range parts {
		binary.LittleEndian.PutUint32(buf[offset:offset+4], uint32(len(p)))
		offset += 4
		copy(buf[offset:], p)
		offset += len(p)
	}

	return buf
}

// DecodeClientVersions decodes a list of ClientVersionV1 from SSZ bytes.
func DecodeClientVersions(buf []byte) ([]ClientVersionV1, error) {
	if len(buf) < 4 {
		return nil, fmt.Errorf("ClientVersions: buffer too short")
	}

	count := binary.LittleEndian.Uint32(buf[0:4])
	if count > 16 {
		return nil, fmt.Errorf("ClientVersions: too many versions (%d > 16)", count)
	}

	versions := make([]ClientVersionV1, 0, count)
	offset := uint32(4)

	for i := uint32(0); i < count; i++ {
		if offset+4 > uint32(len(buf)) {
			return nil, fmt.Errorf("ClientVersions: unexpected end of buffer")
		}
		cvLen := binary.LittleEndian.Uint32(buf[offset : offset+4])
		offset += 4
		if offset+cvLen > uint32(len(buf)) {
			return nil, fmt.Errorf("ClientVersions: truncated")
		}
		cv, err := DecodeClientVersion(buf[offset : offset+cvLen])
		if err != nil {
			return nil, err
		}
		versions = append(versions, *cv)
		offset += cvLen
	}

	return versions, nil
}

// engineVersionToPayloadVersion maps Engine API versions to ExecutionPayload SSZ versions.
// Engine V4 = Electra, which reuses the Deneb payload layout (version 3).
// Engine V5 = Gloas, which adds slot_number + block_access_list (version 4).
func engineVersionToPayloadVersion(engineVersion int) int {
	if engineVersion == 4 {
		return 3 // Electra uses Deneb payload layout
	}
	if engineVersion >= 5 {
		return 4 // Gloas and beyond use the extended layout
	}
	return engineVersion
}

// --- ExecutionPayload SSZ encoding/decoding ---
//
// The ExecutionPayload SSZ encoding follows the Ethereum consensus specs SSZ container layout.
// Fields are version-dependent:
//   - V1 (Bellatrix): base fields
//   - V2 (Capella): + withdrawals
//   - V3 (Deneb): + blob_gas_used, excess_blob_gas
//   - V4 (Gloas): + slot_number, block_access_list
//
// The SSZ container has a fixed part (with offsets for variable-length fields)
// followed by a variable part containing the actual variable-length data.

// executionPayloadFixedSize returns the fixed part size for a given version.
func executionPayloadFixedSize(version int) int {
	// Base (V1/Bellatrix): parent_hash(32) + fee_recipient(20) + state_root(32) +
	// receipts_root(32) + logs_bloom(256) + prev_randao(32) + block_number(8) +
	// gas_limit(8) + gas_used(8) + timestamp(8) + extra_data_offset(4) +
	// base_fee_per_gas(32) + block_hash(32) + transactions_offset(4) = 508
	size := 508
	if version >= 2 {
		size += 4 // withdrawals_offset
	}
	if version >= 3 {
		size += 8 + 8 // blob_gas_used + excess_blob_gas
	}
	if version >= 4 {
		size += 8 + 4 // slot_number + block_access_list_offset
	}
	return size
}

// uint256ToSSZBytes converts a big.Int to 32-byte little-endian SSZ representation.
func uint256ToSSZBytes(val *big.Int) []byte {
	buf := make([]byte, 32)
	if val == nil {
		return buf
	}
	b := val.Bytes() // big-endian, minimal
	// Copy into buf in reverse (little-endian)
	for i, v := range b {
		buf[len(b)-1-i] = v
	}
	return buf
}

// sszBytesToUint256 converts 32-byte little-endian SSZ bytes to a big.Int.
func sszBytesToUint256(buf []byte) *big.Int {
	// Convert from little-endian to big-endian
	be := make([]byte, 32)
	for i := 0; i < 32; i++ {
		be[31-i] = buf[i]
	}
	return new(big.Int).SetBytes(be)
}

// encodeTransactionsSSZ encodes a list of transactions as an SSZ list of variable-length items.
// Layout: N offsets (4 bytes each) followed by transaction data.
func encodeTransactionsSSZ(txs []hexutil.Bytes) []byte {
	if len(txs) == 0 {
		return nil
	}
	// Calculate total size
	offsetsSize := len(txs) * 4
	dataSize := 0
	for _, tx := range txs {
		dataSize += len(tx)
	}
	buf := make([]byte, offsetsSize+dataSize)

	// Write offsets (relative to start of this list data)
	dataStart := offsetsSize
	for i, tx := range txs {
		binary.LittleEndian.PutUint32(buf[i*4:(i+1)*4], uint32(dataStart))
		dataStart += len(tx)
	}
	// Write transaction data
	pos := offsetsSize
	for _, tx := range txs {
		copy(buf[pos:], tx)
		pos += len(tx)
	}
	return buf
}

// decodeTransactionsSSZ decodes an SSZ-encoded list of variable-length transactions.
func decodeTransactionsSSZ(buf []byte) ([]hexutil.Bytes, error) {
	if len(buf) == 0 {
		return nil, nil
	}
	if len(buf) < 4 {
		return nil, fmt.Errorf("transactions SSZ: buffer too short")
	}
	// The first offset tells us how many offsets there are
	firstOffset := binary.LittleEndian.Uint32(buf[0:4])
	if firstOffset%4 != 0 {
		return nil, fmt.Errorf("transactions SSZ: first offset not aligned (%d)", firstOffset)
	}
	count := firstOffset / 4
	if count == 0 {
		return nil, nil
	}
	if firstOffset > uint32(len(buf)) {
		return nil, fmt.Errorf("transactions SSZ: first offset out of bounds")
	}

	// Read all offsets
	offsets := make([]uint32, count)
	for i := uint32(0); i < count; i++ {
		offsets[i] = binary.LittleEndian.Uint32(buf[i*4 : (i+1)*4])
	}

	txs := make([]hexutil.Bytes, count)
	for i := uint32(0); i < count; i++ {
		start := offsets[i]
		var end uint32
		if i+1 < count {
			end = offsets[i+1]
		} else {
			end = uint32(len(buf))
		}
		if start > uint32(len(buf)) || end > uint32(len(buf)) || start > end {
			return nil, fmt.Errorf("transactions SSZ: invalid offset at index %d", i)
		}
		tx := make(hexutil.Bytes, end-start)
		copy(tx, buf[start:end])
		txs[i] = tx
	}
	return txs, nil
}

// SSZ Withdrawal layout: index(8) + validator_index(8) + address(20) + amount(8) = 44 bytes
const withdrawalSSZSize = 44

func encodeWithdrawalsSSZ(withdrawals []*types.Withdrawal) []byte {
	if withdrawals == nil {
		return nil
	}
	buf := make([]byte, len(withdrawals)*withdrawalSSZSize)
	for i, w := range withdrawals {
		off := i * withdrawalSSZSize
		binary.LittleEndian.PutUint64(buf[off:off+8], w.Index)
		binary.LittleEndian.PutUint64(buf[off+8:off+16], w.Validator)
		copy(buf[off+16:off+36], w.Address[:])
		binary.LittleEndian.PutUint64(buf[off+36:off+44], w.Amount)
	}
	return buf
}

func decodeWithdrawalsSSZ(buf []byte) ([]*types.Withdrawal, error) {
	if len(buf) == 0 {
		return []*types.Withdrawal{}, nil
	}
	if len(buf)%withdrawalSSZSize != 0 {
		return nil, fmt.Errorf("withdrawals SSZ: buffer length %d not divisible by %d", len(buf), withdrawalSSZSize)
	}
	count := len(buf) / withdrawalSSZSize
	withdrawals := make([]*types.Withdrawal, count)
	for i := 0; i < count; i++ {
		off := i * withdrawalSSZSize
		withdrawals[i] = &types.Withdrawal{
			Index:     binary.LittleEndian.Uint64(buf[off : off+8]),
			Validator: binary.LittleEndian.Uint64(buf[off+8 : off+16]),
			Amount:    binary.LittleEndian.Uint64(buf[off+36 : off+44]),
		}
		copy(withdrawals[i].Address[:], buf[off+16:off+36])
	}
	return withdrawals, nil
}

// EncodeExecutionPayloadSSZ encodes an ExecutionPayload to SSZ bytes.
// The version parameter determines which fields are included:
//
//	1=Bellatrix, 2=Capella, 3=Deneb, 4=Gloas
func EncodeExecutionPayloadSSZ(ep *ExecutionPayload, version int) []byte {
	fixedSize := executionPayloadFixedSize(version)

	// Prepare variable-length field data
	extraData := []byte(ep.ExtraData)
	txData := encodeTransactionsSSZ(ep.Transactions)
	var withdrawalData []byte
	if version >= 2 {
		withdrawalData = encodeWithdrawalsSSZ(ep.Withdrawals)
	}
	var blockAccessListData []byte
	if version >= 4 {
		blockAccessListData = []byte(ep.BlockAccessList)
	}

	totalVarSize := len(extraData) + len(txData)
	if version >= 2 {
		totalVarSize += len(withdrawalData)
	}
	if version >= 4 {
		totalVarSize += len(blockAccessListData)
	}

	buf := make([]byte, fixedSize+totalVarSize)
	pos := 0

	// Fixed fields
	copy(buf[pos:pos+32], ep.ParentHash[:])
	pos += 32
	copy(buf[pos:pos+20], ep.FeeRecipient[:])
	pos += 20
	copy(buf[pos:pos+32], ep.StateRoot[:])
	pos += 32
	copy(buf[pos:pos+32], ep.ReceiptsRoot[:])
	pos += 32
	// LogsBloom is always 256 bytes
	if len(ep.LogsBloom) >= 256 {
		copy(buf[pos:pos+256], ep.LogsBloom[:256])
	}
	pos += 256
	copy(buf[pos:pos+32], ep.PrevRandao[:])
	pos += 32
	binary.LittleEndian.PutUint64(buf[pos:pos+8], uint64(ep.BlockNumber))
	pos += 8
	binary.LittleEndian.PutUint64(buf[pos:pos+8], uint64(ep.GasLimit))
	pos += 8
	binary.LittleEndian.PutUint64(buf[pos:pos+8], uint64(ep.GasUsed))
	pos += 8
	binary.LittleEndian.PutUint64(buf[pos:pos+8], uint64(ep.Timestamp))
	pos += 8

	// extra_data offset
	extraDataOffset := fixedSize
	binary.LittleEndian.PutUint32(buf[pos:pos+4], uint32(extraDataOffset))
	pos += 4

	// base_fee_per_gas (uint256, 32 bytes LE)
	var baseFee *big.Int
	if ep.BaseFeePerGas != nil {
		baseFee = ep.BaseFeePerGas.ToInt()
	}
	copy(buf[pos:pos+32], uint256ToSSZBytes(baseFee))
	pos += 32

	copy(buf[pos:pos+32], ep.BlockHash[:])
	pos += 32

	// transactions offset
	txOffset := extraDataOffset + len(extraData)
	binary.LittleEndian.PutUint32(buf[pos:pos+4], uint32(txOffset))
	pos += 4

	if version >= 2 {
		// withdrawals offset
		wdOffset := txOffset + len(txData)
		binary.LittleEndian.PutUint32(buf[pos:pos+4], uint32(wdOffset))
		pos += 4
	}

	if version >= 3 {
		var blobGasUsed, excessBlobGas uint64
		if ep.BlobGasUsed != nil {
			blobGasUsed = uint64(*ep.BlobGasUsed)
		}
		if ep.ExcessBlobGas != nil {
			excessBlobGas = uint64(*ep.ExcessBlobGas)
		}
		binary.LittleEndian.PutUint64(buf[pos:pos+8], blobGasUsed)
		pos += 8
		binary.LittleEndian.PutUint64(buf[pos:pos+8], excessBlobGas)
		pos += 8
	}

	if version >= 4 {
		var slotNumber uint64
		if ep.SlotNumber != nil {
			slotNumber = uint64(*ep.SlotNumber)
		}
		binary.LittleEndian.PutUint64(buf[pos:pos+8], slotNumber)
		pos += 8

		// block_access_list offset
		balOffset := extraDataOffset + len(extraData) + len(txData)
		if version >= 2 {
			balOffset += len(withdrawalData)
		}
		binary.LittleEndian.PutUint32(buf[pos:pos+4], uint32(balOffset))
		pos += 4
	}

	// Variable part
	copy(buf[extraDataOffset:], extraData)
	copy(buf[txOffset:], txData)
	if version >= 2 {
		wdOffset := txOffset + len(txData)
		copy(buf[wdOffset:], withdrawalData)
	}
	if version >= 4 {
		balOffset := extraDataOffset + len(extraData) + len(txData)
		if version >= 2 {
			balOffset += len(withdrawalData)
		}
		copy(buf[balOffset:], blockAccessListData)
	}

	return buf
}

// DecodeExecutionPayloadSSZ decodes SSZ bytes into an ExecutionPayload.
func DecodeExecutionPayloadSSZ(buf []byte, version int) (*ExecutionPayload, error) {
	fixedSize := executionPayloadFixedSize(version)
	if len(buf) < fixedSize {
		return nil, fmt.Errorf("ExecutionPayload SSZ: buffer too short (%d < %d)", len(buf), fixedSize)
	}

	ep := &ExecutionPayload{}
	pos := 0

	copy(ep.ParentHash[:], buf[pos:pos+32])
	pos += 32
	copy(ep.FeeRecipient[:], buf[pos:pos+20])
	pos += 20
	copy(ep.StateRoot[:], buf[pos:pos+32])
	pos += 32
	copy(ep.ReceiptsRoot[:], buf[pos:pos+32])
	pos += 32
	ep.LogsBloom = make(hexutil.Bytes, 256)
	copy(ep.LogsBloom, buf[pos:pos+256])
	pos += 256
	copy(ep.PrevRandao[:], buf[pos:pos+32])
	pos += 32
	ep.BlockNumber = hexutil.Uint64(binary.LittleEndian.Uint64(buf[pos : pos+8]))
	pos += 8
	ep.GasLimit = hexutil.Uint64(binary.LittleEndian.Uint64(buf[pos : pos+8]))
	pos += 8
	ep.GasUsed = hexutil.Uint64(binary.LittleEndian.Uint64(buf[pos : pos+8]))
	pos += 8
	ep.Timestamp = hexutil.Uint64(binary.LittleEndian.Uint64(buf[pos : pos+8]))
	pos += 8

	extraDataOffset := binary.LittleEndian.Uint32(buf[pos : pos+4])
	pos += 4

	baseFee := sszBytesToUint256(buf[pos : pos+32])
	ep.BaseFeePerGas = (*hexutil.Big)(baseFee)
	pos += 32

	copy(ep.BlockHash[:], buf[pos:pos+32])
	pos += 32

	txOffset := binary.LittleEndian.Uint32(buf[pos : pos+4])
	pos += 4

	var wdOffset uint32
	if version >= 2 {
		wdOffset = binary.LittleEndian.Uint32(buf[pos : pos+4])
		pos += 4
	}

	if version >= 3 {
		blobGasUsed := hexutil.Uint64(binary.LittleEndian.Uint64(buf[pos : pos+8]))
		ep.BlobGasUsed = &blobGasUsed
		pos += 8
		excessBlobGas := hexutil.Uint64(binary.LittleEndian.Uint64(buf[pos : pos+8]))
		ep.ExcessBlobGas = &excessBlobGas
		pos += 8
	}

	var balOffset uint32
	if version >= 4 {
		slotNumber := hexutil.Uint64(binary.LittleEndian.Uint64(buf[pos : pos+8]))
		ep.SlotNumber = &slotNumber
		pos += 8
		balOffset = binary.LittleEndian.Uint32(buf[pos : pos+4])
		pos += 4
	}

	// Decode variable-length fields using offsets
	// extra_data: from extraDataOffset to txOffset
	if extraDataOffset > uint32(len(buf)) || txOffset > uint32(len(buf)) || extraDataOffset > txOffset {
		return nil, fmt.Errorf("ExecutionPayload SSZ: invalid extra_data/transactions offsets")
	}
	ep.ExtraData = make(hexutil.Bytes, txOffset-extraDataOffset)
	copy(ep.ExtraData, buf[extraDataOffset:txOffset])

	// Determine end of transactions
	var txEnd uint32
	if version >= 2 {
		txEnd = wdOffset
	} else {
		txEnd = uint32(len(buf))
	}
	if txOffset > txEnd {
		return nil, fmt.Errorf("ExecutionPayload SSZ: transactions offset > end")
	}

	// Decode transactions
	txBuf := buf[txOffset:txEnd]
	txs, err := decodeTransactionsSSZ(txBuf)
	if err != nil {
		return nil, fmt.Errorf("ExecutionPayload SSZ: %w", err)
	}
	ep.Transactions = txs
	if ep.Transactions == nil {
		ep.Transactions = []hexutil.Bytes{}
	}

	// Decode withdrawals
	if version >= 2 {
		var wdEnd uint32
		if version >= 4 {
			wdEnd = balOffset
		} else {
			wdEnd = uint32(len(buf))
		}
		if wdOffset > wdEnd || wdEnd > uint32(len(buf)) {
			return nil, fmt.Errorf("ExecutionPayload SSZ: invalid withdrawals offset")
		}
		wds, err := decodeWithdrawalsSSZ(buf[wdOffset:wdEnd])
		if err != nil {
			return nil, fmt.Errorf("ExecutionPayload SSZ: %w", err)
		}
		ep.Withdrawals = wds
	}

	// Decode block access list
	if version >= 4 {
		if balOffset > uint32(len(buf)) {
			return nil, fmt.Errorf("ExecutionPayload SSZ: block_access_list offset out of bounds")
		}
		ep.BlockAccessList = make(hexutil.Bytes, uint32(len(buf))-balOffset)
		copy(ep.BlockAccessList, buf[balOffset:])
	}

	return ep, nil
}

// --- NewPayload request SSZ encoding/decoding ---
//
// V1/V2: The request body is just the SSZ-encoded ExecutionPayload.
//
// V3 NewPayloadRequest SSZ container:
//   Fixed part: ep_offset(4) + blob_hashes_offset(4) + parent_beacon_block_root(32) = 40 bytes
//   Variable: ExecutionPayload data, blob hashes (N * 32 bytes)
//
// V4 NewPayloadRequest SSZ container:
//   Fixed part: ep_offset(4) + blob_hashes_offset(4) + parent_beacon_block_root(32) + requests_offset(4) = 44 bytes
//   Variable: ExecutionPayload data, blob hashes, execution requests

// EncodeNewPayloadRequestSSZ encodes a newPayload request to SSZ.
func EncodeNewPayloadRequestSSZ(
	ep *ExecutionPayload,
	blobHashes []common.Hash,
	parentBeaconBlockRoot *common.Hash,
	executionRequests []hexutil.Bytes,
	version int,
) []byte {
	payloadVersion := engineVersionToPayloadVersion(version)
	if version <= 2 {
		return EncodeExecutionPayloadSSZ(ep, payloadVersion)
	}

	epBytes := EncodeExecutionPayloadSSZ(ep, payloadVersion)
	blobHashBytes := make([]byte, len(blobHashes)*32)
	for i, h := range blobHashes {
		copy(blobHashBytes[i*32:(i+1)*32], h[:])
	}

	if version == 3 {
		fixedSize := 40 // ep_offset(4) + blob_hashes_offset(4) + parent_beacon_block_root(32)
		buf := make([]byte, fixedSize+len(epBytes)+len(blobHashBytes))

		// ep offset
		binary.LittleEndian.PutUint32(buf[0:4], uint32(fixedSize))
		// blob hashes offset
		binary.LittleEndian.PutUint32(buf[4:8], uint32(fixedSize+len(epBytes)))
		// parent beacon block root
		if parentBeaconBlockRoot != nil {
			copy(buf[8:40], parentBeaconBlockRoot[:])
		}
		// Variable
		copy(buf[fixedSize:], epBytes)
		copy(buf[fixedSize+len(epBytes):], blobHashBytes)
		return buf
	}

	// V4+
	// Encode execution requests as structured SSZ Container for Prysm compatibility
	reqBytes := encodeStructuredExecutionRequestsSSZ(executionRequests)

	fixedSize := 44 // ep_offset(4) + blob_hashes_offset(4) + parent_beacon_block_root(32) + requests_offset(4)
	buf := make([]byte, fixedSize+len(epBytes)+len(blobHashBytes)+len(reqBytes))

	binary.LittleEndian.PutUint32(buf[0:4], uint32(fixedSize))
	binary.LittleEndian.PutUint32(buf[4:8], uint32(fixedSize+len(epBytes)))
	if parentBeaconBlockRoot != nil {
		copy(buf[8:40], parentBeaconBlockRoot[:])
	}
	binary.LittleEndian.PutUint32(buf[40:44], uint32(fixedSize+len(epBytes)+len(blobHashBytes)))

	copy(buf[fixedSize:], epBytes)
	copy(buf[fixedSize+len(epBytes):], blobHashBytes)
	copy(buf[fixedSize+len(epBytes)+len(blobHashBytes):], reqBytes)
	return buf
}

// DecodeNewPayloadRequestSSZ decodes a newPayload request from SSZ.
func DecodeNewPayloadRequestSSZ(buf []byte, version int) (
	ep *ExecutionPayload,
	blobHashes []common.Hash,
	parentBeaconBlockRoot *common.Hash,
	executionRequests []hexutil.Bytes,
	err error,
) {
	payloadVersion := engineVersionToPayloadVersion(version)
	if version <= 2 {
		ep, err = DecodeExecutionPayloadSSZ(buf, payloadVersion)
		return
	}

	if version == 3 {
		if len(buf) < 40 {
			err = fmt.Errorf("NewPayloadV3 SSZ: buffer too short (%d < 40)", len(buf))
			return
		}
		epOffset := binary.LittleEndian.Uint32(buf[0:4])
		blobHashOffset := binary.LittleEndian.Uint32(buf[4:8])
		root := common.BytesToHash(buf[8:40])
		parentBeaconBlockRoot = &root

		if epOffset > uint32(len(buf)) || blobHashOffset > uint32(len(buf)) || epOffset > blobHashOffset {
			err = fmt.Errorf("NewPayloadV3 SSZ: invalid offsets")
			return
		}
		ep, err = DecodeExecutionPayloadSSZ(buf[epOffset:blobHashOffset], payloadVersion)
		if err != nil {
			return
		}
		blobHashBuf := buf[blobHashOffset:]
		if len(blobHashBuf)%32 != 0 {
			err = fmt.Errorf("NewPayloadV3 SSZ: blob hashes not aligned")
			return
		}
		blobHashes = make([]common.Hash, len(blobHashBuf)/32)
		for i := range blobHashes {
			copy(blobHashes[i][:], blobHashBuf[i*32:(i+1)*32])
		}
		return
	}

	// V4+
	if len(buf) < 44 {
		err = fmt.Errorf("NewPayloadV4 SSZ: buffer too short (%d < 44)", len(buf))
		return
	}
	epOffset := binary.LittleEndian.Uint32(buf[0:4])
	blobHashOffset := binary.LittleEndian.Uint32(buf[4:8])
	root := common.BytesToHash(buf[8:40])
	parentBeaconBlockRoot = &root
	reqOffset := binary.LittleEndian.Uint32(buf[40:44])

	if epOffset > uint32(len(buf)) || blobHashOffset > uint32(len(buf)) || reqOffset > uint32(len(buf)) {
		err = fmt.Errorf("NewPayloadV4 SSZ: offsets out of bounds")
		return
	}
	ep, err = DecodeExecutionPayloadSSZ(buf[epOffset:blobHashOffset], payloadVersion)
	if err != nil {
		return
	}
	blobHashBuf := buf[blobHashOffset:reqOffset]
	if len(blobHashBuf)%32 != 0 {
		err = fmt.Errorf("NewPayloadV4 SSZ: blob hashes not aligned")
		return
	}
	blobHashes = make([]common.Hash, len(blobHashBuf)/32)
	for i := range blobHashes {
		copy(blobHashes[i][:], blobHashBuf[i*32:(i+1)*32])
	}

	executionRequests, err = decodeStructuredExecutionRequestsSSZ(buf[reqOffset:])
	return
}

// encodeExecutionRequestsSSZ encodes execution requests as SSZ list of variable items.
func encodeExecutionRequestsSSZ(reqs []hexutil.Bytes) []byte {
	if len(reqs) == 0 {
		return nil
	}
	offsetsSize := len(reqs) * 4
	dataSize := 0
	for _, r := range reqs {
		dataSize += len(r)
	}
	buf := make([]byte, offsetsSize+dataSize)
	dataStart := offsetsSize
	for i, r := range reqs {
		binary.LittleEndian.PutUint32(buf[i*4:(i+1)*4], uint32(dataStart))
		dataStart += len(r)
	}
	pos := offsetsSize
	for _, r := range reqs {
		copy(buf[pos:], r)
		pos += len(r)
	}
	return buf
}

func decodeExecutionRequestsSSZ(buf []byte) ([]hexutil.Bytes, error) {
	if len(buf) == 0 {
		return nil, nil
	}
	if len(buf) < 4 {
		return nil, fmt.Errorf("execution requests SSZ: buffer too short")
	}
	firstOffset := binary.LittleEndian.Uint32(buf[0:4])
	if firstOffset%4 != 0 || firstOffset > uint32(len(buf)) {
		return nil, fmt.Errorf("execution requests SSZ: invalid first offset")
	}
	count := firstOffset / 4
	offsets := make([]uint32, count)
	for i := uint32(0); i < count; i++ {
		offsets[i] = binary.LittleEndian.Uint32(buf[i*4 : (i+1)*4])
	}
	reqs := make([]hexutil.Bytes, count)
	for i := uint32(0); i < count; i++ {
		start := offsets[i]
		var end uint32
		if i+1 < count {
			end = offsets[i+1]
		} else {
			end = uint32(len(buf))
		}
		if start > uint32(len(buf)) || end > uint32(len(buf)) || start > end {
			return nil, fmt.Errorf("execution requests SSZ: invalid offset at index %d", i)
		}
		r := make(hexutil.Bytes, end-start)
		copy(r, buf[start:end])
		reqs[i] = r
	}
	return reqs, nil
}

// encodeStructuredExecutionRequestsSSZ encodes execution requests as a structured SSZ Container
// that Prysm can UnmarshalSSZ. The container has 3 offsets (deposits, withdrawals, consolidations)
// followed by the raw SSZ data for each list.
//
// Container layout:
//
//	Fixed: deposits_offset(4) + withdrawals_offset(4) + consolidations_offset(4) = 12 bytes
//	Variable: deposits_ssz + withdrawals_ssz + consolidations_ssz
//
// The input flat format is []hexutil.Bytes where each item is: type_byte + ssz_data
func encodeStructuredExecutionRequestsSSZ(reqs []hexutil.Bytes) []byte {
	var depositsData, withdrawalsData, consolidationsData []byte

	for _, r := range reqs {
		if len(r) < 1 {
			continue
		}
		switch r[0] {
		case 0x00: // deposits
			depositsData = append(depositsData, r[1:]...)
		case 0x01: // withdrawals
			withdrawalsData = append(withdrawalsData, r[1:]...)
		case 0x02: // consolidations
			consolidationsData = append(consolidationsData, r[1:]...)
		}
	}

	fixedSize := 12 // 3 offsets * 4 bytes
	totalVar := len(depositsData) + len(withdrawalsData) + len(consolidationsData)
	buf := make([]byte, fixedSize+totalVar)

	depositsOffset := fixedSize
	withdrawalsOffset := depositsOffset + len(depositsData)
	consolidationsOffset := withdrawalsOffset + len(withdrawalsData)

	binary.LittleEndian.PutUint32(buf[0:4], uint32(depositsOffset))
	binary.LittleEndian.PutUint32(buf[4:8], uint32(withdrawalsOffset))
	binary.LittleEndian.PutUint32(buf[8:12], uint32(consolidationsOffset))

	copy(buf[depositsOffset:], depositsData)
	copy(buf[withdrawalsOffset:], withdrawalsData)
	copy(buf[consolidationsOffset:], consolidationsData)

	return buf
}

// decodeStructuredExecutionRequestsSSZ decodes a structured SSZ Container of execution requests
// into the flat format used by Erigon ([]hexutil.Bytes where each item is type_byte + ssz_data).
func decodeStructuredExecutionRequestsSSZ(buf []byte) ([]hexutil.Bytes, error) {
	if len(buf) == 0 {
		return []hexutil.Bytes{}, nil
	}
	if len(buf) < 12 {
		return nil, fmt.Errorf("structured execution requests SSZ: buffer too short (%d < 12)", len(buf))
	}

	depositsOffset := binary.LittleEndian.Uint32(buf[0:4])
	withdrawalsOffset := binary.LittleEndian.Uint32(buf[4:8])
	consolidationsOffset := binary.LittleEndian.Uint32(buf[8:12])

	if depositsOffset > uint32(len(buf)) || withdrawalsOffset > uint32(len(buf)) || consolidationsOffset > uint32(len(buf)) {
		return nil, fmt.Errorf("structured execution requests SSZ: offsets out of bounds")
	}
	if depositsOffset > withdrawalsOffset || withdrawalsOffset > consolidationsOffset {
		return nil, fmt.Errorf("structured execution requests SSZ: offsets not in order")
	}

	// Always return non-nil slice (engine requires non-nil for V4+ even if empty).
	reqs := make([]hexutil.Bytes, 0, 3)

	// Deposits (type 0x00)
	depositsData := buf[depositsOffset:withdrawalsOffset]
	if len(depositsData) > 0 {
		r := make(hexutil.Bytes, 1+len(depositsData))
		r[0] = 0x00
		copy(r[1:], depositsData)
		reqs = append(reqs, r)
	}

	// Withdrawals (type 0x01)
	withdrawalsData := buf[withdrawalsOffset:consolidationsOffset]
	if len(withdrawalsData) > 0 {
		r := make(hexutil.Bytes, 1+len(withdrawalsData))
		r[0] = 0x01
		copy(r[1:], withdrawalsData)
		reqs = append(reqs, r)
	}

	// Consolidations (type 0x02)
	consolidationsData := buf[consolidationsOffset:]
	if len(consolidationsData) > 0 {
		r := make(hexutil.Bytes, 1+len(consolidationsData))
		r[0] = 0x02
		copy(r[1:], consolidationsData)
		reqs = append(reqs, r)
	}

	return reqs, nil
}

// --- GetPayload response SSZ encoding ---
//
// V1: The response body is just the SSZ-encoded ExecutionPayload.
//
// V2+ GetPayloadResponse SSZ container:
//   Fixed part: ep_offset(4) + block_value(32) + blobs_bundle_offset(4) +
//               should_override_builder(1) + requests_offset(4) = 45 bytes
//   Variable: ExecutionPayload, BlobsBundle, ExecutionRequests

const getPayloadResponseFixedSize = 45

// EncodeGetPayloadResponseSSZ encodes a GetPayloadResponse to SSZ.
func EncodeGetPayloadResponseSSZ(resp *GetPayloadResponse, version int) []byte {
	if version == 1 {
		return EncodeExecutionPayloadSSZ(resp.ExecutionPayload, 1)
	}

	payloadVersion := engineVersionToPayloadVersion(version)
	epBytes := EncodeExecutionPayloadSSZ(resp.ExecutionPayload, payloadVersion)
	blobsBytes := encodeBlobsBundleSSZ(resp.BlobsBundle)
	reqBytes := encodeStructuredExecutionRequestsSSZ(resp.ExecutionRequests)

	buf := make([]byte, getPayloadResponseFixedSize+len(epBytes)+len(blobsBytes)+len(reqBytes))

	// ep offset
	binary.LittleEndian.PutUint32(buf[0:4], uint32(getPayloadResponseFixedSize))

	// block_value (uint256 LE)
	if resp.BlockValue != nil {
		copy(buf[4:36], uint256ToSSZBytes(resp.BlockValue.ToInt()))
	}

	// blobs_bundle offset
	blobsOffset := getPayloadResponseFixedSize + len(epBytes)
	binary.LittleEndian.PutUint32(buf[36:40], uint32(blobsOffset))

	// should_override_builder
	if resp.ShouldOverrideBuilder {
		buf[40] = 1
	}

	// execution_requests offset
	reqOffset := blobsOffset + len(blobsBytes)
	binary.LittleEndian.PutUint32(buf[41:45], uint32(reqOffset))

	// Variable data
	copy(buf[getPayloadResponseFixedSize:], epBytes)
	copy(buf[blobsOffset:], blobsBytes)
	copy(buf[reqOffset:], reqBytes)

	return buf
}

// DecodeGetPayloadResponseSSZ decodes SSZ bytes into a GetPayloadResponse.
func DecodeGetPayloadResponseSSZ(buf []byte, version int) (*GetPayloadResponse, error) {
	if version == 1 {
		ep, err := DecodeExecutionPayloadSSZ(buf, 1)
		if err != nil {
			return nil, err
		}
		return &GetPayloadResponse{ExecutionPayload: ep}, nil
	}

	if len(buf) < getPayloadResponseFixedSize {
		return nil, fmt.Errorf("GetPayloadResponse SSZ: buffer too short (%d < %d)", len(buf), getPayloadResponseFixedSize)
	}

	resp := &GetPayloadResponse{}

	epOffset := binary.LittleEndian.Uint32(buf[0:4])
	blockValue := sszBytesToUint256(buf[4:36])
	resp.BlockValue = (*hexutil.Big)(blockValue)
	blobsOffset := binary.LittleEndian.Uint32(buf[36:40])
	resp.ShouldOverrideBuilder = buf[40] == 1
	reqOffset := binary.LittleEndian.Uint32(buf[41:45])

	// Decode ExecutionPayload
	if epOffset > uint32(len(buf)) || blobsOffset > uint32(len(buf)) {
		return nil, fmt.Errorf("GetPayloadResponse SSZ: offsets out of bounds")
	}
	payloadVersion := engineVersionToPayloadVersion(version)
	ep, err := DecodeExecutionPayloadSSZ(buf[epOffset:blobsOffset], payloadVersion)
	if err != nil {
		return nil, err
	}
	resp.ExecutionPayload = ep

	// Decode BlobsBundle
	if blobsOffset > reqOffset || reqOffset > uint32(len(buf)) {
		return nil, fmt.Errorf("GetPayloadResponse SSZ: invalid blobs/requests offsets")
	}
	bundle, err := decodeBlobsBundleSSZ(buf[blobsOffset:reqOffset])
	if err != nil {
		return nil, err
	}
	resp.BlobsBundle = bundle

	// Decode ExecutionRequests
	if reqOffset < uint32(len(buf)) {
		reqs, err := decodeStructuredExecutionRequestsSSZ(buf[reqOffset:])
		if err != nil {
			return nil, err
		}
		resp.ExecutionRequests = reqs
	}

	return resp, nil
}

// --- BlobsBundle SSZ encoding ---
//
// SSZ container:
//   Fixed part: commitments_offset(4) + proofs_offset(4) + blobs_offset(4) = 12 bytes
//   Variable: commitments (N*48), proofs (N*48), blobs (N*131072)

const blobsBundleFixedSize = 12

func encodeBlobsBundleSSZ(bundle *BlobsBundle) []byte {
	if bundle == nil {
		return nil
	}

	commitmentsData := encodeFixedSizeList(bundle.Commitments)
	proofsData := encodeFixedSizeList(bundle.Proofs)
	blobsData := encodeFixedSizeList(bundle.Blobs)

	totalVar := len(commitmentsData) + len(proofsData) + len(blobsData)
	buf := make([]byte, blobsBundleFixedSize+totalVar)

	commitmentsOffset := blobsBundleFixedSize
	proofsOffset := commitmentsOffset + len(commitmentsData)
	blobsOffset := proofsOffset + len(proofsData)

	binary.LittleEndian.PutUint32(buf[0:4], uint32(commitmentsOffset))
	binary.LittleEndian.PutUint32(buf[4:8], uint32(proofsOffset))
	binary.LittleEndian.PutUint32(buf[8:12], uint32(blobsOffset))

	copy(buf[commitmentsOffset:], commitmentsData)
	copy(buf[proofsOffset:], proofsData)
	copy(buf[blobsOffset:], blobsData)

	return buf
}

func decodeBlobsBundleSSZ(buf []byte) (*BlobsBundle, error) {
	if len(buf) == 0 {
		return nil, nil
	}
	if len(buf) < blobsBundleFixedSize {
		return nil, fmt.Errorf("BlobsBundle SSZ: buffer too short")
	}

	commitmentsOffset := binary.LittleEndian.Uint32(buf[0:4])
	proofsOffset := binary.LittleEndian.Uint32(buf[4:8])
	blobsOffset := binary.LittleEndian.Uint32(buf[8:12])

	if commitmentsOffset > uint32(len(buf)) || proofsOffset > uint32(len(buf)) || blobsOffset > uint32(len(buf)) {
		return nil, fmt.Errorf("BlobsBundle SSZ: offsets out of bounds")
	}

	bundle := &BlobsBundle{}

	// Commitments (each 48 bytes)
	commBuf := buf[commitmentsOffset:proofsOffset]
	if len(commBuf) > 0 {
		if len(commBuf)%48 != 0 {
			return nil, fmt.Errorf("BlobsBundle SSZ: commitments not aligned to 48 bytes")
		}
		bundle.Commitments = make([]hexutil.Bytes, len(commBuf)/48)
		for i := range bundle.Commitments {
			c := make(hexutil.Bytes, 48)
			copy(c, commBuf[i*48:(i+1)*48])
			bundle.Commitments[i] = c
		}
	}

	// Proofs (each 48 bytes)
	proofBuf := buf[proofsOffset:blobsOffset]
	if len(proofBuf) > 0 {
		if len(proofBuf)%48 != 0 {
			return nil, fmt.Errorf("BlobsBundle SSZ: proofs not aligned to 48 bytes")
		}
		bundle.Proofs = make([]hexutil.Bytes, len(proofBuf)/48)
		for i := range bundle.Proofs {
			p := make(hexutil.Bytes, 48)
			copy(p, proofBuf[i*48:(i+1)*48])
			bundle.Proofs[i] = p
		}
	}

	// Blobs (each 131072 bytes)
	blobBuf := buf[blobsOffset:]
	if len(blobBuf) > 0 {
		if len(blobBuf)%131072 != 0 {
			return nil, fmt.Errorf("BlobsBundle SSZ: blobs not aligned to 131072 bytes")
		}
		bundle.Blobs = make([]hexutil.Bytes, len(blobBuf)/131072)
		for i := range bundle.Blobs {
			b := make(hexutil.Bytes, 131072)
			copy(b, blobBuf[i*131072:(i+1)*131072])
			bundle.Blobs[i] = b
		}
	}

	return bundle, nil
}

// encodeFixedSizeList concatenates a list of byte slices.
func encodeFixedSizeList(items []hexutil.Bytes) []byte {
	totalLen := 0
	for _, item := range items {
		totalLen += len(item)
	}
	buf := make([]byte, totalLen)
	pos := 0
	for _, item := range items {
		copy(buf[pos:], item)
		pos += len(item)
	}
	return buf
}

// EncodeGetBlobsRequest encodes a list of versioned hashes for the get_blobs SSZ request.
func EncodeGetBlobsRequest(hashes []common.Hash) []byte {
	buf := make([]byte, 4+len(hashes)*32)
	binary.LittleEndian.PutUint32(buf[0:4], uint32(len(hashes)))
	for i, h := range hashes {
		copy(buf[4+i*32:4+(i+1)*32], h[:])
	}
	return buf
}

// DecodeGetBlobsRequest decodes a list of versioned hashes from SSZ bytes.
func DecodeGetBlobsRequest(buf []byte) ([]common.Hash, error) {
	if len(buf) < 4 {
		return nil, fmt.Errorf("GetBlobsRequest: buffer too short")
	}
	count := binary.LittleEndian.Uint32(buf[0:4])
	if 4+count*32 > uint32(len(buf)) {
		return nil, fmt.Errorf("GetBlobsRequest: buffer too short for %d hashes", count)
	}
	hashes := make([]common.Hash, count)
	for i := uint32(0); i < count; i++ {
		copy(hashes[i][:], buf[4+i*32:4+(i+1)*32])
	}
	return hashes, nil
}
