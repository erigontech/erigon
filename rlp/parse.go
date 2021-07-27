package rlp

import (
	"fmt"

	"github.com/holiman/uint256"
)

// ParseBeInt parses Big Endian representation of an integer from given payload at given position
func ParseBeInt(payload []byte, pos, length int) (int, error) {
	var r int
	if length > 0 && payload[pos] == 0 {
		return 0, fmt.Errorf("integer encoding for RLP must not have leading zeros: %x", payload[pos:pos+length])
	}
	for _, b := range payload[pos : pos+length] {
		r = (r << 8) | int(b)
	}
	return r, nil
}

// ParsePrefix parses RLP ParsePrefix from given payload at given position. It returns the offset and length of the RLP element
// as well as the indication of whether it is a list of string
func ParsePrefix(payload []byte, pos int) (dataPos int, dataLen int, list bool, err error) {
	switch first := payload[pos]; {
	case first < 128:
		dataPos = pos
		dataLen = 1
		list = false
	case first < 184:
		// string of len < 56, and it is non-legacy transaction
		dataPos = pos + 1
		dataLen = int(first) - 128
		list = false
	case first < 192:
		// string of len >= 56, and it is non-legacy transaction
		beLen := int(first) - 183
		dataPos = pos + 1 + beLen
		dataLen, err = ParseBeInt(payload, pos+1, beLen)
		list = false
	case first < 248:
		// list of len < 56, and it is a legacy transaction
		dataPos = pos + 1
		dataLen = int(first) - 192
		list = true
	default:
		// list of len >= 56, and it is a legacy transaction
		beLen := int(first) - 247
		dataPos = pos + 1 + beLen
		dataLen, err = ParseBeInt(payload, pos+1, beLen)
		list = true
	}
	return
}

// ParseUint64 parses uint64 number from given payload at given position
func ParseUint64(payload []byte, pos int) (int, uint64, error) {
	dataPos, dataLen, list, err := ParsePrefix(payload, pos)
	if err != nil {
		return 0, 0, err
	}
	if list {
		return 0, 0, fmt.Errorf("uint64 must be a string, not list")
	}
	if dataPos+dataLen >= len(payload) {
		return 0, 0, fmt.Errorf("unexpected end of payload")
	}
	if dataLen > 8 {
		return 0, 0, fmt.Errorf("uint64 must not be more than 8 bytes long, got %d", dataLen)
	}
	if dataLen > 0 && payload[dataPos] == 0 {
		return 0, 0, fmt.Errorf("integer encoding for RLP must not have leading zeros: %x", payload[dataPos:dataPos+dataLen])
	}
	var r uint64
	for _, b := range payload[dataPos : dataPos+dataLen] {
		r = (r << 8) | uint64(b)
	}
	return dataPos + dataLen, r, nil
}

// ParseUint256 parses uint256 number from given payload at given position
func ParseUint256(payload []byte, pos int, x *uint256.Int) (int, error) {
	dataPos, dataLen, list, err := ParsePrefix(payload, pos)
	if err != nil {
		return 0, err
	}
	if list {
		return 0, fmt.Errorf("uint256 must be a string, not list")
	}
	if dataPos+dataLen > len(payload) {
		return 0, fmt.Errorf("unexpected end of payload")
	}
	if dataLen > 32 {
		return 0, fmt.Errorf("uint256 must not be more than 8 bytes long, got %d", dataLen)
	}
	if dataLen > 0 && payload[dataPos] == 0 {
		return 0, fmt.Errorf("integer encoding for RLP must not have leading zeros: %x", payload[dataPos:dataPos+dataLen])
	}
	x.SetBytes(payload[dataPos : dataPos+dataLen])
	return dataPos + dataLen, nil
}

// ParseHash extracts the next hash from the RLP encoding (payload) from a given position.
// It appends the hash to the given slice, reusing the space if there is enough capacity
// The first returned value is the slice where hash is appended to.
// The second returned value is the new position in the RLP payload after the extraction
// of the hash.
func ParseHash(payload []byte, pos int, hashbuf []byte) ([]byte, int, error) {
	payloadLen := len(payload)
	dataPos, dataLen, list, err := ParsePrefix(payload, pos)
	if err != nil {
		return nil, 0, fmt.Errorf("%s: hash len: %w", ParseHashErrorPrefix, err)
	}
	if list {
		return nil, 0, fmt.Errorf("%s: hash must be a string, not list", ParseHashErrorPrefix)
	}
	if dataPos+dataLen > payloadLen {
		return nil, 0, fmt.Errorf("%s: unexpected end of payload after hash", ParseHashErrorPrefix)
	}
	if dataLen != 32 {
		return nil, 0, fmt.Errorf("%s: hash must be 32 bytes long", ParseHashErrorPrefix)
	}
	var hash []byte
	if total := len(hashbuf) + 32; cap(hashbuf) >= total {
		hash = hashbuf[:32] // Reuse the space in pkbuf, is it has enough capacity
	} else {
		hash = make([]byte, total)
		copy(hash, hashbuf)
	}
	copy(hash, payload[dataPos:dataPos+dataLen])
	return hash, dataPos + dataLen, nil
}

const ParseHashErrorPrefix = "parse hash payload"
