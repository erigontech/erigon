/*
   Copyright 2021 Erigon contributors

   Licensed under the Apache License, Version 2.0 (the "License");
   you may not use this file except in compliance with the License.
   You may obtain a copy of the License at

       http://www.apache.org/licenses/LICENSE-2.0

   Unless required by applicable law or agreed to in writing, software
   distributed under the License is distributed on an "AS IS" BASIS,
   WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
   See the License for the specific language governing permissions and
   limitations under the License.
*/

package txpool

import (
	"encoding/binary"
	"fmt"
	"math/bits"
)

const ParseHashErrorPrefix = "parse hash payload"

// ParseHash extracts the next hash from the RLP encoding (payload) from a given position.
// It appends the hash to the given slice, reusing the space if there is enough capacity
// The first returned value is the slice where hash is appended to.
// The second returned value is the new position in the RLP payload after the extraction
// of the hash.
func ParseHash(payload []byte, pos int, hashbuf []byte) ([]byte, int, error) {
	payloadLen := len(payload)
	dataPos, dataLen, list, err := prefix(payload, pos)
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

// ParseHashesCount looks at the RLP length prefix for list of 32-byte hashes
// and returns number of hashes in the list to expect
func ParseHashesCount(payload []byte, pos int) (int, int, error) {
	payloadLen := len(payload)
	dataPos, dataLen, list, err := prefix(payload, pos)
	if err != nil {
		return 0, 0, fmt.Errorf("%s: hashes len: %w", ParseHashErrorPrefix, err)
	}
	if !list {
		return 0, 0, fmt.Errorf("%s: hashes must be a list, not string", ParseHashErrorPrefix)
	}
	if dataPos+dataLen > payloadLen {
		return 0, 0, fmt.Errorf("%s: unexpected end of payload after hashes", ParseHashErrorPrefix)
	}
	if dataLen%33 != 0 {
		return 0, 0, fmt.Errorf("%s: hashes len must be multiple of 33", ParseHashErrorPrefix)
	}
	return dataLen / 33, dataPos, nil
}

// EncodeHashes produces RLP encoding of given number of hashes, as RLP list
// It appends encoding to the given given slice (encodeBuf), reusing the space
// there is there is enough capacity.
// The first returned value is the slice where encodinfg
func EncodeHashes(hashes []byte, count int, encodeBuf []byte) ([]byte, error) {
	dataLen := count * 33
	var beLen int
	if dataLen >= 56 {
		beLen = (bits.Len64(uint64(dataLen)) + 7) / 8
	}
	prefixLen := 1 + beLen
	var encoding []byte
	if total := len(encodeBuf) + dataLen + prefixLen; cap(encodeBuf) >= total {
		encoding = encodeBuf[:dataLen+prefixLen] // Reuse the space in pkbuf, is it has enough capacity
	} else {
		encoding = make([]byte, total)
		copy(encoding, encodeBuf)
	}
	if dataLen < 56 {
		encoding[0] = 192 + byte(dataLen)
	} else {
		encoding[0] = 247 + byte(beLen)
		binary.BigEndian.PutUint64(encoding[1:], uint64(beLen))
		copy(encoding[1:], encoding[9-beLen:9])
	}
	hashP := 0
	encP := prefixLen
	for i := 0; i < count; i++ {
		encoding[encP] = 128 + 32
		copy(encoding[encP+1:encP+33], hashes[hashP:hashP+32])
		encP += 33
		hashP += 32
	}
	return encoding, nil
}

// EncodeGetPooledTransactions66 produces encoding of GetPooledTransactions66 packet
func EncodeGetPooledTransactions66(hashes []byte, count int, requestId uint64, encodeBuf []byte) ([]byte, error) {
	requestIdLen := (bits.Len64(requestId) + 7) / 8
	hashesLen := count * 33
	var hashesBeLen int
	if hashesLen >= 56 {
		hashesBeLen = (bits.Len64(uint64(hashesLen)) + 7) / 8
	}
	dataLen := requestIdLen + hashesLen + 1 + hashesBeLen
	if requestId == 0 || requestId >= 128 {
		dataLen++
	}
	var dataBeLen int
	if dataLen >= 56 {
		dataBeLen = (bits.Len64(uint64(dataLen)) + 7) / 8
	}
	prefixLen := 1 + dataBeLen
	var encoding []byte
	if total := len(encodeBuf) + dataLen + prefixLen; cap(encodeBuf) >= total {
		encoding = encodeBuf[:dataLen+prefixLen] // Reuse the space in pkbuf, is it has enough capacity
	} else {
		encoding = make([]byte, total)
		copy(encoding, encodeBuf)
	}
	pos := 0
	// Length prefix for the entire structure
	if dataLen < 56 {
		encoding[pos] = 192 + byte(dataLen)
	} else {
		encoding[pos] = 247 + byte(dataBeLen)
		binary.BigEndian.PutUint64(encoding[pos+1:], uint64(dataBeLen))
		copy(encoding[pos+1:], encoding[pos+9-dataBeLen:pos+9])
	}
	pos += prefixLen
	// encode requestId
	if requestId == 0 || requestId > 128 {
		encoding[pos] = 128 + byte(requestIdLen)
	} else {
		encoding[pos] = byte(requestId)
	}
	pos++
	if requestId > 128 {
		binary.BigEndian.PutUint64(encoding[pos:], requestId)
		copy(encoding[pos:], encoding[pos+8-requestIdLen:pos+8])
		pos += requestIdLen
	}
	// Encode length prefix for hashes
	if hashesLen < 56 {
		encoding[pos] = 192 + byte(hashesLen)
		pos++
	} else {
		encoding[pos] = 247 + byte(hashesBeLen)
		pos++
		binary.BigEndian.PutUint64(encoding[pos:], uint64(hashesBeLen))
		copy(encoding[pos:], encoding[pos+8-hashesBeLen:pos+8])
		pos += hashesBeLen
	}
	hashP := 0
	for i := 0; i < count; i++ {
		encoding[pos] = 128 + 32
		pos++
		copy(encoding[pos:pos+32], hashes[hashP:hashP+32])
		pos += 32
		hashP += 32
	}
	return encoding, nil
}
