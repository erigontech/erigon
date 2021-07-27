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
	"fmt"

	"github.com/ledgerwatch/erigon-lib/rlp"
)

const ParseHashErrorPrefix = "parse hash payload"

type NewPooledTransactionHashesPacket [][32]byte

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
func EncodeHashes(hashes Hashes, encodeBuf []byte) ([]byte, error) {
	hashesLen := len(hashes) / 32 * 33
	dataLen := hashesLen
	prefixLen := rlp.ListPrefixLen(hashesLen)
	var encoding []byte
	if total := prefixLen + dataLen; cap(encodeBuf) >= total {
		encoding = encodeBuf[:total] // Reuse the space in pkbuf, is it has enough capacity
	} else {
		encoding = make([]byte, total)
		copy(encoding, encodeBuf)
	}
	rlp.ListPrefix(hashesLen, encoding)
	pos := prefixLen
	for i := 0; i < len(hashes); i += 32 {
		rlp.EncodeHash(hashes[i:i+32], encoding[pos:])
		pos += 33
	}
	return encoding, nil
}

// EncodeGetPooledTransactions66 produces encoding of GetPooledTransactions66 packet
func EncodeGetPooledTransactions66(hashes []byte, requestId uint64, encodeBuf []byte) ([]byte, error) {
	hashesLen := len(hashes) / 32 * 33
	dataLen := rlp.ListPrefixLen(hashesLen) + hashesLen + rlp.U64Len(requestId)
	prefixLen := rlp.ListPrefixLen(dataLen)
	var encoding []byte
	if total := dataLen + prefixLen; cap(encodeBuf) >= total {
		encoding = encodeBuf[:total] // Reuse the space in pkbuf, is it has enough capacity
	} else {
		encoding = make([]byte, total)
		copy(encoding, encodeBuf)
	}
	// Length prefix for the entire structure
	rlp.ListPrefix(dataLen, encoding)
	pos := prefixLen
	// encode requestId
	rlp.U64(requestId, encoding[pos:])
	pos += rlp.U64Len(requestId)
	// Encode length prefix for hashes
	rlp.ListPrefix(hashesLen, encoding[pos:])
	pos += rlp.ListPrefixLen(hashesLen)

	for i := 0; i < len(hashes); i += 32 {
		rlp.EncodeHash(hashes[i:i+32], encoding[pos:pos+33])
		pos += 33
	}
	return encoding, nil
}
