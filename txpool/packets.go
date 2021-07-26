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
	dataLen := len(hashes) / 32 * 33
	prefixLen := rlpListPrefixLen(dataLen)
	var encoding []byte
	if total := dataLen + prefixLen; cap(encodeBuf) >= total {
		encoding = encodeBuf[:total] // Reuse the space in pkbuf, is it has enough capacity
	} else {
		encoding = make([]byte, total)
		copy(encoding, encodeBuf)
	}
	rlpListPrefix(dataLen, encoding)
	encP := prefixLen
	for i := 0; i < len(hashes); i += 32 {
		rlpEncodeString(hashes[i:i+32], encoding[encP:])
		encP += 33
	}
	return encoding, nil
}

func rlpListPrefixLen(dataLen int) int {
	if dataLen >= 56 {
		return 1 + (bits.Len64(uint64(dataLen))+7)/8
	}
	return 1
}
func rlpListPrefix(dataLen int, to []byte) {
	if dataLen >= 56 {
		_ = to[9]
		beLen := (bits.Len64(uint64(dataLen)) + 7) / 8
		binary.BigEndian.PutUint64(to[1:], uint64(dataLen))
		to[8-beLen] = 247 + byte(beLen)
		copy(to, to[8-beLen:9])
		return
	}
	to[0] = 192 + byte(dataLen)
}
func rlpU64Len(i uint64) int {
	if i > 128 {
		return 1 + (bits.Len64(i)+7)/8
	}
	return 1
}

func rlpU64(i uint64, to []byte) {
	/*
		if requestId == 0 || requestId > 128 {
			encoding[pos] = 128 + byte(requestIdLen)
		} else {
			encoding[pos] = byte(requestId)
		}
	*/

	if i > 128 {
		l := (bits.Len64(i) + 7) / 8
		to[0] = 128 + byte(l)
		binary.BigEndian.PutUint64(to[1:], i)
		copy(to[1:], to[1+8-l:1+8])
		return
	}
	if i == 0 {
		to[0] = 128
		return
	}
	to[0] = byte(i)
	return

	//if i == 0 {
	//	w.str = append(w.str, 0x80)
	//} else if i < 128 {
	//w.str = append(w.str, byte(i))
	//} else {
	//	s := putint(w.sizebuf[1:], i)
	//	w.sizebuf[0] = 0x80 + byte(s)
	//	w.str = append(w.str, w.sizebuf[:s+1]...)
	//}

}

func rlpEncodeString(s []byte, to []byte) {
	switch {
	case len(s) > 56:
		beLen := (bits.Len(uint(len(s))) + 7) / 8
		binary.BigEndian.PutUint64(to[1:], uint64(len(s)))
		_ = to[beLen+len(s)]

		to[8-beLen] = byte(beLen) + 183
		copy(to, to[8-beLen:9])
		copy(to[1+beLen:], s)
	case len(s) == 0:
		to[0] = 128
	case len(s) == 1:
		_ = to[1]
		if s[0] >= 128 {
			to[0] = 129
		}
		copy(to[1:], s)
	default: // 1<s<56
		_ = to[len(s)]
		to[0] = byte(len(s)) + 128
		copy(to[1:], s)
	}
}

// we know that it's 32bytes long, and we know that we have enough space
func rlpEncodeHash(h, to []byte) {
	_ = to[32] // early bounds check to guarantee safety of writes below
	to[0] = 128 + 32
	copy(to[1:33], h)
}

// EncodeGetPooledTransactions66 produces encoding of GetPooledTransactions66 packet
func EncodeGetPooledTransactions66(hashes []byte, requestId uint64, encodeBuf []byte) ([]byte, error) {
	requestIdLen := (bits.Len64(requestId) + 7) / 8
	hashesLen := len(hashes) / 32 * 33
	var hashesBeLen int
	if hashesLen >= 56 {
		hashesBeLen = (bits.Len64(uint64(hashesLen)) + 7) / 8
	}
	dataLen := requestIdLen + hashesLen + 1 + hashesBeLen
	if requestId == 0 || requestId >= 128 {
		dataLen++
	}
	prefixLen := rlpListPrefixLen(dataLen)
	var encoding []byte
	if total := dataLen + prefixLen; cap(encodeBuf) >= total {
		encoding = encodeBuf[:dataLen+prefixLen] // Reuse the space in pkbuf, is it has enough capacity
	} else {
		encoding = make([]byte, total)
		copy(encoding, encodeBuf)
	}
	// Length prefix for the entire structure
	rlpListPrefix(dataLen, encoding)
	pos := prefixLen
	// encode requestId
	rlpU64(requestId, encoding[pos:])
	//if requestId == 0 || requestId > 128 {
	//	encoding[pos] = 128 + byte(requestIdLen)
	//} else {
	//	encoding[pos] = byte(requestId)
	//}
	//pos++
	if requestId > 128 {
		//binary.BigEndian.PutUint64(encoding[pos:], requestId)
		//copy(encoding[pos:], encoding[pos+8-requestIdLen:pos+8])
		//pos += requestIdLen
	}
	fmt.Printf("aa: %d,%d,%d\n", requestId, rlpU64Len(requestId), requestIdLen)
	pos += rlpU64Len(requestId)
	// Encode length prefix for hashes
	rlpListPrefix(hashesLen, encoding[pos:])
	pos += rlpListPrefixLen(hashesLen)

	for i := 0; i < len(hashes); i += 32 {
		rlpEncodeHash(hashes[i:i+32], encoding[pos:pos+33])
		pos += 33
	}
	return encoding, nil
}
