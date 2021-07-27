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

type NewPooledTransactionHashesPacket [][32]byte

// ParseHashesCount looks at the RLP length ParsePrefix for list of 32-byte hashes
// and returns number of hashes in the list to expect
func ParseHashesCount(payload Hashes, pos int) (int, int, error) {
	payloadLen := len(payload)
	dataPos, dataLen, list, err := rlp.ParsePrefix(payload, pos)
	if err != nil {
		return 0, 0, fmt.Errorf("%s: hashes len: %w", rlp.ParseHashErrorPrefix, err)
	}
	if !list {
		return 0, 0, fmt.Errorf("%s: hashes must be a list, not string", rlp.ParseHashErrorPrefix)
	}
	if dataPos+dataLen > payloadLen {
		return 0, 0, fmt.Errorf("%s: unexpected end of payload after hashes", rlp.ParseHashErrorPrefix)
	}
	if dataLen%33 != 0 {
		return 0, 0, fmt.Errorf("%s: hashes len must be multiple of 33", rlp.ParseHashErrorPrefix)
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
	encodeBuf = ensureEnoughSize(encodeBuf, prefixLen+dataLen)
	rlp.EncodeListPrefix(hashesLen, encodeBuf)
	pos := prefixLen
	for i := 0; i < len(hashes); i += 32 {
		rlp.EncodeHash(hashes[i:i+32], encodeBuf[pos:])
		pos += 33
	}
	return encodeBuf, nil
}

func ensureEnoughSize(in []byte, size int) []byte {
	if cap(in) < size {
		return make([]byte, size)
	}
	return in[:size] // Reuse the space in pkbuf, is it has enough capacity
}

// EncodeGetPooledTransactions66 produces encoding of GetPooledTransactions66 packet
func EncodeGetPooledTransactions66(hashes []byte, requestId uint64, encodeBuf []byte) ([]byte, error) {
	hashesLen := len(hashes) / 32 * 33
	dataLen := rlp.ListPrefixLen(hashesLen) + hashesLen + rlp.U64Len(requestId)
	prefixLen := rlp.ListPrefixLen(dataLen)
	encodeBuf = ensureEnoughSize(encodeBuf, prefixLen+dataLen)
	// Length ParsePrefix for the entire structure
	rlp.EncodeListPrefix(dataLen, encodeBuf)
	pos := prefixLen
	// encode requestId
	rlp.EncodeU64(requestId, encodeBuf[pos:])
	pos += rlp.U64Len(requestId)
	// Encode length ParsePrefix for hashes
	rlp.EncodeListPrefix(hashesLen, encodeBuf[pos:])
	pos += rlp.ListPrefixLen(hashesLen)

	for i := 0; i < len(hashes); i += 32 {
		rlp.EncodeHash(hashes[i:i+32], encodeBuf[pos:pos+33])
		pos += 33
	}
	return encodeBuf, nil
}
