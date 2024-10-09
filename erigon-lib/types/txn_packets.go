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

package types

import (
	"errors"
	"fmt"

	"github.com/ledgerwatch/erigon-lib/common"
	"github.com/ledgerwatch/erigon-lib/common/length"
	"github.com/ledgerwatch/erigon-lib/rlp"
)

type NewPooledTransactionHashesPacket [][length.Hash]byte

// ParseHashesCount looks at the RLP length Prefix for list of 32-byte hashes
// and returns number of hashes in the list to expect
func ParseHashesCount(payload []byte, pos int) (count int, dataPos int, err error) {
	dataPos, dataLen, err := rlp.List(payload, pos)
	if err != nil {
		return 0, 0, fmt.Errorf("%s: hashes len: %w", rlp.ParseHashErrorPrefix, err)
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
func EncodeHashes(hashes []byte, encodeBuf []byte) []byte {
	hashesLen := len(hashes) / length.Hash * 33
	dataLen := hashesLen
	encodeBuf = common.EnsureEnoughSize(encodeBuf, rlp.ListPrefixLen(hashesLen)+dataLen)
	rlp.EncodeHashes(hashes, encodeBuf)
	return encodeBuf
}

// ParseHash extracts the next hash from the RLP encoding (payload) from a given position.
// It appends the hash to the given slice, reusing the space if there is enough capacity
// The first returned value is the slice where hash is appended to.
// The second returned value is the new position in the RLP payload after the extraction
// of the hash.
func ParseHash(payload []byte, pos int, hashbuf []byte) ([]byte, int, error) {
	hashbuf = common.EnsureEnoughSize(hashbuf, length.Hash)
	pos, err := rlp.ParseHash(payload, pos, hashbuf)
	if err != nil {
		return nil, 0, fmt.Errorf("%s: hash len: %w", rlp.ParseHashErrorPrefix, err)
	}
	return hashbuf, pos, nil
}

// EncodeGetPooledTransactions66 produces encoding of GetPooledTransactions66 packet
func EncodeGetPooledTransactions66(hashes []byte, requestID uint64, encodeBuf []byte) ([]byte, error) {
	pos := 0
	hashesLen := len(hashes) / length.Hash * 33
	dataLen := rlp.ListPrefixLen(hashesLen) + hashesLen + rlp.U64Len(requestID)
	encodeBuf = common.EnsureEnoughSize(encodeBuf, rlp.ListPrefixLen(dataLen)+dataLen)
	// Length Prefix for the entire structure
	pos += rlp.EncodeListPrefix(dataLen, encodeBuf[pos:])
	pos += rlp.EncodeU64(requestID, encodeBuf[pos:])
	pos += rlp.EncodeHashes(hashes, encodeBuf[pos:])
	_ = pos
	return encodeBuf, nil
}

func ParseGetPooledTransactions66(payload []byte, pos int, hashbuf []byte) (requestID uint64, hashes []byte, newPos int, err error) {
	pos, _, err = rlp.List(payload, pos)
	if err != nil {
		return 0, hashes, 0, err
	}

	pos, requestID, err = rlp.U64(payload, pos)
	if err != nil {
		return 0, hashes, 0, err
	}
	var hashesCount int
	hashesCount, pos, err = ParseHashesCount(payload, pos)
	if err != nil {
		return 0, hashes, 0, err
	}
	hashes = common.EnsureEnoughSize(hashbuf, length.Hash*hashesCount)

	for i := 0; i < hashesCount; i++ {
		pos, err = rlp.ParseHash(payload, pos, hashes[i*length.Hash:])
		if err != nil {
			return 0, hashes, 0, err
		}
	}
	return requestID, hashes, pos, nil
}

// == Pooled transactions ==

// TODO(eip-4844) wrappedWithBlobs = true?
func EncodePooledTransactions66(txsRlp [][]byte, requestID uint64, encodeBuf []byte) []byte {
	pos := 0
	txsRlpLen := 0
	for i := range txsRlp {
		_, _, isLegacy, _ := rlp.Prefix(txsRlp[i], 0)
		if isLegacy {
			txsRlpLen += len(txsRlp[i])
		} else {
			txsRlpLen += rlp.StringLen(txsRlp[i])
		}
	}
	dataLen := rlp.U64Len(requestID) + rlp.ListPrefixLen(txsRlpLen) + txsRlpLen

	encodeBuf = common.EnsureEnoughSize(encodeBuf, rlp.ListPrefixLen(dataLen)+dataLen)

	// Length Prefix for the entire structure
	pos += rlp.EncodeListPrefix(dataLen, encodeBuf[pos:])
	pos += rlp.EncodeU64(requestID, encodeBuf[pos:])
	pos += rlp.EncodeListPrefix(txsRlpLen, encodeBuf[pos:])
	for i := range txsRlp {
		_, _, isLegacy, _ := rlp.Prefix(txsRlp[i], 0)
		if isLegacy {
			copy(encodeBuf[pos:], txsRlp[i])
			pos += len(txsRlp[i])
		} else {
			pos += rlp.EncodeString(txsRlp[i], encodeBuf[pos:])
		}
	}
	_ = pos
	return encodeBuf
}

// TODO(eip-4844) wrappedWithBlobs = false?
func EncodeTransactions(txsRlp [][]byte, encodeBuf []byte) []byte {
	pos := 0
	dataLen := 0
	for i := range txsRlp {
		_, _, isLegacy, _ := rlp.Prefix(txsRlp[i], 0)
		if isLegacy {
			dataLen += len(txsRlp[i])
		} else {
			dataLen += rlp.StringLen(txsRlp[i])
		}
	}

	encodeBuf = common.EnsureEnoughSize(encodeBuf, rlp.ListPrefixLen(dataLen)+dataLen)
	// Length Prefix for the entire structure
	pos += rlp.EncodeListPrefix(dataLen, encodeBuf[pos:])
	for i := range txsRlp {
		_, _, isLegacy, _ := rlp.Prefix(txsRlp[i], 0)
		if isLegacy {
			copy(encodeBuf[pos:], txsRlp[i])
			pos += len(txsRlp[i])
		} else {
			pos += rlp.EncodeString(txsRlp[i], encodeBuf[pos:])
		}
	}
	_ = pos
	return encodeBuf
}

func ParseTransactions(payload []byte, pos int, ctx *TxParseContext, txSlots *TxSlots, validateHash func([]byte) error) (newPos int, err error) {
	pos, _, err = rlp.List(payload, pos)
	if err != nil {
		return 0, err
	}

	for i := 0; pos < len(payload); i++ {
		txSlots.Resize(uint(i + 1))
		txSlots.Txs[i] = &TxSlot{}
		pos, err = ctx.ParseTransaction(payload, pos, txSlots.Txs[i], txSlots.Senders.At(i), true /* hasEnvelope */, true /* wrappedWithBlobs */, validateHash)
		if err != nil {
			if errors.Is(err, ErrRejected) {
				txSlots.Resize(uint(i))
				i--
				continue
			}
			return 0, err
		}
	}
	return pos, nil
}

func ParsePooledTransactions66(payload []byte, pos int, ctx *TxParseContext, txSlots *TxSlots, validateHash func([]byte) error) (requestID uint64, newPos int, err error) {
	p, _, err := rlp.List(payload, pos)
	if err != nil {
		return requestID, 0, err
	}
	p, requestID, err = rlp.U64(payload, p)
	if err != nil {
		return requestID, 0, err
	}
	p, _, err = rlp.List(payload, p)
	if err != nil {
		return requestID, 0, err
	}

	for i := 0; p < len(payload); i++ {
		txSlots.Resize(uint(i + 1))
		txSlots.Txs[i] = &TxSlot{}
		p, err = ctx.ParseTransaction(payload, p, txSlots.Txs[i], txSlots.Senders.At(i), true /* hasEnvelope */, true /* wrappedWithBlobs */, validateHash)
		if err != nil {
			if errors.Is(err, ErrRejected) {
				txSlots.Resize(uint(i))
				i--
				continue
			}
			return requestID, 0, err
		}
	}
	return requestID, p, nil
}
