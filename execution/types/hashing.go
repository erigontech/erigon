// Copyright 2014 The go-ethereum Authors
// (original work)
// Copyright 2024 The Erigon Authors
// (modifications)
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

package types

import (
	"bytes"
	"io"

	"github.com/erigontech/erigon/common"
	"github.com/erigontech/erigon/common/crypto"
	"github.com/erigontech/erigon/common/empty"
	"github.com/erigontech/erigon/execution/commitment/trie"
	"github.com/erigontech/erigon/execution/rlp"
)

type DerivableList interface {
	Len() int
	EncodeIndex(i int, w *bytes.Buffer)
}

func DeriveSha(list DerivableList) common.Hash {
	s := NewListHashStream()
	for range list.Len() {
		s.Update(list)
	}
	return s.Hash()
}

type bytesWriter interface {
	WriteByte(byte) error
}

// hexTapeWriter hex-encodes data and writes it directly to a tape.
type hexWriter struct {
	w io.ByteWriter
}

func (w *hexWriter) WriteByte(b byte) error {
	if err := w.w.WriteByte(b / 16); err != nil {
		return err
	}
	return w.w.WriteByte(b % 16)
}

func (w *hexWriter) Commit() error {
	return w.w.WriteByte(16)
}

func retain(_ []byte) bool {
	return false
}

func encodeUint(i uint, buffer bytesWriter) {
	if i == 0 {
		_ = buffer.WriteByte(byte(rlp.EmptyStringCode))
		return
	}

	if i < 128 {
		_ = buffer.WriteByte(byte(i))
		return
	}

	size := intsize(i)
	_ = buffer.WriteByte(rlp.EmptyStringCode + byte(size))
	for j := 1; j <= size; j++ {
		shift := uint((size - j) * 8)
		w := byte(i >> shift)
		_ = buffer.WriteByte(w)
	}
}

// intsize computes the minimum number of bytes required to store i.
func intsize(i uint) (size int) {
	for size = 1; ; size++ {
		if i >>= 8; i == 0 {
			return size
		}
	}
}

func RawRlpHash(rawRlpData rlp.RawValue) common.Hash {
	return crypto.Keccak256Hash(rawRlpData)
}

// TransactionHashFromEncoding returns the hash of a transaction in any encoding DecodeTransaction accepts.
func TransactionHashFromEncoding(enc []byte) (common.Hash, error) {
	binary, err := BinaryFromStoredTxn(enc)
	if err != nil {
		return common.Hash{}, err
	}
	return crypto.Keccak256Hash(binary), nil
}

func RlpHash(x any) common.Hash {
	sha := crypto.NewKeccakState()
	rlp.Encode(sha, x) //nolint:errcheck
	h := crypto.FinalizeHash(sha)
	crypto.ReturnToPool(sha)
	return h
}

// prefixSlices contains one-byte slices for all possible prefix values (0–255).
// Each entry is pre-allocated once during init so we can write a single prefix
// byte into the hasher without creating a new slice every time.
// This avoids per-call heap allocations when hashing prefixed payloads.
var prefixSlices [256][]byte

func init() {
	for i := range prefixSlices {
		prefixSlices[i] = []byte{byte(i)}
	}
}

// prefixedRlpHash writes the prefix into the hasher before rlp-encoding the
// given interface. It's used for typed transactions.
func prefixedRlpHash(prefix byte, x any) common.Hash {
	sha := crypto.NewKeccakState()
	defer crypto.ReturnToPool(sha)
	sha.Write(prefixSlices[prefix]) //nolint:errcheck
	if err := rlp.Encode(sha, x); err != nil {
		panic(err)
	}
	return crypto.FinalizeHash(sha)
}

// rlpPayloadHash hashes keccak256 of whatever encode writes, using a pooled
// hasher and scratch buffer so callers avoid the reflection-based RlpHash.
func rlpPayloadHash(encode func(w io.Writer, buf []byte) error) common.Hash {
	sha := crypto.NewKeccakState()
	defer crypto.ReturnToPool(sha)
	buf := rlp.NewEncodingBuf()
	defer buf.Release()
	if err := encode(sha, buf[:]); err != nil {
		panic(err)
	}
	return crypto.FinalizeHash(sha)
}

// prefixedPayloadHash hashes keccak256(prefix || payload).
func prefixedPayloadHash(prefix byte, encode func(w io.Writer, buf []byte) error) common.Hash {
	return rlpPayloadHash(func(w io.Writer, buf []byte) error {
		if _, err := w.Write(prefixSlices[prefix]); err != nil {
			return err
		}
		return encode(w, buf)
	})
}

type ListHashStream struct {
	hb                       *trie.HashBuilder
	groups, branches, hashes []uint16
	leaf                     trie.GenStructStepLeafData
	key, nextKey             *bytes.Buffer
	hex                      hexWriter
	val, nextVal             *bytes.Buffer
	first                    bytes.Buffer
	count                    int
	root                     common.Hash
	hashed                   bool
}

func NewListHashStream() *ListHashStream {
	return &ListHashStream{
		hb:      trie.NewHashBuilder(false),
		key:     new(bytes.Buffer),
		nextKey: new(bytes.Buffer),
		val:     new(bytes.Buffer),
		nextVal: new(bytes.Buffer),
	}
}

func (s *ListHashStream) Update(list DerivableList) {
	if s.hashed {
		panic("ListHashStream: Update after Hash")
	}
	i := s.count
	s.count++
	if i == 0 {
		list.EncodeIndex(0, &s.first)
		return
	}
	s.nextVal.Reset()
	list.EncodeIndex(i, s.nextVal)
	s.push(i)
	if i == 0x7f {
		s.pushFirst()
	}
}

func (s *ListHashStream) Hash() common.Hash {
	if s.hashed {
		return s.root
	}
	hash := empty.RootHash
	if s.count > 0 {
		if s.count <= 0x7f {
			s.pushFirst()
		}
		s.step(nil)
		hash, _ = s.hb.RootHash()
	}
	s.root, s.hashed = hash, true
	return hash
}

func (s *ListHashStream) pushFirst() {
	s.nextVal = &s.first
	s.push(0)
}

func (s *ListHashStream) push(i int) {
	s.nextKey.Reset()
	s.hex.w = s.nextKey
	encodeUint(uint(i), &s.hex)
	_ = s.hex.Commit()
	if s.key.Len() > 0 {
		s.step(s.nextKey.Bytes())
	}
	s.key, s.nextKey = s.nextKey, s.key
	s.val, s.nextVal = s.nextVal, s.val
}

func (s *ListHashStream) step(succ []byte) {
	s.leaf.Value = rlp.RlpEncodedBytes(s.val.Bytes())
	s.groups, s.branches, s.hashes, _ = trie.GenStructStep(retain, s.key.Bytes(), succ, s.hb, nil, &s.leaf, s.groups, s.branches, s.hashes, false)
}
