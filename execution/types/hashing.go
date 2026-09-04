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
	"fmt"
	"io"

	"github.com/erigontech/erigon/common"
	"github.com/erigontech/erigon/common/crypto"
	"github.com/erigontech/erigon/execution/commitment/trie"
	"github.com/erigontech/erigon/execution/rlp"
)

type DerivableList interface {
	Len() int
	EncodeIndex(i int, w *bytes.Buffer)
}

func DeriveSha(list DerivableList) common.Hash {
	count := list.Len()
	if count < 1 {
		return trie.EmptyRoot
	}

	var value bytes.Buffer
	index := firstDerivationIndex(count)
	builder := newDeriveShaBuilder(index)
	for index >= 0 {
		value.Reset()
		list.EncodeIndex(index, &value)
		nextIndex := nextDerivationIndex(index, count)
		builder.addValue(value.Bytes(), nextIndex)
		index = nextIndex
	}

	return builder.root()
}

// DeriveShaRawTransactions derives a transaction root from the RLP payload of a transaction list.
func DeriveShaRawTransactions(encoded []byte) (common.Hash, error) {
	return deriveShaRawValues(encoded, true)
}

// DeriveShaRawValues derives an indexed trie root from an RLP list payload without materializing its values.
func DeriveShaRawValues(encoded []byte) (common.Hash, error) {
	return deriveShaRawValues(encoded, false)
}

func deriveShaRawValues(encoded []byte, unwrapStringValues bool) (common.Hash, error) {
	count, err := rlp.CountValues(encoded)
	if err != nil {
		return common.Hash{}, err
	}
	if count == 0 {
		return trie.EmptyRoot, nil
	}

	builder := newDeriveShaBuilder(firstDerivationIndex(count))
	indexAfterZero := nextDerivationIndex(0, count)

	// Raw values arrive in numeric order. Hold index 0 so the builder receives
	// RLP-encoded keys in lexicographic order: 1 through 127, then 0, then 128 onward.
	var zeroValue []byte
	for i := 0; len(encoded) > 0; i++ {
		kind, content, rest, err := rlp.Split(encoded)
		if err != nil {
			return common.Hash{}, err
		}
		value := encoded[:len(encoded)-len(rest)]
		if unwrapStringValues && kind != rlp.List {
			// Typed transactions are RLP strings in block bodies, but their trie
			// values exclude the string wrapper. Legacy transactions remain lists.
			value = content
		}

		if i == 0 {
			zeroValue = value
		} else {
			if i == indexAfterZero {
				builder.addValue(zeroValue, indexAfterZero)
			}
			builder.addValue(value, nextDerivationIndex(i, count))
		}
		encoded = rest
	}
	if indexAfterZero < 0 {
		builder.addValue(zeroValue, indexAfterZero)
	}

	return builder.root(), nil
}

// RLP-encoded trie indices sort as 1 through 127, then 0, then 128 and above.
func firstDerivationIndex(count int) int {
	if count <= 0 {
		return -1
	}
	if count == 1 {
		return 0
	}
	return 1
}

func nextDerivationIndex(index, count int) int {
	switch {
	case index == 0 && count > 128:
		return 128
	case index == 0:
		return -1
	case index < 127 && index < count-1:
		return index + 1
	case index <= 127:
		return 0
	case index < count-1:
		return index + 1
	}
	return -1
}

type deriveShaBuilder struct {
	currentKey  bytes.Buffer
	nextKey     bytes.Buffer
	hashBuilder *trie.HashBuilder
	keyWriter   hexWriter
	groups      []uint16
	branches    []uint16
	hashes      []uint16
	leafData    trie.GenStructStepLeafData
}

func newDeriveShaBuilder(firstIndex int) *deriveShaBuilder {
	builder := &deriveShaBuilder{hashBuilder: trie.NewHashBuilder(false)}
	builder.keyWriter.w = &builder.nextKey
	builder.hashBuilder.Reset()
	builder.prepareNextKey(firstIndex)
	return builder
}

// addValue inserts value under the prepared key and prepares nextIndex as its successor.
func (b *deriveShaBuilder) addValue(value []byte, nextIndex int) {
	b.currentKey.Reset()
	b.currentKey.Write(b.nextKey.Bytes())
	b.nextKey.Reset()
	b.prepareNextKey(nextIndex)
	if b.currentKey.Len() == 0 {
		return
	}

	b.leafData.Value = rlp.RlpEncodedBytes(value)
	b.groups, b.branches, b.hashes, _ = trie.GenStructStep(
		retain,
		b.currentKey.Bytes(),
		b.nextKey.Bytes(),
		b.hashBuilder,
		nil,
		&b.leafData,
		b.groups,
		b.branches,
		b.hashes,
		false,
	)
}

func (b *deriveShaBuilder) prepareNextKey(index int) {
	if index < 0 {
		return
	}
	encodeUint(uint(index), &b.keyWriter)
	if err := b.keyWriter.Commit(); err != nil {
		panic(fmt.Errorf("fatal in DeriveSha: %w", err))
	}
}

func (b *deriveShaBuilder) root() common.Hash {
	hash, _ := b.hashBuilder.RootHash()
	return hash
}

type bytesWriter interface {
	WriteByte(byte) error
}

// hexWriter writes bytes as trie-key nibbles; Commit appends the leaf terminator.
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
