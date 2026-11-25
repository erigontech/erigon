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
	"runtime"
	"sync"
	"sync/atomic"

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
	n := list.Len()
	if n < 1 {
		return empty.RootHash
	}
	if n >= parallelDeriveShaMin && runtime.GOMAXPROCS(0) > 1 {
		return deriveShaParallel(list, n)
	}
	leafOf := newLeafEncoder(list)
	return trieRoot(n, func(j int, w *bytes.Buffer) { writeHexKey(lexIndex(j, n), w) }, func(j int) trie.GenStructStepData { return leafOf(lexIndex(j, n)) })
}

// deriveShaGroup indexes from a multiple of it share every key byte but the last,
// so each full group above the first is one subtrie.
const deriveShaGroup = 256

const parallelDeriveShaMin = 4 * deriveShaGroup

// deriveShaParallel hashes the full groups concurrently and joins them as hash
// nodes at their prefixes with the remaining leaves.
func deriveShaParallel(list DerivableList, n int) common.Hash {
	groups := n/deriveShaGroup - 1
	hashes := make([]trie.GenStructStepHashData, groups)
	var next atomic.Int64
	var wg sync.WaitGroup
	for range min(runtime.GOMAXPROCS(0), groups) {
		wg.Go(func() {
			leafOf := newLeafEncoder(list)
			for g := int(next.Add(1)) - 1; g < groups; g = int(next.Add(1)) - 1 {
				first := (g + 1) * deriveShaGroup
				hashes[g].Hash = trieRoot(deriveShaGroup, func(j int, w *bytes.Buffer) {
					w.Write([]byte{byte(j >> 4), byte(j & 0xf), 16})
				}, func(j int) trie.GenStructStepData { return leafOf(first + j) })
			}
		})
	}
	wg.Wait()
	tail := (groups + 1) * deriveShaGroup
	// The entry at a group position stands for that whole group.
	index := func(j int) (int, bool) {
		switch {
		case j < deriveShaGroup:
			return lexIndex(j, n), false
		case j < deriveShaGroup+groups:
			return (j - deriveShaGroup + 1) * deriveShaGroup, true
		default:
			return tail + j - deriveShaGroup - groups, false
		}
	}
	leafOf := newLeafEncoder(list)
	return trieRoot(deriveShaGroup+groups+n-tail, func(j int, w *bytes.Buffer) {
		i, group := index(j)
		writeHexKey(i, w)
		if group {
			w.Truncate(w.Len() - 3)
		}
	}, func(j int) trie.GenStructStepData {
		i, group := index(j)
		if group {
			return &hashes[i/deriveShaGroup-1]
		}
		return leafOf(i)
	})
}

// trieRoot hashes count entries in key order: keyOf writes entry j's hex key, dataOf returns its data.
func trieRoot(count int, keyOf func(j int, w *bytes.Buffer), dataOf func(j int) trie.GenStructStepData) common.Hash {
	var curr, succ bytes.Buffer
	hb := trie.NewHashBuilder(false)
	var groups, branches, hashes []uint16
	keyOf(0, &succ)
	for j := range count {
		curr.Reset()
		curr.Write(succ.Bytes())
		succ.Reset()
		if j+1 < count {
			keyOf(j+1, &succ)
		}
		groups, branches, hashes, _ = trie.GenStructStep(retain, curr.Bytes(), succ.Bytes(), hb, nil /* hashCollector */, dataOf(j), groups, branches, hashes, false)
	}
	hash, _ := hb.RootHash()
	return hash
}

// newLeafEncoder returns a func encoding item i as leaf data, valid until its next call.
func newLeafEncoder(list DerivableList) func(i int) trie.GenStructStepData {
	var value bytes.Buffer
	var leaf trie.GenStructStepLeafData
	return func(i int) trie.GenStructStepData {
		value.Reset()
		list.EncodeIndex(i, &value)
		leaf.Value = rlp.RlpEncodedBytes(value.Bytes())
		return &leaf
	}
}

func writeHexKey(i int, w *bytes.Buffer) {
	hw := &hexWriter{w}
	encodeUint(uint(i), hw)
	if err := hw.Commit(); err != nil {
		panic(fmt.Errorf("fatal in DeriveSha: %w", err))
	}
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

// lexIndex returns the index at position j of the n keys in lex order. Keys are RLP
// encoded indexes: 0 encodes as 0x80, so it sorts after 1..127 and before 128.
func lexIndex(j, n int) int {
	switch last := min(127, n-1); {
	case j < last:
		return j + 1
	case j == last:
		return 0
	default:
		return j
	}
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
