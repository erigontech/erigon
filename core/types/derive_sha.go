// Copyniright 2014 The go-ethereum Authors
// This file is part of the go-ethereum library.
//
// The go-ethereum library is free software: you can redistribute it and/or modify
// it under the terms of the GNU Lesser General Public License as published by
// the Free Software Foundation, either version 3 of the License, or
// (at your option) any later version.
//
// The go-ethereum library is distributed in the hope that it will be useful,
// but WITHOUT ANY WARRANTY; without even the implied warranty of
// MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE. See the
// GNU Lesser General Public License for more details.
//
// You should have received a copy of the GNU Lesser General Public License
// along with the go-ethereum library. If not, see <http://www.gnu.org/licenses/>.

package types

import (
	"bytes"
	"fmt"
	"io"

	"github.com/ledgerwatch/turbo-geth/common"
	"github.com/ledgerwatch/turbo-geth/rlp"
	"github.com/ledgerwatch/turbo-geth/trie"
	"github.com/ledgerwatch/turbo-geth/trie/rlphacks"
)

type DerivableList interface {
	Len() int
	GetRlp(i int) []byte
}

func DeriveSha(list DerivableList) common.Hash {
	if list.Len() < 1 {
		return trie.EmptyRoot
	}

	var curr bytes.Buffer
	var succ bytes.Buffer
	var value bytes.Buffer

	hb := trie.NewHashBuilder(false)

	hb.Reset()
	curr.Reset()
	succ.Reset()

	hexWriter := &hexWriter{&succ}

	var groups []uint16
	var leafData trie.GenStructStepLeafData

	traverseInLexOrder(list, func(i int, next int) {
		curr.Reset()
		curr.Write(succ.Bytes())
		succ.Reset()

		if next >= 0 {
			encodeUint(uint(next), hexWriter)
			if err := hexWriter.Commit(); err != nil {
				panic(fmt.Errorf("fatal in DeriveSha: %w", err))
			}
		}

		value.Reset()

		if curr.Len() > 0 {
			value.Write(list.GetRlp(i))
			leafData.Value = rlphacks.RlpEncodedBytes(value.Bytes())
			groups, _ = trie.GenStructStep(hashOnly, curr.Bytes(), succ.Bytes(), hb, &leafData, groups, false)
		}
	})

	hash, _ := hb.RootHash()
	return hash
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

func adjustIndex(i int, l int) int {
	if i >= 0 && i < 127 && i < l-1 {
		return i + 1
	} else if i == 127 || (i < 127 && i >= l-1) {
		return 0
	}
	return i
}

// traverseInLexOrder traverses the list indices in the order suitable for HashBuilder.
// HashBuilder requires keys to be in the lexicographical order. Our keys are unit indices in RLP encoding in hex.
// In RLP encoding 0 is 0080 where 1 is 000110, 2 is 000210, etc up until 128 which is 0801080010.
// So, knowing that we can order indices in the right order even w/o really sorting them. Only 0 is misplaced, and should take the position after 127.
// So, in the end we transform [0,...,127,128,...n] to [1,...,127,0,128,...,n] which will be [000110....070f10, 080010, 0801080010....] in hex encoding.
func traverseInLexOrder(list DerivableList, traverser func(int, int)) {
	for i := -1; i < list.Len(); i++ {
		adjustedIndex := adjustIndex(i, list.Len())
		nextIndex := i + 1
		if nextIndex >= list.Len() {
			nextIndex = -1
		}
		nextIndex = adjustIndex(nextIndex, list.Len())

		traverser(adjustedIndex, nextIndex)
	}
}

func hashOnly(_ []byte) bool {
	return true
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
