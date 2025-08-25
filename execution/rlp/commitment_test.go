// Copyright 2024 The Erigon Authors
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

package rlp

import (
	"bytes"
	"testing"
)

func TestFastDoubleRlpForByteArrays(t *testing.T) {
	if testing.Short() {
		t.Skip()
	}

	t.Parallel()
	for i := 0; i < 256; i++ {
		doTestWithByte(t, byte(i), 1)
	}
	doTestWithByte(t, 0x0, 10_000)
	doTestWithByte(t, 0xC, 10_000)
	doTestWithByte(t, 0xAB, 10_000)
}

func doTestWithByte(t *testing.T, b byte, iterations int) {
	t.Helper()

	var prefixBuf [8]byte
	buffer := new(bytes.Buffer)

	for i := 0; i < iterations; i++ {
		buffer.WriteByte(b)
		source := buffer.Bytes()

		encSingle, _ := EncodeToBytes(source)

		encDouble, _ := EncodeToBytes(encSingle)

		if RlpSerializableBytes(source).DoubleRLPLen() != len(encDouble) {
			t.Errorf("source [%2x * %d] wrong RlpSerializableBytes#DoubleRLPLen prediction: %d (expected %d)", source[0], len(source), RlpSerializableBytes(source).DoubleRLPLen(), len(encDouble))
		}

		if RlpEncodedBytes(encSingle).DoubleRLPLen() != len(encDouble) {
			t.Errorf("source [%2x * %d] wrong RlpEncodedBytes#DoubleRLPLen prediction: %d (expected %d)", source[0], len(source), RlpEncodedBytes(encSingle).DoubleRLPLen(), len(encDouble))
		}

		buffDouble := new(bytes.Buffer)
		if err := RlpSerializableBytes(source).ToDoubleRLP(buffDouble, prefixBuf[:]); err != nil {
			t.Errorf("test failed, err = %v", err)
		}

		buffSingle := new(bytes.Buffer)
		if err := RlpEncodedBytes(encSingle).ToDoubleRLP(buffSingle, prefixBuf[:]); err != nil {
			t.Errorf("test failed, err = %v", err)
		}

		if !bytes.Equal(buffDouble.Bytes(), encDouble) {
			t.Errorf("source [%2x * %d] wrong RlpSerializableBytes#ToDoubleRLP prediction: %x (expected %x)", source[0], len(source), displayOf(buffDouble.Bytes()), displayOf(encDouble))
		}

		if !bytes.Equal(buffSingle.Bytes(), encDouble) {
			t.Errorf("source [%2x * %d] wrong RlpEncodedBytes#ToDoubleRLP prediction: %x (expected %x)", source[0], len(source), displayOf(buffSingle.Bytes()), displayOf(encDouble))
		}
	}
}

func displayOf(bytes []byte) []byte {
	if len(bytes) < 20 {
		return bytes
	}

	return bytes[:20]
}
