//go:build integration

package rlphacks

import (
	"bytes"
	"testing"

	"github.com/ledgerwatch/erigon/rlp"
)

func TestFastDoubleRlpForByteArrays(t *testing.T) {
	for i := 0; i < 256; i++ {
		doTestWithByte(t, byte(i), 1)
	}
	doTestWithByte(t, 0x0, 100000)
	doTestWithByte(t, 0xC, 100000)
	doTestWithByte(t, 0xAB, 100000)
}

func doTestWithByte(t *testing.T, b byte, iterations int) {
	var prefixBuf [8]byte
	buffer := new(bytes.Buffer)

	for i := 0; i < iterations; i++ {
		buffer.WriteByte(b)
		source := buffer.Bytes()

		encSingle, _ := rlp.EncodeToBytes(source)

		encDouble, _ := rlp.EncodeToBytes(encSingle)

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
