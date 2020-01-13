package trie

import (
	"bytes"
	"math/big"
	"testing"

	"github.com/ugorji/go/codec"
)

func TestDecodeBytes(t *testing.T) {

	var cbor codec.CborHandle

	var buffer bytes.Buffer

	encoder := codec.NewEncoder(&buffer, &cbor)

	bytes1 := []byte("test1")
	bytes2 := []byte("abcd2")

	if err := encoder.Encode(bytes1); err != nil {
		t.Error(err)
	}

	if err := encoder.Encode(bytes2); err != nil {
		t.Error(err)
	}

	decoded1, err := decodeByteArray(&buffer)
	if err != nil {
		t.Error(err)
	}
	if !bytes.Equal(decoded1, bytes1) {
		t.Errorf("failed to decode bytes, expected %v got %v", bytes1, decoded1)
	}

	decoded2, err := decodeByteArray(&buffer)
	if err != nil {
		t.Error(err)
	}
	if !bytes.Equal(decoded2, bytes2) {
		t.Errorf("failed to decode bytes, expected %v got %v", bytes1, decoded1)
	}
}

func TestAccountCompactWriteTo(t *testing.T) {
	key := []byte("l")
	acc := &OperandLeafAccount{
		key,
		0,
		*big.NewInt(0),
		false,
		false,
	}

	var buff bytes.Buffer

	collector := NewWitnessStatsCollector(&buff)

	err := acc.WriteTo(collector)
	if err != nil {
		t.Error(err)
	}

	b := buff.Bytes()

	expectedLen := 1 /* opcode */ + 1 /* CBOR prefix for key */ + len(key) + 1 /* flags */

	if len(b) != expectedLen {
		t.Errorf("unexpected serialization len for default fields, expected %d (no fields seralized), got %d (raw: %v)", expectedLen, len(b), b)
	}
}

func TestAccountFullWriteTo(t *testing.T) {
	key := []byte("l")
	acc := &OperandLeafAccount{
		key,
		20,
		*big.NewInt(10),
		true,
		true,
	}

	var buff bytes.Buffer

	collector := NewWitnessStatsCollector(&buff)

	err := acc.WriteTo(collector)
	if err != nil {
		t.Error(err)
	}

	b := buff.Bytes()

	compactLen := 1 /* opcode */ + 1 /* CBOR prefix for key */ + len(key) + 1 /* flags */

	if len(b) <= compactLen {
		t.Errorf("unexpected serialization expected to be bigger than %d (fields seralized), got %d (raw: %v)", compactLen, len(b), b)
	}

	flags := b[3]

	expectedFlags := byte(0)
	expectedFlags |= flagStorage
	expectedFlags |= flagCode
	expectedFlags |= flagNonce
	expectedFlags |= flagBalance

	if flags != expectedFlags {
		t.Errorf("unexpected flags value (expected %b, got %b)", expectedFlags, flags)
	}
}

func TestAccountPartialNoNonceWriteTo(t *testing.T) {
	key := []byte("l")
	acc := &OperandLeafAccount{
		key,
		0,
		*big.NewInt(10),
		true,
		true,
	}

	var buff bytes.Buffer

	collector := NewWitnessStatsCollector(&buff)

	err := acc.WriteTo(collector)
	if err != nil {
		t.Error(err)
	}

	b := buff.Bytes()

	compactLen := 1 /* opcode */ + 1 /* CBOR prefix for key */ + len(key) + 1 /* flags */

	if len(b) <= compactLen {
		t.Errorf("unexpected serialization expected to be bigger than %d (fields seralized), got %d (raw: %v)", compactLen, len(b), b)
	}

	flags := b[3]

	expectedFlags := byte(0)
	expectedFlags |= flagStorage
	expectedFlags |= flagCode
	expectedFlags |= flagBalance

	if flags != expectedFlags {
		t.Errorf("unexpected flags value (expected %b, got %b)", expectedFlags, flags)
	}
}

func TestAccountPartialNoBalanceWriteTo(t *testing.T) {
	key := []byte("l")
	acc := &OperandLeafAccount{
		key,
		22,
		*big.NewInt(0),
		true,
		true,
	}

	var buff bytes.Buffer

	collector := NewWitnessStatsCollector(&buff)

	err := acc.WriteTo(collector)
	if err != nil {
		t.Error(err)
	}

	b := buff.Bytes()

	compactLen := 1 /* opcode */ + 1 /* CBOR prefix for key */ + len(key) + 1 /* flags */

	if len(b) <= compactLen {
		t.Errorf("unexpected serialization expected to be bigger than %d (fields seralized), got %d (raw: %v)", compactLen, len(b), b)
	}

	flags := b[3]

	expectedFlags := byte(0)
	expectedFlags |= flagStorage
	expectedFlags |= flagCode
	expectedFlags |= flagNonce

	if flags != expectedFlags {
		t.Errorf("unexpected flags value (expected %b, got %b)", expectedFlags, flags)
	}
}
