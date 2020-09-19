package trie

import (
	"bytes"
	"math/big"
	"testing"

	"github.com/ugorji/go/codec"
)

func TestOperatoLoaderByteArray(t *testing.T) {

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

	loader1 := NewOperatorUnmarshaller(&buffer)

	decoded1, err := loader1.ReadByteArray()
	if err != nil {
		t.Error(err)
	}
	if !bytes.Equal(decoded1, bytes1) {
		t.Errorf("failed to decode bytes, expected %v got %v", bytes1, decoded1)
	}

	decoded2, err := loader1.ReadByteArray()
	if err != nil {
		t.Error(err)
	}
	if !bytes.Equal(decoded2, bytes2) {
		t.Errorf("failed to decode bytes, expected %v got %v", bytes1, decoded1)
	}
}

func TestAccountBigBalance(t *testing.T) {
	key := []byte("l")
	balance := big.NewInt(0)
	var ok bool
	balance, ok = balance.SetString("92233720368547758080", 10)
	if !ok {
		t.Error("fail to set balance to a large number")
	}
	acc := &OperatorLeafAccount{
		key,
		0,
		balance,
		false,
		false,
		0,
	}

	var buff bytes.Buffer

	collector := NewOperatorMarshaller(&buff)

	err := acc.WriteTo(collector)
	if err != nil {
		t.Error(err)
	}

	// discard the opcode
	_, err = buff.ReadByte()
	if err != nil {
		t.Error(err)
	}

	acc2 := &OperatorLeafAccount{}
	loader := NewOperatorUnmarshaller(&buff)
	if err := acc2.LoadFrom(loader); err != nil {
		t.Error(err)
	}

	if acc2.Balance.Cmp(acc.Balance) != 0 {
		t.Errorf("wrong deserialization of balance (expected: %s got %s)", acc.Balance.String(), acc2.Balance.String())
	}

}

func TestAccountCompactWriteTo(t *testing.T) {
	key := []byte("l")
	acc := &OperatorLeafAccount{
		key,
		0,
		big.NewInt(0),
		false,
		false,
		0,
	}

	var buff bytes.Buffer

	collector := NewOperatorMarshaller(&buff)

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
	acc := &OperatorLeafAccount{
		key,
		20,
		big.NewInt(10),
		true,
		true,
		0,
	}

	var buff bytes.Buffer

	collector := NewOperatorMarshaller(&buff)

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
	acc := &OperatorLeafAccount{
		key,
		0,
		big.NewInt(10),
		true,
		true,
		0,
	}

	var buff bytes.Buffer

	collector := NewOperatorMarshaller(&buff)

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
	acc := &OperatorLeafAccount{
		key,
		22,
		big.NewInt(0),
		true,
		true,
		0,
	}

	var buff bytes.Buffer

	collector := NewOperatorMarshaller(&buff)

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

func TestKeySerialization(t *testing.T) {
	for _, key := range [][]byte{
		{1, 16},
		{1, 2, 3, 4, 5},
		{1, 2, 3, 4, 5, 6},
		{},
		{3, 9, 1},
		{1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 15, 15, 15, 15, 15},
		{1},
		{1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 15, 15, 15, 15, 16},
	} {
		b := keyNibblesToBytes(key)

		key2 := keyBytesToNibbles(b)

		if !bytes.Equal(key, key2) {
			t.Errorf("wrong deserialization, expected %x got %x", key, key2)
		}

	}
}
