package trie

import "testing"

import "github.com/ugorji/go/codec"

import "bytes"

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
