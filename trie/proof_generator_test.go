package trie

import (
	"bytes"
	"testing"

	"github.com/ledgerwatch/turbo-geth/common"
)

func TestKeyValue(t *testing.T) {
	bwb := NewBlockWitnessBuilder()
	if err := bwb.keyValue([]byte("key"), []byte("value")); err != nil {
		t.Errorf("Could not set key-value: %v", err)
	}
	if !bytes.Equal(common.FromHex("0x436b6579"), bwb.Keys.buffer.Bytes()) {
		t.Errorf("Expected 0x436b6579 in keys tape, got: %x", bwb.Keys.buffer.Bytes())
	}
	if !bytes.Equal(common.FromHex("0x4576616c7565"), bwb.Values.buffer.Bytes()) {
		t.Errorf("Expected 0x4576616c7565 in values tape, got: %x", bwb.Values.buffer.Bytes())
	}
}

func TestLeaf(t *testing.T) {
	bwb := NewBlockWitnessBuilder()
	if err := bwb.leaf(56); err != nil {
		t.Errorf("Could not call leaf: %v", err)
	}
	if !bytes.Equal(common.FromHex("0x001838"), bwb.Structure.buffer.Bytes()) {
		t.Errorf("Expected 0x001838 in structure tape, got: %x", bwb.Structure.buffer.Bytes())
	}
}
