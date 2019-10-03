package trie

import (
	"bytes"
	"testing"

	"github.com/ledgerwatch/turbo-geth/common"
)

func TestSupplyKeyValue(t *testing.T) {
	bwb := NewBlockWitnessBuilder()
	if err := bwb.supplyKeyValue([]byte("key"), []byte("value")); err != nil {
		t.Errorf("Could not supply key and value: %v", err)
	}
	if !bytes.Equal(common.FromHex("0x436b6579"), bwb.Keys.buffer.Bytes()) {
		t.Errorf("Expected 0x436b6579 in keys tape, got: %x", bwb.Keys.buffer.Bytes())
	}
	if !bytes.Equal(common.FromHex("0x4576616c7565"), bwb.Values.buffer.Bytes()) {
		t.Errorf("Expected 0x4576616c7565 in values tape, got: %x", bwb.Values.buffer.Bytes())
	}
}

func TestSupplyHash(t *testing.T) {
	bwb := NewBlockWitnessBuilder()
	if err := bwb.supplyHash(common.HexToHash("0x9583498348fc48393abc")); err != nil {
		t.Errorf("Could not supply hash: %v", err)
	}
	if !bytes.Equal(common.FromHex("0x5820000000000000000000000000000000000000000000009583498348fc48393abc"), bwb.Hashes.buffer.Bytes()) {
		t.Errorf("Expected 0x5820000000000000000000000000000000000000000000009583498348fc48393abc in hash tape, got: %x", bwb.Hashes.buffer.Bytes())
	}
}

func TestSupplyCode(t *testing.T) {
	bwb := NewBlockWitnessBuilder()
	if err := bwb.supplyCode(common.FromHex("0x9583498348fc48393abc58bc")); err != nil {
		t.Errorf("Could not supply code: %v", err)
	}
	if !bytes.Equal(common.FromHex("0x4c9583498348fc48393abc58bc"), bwb.Codes.buffer.Bytes()) {
		t.Errorf("Expected 0x4c9583498348fc48393abc58bc in codes tape, got: %x", bwb.Codes.buffer.Bytes())
	}
}

func TestOpLeaf(t *testing.T) {
	bwb := NewBlockWitnessBuilder()
	if err := bwb.leaf(56); err != nil {
		t.Errorf("Could not call leaf: %v", err)
	}
	if !bytes.Equal(common.FromHex("0x001838"), bwb.Structure.buffer.Bytes()) {
		t.Errorf("Expected 0x001838 in structure tape, got: %x", bwb.Structure.buffer.Bytes())
	}
}
func TestOpLeafHash(t *testing.T) {
	bwb := NewBlockWitnessBuilder()
	if err := bwb.leafHash(56); err != nil {
		t.Errorf("Could not call leafHash: %v", err)
	}
	if !bytes.Equal(common.FromHex("0x011838"), bwb.Structure.buffer.Bytes()) {
		t.Errorf("Expected 0x011838 in structure tape, got: %x", bwb.Structure.buffer.Bytes())
	}
}

func TestOpExtension(t *testing.T) {
	bwb := NewBlockWitnessBuilder()
	if err := bwb.extension(common.FromHex("0x0f05")); err != nil {
		t.Errorf("Could not call extension: %v", err)
	}
	if !bytes.Equal(common.FromHex("0x02420f05"), bwb.Structure.buffer.Bytes()) {
		t.Errorf("Expected 0x02420f05 in structure tape, got: %x", bwb.Structure.buffer.Bytes())
	}
}

func TestOpExtensionHash(t *testing.T) {
	bwb := NewBlockWitnessBuilder()
	if err := bwb.extensionHash(common.FromHex("0x0f05")); err != nil {
		t.Errorf("Could not call extensionHash: %v", err)
	}
	if !bytes.Equal(common.FromHex("0x03420f05"), bwb.Structure.buffer.Bytes()) {
		t.Errorf("Expected 0x03420f05 in structure tape, got: %x", bwb.Structure.buffer.Bytes())
	}
}

func TestOpBranch(t *testing.T) {
	bwb := NewBlockWitnessBuilder()
	if err := bwb.branch(1 + 4); err != nil {
		t.Errorf("Could not call branch: %v", err)
	}
	if !bytes.Equal(common.FromHex("0x0405"), bwb.Structure.buffer.Bytes()) {
		t.Errorf("Expected 0x0405 in structure tape, got: %x", bwb.Structure.buffer.Bytes())
	}
}

func TestOpBranchHash(t *testing.T) {
	bwb := NewBlockWitnessBuilder()
	if err := bwb.branchHash(1 + 4); err != nil {
		t.Errorf("Could not call branchHash: %v", err)
	}
	if !bytes.Equal(common.FromHex("0x0505"), bwb.Structure.buffer.Bytes()) {
		t.Errorf("Expected 0x0505 in structure tape, got: %x", bwb.Structure.buffer.Bytes())
	}
}

func TestOpHash(t *testing.T) {
	bwb := NewBlockWitnessBuilder()
	if err := bwb.hash(3); err != nil {
		t.Errorf("Could not call hash: %v", err)
	}
	if !bytes.Equal(common.FromHex("0x0603"), bwb.Structure.buffer.Bytes()) {
		t.Errorf("Expected 0x0603 in structure tape, got: %x", bwb.Structure.buffer.Bytes())
	}
}

func TestOpCode(t *testing.T) {
	bwb := NewBlockWitnessBuilder()
	if err := bwb.code(); err != nil {
		t.Errorf("Could not call code: %v", err)
	}
	if !bytes.Equal(common.FromHex("0x07"), bwb.Structure.buffer.Bytes()) {
		t.Errorf("Expected 0x07 in structure tape, got: %x", bwb.Structure.buffer.Bytes())
	}
}

func TestOpCodeHash(t *testing.T) {
	bwb := NewBlockWitnessBuilder()
	if err := bwb.codeHash(); err != nil {
		t.Errorf("Could not call codeHash: %v", err)
	}
	if !bytes.Equal(common.FromHex("0x08"), bwb.Structure.buffer.Bytes()) {
		t.Errorf("Expected 0x08 in structure tape, got: %x", bwb.Structure.buffer.Bytes())
	}
}

func TestOpContractLeaf(t *testing.T) {
	bwb := NewBlockWitnessBuilder()
	if err := bwb.contractLeaf(56); err != nil {
		t.Errorf("Could not call contractLeaf: %v", err)
	}
	if !bytes.Equal(common.FromHex("0x091838"), bwb.Structure.buffer.Bytes()) {
		t.Errorf("Expected 0x001838 in structure tape, got: %x", bwb.Structure.buffer.Bytes())
	}
}
func TestOpContractLeafHash(t *testing.T) {
	bwb := NewBlockWitnessBuilder()
	if err := bwb.contractLeafHash(56); err != nil {
		t.Errorf("Could not call contractLeafHash: %v", err)
	}
	if !bytes.Equal(common.FromHex("0x0a1838"), bwb.Structure.buffer.Bytes()) {
		t.Errorf("Expected 0x011838 in structure tape, got: %x", bwb.Structure.buffer.Bytes())
	}
}

func TestOpEmptyRoot(t *testing.T) {
	bwb := NewBlockWitnessBuilder()
	if err := bwb.emptyRoot(); err != nil {
		t.Errorf("Could not call emptyRoot: %v", err)
	}
	if !bytes.Equal(common.FromHex("0x0b"), bwb.Structure.buffer.Bytes()) {
		t.Errorf("Expected 0x0b in structure tape, got: %x", bwb.Structure.buffer.Bytes())
	}
}
