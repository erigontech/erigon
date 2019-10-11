package trie

import (
	"bytes"
	"testing"

	"github.com/ledgerwatch/turbo-geth/common"
)

func TestSupplyKeyValue(t *testing.T) {
	bwb := NewBlockWitnessBuilder()
	if err := bwb.supplyKey([]byte("key")); err != nil {
		t.Errorf("Could not supply key: %v", err)
	}
	if !bytes.Equal(common.FromHex("0x436b6579"), bwb.Keys.buffer.Bytes()) {
		t.Errorf("Expected 0x436b6579 in keys tape, got: %x", bwb.Keys.buffer.Bytes())
	}
	if err := bwb.supplyValue([]byte("value")); err != nil {
		t.Errorf("Could not supply value: %v", err)
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

func TestOpAccountLeaf(t *testing.T) {
	bwb := NewBlockWitnessBuilder()
	if err := bwb.accountLeaf(56, 3); err != nil {
		t.Errorf("Could not call acccountLeaf: %v", err)
	}
	expected := common.FromHex("0x08183803")
	if !bytes.Equal(expected, bwb.Structure.buffer.Bytes()) {
		t.Errorf("Expected %x in structure tape, got: %x", expected, bwb.Structure.buffer.Bytes())
	}
}
func TestOpAccountLeafHash(t *testing.T) {
	bwb := NewBlockWitnessBuilder()
	if err := bwb.accountLeafHash(56, 3); err != nil {
		t.Errorf("Could not call accountLeafHash: %v", err)
	}
	expected := common.FromHex("0x09183803")
	if !bytes.Equal(expected, bwb.Structure.buffer.Bytes()) {
		t.Errorf("Expected %x in structure tape, got: %x", expected, bwb.Structure.buffer.Bytes())
	}
}

func TestOpEmptyRoot(t *testing.T) {
	bwb := NewBlockWitnessBuilder()
	if err := bwb.emptyRoot(); err != nil {
		t.Errorf("Could not call emptyRoot: %v", err)
	}
	expected := common.FromHex("0x0a")
	if !bytes.Equal(expected, bwb.Structure.buffer.Bytes()) {
		t.Errorf("Expected %x in structure tape, got: %x", expected, bwb.Structure.buffer.Bytes())
	}
}

func TestMakeBlockWitness(t *testing.T) {
	tr := New(common.Hash{})
	tr.Update([]byte("ABCD0001"), []byte("val1"), 0)
	tr.Update([]byte("ABCE0002"), []byte("val2"), 0)
	bwb := NewBlockWitnessBuilder()
	rs := NewResolveSet(2)
	if err := bwb.MakeBlockWitness(tr, rs, func(codeHash common.Hash) []byte { return nil }); err != nil {
		t.Errorf("Could not make block witness: %v", err)
	}
	expected := common.FromHex("0x0601024704010402040304")
	if !bytes.Equal(expected, bwb.Structure.buffer.Bytes()) {
		t.Errorf("Expected %x in structure tape, got: %x", expected, bwb.Structure.buffer.Bytes())
	}
}

func TestSerialiseBlockWitness(t *testing.T) {
	tr := New(common.Hash{})
	tr.Update([]byte("ABCD0001"), []byte("val1"), 0)
	tr.Update([]byte("ABCE0002"), []byte("val2"), 0)
	bwb := NewBlockWitnessBuilder()
	rs := NewResolveSet(2)
	if err := bwb.MakeBlockWitness(tr, rs, func(codeHash common.Hash) []byte { return nil }); err != nil {
		t.Errorf("Could not make block witness: %v", err)
	}
	var b bytes.Buffer
	if err := bwb.WriteTo(&b); err != nil {
		t.Errorf("Could not make block witness: %v", err)
	}
	expected := common.FromHex("0xa76862616c616e6365730065636f64657300666861736865731822646b65797300666e6f6e63657300697374727563747572650b6676616c75657300582023181a62d35fe01562158be610f84e047f99f5e74d896da21682d925964ece3a0601024704010402040304")
	if !bytes.Equal(expected, b.Bytes()) {
		t.Errorf("Expected %x, got: %x", expected, b.Bytes())
	}
	tr1, _, err := BlockWitnessToTrie(b.Bytes())
	if err != nil {
		t.Errorf("Could not restore trie from the block witness: %v", err)
	}
	if tr.Hash() != tr1.Hash() {
		t.Errorf("Reconstructed block witness has different root hash than source trie")
	}
}
