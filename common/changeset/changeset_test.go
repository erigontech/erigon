package changeset

import (
	"bytes"
	"testing"

	"github.com/stretchr/testify/assert"

	"github.com/ledgerwatch/turbo-geth/common"
)

func createTestChangeSet() []byte {
	// empty AccountChangeSet first
	ch := NewChangeSet()
	// add some entries
	_ = ch.Add(common.FromHex("56fb07ee"), common.FromHex("f7f6db1eb17c6d582078e0ffdd0c"))
	_ = ch.Add(common.FromHex("a5e4c9a1"), common.FromHex("b1e9b5c16355eede662031dd621d08faf4ea"))
	_ = ch.Add(common.FromHex("22bb06f4"), common.FromHex("862cf52b74f1cea41ddd8ffa4b3e7c7790"))
	encoded, _ := EncodeChangeSet(ch)
	return encoded
}

func TestEncoding(t *testing.T) {
	// empty AccountChangeSet first
	ch := NewChangeSet()
	_, err := EncodeChangeSet(ch)
	assert.NoError(t, err)

	// add some entries
	err = ch.Add(common.FromHex("56fb07ee"), common.FromHex("f7f6db1eb17c6d582078e0ffdd0c"))
	assert.NoError(t, err)
	err = ch.Add(common.FromHex("a5e4c9a1"), common.FromHex("b1e9b5c16355eede662031dd621d08faf4ea"))
	assert.NoError(t, err)
	err = ch.Add(common.FromHex("22bb06f4"), common.FromHex("862cf52b74f1cea41ddd8ffa4b3e7c7790"))
	assert.NoError(t, err)
}

func TestFindLast(t *testing.T) {
	encoded := createTestChangeSet()
	val, err := FindLast(encoded, common.FromHex("56fb07ee"))
	assert.NoError(t, err)
	if !bytes.Equal(val, common.FromHex("f7f6db1eb17c6d582078e0ffdd0c")) {
		t.Error("Invalid value")
	}
}
