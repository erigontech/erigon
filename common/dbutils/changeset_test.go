package dbutils

import (
	"testing"

	"github.com/stretchr/testify/assert"

	"github.com/ledgerwatch/turbo-geth/common"
)

func TestEncoding(t *testing.T) {
	// empty ChangeSet first
	ch := NewChangeSet()
	encoded, err := ch.Encode()
	assert.NoError(t, err)
	decoded, err := DecodeChangeSet(encoded)
	assert.NoError(t, err)
	assert.Equal(t, 0, decoded.Len())

	// add some entries
	err = ch.Add(common.FromHex("56fb07ee"), common.FromHex("f7f6db1eb17c6d582078e0ffdd0c"))
	assert.NoError(t, err)
	err = ch.Add(common.FromHex("a5e4c9a1"), common.FromHex("b1e9b5c16355eede662031dd621d08faf4ea"))
	assert.NoError(t, err)
	err = ch.Add(common.FromHex("22bb06f4"), common.FromHex("862cf52b74f1cea41ddd8ffa4b3e7c7790"))
	assert.NoError(t, err)

	// test Decode(Encode(ch)) == ch
	encoded, err = ch.Encode()
	assert.NoError(t, err)
	decoded, err = DecodeChangeSet(encoded)
	assert.NoError(t, err)
	assert.Equal(t, ch, decoded)
}
