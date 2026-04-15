package enr

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestGetSetChainToml(t *testing.T) {
	ct := ChainToml{
		AuthoritativeBlocks: 123456789,
		KnownBlocks:         234567890,
		InfoHash:            [20]byte{0x01, 0x02, 0x03, 0x04, 0x05, 0x06, 0x07, 0x08, 0x09, 0x0a, 0x0b, 0x0c, 0x0d, 0x0e, 0x0f, 0x10, 0x11, 0x12, 0x13, 0x14},
	}

	var r Record
	r.Set(ct)

	var ct2 ChainToml
	require.NoError(t, r.Load(&ct2))
	assert.Equal(t, ct.AuthoritativeBlocks, ct2.AuthoritativeBlocks)
	assert.Equal(t, ct.KnownBlocks, ct2.KnownBlocks)
	assert.Equal(t, ct.InfoHash, ct2.InfoHash)
}

func TestChainTomlENRKey(t *testing.T) {
	ct := ChainToml{}
	assert.Equal(t, "chain-toml", ct.ENRKey())
}

func TestChainTomlZeroValue(t *testing.T) {
	ct := ChainToml{}
	var r Record
	r.Set(ct)

	var ct2 ChainToml
	require.NoError(t, r.Load(&ct2))
	assert.Equal(t, uint64(0), ct2.AuthoritativeBlocks)
	assert.Equal(t, uint64(0), ct2.KnownBlocks)
	assert.Equal(t, [20]byte{}, ct2.InfoHash)
}

func TestChainTomlNotFound(t *testing.T) {
	var r Record
	var ct ChainToml
	err := r.Load(&ct)
	require.Error(t, err)
	assert.True(t, IsNotFound(err))
}

func TestChainTomlFitsInENR(t *testing.T) {
	// Verify that ChainToml fits within the ENR size limit when combined
	// with typical entries (id, ip, udp, tcp).
	var r Record
	r.Set(IDv4)
	r.Set(IPv4{192, 168, 1, 1})
	r.Set(UDP(30303))
	r.Set(TCP(30303))
	r.Set(ChainToml{
		AuthoritativeBlocks: ^uint64(0), // worst case: max uint64
		KnownBlocks:         ^uint64(0),
		InfoHash:            [20]byte{0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff},
	})

	// The record should still be under the size limit.
	// Note: Size() works on unsigned records too; signing adds ~100 bytes.
	assert.Less(t, r.Size(), uint64(SizeLimit), "ChainToml entry should fit within ENR size limit with typical entries")
}
