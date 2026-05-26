package enr

import (
	"bytes"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/erigontech/erigon/execution/rlp"
)

func TestGetSetChainToml(t *testing.T) {
	ct := ChainToml{
		AuthoritativeBlocks: 123456789,
		KnownBlocks:         234567890,
		InfoHash:            [20]byte{0x01, 0x02, 0x03, 0x04, 0x05, 0x06, 0x07, 0x08, 0x09, 0x0a, 0x0b, 0x0c, 0x0d, 0x0e, 0x0f, 0x10, 0x11, 0x12, 0x13, 0x14},
		ContentUCANHash:     [20]byte{0xaa, 0xbb, 0xcc, 0xdd, 0xee, 0xff, 0x01, 0x02, 0x03, 0x04, 0x05, 0x06, 0x07, 0x08, 0x09, 0x0a, 0x0b, 0x0c, 0x0d, 0x0e},
		V2InfoHash:          [20]byte{0x21, 0x22, 0x23, 0x24, 0x25, 0x26, 0x27, 0x28, 0x29, 0x2a, 0x2b, 0x2c, 0x2d, 0x2e, 0x2f, 0x30, 0x31, 0x32, 0x33, 0x34},
		MinStep:             4096,
	}

	var r Record
	r.Set(ct)

	var ct2 ChainToml
	require.NoError(t, r.Load(&ct2))
	assert.Equal(t, ct.AuthoritativeBlocks, ct2.AuthoritativeBlocks)
	assert.Equal(t, ct.KnownBlocks, ct2.KnownBlocks)
	assert.Equal(t, ct.InfoHash, ct2.InfoHash)
	assert.Equal(t, ct.ContentUCANHash, ct2.ContentUCANHash)
	assert.Equal(t, ct.V2InfoHash, ct2.V2InfoHash)
	assert.Equal(t, ct.MinStep, ct2.MinStep)
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

// TestChainTomlDecodeOlderEncoding pins the back-compat decoder: an
// older publisher that emits only the first 6 fields (no V2InfoHash,
// no MinStep) must decode cleanly with the new fields defaulting to
// zero. Trailing-RLP-fields back-compat is the spec invariant that
// lets us extend the ENR without breaking peers.
func TestChainTomlDecodeOlderEncoding(t *testing.T) {
	// Encode using a shadow struct that mirrors the pre-V2InfoHash
	// shape (6 fields, no trailing V2InfoHash + MinStep).
	type chainTomlPreV2 struct {
		AuthoritativeBlocks uint64
		KnownBlocks         uint64
		InfoHash            [20]byte
		DomainSteps         uint64
		MergeDepth          uint64
		ContentUCANHash     [20]byte
	}
	old := chainTomlPreV2{
		AuthoritativeBlocks: 100,
		KnownBlocks:         200,
		InfoHash:            [20]byte{0xab},
		DomainSteps:         4096,
		MergeDepth:          1024,
		ContentUCANHash:     [20]byte{0xcd},
	}
	var buf bytes.Buffer
	require.NoError(t, rlp.Encode(&buf, &old))

	var decoded ChainToml
	require.NoError(t, rlp.DecodeBytes(buf.Bytes(), &decoded))
	assert.Equal(t, uint64(100), decoded.AuthoritativeBlocks)
	assert.Equal(t, [20]byte{0xab}, decoded.InfoHash)
	assert.Equal(t, [20]byte{0xcd}, decoded.ContentUCANHash)
	assert.Equal(t, [20]byte{}, decoded.V2InfoHash, "missing trailing field defaults to zero")
	assert.Equal(t, uint64(0), decoded.MinStep, "missing trailing field defaults to zero")
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
