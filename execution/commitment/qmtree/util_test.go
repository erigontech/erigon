package qmtree

import (
	"encoding/binary"
	"encoding/hex"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestHash2(t *testing.T) {
	hasher := Sha256Hasher{}
	hash2 := hasher.hash2(8, []byte("hello"), []byte("world"))
	require.Equal(t,
		hex.EncodeToString(hash2[:]),
		"8e6fc50a3f98a3c314021b89688ca83a9b5697ca956e211198625fc460ddf1e9")

	hash2x := hasher.hash2x(8, []byte("world"), []byte("hello"), true)
	require.Equal(t,
		hex.EncodeToString(hash2x[:]),
		"8e6fc50a3f98a3c314021b89688ca83a9b5697ca956e211198625fc460ddf1e9")
}

func TestNodeHash(t *testing.T) {
	hasher := Sha256Hasher{}
	nodeHash := hasher.nodeHash(8, []byte("hello"), []byte("world"))
	require.Equal(t,
		hex.EncodeToString(nodeHash[:]),
		"8e6fc50a3f98a3c314021b89688ca83a9b5697ca956e211198625fc460ddf1e9")
}

func TestEncodeDecodeN64(t *testing.T) {
	v := []byte{0x11, 0x22, 0x33, 0x44, 0x55, 0x66, 0x77, 0x88}
	require.Equal(t, int(binary.LittleEndian.Uint64(v)), -8613303245920329199)
	require.Equal(t, binary.LittleEndian.Uint64(v), uint64(0x8877665544332211))
	target := make([]byte, 8)
	source := -8613303245920329199
	binary.LittleEndian.PutUint64(target[:], uint64(source))
	require.Equal(t, target, v)
	binary.LittleEndian.PutUint64(target[:], 0x8877665544332211)
	require.Equal(t, target, v)
}
