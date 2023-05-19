package merkle_tree

import "encoding/binary"

// PackUint64IntoChunks packs a list of uint64 values into 32 byte roots.
func PackUint64IntoChunks(vals []uint64) [][32]byte {
	numChunks := (len(vals) + 3) / 4
	chunks := make([][32]byte, numChunks)
	for i := 0; i < len(vals); i++ {
		chunkIndex := i / 4
		byteIndex := (i % 4) * 8
		binary.LittleEndian.PutUint64(chunks[chunkIndex][byteIndex:byteIndex+8], vals[i])
	}
	return chunks
}
