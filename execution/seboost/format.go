package seboost

import (
	"encoding/binary"
	"math/bits"
)

// File format constants
var fileMagic = [4]byte{'S', 'B', 'T', 'X'}

const fileVersion = 0x01

// blocksPerFile is the number of blocks covered by each seboost file.
const blocksPerFile = 500_000

// minTxCount is the minimum number of entries (system tx + user txs) for a
// block to be written. Blocks with fewer entries are skipped.
const minTxCount = 3

const (
	formatBitmap byte = 0
	formatSparse byte = 1
)

// estimateBitmapSize returns the bitmap encoding size in bytes without allocating.
func estimateBitmapSize(n int) int {
	totalBits := n * (n - 1) / 2
	return (totalBits + 7) / 8
}

// estimateSparseSize computes the exact sparse encoding size without allocating.
func estimateSparseSize(deps map[int]map[int]bool, n int) int {
	var tmp [binary.MaxVarintLen64]byte
	size := 0
	for i := 0; i < n; i++ {
		row := deps[i]
		size += binary.PutUvarint(tmp[:], uint64(len(row)))
		for j := range row {
			size += binary.PutUvarint(tmp[:], uint64(j))
		}
	}
	return size
}

// encodeBitmap encodes a dependency map as a lower-triangular bit matrix.
// For n entries, row i has i bits (for j=0..i-1), bit=1 means entry i depends
// on entry j. Layout: row-major packed bits, LSB-first within each byte.
// Total bits = n*(n-1)/2, padded to full bytes.
func encodeBitmap(deps map[int]map[int]bool, n int) []byte {
	totalBits := n * (n - 1) / 2
	totalBytes := (totalBits + 7) / 8
	buf := make([]byte, totalBytes)

	bitOffset := 0
	for i := 1; i < n; i++ {
		row := deps[i]
		for j := 0; j < i; j++ {
			if row != nil && row[j] {
				byteIdx := bitOffset / 8
				bitIdx := uint(bitOffset % 8)
				buf[byteIdx] |= 1 << bitIdx
			}
			bitOffset++
		}
	}
	return buf
}

// encodeSparse encodes a dependency map as per-entry dep lists.
// For each entry i (0..n-1): varint depCount, then depCount varints (dep indices).
func encodeSparse(deps map[int]map[int]bool, n int) []byte {
	var buf []byte
	tmp := make([]byte, binary.MaxVarintLen64)

	for i := 0; i < n; i++ {
		row := deps[i]
		count := len(row)
		k := binary.PutUvarint(tmp, uint64(count))
		buf = append(buf, tmp[:k]...)
		for j := range row {
			k = binary.PutUvarint(tmp, uint64(j))
			buf = append(buf, tmp[:k]...)
		}
	}
	return buf
}

// chooseEncoding estimates both sizes first, then encodes only the winner.
// This avoids the cost of encoding the losing format.
func chooseEncoding(deps map[int]map[int]bool, n int) (payload []byte, format byte, bitmapBytes, sparseBytes int) {
	bitmapBytes = estimateBitmapSize(n)
	sparseBytes = estimateSparseSize(deps, n)
	if bitmapBytes <= sparseBytes {
		return encodeBitmap(deps, n), formatBitmap, bitmapBytes, sparseBytes
	}
	return encodeSparse(deps, n), formatSparse, bitmapBytes, sparseBytes
}

// decodeBitmap decodes a lower-triangular bit matrix back to per-entry dep lists.
func decodeBitmap(payload []byte, n int) [][]int {
	result := make([][]int, n)
	bitOffset := 0
	for i := 1; i < n; i++ {
		for j := 0; j < i; j++ {
			byteIdx := bitOffset / 8
			bitIdx := uint(bitOffset % 8)
			if byteIdx < len(payload) && payload[byteIdx]&(1<<bitIdx) != 0 {
				result[i] = append(result[i], j)
			}
			bitOffset++
		}
	}
	return result
}

// decodeSparse decodes per-entry dep lists.
func decodeSparse(payload []byte, n int) [][]int {
	result := make([][]int, n)
	off := 0
	for i := 0; i < n; i++ {
		count, k := binary.Uvarint(payload[off:])
		off += k
		if count > 0 {
			deps := make([]int, count)
			for d := range deps {
				v, k := binary.Uvarint(payload[off:])
				off += k
				deps[d] = int(v)
			}
			result[i] = deps
		}
	}
	return result
}

// zeroDepsCount counts entries with no dependencies.
func zeroDepsCount(deps map[int]map[int]bool, n int) int {
	count := 0
	for i := 0; i < n; i++ {
		if len(deps[i]) == 0 {
			count++
		}
	}
	return count
}

// fileRange returns the start and end block numbers for the file that covers blockNum.
func fileRange(blockNum uint64) (start, end uint64) {
	start = (blockNum / blocksPerFile) * blocksPerFile
	end = start + blocksPerFile - 1
	return
}

// popcount returns the number of set bits in a byte slice.
func popcount(b []byte) int {
	n := 0
	for _, v := range b {
		n += bits.OnesCount8(v)
	}
	return n
}
