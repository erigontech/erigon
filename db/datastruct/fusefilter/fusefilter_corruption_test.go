package fusefilter

import (
	"bytes"
	"encoding/binary"
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/require"
)

// validBlob builds a real, valid filter blob over `n` keys and returns its bytes.
func validBlob(t *testing.T, n int) []byte {
	t.Helper()
	w, err := NewWriterOffHeap(filepath.Join(t.TempDir(), "v"))
	require.NoError(t, err)
	defer w.Close()
	for i := 0; i < n; i++ {
		require.NoError(t, w.AddHash(uint64(i)))
	}
	var buf bytes.Buffer
	_, err = w.BuildTo(&buf)
	require.NoError(t, err)
	return buf.Bytes()
}

// validShardedBlob builds a real, valid sharded blob over `n` keys.
func validShardedBlob(t *testing.T, n int) []byte {
	t.Helper()
	w, err := NewWriterSharded(filepath.Join(t.TempDir(), "v"))
	require.NoError(t, err)
	defer w.Close()
	for i := 0; i < n; i++ {
		require.NoError(t, w.AddHash(uint64(i)))
	}
	var buf bytes.Buffer
	_, err = w.BuildTo(&buf)
	require.NoError(t, err)
	return buf.Bytes()
}

// header field offsets inside a per-shard fusefilter blob (see writeFilter):
//
//	0..4    version+features
//	4..8    SegmentCount
//	8..12   SegmentCountLength
//	12..20  Seed
//	20..24  SegmentLength
//	24..28  SegmentLengthMask
//	28..36  fingerprints length
const (
	hdrSegmentCount       = 4
	hdrSegmentCountLength = 8
	hdrSegmentLength      = 20
	hdrSegmentLengthMask  = 24
	hdrFingerprintsLen    = 28
)

func mutateU32(b []byte, off int, v uint32) []byte {
	out := append([]byte(nil), b...)
	binary.BigEndian.PutUint32(out[off:], v)
	return out
}
func mutateU64(b []byte, off int, v uint64) []byte {
	out := append([]byte(nil), b...)
	binary.BigEndian.PutUint64(out[off:], v)
	return out
}

func TestNewReaderOnBytes_RejectsCorrupt(t *testing.T) {
	require := require.New(t)
	good := validBlob(t, 1000)

	t.Run("too_short_for_header", func(t *testing.T) {
		_, _, err := NewReaderOnBytes(good[:headerSize-1], "x")
		require.Error(err)
	})

	t.Run("fingerprints_length_overflows", func(t *testing.T) {
		// fingerprintsLen == len(data)+1 → runs past the available bytes.
		bad := append([]byte(nil), good...)
		// Set fingerprints length to a value larger than what's actually present.
		dataLen := uint64(len(bad)-headerSize) + 1
		binary.BigEndian.PutUint64(bad[hdrFingerprintsLen:], dataLen)
		_, _, err := NewReaderOnBytes(bad, "x")
		require.Error(err)
	})

	t.Run("zero_SegmentLength", func(t *testing.T) {
		bad := mutateU32(good, hdrSegmentLength, 0)
		_, _, err := NewReaderOnBytes(bad, "x")
		require.Error(err)
	})

	t.Run("zero_SegmentCount", func(t *testing.T) {
		bad := mutateU32(good, hdrSegmentCount, 0)
		_, _, err := NewReaderOnBytes(bad, "x")
		require.Error(err)
	})

	t.Run("non_power_of_two_SegmentLength", func(t *testing.T) {
		bad := mutateU32(good, hdrSegmentLength, 1024+1)
		bad = mutateU32(bad, hdrSegmentLengthMask, 1024)
		_, _, err := NewReaderOnBytes(bad, "x")
		require.Error(err)
	})

	t.Run("inconsistent_mask", func(t *testing.T) {
		// SegmentLength is a power of two but mask doesn't match — would let
		// h1, h2 indices escape the per-segment range.
		r, _, err := NewReaderOnBytes(good, "x")
		require.NoError(err)
		segLen := r.inner.SegmentLength
		bad := mutateU32(good, hdrSegmentLengthMask, segLen) // should be segLen-1
		_, _, err = NewReaderOnBytes(bad, "x")
		require.Error(err)
	})

	t.Run("inconsistent_SegmentCountLength", func(t *testing.T) {
		r, _, err := NewReaderOnBytes(good, "x")
		require.NoError(err)
		// Lie: claim SegmentCountLength is one larger than SegmentCount*SegmentLength.
		bad := mutateU32(good, hdrSegmentCountLength, r.inner.SegmentCountLength+1)
		_, _, err = NewReaderOnBytes(bad, "x")
		require.Error(err)
	})

	t.Run("fingerprints_too_short_for_geometry", func(t *testing.T) {
		// Truncate fingerprints to a value that fits in the byte stream but is
		// smaller than (SegmentCount+2)*SegmentLength — the OOB-read condition.
		// We do this by claiming the data section is smaller than required.
		r, _, err := NewReaderOnBytes(good, "x")
		require.NoError(err)
		required := (uint64(r.inner.SegmentCount) + 2) * uint64(r.inner.SegmentLength)
		require.Greater(required, uint64(0))
		smaller := required - 1
		// Build a payload with fingerprintsLen=smaller, but we need at least that
		// many bytes after the header. The original blob is bigger so this is fine.
		bad := append([]byte(nil), good[:headerSize+int(smaller)]...)
		binary.BigEndian.PutUint64(bad[hdrFingerprintsLen:], smaller)
		_, _, err = NewReaderOnBytes(bad, "x")
		require.Error(err)
	})

	t.Run("SegmentCount_times_SegmentLength_overflows", func(t *testing.T) {
		// Force SegmentCount * SegmentLength to overflow uint32. Pair both fields
		// with a SegmentLengthMask consistent with SegmentLength so we hit the
		// overflow check, not the mask check.
		bad := mutateU32(good, hdrSegmentCount, 1<<20)
		bad = mutateU32(bad, hdrSegmentLength, 1<<13) // 8K, power of two
		bad = mutateU32(bad, hdrSegmentLengthMask, (1<<13)-1)
		// 1<<20 * 1<<13 = 1<<33 > MaxUint32
		_, _, err := NewReaderOnBytes(bad, "x")
		require.Error(err)
	})
}

func TestNewReaderShardedOnBytes_RejectsCorrupt(t *testing.T) {
	require := require.New(t)
	good := validShardedBlob(t, 1000)

	t.Run("too_short_for_outer_header", func(t *testing.T) {
		_, _, err := NewReaderShardedOnBytes(good[:3], "x")
		require.Error(err)
	})

	t.Run("unsupported_outer_version", func(t *testing.T) {
		bad := append([]byte(nil), good...)
		bad[0] = 99
		_, _, err := NewReaderShardedOnBytes(bad, "x")
		require.Error(err)
	})

	t.Run("truncated_shard_table", func(t *testing.T) {
		// Outer header (4 bytes) ok, but cut off mid-shard-table.
		_, _, err := NewReaderShardedOnBytes(good[:5], "x")
		require.Error(err)
	})

	t.Run("shard_size_overflows_remainder", func(t *testing.T) {
		// First 8-byte size in the shard table claims a blob bigger than what's
		// left in the buffer.
		bad := append([]byte(nil), good...)
		binary.BigEndian.PutUint64(bad[4:], 1<<40)
		_, _, err := NewReaderShardedOnBytes(bad, "x")
		require.Error(err)
	})

	t.Run("shard_size_smaller_than_header", func(t *testing.T) {
		// Find first non-empty shard slot and shrink it to less than headerSize.
		bad := append([]byte(nil), good...)
		offset := 4
		for i := 0; i < 256; i++ {
			sz := binary.BigEndian.Uint64(bad[offset:])
			if sz != 0 {
				binary.BigEndian.PutUint64(bad[offset:], 4) // < filterBlobHeaderSize
				break
			}
			offset += 8
		}
		_, _, err := NewReaderShardedOnBytes(bad, "x")
		require.Error(err)
	})

	t.Run("corrupt_inner_shard_geometry", func(t *testing.T) {
		// Find first non-empty shard, zero out its SegmentLength → inner
		// validation should reject without ever looking at fingerprints.
		bad := append([]byte(nil), good...)
		offset := 4
		for i := 0; i < 256; i++ {
			sz := binary.BigEndian.Uint64(bad[offset:])
			offset += 8
			if sz != 0 {
				// Zero the SegmentLength field of the inner shard header.
				binary.BigEndian.PutUint32(bad[offset+hdrSegmentLength:], 0)
				break
			}
		}
		_, _, err := NewReaderShardedOnBytes(bad, "x")
		require.Error(err)
	})
}
