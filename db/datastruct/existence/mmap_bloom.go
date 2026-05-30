// Copyright 2026 The Erigon Authors
// SPDX-License-Identifier: LGPL-3.0-or-later

package existence

import (
	"bytes"
	"encoding/binary"
	"fmt"
	"os"
	"unsafe"

	"github.com/edsrzf/mmap-go"
)

// mmapBloom is a memory-mapped read-only bloom filter, binary-compatible with
// holiman/bloomfilter v02 files. Same ContainsHash semantics; the bit array
// lives in the OS page cache instead of the Go heap, so opening many large
// existence-filter files no longer pins multiple GB of heap.
type mmapBloom struct {
	file *os.File
	mmap mmap.MMap // backing region — header + keys + bits + trailing sha
	keys []uint64  // copied into Go memory (small: K=3..10 entries)
	bits []uint64  // aliases the mmap region; do not write to
	m    uint64    // number of bits
	n    uint64    // number of inserted elements (informational)
}

// bloom v02 on-disk layout:
//
//	offset  0  | 8B   | magic = 8 zero bytes
//	offset  8  | 4B   | version = "v02\n"
//	offset 12  | 8B   | k uint64 little-endian (number of hash keys)
//	offset 20  | 8B   | n uint64 little-endian (count of inserted entries)
//	offset 28  | 8B   | m uint64 little-endian (number of bits)
//	offset 36  | k*8B | keys[k] uint64 little-endian
//	offset 36+ | …    | bits[ceil(m/64)] uint64 little-endian
//	tail       | 48B  | sha512/384 hash of all previous bytes (ignored on read; matches NoVerify behaviour)
//
// The bits offset (36 + k*8) is generally not 8-byte aligned, but x86_64
// tolerates misaligned uint64 reads with negligible cost; building this for
// non-x86 would require either padding the file or copying the bits to a
// new-aligned buffer (defeats the purpose).
const (
	bloomHeaderSize = 36      // 8 magic + 4 version + 3*8 (k/n/m)
	bloomTrailerLen = 48      // sha512/384
	bloomMaxKeys    = 1 << 10 // sanity bound for k
	bloomMaxBits    = 1 << 48 // sanity bound for m
)

// openMmapBloom mmaps the bloom-filter file at filePath read-only and returns
// a reader that satisfies ContainsHash queries directly from the mmap. The
// caller must Close the returned bloom to release the mapping.
func openMmapBloom(filePath string) (*mmapBloom, error) {
	f, err := os.Open(filePath)
	if err != nil {
		return nil, err
	}
	st, err := f.Stat()
	if err != nil {
		f.Close()
		return nil, err
	}
	size := st.Size()
	if size < int64(bloomHeaderSize+bloomTrailerLen) {
		f.Close()
		return nil, fmt.Errorf("bloom file too small: %s (%d bytes)", filePath, size)
	}

	region, err := mmap.MapRegion(f, int(size), mmap.RDONLY, 0, 0)
	if err != nil {
		f.Close()
		return nil, fmt.Errorf("mmap %s: %w", filePath, err)
	}

	// header: 8 magic + 4 version + 3*8 k/n/m
	if !bytes.Equal(region[:8], []byte{0, 0, 0, 0, 0, 0, 0, 0}) {
		region.Unmap() //nolint:errcheck
		f.Close()
		return nil, fmt.Errorf("bloom %s: bad magic", filePath)
	}
	if !bytes.Equal(region[8:12], []byte("v02\n")) {
		region.Unmap() //nolint:errcheck
		f.Close()
		return nil, fmt.Errorf("bloom %s: unsupported version %q", filePath, region[8:12])
	}
	k := binary.LittleEndian.Uint64(region[12:20])
	n := binary.LittleEndian.Uint64(region[20:28])
	m := binary.LittleEndian.Uint64(region[28:36])
	if k == 0 || k > bloomMaxKeys {
		region.Unmap() //nolint:errcheck
		f.Close()
		return nil, fmt.Errorf("bloom %s: implausible k=%d", filePath, k)
	}
	if m == 0 || m > bloomMaxBits {
		region.Unmap() //nolint:errcheck
		f.Close()
		return nil, fmt.Errorf("bloom %s: implausible m=%d", filePath, m)
	}

	keysOffset := uint64(bloomHeaderSize)
	bitsOffset := keysOffset + k*8
	bitsLen := (m + 63) / 64
	bitsSize := bitsLen * 8
	expectedSize := bitsOffset + bitsSize + bloomTrailerLen
	if uint64(size) != expectedSize {
		region.Unmap() //nolint:errcheck
		f.Close()
		return nil, fmt.Errorf("bloom %s: size mismatch want=%d got=%d (k=%d, m=%d)", filePath, expectedSize, size, k, m)
	}

	// Copy the (small) keys array into Go memory so it's stable across any
	// future mmap-backing changes and so we can compare/hash it without
	// re-reading from the mmap each ContainsHash call.
	keys := make([]uint64, k)
	for i := uint64(0); i < k; i++ {
		keys[i] = binary.LittleEndian.Uint64(region[keysOffset+i*8 : keysOffset+(i+1)*8])
	}

	// Alias the bits region as []uint64. The mmap.MMap is itself a []byte that
	// stays alive for the lifetime of the bloom; bits is a view over it, so no
	// GC concerns. Misaligned access is intentional (see comment above).
	bits := unsafe.Slice((*uint64)(unsafe.Pointer(&region[bitsOffset])), int(bitsLen))

	return &mmapBloom{
		file: f,
		mmap: region,
		keys: keys,
		bits: bits,
		m:    m,
		n:    n,
	}, nil
}

// ContainsHash mirrors github.com/holiman/bloomfilter/v2.Filter.ContainsHash
// (same rotation constant, same key-mixing loop) so it answers identically on
// the same on-disk filter.
func (b *mmapBloom) ContainsHash(hash uint64) bool {
	const rotation = 17
	r := uint64(1)
	for n := 0; n < len(b.keys) && r != 0; n++ {
		hash = ((hash << rotation) | (hash >> (64 - rotation))) ^ b.keys[n]
		i := hash % b.m
		r &= (b.bits[i>>6] >> uint(i&0x3f)) & 1
	}
	return r != 0
}

func (b *mmapBloom) Close() error {
	if b == nil {
		return nil
	}
	if b.mmap != nil {
		if err := b.mmap.Unmap(); err != nil {
			b.file.Close() //nolint:errcheck
			return err
		}
		b.mmap = nil
		b.bits = nil
	}
	if b.file != nil {
		err := b.file.Close()
		b.file = nil
		return err
	}
	return nil
}
