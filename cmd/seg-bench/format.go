package main

import (
	"encoding/binary"
	"fmt"
)

// framedWriter / framedReader implement the per-record framing format
// the benchmark uses as its measurement vehicle. See the design note
// (codec-benchmark-design.md §"The per-record framing format") for
// rationale.
//
// Layout:
//   [ u32  record_count                                              ]
//   [ u64  offset_0   ]  ─┐
//   [ u64  offset_1   ]   │ offset table: byte offset of each
//   [ ...             ]   │ codec frame within the frames section.
//   [ u64  offset_N   ]  ─┘
//   [ u64  total_bytes ]   end-of-frames marker (offset_{N+1})
//   [ frame_0 ][ frame_1 ]...[ frame_N ]
//
// Random-access decode for record i:
//   1. Read offset[i] and offset[i+1] from the table (16 bytes total).
//   2. Read offset[i+1] - offset[i] bytes from frames section at
//      framesStart + offset[i].
//   3. Decode one codec frame.

type framedWriter struct {
	offsets []uint64
	frames  []byte
}

func newFramedWriter() *framedWriter { return &framedWriter{} }

// AddFrame appends one per-record codec frame. Caller retains
// ownership of the slice; we copy.
func (fw *framedWriter) AddFrame(frame []byte) {
	fw.offsets = append(fw.offsets, uint64(len(fw.frames)))
	fw.frames = append(fw.frames, frame...)
}

// Bytes serialises header + offset table + frames into a single
// buffer.
func (fw *framedWriter) Bytes() []byte {
	n := len(fw.offsets)
	out := make([]byte, 0, 4+8*(n+1)+len(fw.frames))
	var u32 [4]byte
	binary.LittleEndian.PutUint32(u32[:], uint32(n))
	out = append(out, u32[:]...)
	var u64 [8]byte
	for _, off := range fw.offsets {
		binary.LittleEndian.PutUint64(u64[:], off)
		out = append(out, u64[:]...)
	}
	binary.LittleEndian.PutUint64(u64[:], uint64(len(fw.frames)))
	out = append(out, u64[:]...)
	out = append(out, fw.frames...)
	return out
}

type framedReader struct {
	buf         []byte
	count       int
	offsets     []uint64 // length count+1
	framesStart int
}

func newFramedReader(buf []byte) (*framedReader, error) {
	if len(buf) < 4 {
		return nil, fmt.Errorf("framed buffer too small (%d bytes)", len(buf))
	}
	count := int(binary.LittleEndian.Uint32(buf[:4]))
	needHdr := 4 + 8*(count+1)
	if needHdr > len(buf) {
		return nil, fmt.Errorf("framed buffer header truncated: count=%d need=%d have=%d", count, needHdr, len(buf))
	}
	offsets := make([]uint64, count+1)
	for i := 0; i <= count; i++ {
		offsets[i] = binary.LittleEndian.Uint64(buf[4+8*i:])
	}
	framesStart := needHdr
	// Sanity: last offset must equal len(frames section).
	expectedTotal := offsets[count]
	if framesStart+int(expectedTotal) != len(buf) {
		return nil, fmt.Errorf("framed buffer body size mismatch: expected %d, have %d",
			framesStart+int(expectedTotal), len(buf))
	}
	return &framedReader{
		buf:         buf,
		count:       count,
		offsets:     offsets,
		framesStart: framesStart,
	}, nil
}

// Count returns the number of records.
func (fr *framedReader) Count() int { return fr.count }

// Frame returns the raw codec frame bytes for record i (no decoding).
// Caller must NOT modify the returned slice.
func (fr *framedReader) Frame(i int) ([]byte, error) {
	if i < 0 || i >= fr.count {
		return nil, fmt.Errorf("record index %d out of range [0, %d)", i, fr.count)
	}
	start := fr.framesStart + int(fr.offsets[i])
	end := fr.framesStart + int(fr.offsets[i+1])
	if end > len(fr.buf) {
		return nil, fmt.Errorf("frame %d end past buffer", i)
	}
	return fr.buf[start:end], nil
}
