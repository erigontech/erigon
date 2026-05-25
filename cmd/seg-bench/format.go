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

// wholeStreamWriter / wholeStreamReader implement the whole-file framing
// mode used by the Q2 (interchange codec) comparison. The whole record
// stream is length-prefix-delimited (4 byte little-endian length + bytes
// per record) then handed to the codec as a single buffer. The codec
// frames the *whole* stream as one frame.
//
// This is the bulk-sync-optimal shape: smaller compressed output (no
// per-record offset table, codec gets the full window for pattern
// reuse), but random access requires decoding the entire stream and
// walking forward to the target record.
//
// Layout of the *raw* (uncompressed) buffer fed to the codec:
//
//	[ u32 len_0 ][ record_0 ][ u32 len_1 ][ record_1 ]...[ u32 len_N ][ record_N ]
//
// The codec output is opaque codec bytes — one frame per file.

type wholeStreamWriter struct {
	raw []byte
}

func newWholeStreamWriter() *wholeStreamWriter { return &wholeStreamWriter{} }

// AddRecord appends one record to the length-prefix-framed raw buffer.
func (w *wholeStreamWriter) AddRecord(rec []byte) {
	var u32 [4]byte
	binary.LittleEndian.PutUint32(u32[:], uint32(len(rec)))
	w.raw = append(w.raw, u32[:]...)
	w.raw = append(w.raw, rec...)
}

// Bytes returns the raw length-prefix-framed buffer to hand to a codec.
func (w *wholeStreamWriter) Bytes() []byte { return w.raw }

// wholeStreamReader iterates records out of a decoded whole-stream buffer.
type wholeStreamReader struct {
	buf []byte
	pos int
	cnt int
}

func newWholeStreamReader(buf []byte) *wholeStreamReader {
	r := &wholeStreamReader{buf: buf}
	// Count records once for reporting / random-access bounds.
	p := 0
	for p+4 <= len(buf) {
		n := int(binary.LittleEndian.Uint32(buf[p:]))
		p += 4 + n
		r.cnt++
	}
	return r
}

// Count returns the total number of records in the decoded buffer.
func (r *wholeStreamReader) Count() int { return r.cnt }

// Reset rewinds the iterator to the start.
func (r *wholeStreamReader) Reset() { r.pos = 0 }

// Next returns the next record (zero-copy slice into the buffer) and
// advances. Returns nil, false at end-of-stream.
func (r *wholeStreamReader) Next() ([]byte, bool) {
	if r.pos+4 > len(r.buf) {
		return nil, false
	}
	n := int(binary.LittleEndian.Uint32(r.buf[r.pos:]))
	r.pos += 4
	if r.pos+n > len(r.buf) {
		return nil, false
	}
	rec := r.buf[r.pos : r.pos+n]
	r.pos += n
	return rec, true
}

// At returns record i by walking from the start. O(i) — whole-stream
// framing's random-access cost on top of the full-stream decode cost.
func (r *wholeStreamReader) At(i int) ([]byte, error) {
	if i < 0 || i >= r.cnt {
		return nil, fmt.Errorf("record index %d out of range [0, %d)", i, r.cnt)
	}
	p := 0
	for j := 0; j <= i; j++ {
		if p+4 > len(r.buf) {
			return nil, fmt.Errorf("whole-stream truncated before record %d", i)
		}
		n := int(binary.LittleEndian.Uint32(r.buf[p:]))
		p += 4
		if j == i {
			if p+n > len(r.buf) {
				return nil, fmt.Errorf("whole-stream truncated at record %d body", i)
			}
			return r.buf[p : p+n], nil
		}
		p += n
	}
	return nil, fmt.Errorf("unreachable")
}
