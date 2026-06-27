// Copyright 2026 The Erigon Authors
// This file is part of Erigon.
//
// Erigon is free software: you can redistribute it and/or modify
// it under the terms of the GNU Lesser General Public License as published by
// the Free Software Foundation, either version 3 of the License, or
// (at your option) any later version.
//
// Erigon is distributed in the hope that it will be useful,
// but WITHOUT ANY WARRANTY; without even the implied warranty of
// MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE. See the
// GNU Lesser General Public License for more details.
//
// You should have received a copy of the GNU Lesser General Public License
// along with Erigon. If not, see <http://www.gnu.org/licenses/>.

// Package valfile implements an append-only external file holding raw domain
// values addressed by byte offset, so MDBX stores only a small offset+len Handle
// and the bulk bytes stay out of the B-tree.
package valfile

import (
	"bufio"
	"fmt"
	"io"
	"os"
	"sync"

	"github.com/c2h5oh/datasize"
)

// magic is the 3-byte tag plus a 1-byte format version.
var magic = [4]byte{'C', 'V', 'L', 1}

const headerLen = uint64(len(magic))

// Writer appends values to a step's value-file. Not safe for concurrent use.
type Writer struct {
	f       *os.File
	w       *bufio.Writer
	path    string
	pos     uint64 // logical EOF, including buffered (unsynced) bytes
	count   uint64
	noFsync bool
}

// NewWriter creates (or truncates) the file and writes the header.
func NewWriter(path string) (*Writer, error) {
	f, err := os.OpenFile(path, os.O_CREATE|os.O_TRUNC|os.O_RDWR, 0o644)
	if err != nil {
		return nil, err
	}
	w := &Writer{f: f, w: getBufioWriter(f), path: path}
	if _, err := w.w.Write(magic[:]); err != nil {
		f.Close()
		return nil, err
	}
	w.pos = headerLen
	return w, nil
}

// OpenWriter reopens an existing file to continue appending at its current EOF.
// Used on restart: any bytes past the last committed handle are left in place as
// harmless garbage and re-execution appends fresh values after them.
func OpenWriter(path string) (*Writer, error) {
	f, err := os.OpenFile(path, os.O_RDWR, 0o644)
	if err != nil {
		return nil, err
	}
	stat, err := f.Stat()
	if err != nil {
		f.Close()
		return nil, err
	}
	size := uint64(stat.Size())
	if size < headerLen {
		f.Close()
		return nil, fmt.Errorf("valfile %s too small to reopen: %d bytes", path, size)
	}
	var hdr [len(magic)]byte
	if _, err := f.ReadAt(hdr[:], 0); err != nil {
		f.Close()
		return nil, err
	}
	if hdr != magic {
		f.Close()
		return nil, fmt.Errorf("valfile %s: bad magic", path)
	}
	if _, err := f.Seek(int64(size), 0); err != nil {
		f.Close()
		return nil, err
	}
	return &Writer{f: f, w: getBufioWriter(f), path: path, pos: size}, nil
}

// DisableFsync turns off the pre-commit fsync: Sync still flushes buffered bytes
// to the fd (so readers see them) but skips fdatasync. Used by tests and to mirror
// MDBX's no-fsync mode, where durability is intentionally relaxed.
func (w *Writer) DisableFsync() { w.noFsync = true }

// Append writes v at the current EOF and returns the Handle the caller persists
// in MDBX. The bytes are durable only after a successful Sync.
func (w *Writer) Append(v []byte) (Handle, error) {
	h := Handle{Offset: w.pos, Len: uint32(len(v))}
	if _, err := w.w.Write(v); err != nil {
		return Handle{}, err
	}
	w.pos += uint64(len(v))
	w.count++
	return h, nil
}

// Sync flushes buffered bytes and fdatasyncs. Callers MUST Sync before committing
// the MDBX handles that reference the appended values.
func (w *Writer) Sync() error {
	if err := w.w.Flush(); err != nil {
		return err
	}
	if w.noFsync {
		return nil
	}
	return w.f.Sync()
}

// Size is the current logical EOF (including buffered bytes).
func (w *Writer) Size() uint64 { return w.pos }

// Count is the number of appended values.
func (w *Writer) Count() uint64 { return w.count }

// Close flushes and closes the file (does not fsync; call Sync for durability).
func (w *Writer) Close() error {
	if w.f == nil {
		return nil
	}
	defer func() { w.f = nil }()
	flushErr := w.w.Flush()
	closeErr := w.f.Close()
	putBufioWriter(w.w)
	w.w = nil
	if flushErr != nil {
		return flushErr
	}
	return closeErr
}

// Reader serves values by Handle via pread, which is safe while the file is
// still being appended and safe for concurrent readers.
type Reader struct {
	f    *os.File
	path string
}

// OpenReader opens a value-file read-only and validates its header.
func OpenReader(path string) (*Reader, error) {
	f, err := os.Open(path)
	if err != nil {
		return nil, err
	}
	var hdr [len(magic)]byte
	if _, err := f.ReadAt(hdr[:], 0); err != nil {
		f.Close()
		return nil, fmt.Errorf("valfile %s: %w", path, err)
	}
	if hdr != magic {
		f.Close()
		return nil, fmt.Errorf("valfile %s: bad magic", path)
	}
	return &Reader{f: f, path: path}, nil
}

// Get returns the value referenced by h. The result is written into dst when it
// has capacity, otherwise a new slice is allocated. Reading past EOF errors
// rather than panics.
func (r *Reader) Get(h Handle, dst []byte) ([]byte, error) {
	if h.Offset < headerLen {
		return nil, fmt.Errorf("valfile %s: handle offset %d inside header", r.path, h.Offset)
	}
	if cap(dst) >= int(h.Len) {
		dst = dst[:h.Len]
	} else {
		dst = make([]byte, h.Len)
	}
	if _, err := r.f.ReadAt(dst, int64(h.Offset)); err != nil {
		return nil, fmt.Errorf("valfile %s: read at %d len %d: %w", r.path, h.Offset, h.Len, err)
	}
	return dst, nil
}

// FilePath is the path the reader was opened from.
func (r *Reader) FilePath() string { return r.path }

// Close closes the file.
func (r *Reader) Close() error {
	if r.f == nil {
		return nil
	}
	defer func() { r.f = nil }()
	return r.f.Close()
}

// Erigon doesn't create tons of bufio readers/writers, but it has tons of
// parallel small unit-tests which each create many small files and bufio
// readers/writers — pooling avoids the allocation pressure in that scenario.
var bufioWriterPool = sync.Pool{New: func() any { return bufio.NewWriterSize(nil, int(512*datasize.KB)) }}

func getBufioWriter(w io.Writer) *bufio.Writer {
	bw := bufioWriterPool.Get().(*bufio.Writer)
	bw.Reset(w)
	return bw
}

// Reset(nil) before Put is required: without it the pool entry retains a
// reference to the underlying io.Writer/io.Reader, keeping it alive until the
// next GC cycle or until the entry is reused — whichever comes first.
func putBufioWriter(w *bufio.Writer) { w.Reset(nil); bufioWriterPool.Put(w) }
