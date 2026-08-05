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

// Package tempbal is a file-backed store for synthetic ("temp") Block Access
// Lists generated for blocks that carry no BAL of their own. It is optimised
// for a single forward pass over a contiguous block range: the log is
// memory-mapped and advised MADV_SEQUENTIAL, so reads are served from OS page
// cache with kernel readahead and never held on the Go heap — keeping heap
// small so it doesn't evict the mmapped state-snapshot pages under measurement.
package tempbal

import (
	"bufio"
	"encoding/binary"
	"io"
	"os"
	"path/filepath"

	"github.com/erigontech/erigon/common"
	"github.com/erigontech/erigon/common/length"
	"github.com/erigontech/erigon/common/mmap"
)

const (
	fileName   = "temp-bal.v1.log"
	magic      = "TMPBAL01"
	headerSize = len(magic)
	// record prefix: blockNum(u64) + hash + payloadLen(u32)
	recPrefix = 8 + length.Hash + 4
)

// Log layout (little-endian): the file magic, then a sequence of records:
//   blockNum uint64 | hash [32]byte | payloadLen uint32 | payload [payloadLen]byte

// walkRecords calls fn for each intact record in the mapped log, stopping at
// EOF or the first torn trailing record (a crash mid-append).
func walkRecords(data []byte, fn func(blockNum uint64, hash common.Hash, payloadOff, payloadLen int)) {
	for off := headerSize; off+recPrefix <= len(data); {
		blockNum := binary.LittleEndian.Uint64(data[off : off+8])
		var hash common.Hash
		copy(hash[:], data[off+8:off+8+length.Hash])
		payloadLen := int(binary.LittleEndian.Uint32(data[off+8+length.Hash : off+recPrefix]))
		payloadOff := off + recPrefix
		if payloadOff+payloadLen > len(data) {
			return
		}
		fn(blockNum, hash, payloadOff, payloadLen)
		off = payloadOff + payloadLen
	}
}

// Writer appends BAL records for strictly increasing block numbers.
type Writer struct {
	f        *os.File
	buf      *bufio.Writer
	lastNum  uint64
	haveLast bool
}

// NewWriter opens (creating if needed) the temp-BAL log under dir for append.
func NewWriter(dir string) (*Writer, error) {
	if err := os.MkdirAll(dir, 0o755); err != nil {
		return nil, err
	}
	f, err := os.OpenFile(filepath.Join(dir, fileName), os.O_RDWR|os.O_CREATE, 0o644)
	if err != nil {
		return nil, err
	}
	w := &Writer{f: f}
	if err := w.init(); err != nil {
		f.Close()
		return nil, err
	}
	w.buf = bufio.NewWriterSize(f, 1<<20)
	return w, nil
}

func (w *Writer) init() error {
	fi, err := w.f.Stat()
	if err != nil {
		return err
	}
	if fi.Size() <= int64(headerSize) {
		if err := w.f.Truncate(0); err != nil {
			return err
		}
		_, err := w.f.Write([]byte(magic))
		return err
	}
	data, handle, err := mmap.Mmap(w.f, int(fi.Size()))
	if err != nil {
		return err
	}
	walkRecords(data, func(blockNum uint64, _ common.Hash, _, _ int) {
		w.lastNum, w.haveLast = blockNum, true
	})
	mmap.Munmap(data, handle)
	_, err = w.f.Seek(0, io.SeekEnd)
	return err
}

// Append writes bal for blockNum. Records are stored in ascending block order;
// a blockNum at or below the last appended one is already stored (the resume
// overlap after an interrupted run) and is skipped.
func (w *Writer) Append(blockNum uint64, hash common.Hash, bal []byte) error {
	if w.haveLast && blockNum <= w.lastNum {
		return nil
	}
	var hdr [recPrefix]byte
	binary.LittleEndian.PutUint64(hdr[0:8], blockNum)
	copy(hdr[8:8+length.Hash], hash[:])
	binary.LittleEndian.PutUint32(hdr[8+length.Hash:], uint32(len(bal)))
	if _, err := w.buf.Write(hdr[:]); err != nil {
		return err
	}
	if _, err := w.buf.Write(bal); err != nil {
		return err
	}
	w.lastNum, w.haveLast = blockNum, true
	// Flush each record so a crash during a long generation run keeps all
	// blocks written so far (a reopened writer resumes after them).
	return w.buf.Flush()
}

func (w *Writer) Close() error {
	if w.buf != nil {
		if err := w.buf.Flush(); err != nil {
			w.f.Close()
			return err
		}
	}
	return w.f.Close()
}

// Reader memory-maps the temp-BAL log and indexes it by block number.
type Reader struct {
	data   []byte
	handle *[mmap.MaxMapSize]byte
	index  map[uint64]recLoc
}

type recLoc struct {
	hash   common.Hash
	off    int
	length int
}

// OpenReader mmaps the temp-BAL log under dir, advises sequential access
// (kernel readahead) and builds an in-memory offset index by walking record
// headers. The walk runs at open time — before the measured window — so it
// warms the mapping without adding random I/O during execution.
func OpenReader(dir string) (*Reader, error) {
	f, err := os.Open(filepath.Join(dir, fileName))
	if err != nil {
		return nil, err
	}
	defer f.Close()
	fi, err := f.Stat()
	if err != nil {
		return nil, err
	}
	size := int(fi.Size())
	r := &Reader{index: map[uint64]recLoc{}}
	if size <= headerSize {
		return r, nil
	}
	data, handle, err := mmap.Mmap(f, size)
	if err != nil {
		return nil, err
	}
	if err := mmap.MadviseSequential(data); err != nil {
		mmap.Munmap(data, handle)
		return nil, err
	}
	r.data, r.handle = data, handle
	walkRecords(data, func(blockNum uint64, hash common.Hash, payloadOff, payloadLen int) {
		r.index[blockNum] = recLoc{hash: hash, off: payloadOff, length: payloadLen}
	})
	return r, nil
}

// Get returns the stored BAL bytes for blockNum, but only when the record was
// written for the same block hash (guards against feeding a stale/forked BAL).
// The returned slice aliases the mmap and is valid until Close.
func (r *Reader) Get(blockNum uint64, hash common.Hash) ([]byte, bool) {
	loc, ok := r.index[blockNum]
	if !ok || loc.hash != hash {
		return nil, false
	}
	return r.data[loc.off : loc.off+loc.length], true
}

func (r *Reader) Len() int { return len(r.index) }

func (r *Reader) Close() error {
	if r.data == nil {
		return nil
	}
	err := mmap.Munmap(r.data, r.handle)
	r.data, r.handle = nil, nil
	return err
}
