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

// Package offlinebal is a file-backed store for synthetic Block Access
// Lists generated for blocks that carry no BAL of their own. It is optimised
// for a single forward pass over a contiguous block range: the log is
// memory-mapped and advised MADV_SEQUENTIAL, so reads are served from OS page
// cache with kernel readahead and never held on the Go heap — keeping heap
// small so it doesn't evict the mmapped state-snapshot pages under measurement.
package offlinebal

import (
	"bufio"
	"encoding/binary"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"

	"github.com/erigontech/erigon/common"
	"github.com/erigontech/erigon/common/length"
	"github.com/erigontech/erigon/common/mmap"
)

const (
	fileName      = "temp-bal.v1.log"
	indexFileName = "temp-bal.v1.idx"
	magic         = "TMPBAL01"
	indexMagic    = "TMPBALI1"
	headerSize    = len(magic)
	// index header: indexMagic + firstBlock(u64)
	indexHeaderSize = len(indexMagic) + 8
	// record prefix: blockNum(u64) + hash + payloadLen(u32)
	recPrefix = 8 + length.Hash + 4
)

// Log layout (little-endian): the file magic, then a sequence of records:
//   blockNum uint64 | hash [32]byte | payloadLen uint32 | payload [payloadLen]byte
//
// Index layout (little-endian): indexMagic, firstBlock uint64, then one uint64
// log offset per block from firstBlock on; 0 marks a block with no record.
// The writer appends an index entry only after its record is flushed, so the
// index never points past the durable log.

// walkRecords calls fn for each intact record in the mapped log, stopping at
// EOF or the first torn trailing record (a crash mid-append).
func walkRecords(data []byte, fn func(blockNum uint64, hash common.Hash, recOff, payloadLen int)) {
	for off := headerSize; off+recPrefix <= len(data); {
		blockNum := binary.LittleEndian.Uint64(data[off : off+8])
		var hash common.Hash
		copy(hash[:], data[off+8:off+8+length.Hash])
		payloadLen := int(binary.LittleEndian.Uint32(data[off+8+length.Hash : off+recPrefix]))
		if off+recPrefix+payloadLen > len(data) {
			return
		}
		fn(blockNum, hash, off, payloadLen)
		off += recPrefix + payloadLen
	}
}

// Writer appends BAL records for strictly increasing block numbers and keeps
// the offset index in step with the log.
type Writer struct {
	log, idx       *os.File
	logBuf, idxBuf *bufio.Writer
	logEnd         int64
	first, next    uint64
	haveFirst      bool
}

// NewWriter opens (creating if needed) the offline-BAL log and index under dir
// for append. A log without an index is walked once to build it. Log bytes
// past the last indexed record are cut.
func NewWriter(dir string) (*Writer, error) {
	if err := os.MkdirAll(dir, 0o755); err != nil {
		return nil, err
	}
	log, err := os.OpenFile(filepath.Join(dir, fileName), os.O_RDWR|os.O_CREATE, 0o644)
	if err != nil {
		return nil, err
	}
	idx, err := os.OpenFile(filepath.Join(dir, indexFileName), os.O_RDWR|os.O_CREATE, 0o644)
	if err != nil {
		log.Close()
		return nil, err
	}
	w := &Writer{log: log, idx: idx, logBuf: bufio.NewWriterSize(log, 1<<20), idxBuf: bufio.NewWriterSize(idx, 1<<16)}
	if err := w.init(); err != nil {
		log.Close()
		idx.Close()
		return nil, err
	}
	return w, nil
}

func (w *Writer) init() error {
	logInfo, err := w.log.Stat()
	if err != nil {
		return err
	}
	idxInfo, err := w.idx.Stat()
	if err != nil {
		return err
	}
	switch {
	case logInfo.Size() <= int64(headerSize):
		if err := w.resetLog(); err != nil {
			return err
		}
		if err := w.idx.Truncate(0); err != nil {
			return err
		}
	case idxInfo.Size() < int64(indexHeaderSize)+8:
		if err := w.idx.Truncate(0); err != nil {
			return err
		}
		if err := w.buildIndex(logInfo.Size()); err != nil {
			return err
		}
	default:
		if err := w.loadIndex(idxInfo.Size(), logInfo.Size()); err != nil {
			return err
		}
	}
	if err := w.log.Truncate(w.logEnd); err != nil {
		return err
	}
	if _, err := w.log.Seek(w.logEnd, io.SeekStart); err != nil {
		return err
	}
	_, err = w.idx.Seek(0, io.SeekEnd)
	return err
}

func (w *Writer) resetLog() error {
	if err := w.log.Truncate(0); err != nil {
		return err
	}
	if _, err := w.log.WriteAt([]byte(magic), 0); err != nil {
		return err
	}
	w.logEnd = int64(headerSize)
	return nil
}

func (w *Writer) buildIndex(logSize int64) error {
	data, err := mmap.OpenRo(w.log, int(logSize))
	if err != nil {
		return err
	}
	w.logEnd = int64(headerSize)
	walkRecords(data, func(blockNum uint64, _ common.Hash, recOff, payloadLen int) {
		if err == nil && (!w.haveFirst || blockNum >= w.next) {
			err = w.appendIndex(blockNum, int64(recOff))
			w.logEnd = int64(recOff + recPrefix + payloadLen)
		}
	})
	if unmapErr := data.Unmap(); err == nil {
		err = unmapErr
	}
	if err != nil {
		return err
	}
	return w.idxBuf.Flush()
}

func (w *Writer) loadIndex(idxSize, logSize int64) error {
	entries := (idxSize - int64(indexHeaderSize)) / 8
	hdr := make([]byte, indexHeaderSize)
	if _, err := w.idx.ReadAt(hdr, 0); err != nil || string(hdr[:len(indexMagic)]) != indexMagic {
		return fmt.Errorf("offline BAL index %s is corrupt", w.idx.Name())
	}
	if err := w.idx.Truncate(int64(indexHeaderSize) + entries*8); err != nil {
		return err
	}
	var last [8]byte
	if _, err := w.idx.ReadAt(last[:], int64(indexHeaderSize)+(entries-1)*8); err != nil {
		return err
	}
	recOff := int64(binary.LittleEndian.Uint64(last[:]))
	var rec [recPrefix]byte
	if recOff+recPrefix > logSize {
		return fmt.Errorf("offline BAL index %s points past the log", w.idx.Name())
	}
	if _, err := w.log.ReadAt(rec[:], recOff); err != nil {
		return err
	}
	w.logEnd = recOff + recPrefix + int64(binary.LittleEndian.Uint32(rec[8+length.Hash:]))
	if w.logEnd > logSize {
		return fmt.Errorf("offline BAL index %s points past the log", w.idx.Name())
	}
	w.first = binary.LittleEndian.Uint64(hdr[len(indexMagic):])
	w.next = w.first + uint64(entries)
	w.haveFirst = true
	return nil
}

func (w *Writer) appendIndex(blockNum uint64, recOff int64) error {
	var b [8]byte
	if !w.haveFirst {
		w.first, w.next, w.haveFirst = blockNum, blockNum, true
		binary.LittleEndian.PutUint64(b[:], blockNum)
		if _, err := w.idxBuf.WriteString(indexMagic); err != nil {
			return err
		}
		if _, err := w.idxBuf.Write(b[:]); err != nil {
			return err
		}
	}
	var zero [8]byte
	for ; w.next < blockNum; w.next++ {
		if _, err := w.idxBuf.Write(zero[:]); err != nil {
			return err
		}
	}
	binary.LittleEndian.PutUint64(b[:], uint64(recOff))
	if _, err := w.idxBuf.Write(b[:]); err != nil {
		return err
	}
	w.next++
	return nil
}

// Append writes bal for blockNum. Records are stored in ascending block order;
// a blockNum at or below the last appended one is already stored (the resume
// overlap after an interrupted run) and is skipped.
func (w *Writer) Append(blockNum uint64, hash common.Hash, bal []byte) error {
	if w.haveFirst && blockNum < w.next {
		return nil
	}
	var hdr [recPrefix]byte
	binary.LittleEndian.PutUint64(hdr[0:8], blockNum)
	copy(hdr[8:8+length.Hash], hash[:])
	binary.LittleEndian.PutUint32(hdr[8+length.Hash:], uint32(len(bal)))
	if _, err := w.logBuf.Write(hdr[:]); err != nil {
		return err
	}
	if _, err := w.logBuf.Write(bal); err != nil {
		return err
	}
	// Flush each record so a crash during a long generation run keeps all
	// blocks written so far (a reopened writer resumes after them).
	if err := w.logBuf.Flush(); err != nil {
		return err
	}
	recOff := w.logEnd
	w.logEnd += int64(recPrefix + len(bal))
	if err := w.appendIndex(blockNum, recOff); err != nil {
		return err
	}
	return w.idxBuf.Flush()
}

func (w *Writer) Close() error {
	return errors.Join(w.logBuf.Flush(), w.idxBuf.Flush(), w.log.Close(), w.idx.Close())
}

// Reader memory-maps the offline-BAL log and looks records up through the
// offset index, so opening it reads only the index.
type Reader struct {
	data    mmap.Ro
	offsets []byte
	first   uint64
	count   int
}

// OpenReader loads the index and mmaps the log under dir with sequential
// access advice (kernel readahead).
func OpenReader(dir string) (*Reader, error) {
	idx, err := os.ReadFile(filepath.Join(dir, indexFileName))
	if err != nil {
		return nil, err
	}
	r := &Reader{}
	if len(idx) == 0 {
		return r, nil
	}
	if len(idx) < indexHeaderSize || string(idx[:len(indexMagic)]) != indexMagic {
		return nil, fmt.Errorf("offline BAL index in %s is corrupt", dir)
	}
	r.first = binary.LittleEndian.Uint64(idx[len(indexMagic):])
	r.offsets = idx[indexHeaderSize : indexHeaderSize+(len(idx)-indexHeaderSize)/8*8]
	for i := 0; i < len(r.offsets); i += 8 {
		if binary.LittleEndian.Uint64(r.offsets[i:]) != 0 {
			r.count++
		}
	}

	f, err := os.Open(filepath.Join(dir, fileName))
	if err != nil {
		return nil, err
	}
	defer f.Close()
	fi, err := f.Stat()
	if err != nil {
		return nil, err
	}
	if fi.Size() <= int64(headerSize) {
		return &Reader{}, nil
	}
	data, err := mmap.OpenRo(f, int(fi.Size()))
	if err != nil {
		return nil, err
	}
	if err := mmap.MadviseSequential(data); err != nil {
		_ = data.Unmap()
		return nil, err
	}
	r.data = data
	return r, nil
}

// Get returns the stored BAL bytes for blockNum, but only when the record was
// written for the same block hash (guards against feeding a stale/forked BAL).
// The returned slice aliases the mmap and is valid until Close.
func (r *Reader) Get(blockNum uint64, hash common.Hash) ([]byte, bool) {
	if blockNum < r.first || blockNum-r.first >= uint64(len(r.offsets)/8) {
		return nil, false
	}
	off := int(binary.LittleEndian.Uint64(r.offsets[(blockNum-r.first)*8:]))
	if off == 0 || off+recPrefix > len(r.data) ||
		binary.LittleEndian.Uint64(r.data[off:]) != blockNum ||
		common.Hash(r.data[off+8:off+8+length.Hash]) != hash {
		return nil, false
	}
	payloadOff := off + recPrefix
	payloadLen := int(binary.LittleEndian.Uint32(r.data[off+8+length.Hash:]))
	if payloadOff+payloadLen > len(r.data) {
		return nil, false
	}
	return r.data[payloadOff : payloadOff+payloadLen], true
}

func (r *Reader) Len() int { return r.count }

func (r *Reader) Close() error {
	if r.data == nil {
		return nil
	}
	return r.data.Unmap()
}

// Store is the offline-BAL store opened for one mode: a Writer to generate, a
// Reader to use, or neither.
type Store struct {
	Writer *Writer
	Reader *Reader
}

func Open(generate, use bool, dir string) (Store, error) {
	switch {
	case generate && use:
		return Store{}, errors.New("offline BALs: generate and use are mutually exclusive")
	case generate:
		w, err := NewWriter(dir)
		return Store{Writer: w}, err
	case use:
		r, err := OpenReader(dir)
		return Store{Reader: r}, err
	}
	return Store{}, nil
}

func (s Store) Close() error {
	if s.Writer != nil {
		return s.Writer.Close()
	}
	if s.Reader != nil {
		return s.Reader.Close()
	}
	return nil
}
