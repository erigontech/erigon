// Copyright 2022 The Erigon Authors
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

package bitmapdb

import (
	"bufio"
	"encoding/binary"
	"fmt"
	"os"
	"path/filepath"
	"reflect"
	"time"
	"unsafe"

	"github.com/erigontech/erigon-lib/common/dir"

	"github.com/c2h5oh/datasize"
	mmap2 "github.com/edsrzf/mmap-go"

	"github.com/erigontech/erigon-lib/log/v3"
)

type FixedSizeBitmaps struct {
	f                  *os.File
	filePath, fileName string

	data []uint64

	metaData   []byte
	count      uint64 //of keys
	baseDataID uint64 // deducted from all stored values
	version    uint8

	m             mmap2.MMap
	bitsPerBitmap int
	size          int
	modTime       time.Time
}

func OpenFixedSizeBitmaps(filePath string) (*FixedSizeBitmaps, error) {
	_, fName := filepath.Split(filePath)
	idx := &FixedSizeBitmaps{
		filePath: filePath,
		fileName: fName,
	}

	var err error
	idx.f, err = os.Open(filePath)
	if err != nil {
		return nil, fmt.Errorf("OpenFixedSizeBitmaps: %w", err)
	}
	var stat os.FileInfo
	if stat, err = idx.f.Stat(); err != nil {
		return nil, err
	}
	idx.size = int(stat.Size())
	idx.modTime = stat.ModTime()
	idx.m, err = mmap2.MapRegion(idx.f, idx.size, mmap2.RDONLY, 0, 0)
	if err != nil {
		return nil, err
	}
	idx.metaData = idx.m[:MetaHeaderSize]
	idx.data = castToArrU64(idx.m[MetaHeaderSize:])

	idx.version = idx.metaData[0]
	pos := 1
	idx.count = binary.BigEndian.Uint64(idx.metaData[pos : pos+8])
	pos += 8
	idx.baseDataID = binary.BigEndian.Uint64(idx.metaData[pos : pos+8])
	pos += 8
	idx.bitsPerBitmap = int(binary.BigEndian.Uint16(idx.metaData[pos : pos+8]))
	pos += 2 // nolint
	if idx.bitsPerBitmap*int(idx.count)/8 > idx.size-MetaHeaderSize {
		return nil, fmt.Errorf("file metadata doesn't match file length: bitsPerBitmap=%d, count=%d, len=%d, %s", idx.bitsPerBitmap, int(idx.count), idx.size, fName)
	}
	return idx, nil
}

func (bm *FixedSizeBitmaps) FileName() string { return bm.fileName }
func (bm *FixedSizeBitmaps) FilePath() string { return bm.filePath }
func (bm *FixedSizeBitmaps) Close() {
	if bm.m != nil {
		if err := bm.m.Unmap(); err != nil {
			log.Trace("unmap", "err", err, "file", bm.FileName())
		}
		bm.m = nil
	}
	if bm.f != nil {
		if err := bm.f.Close(); err != nil {
			log.Trace("close", "err", err, "file", bm.FileName())
		}
		bm.f = nil
	}
}

func (bm *FixedSizeBitmaps) At(item uint64) (res []uint64, err error) {
	if item > bm.count {
		return nil, fmt.Errorf("too big item number: %d > %d", item, bm.count)
	}

	n := bm.bitsPerBitmap * int(item)
	blkFrom, bitFrom := n/64, n%64
	blkTo := (n+bm.bitsPerBitmap)/64 + 1
	bitTo := 64

	var j uint64
	for i := blkFrom; i < blkTo; i++ {
		if i == blkTo-1 {
			bitTo = (n + bm.bitsPerBitmap) % 64
		}
		for bit := bitFrom; bit < bitTo; bit++ {
			if bm.data[i]&(1<<bit) != 0 {
				res = append(res, j+bm.baseDataID)
			}
			j++
		}
		bitFrom = 0
	}

	return res, nil
}

func (bm *FixedSizeBitmaps) LastAt(item uint64) (last uint64, ok bool, err error) {
	if item > bm.count {
		return 0, false, fmt.Errorf("too big item number: %d > %d", item, bm.count)
	}

	n := bm.bitsPerBitmap * int(item)
	blkFrom, bitFrom := n/64, n%64
	blkTo := (n+bm.bitsPerBitmap)/64 + 1
	bitTo := 64

	var j uint64
	var found bool
	for i := blkFrom; i < blkTo; i++ { // TODO: optimize me. it's copy-paste of method `At`
		if i == blkTo-1 {
			bitTo = (n + bm.bitsPerBitmap) % 64
		}
		for bit := bitFrom; bit < bitTo; bit++ {
			if bm.data[i]&(1<<bit) != 0 {
				last = j
				found = true
			}
			j++
		}
		bitFrom = 0
	}
	return last + bm.baseDataID, found, nil
}

func (bm *FixedSizeBitmaps) First2At(item, after uint64) (fst uint64, snd uint64, ok, ok2 bool, err error) {
	if item > bm.count {
		return 0, 0, false, false, fmt.Errorf("too big item number: %d > %d", item, bm.count)
	}
	n := bm.bitsPerBitmap * int(item)
	blkFrom, bitFrom := n/64, n%64
	blkTo := (n+bm.bitsPerBitmap)/64 + 1
	bitTo := 64

	var j uint64
	for i := blkFrom; i < blkTo; i++ {
		if i == blkTo-1 {
			bitTo = (n + bm.bitsPerBitmap) % 64
		}
		for bit := bitFrom; bit < bitTo; bit++ {
			if bm.data[i]&(1<<bit) != 0 {
				if j >= after {
					if !ok {
						ok = true
						fst = j
					} else {
						ok2 = true
						snd = j
						return
					}
				}
			}
			j++
		}
		bitFrom = 0
	}

	return fst + bm.baseDataID, snd + bm.baseDataID, ok, ok2, err
}

type FixedSizeBitmapsWriter struct {
	f *os.File

	indexFile, tmpIdxFilePath string
	fileName                  string

	data     []uint64 // slice of correct size for the index to work with
	metaData []byte
	m        mmap2.MMap

	version       uint8
	baseDataID    uint64 // deducted from all stored
	count         uint64 // of keys
	size          int
	bitsPerBitmap uint64

	logger  log.Logger
	noFsync bool // fsync is enabled by default, but tests can manually disable
}

const MetaHeaderSize = 64

func NewFixedSizeBitmapsWriter(indexFile string, bitsPerBitmap int, baseDataID, amount uint64, logger log.Logger) (*FixedSizeBitmapsWriter, error) {
	pageSize := os.Getpagesize()
	_, fileName := filepath.Split(indexFile)
	//TODO: use math.SafeMul()
	bytesAmount := MetaHeaderSize + (bitsPerBitmap*int(amount))/8 + 1
	size := (bytesAmount/pageSize + 1) * pageSize // must be page-size-aligned
	idx := &FixedSizeBitmapsWriter{
		indexFile:      indexFile,
		fileName:       fileName,
		tmpIdxFilePath: indexFile + ".tmp",
		bitsPerBitmap:  uint64(bitsPerBitmap),
		size:           size,
		count:          amount,
		version:        1,
		logger:         logger,
		baseDataID:     baseDataID,
	}

	_ = dir.RemoveFile(idx.tmpIdxFilePath)

	var err error
	idx.f, err = os.Create(idx.tmpIdxFilePath)
	if err != nil {
		return nil, err
	}

	if err := growFileToSize(idx.f, idx.size); err != nil {
		return nil, err
	}

	idx.m, err = mmap2.MapRegion(idx.f, idx.size, mmap2.RDWR, 0, 0)
	if err != nil {
		return nil, err
	}

	idx.metaData = idx.m[:MetaHeaderSize]
	idx.data = castToArrU64(idx.m[MetaHeaderSize:])
	//if err := mmap.MadviseNormal(idx.m); err != nil {
	//	return nil, err
	//}
	idx.metaData[0] = idx.version
	//fmt.Printf("build: count=%d, %s\n", idx.count, indexFile)
	binary.BigEndian.PutUint64(idx.metaData[1:], idx.count)
	binary.BigEndian.PutUint64(idx.metaData[1+8:], idx.baseDataID)
	binary.BigEndian.PutUint16(idx.metaData[1+8+8:], uint16(idx.bitsPerBitmap))

	return idx, nil
}
func (w *FixedSizeBitmapsWriter) Close() {
	if w.m != nil {
		if err := w.m.Unmap(); err != nil {
			log.Trace("unmap", "err", err, "file", w.f.Name())
		}
		w.m = nil
	}
	if w.f != nil {
		if err := w.f.Close(); err != nil {
			log.Trace("close", "err", err, "file", w.f.Name())
		}
		w.f = nil
	}
}
func growFileToSize(f *os.File, size int) error {
	pageSize := os.Getpagesize()
	pages := size / pageSize
	wr := bufio.NewWriterSize(f, int(4*datasize.MB))
	page := make([]byte, pageSize)
	for i := 0; i < pages; i++ {
		if _, err := wr.Write(page); err != nil {
			return err
		}
	}
	if err := wr.Flush(); err != nil {
		return err
	}
	return nil
}

// Create a []uint64 view of the file
func castToArrU64(in []byte) []uint64 {
	var view []uint64
	header := (*reflect.SliceHeader)(unsafe.Pointer(&view))
	header.Data = (*reflect.SliceHeader)(unsafe.Pointer(&in)).Data
	header.Len = len(in) / 8
	header.Cap = header.Len
	return view
}

func (w *FixedSizeBitmapsWriter) AddArray(item uint64, listOfValues []uint64) error {
	if item > w.count {
		return fmt.Errorf("too big item number: %d > %d", item, w.count)
	}
	offset := item * w.bitsPerBitmap
	for _, v := range listOfValues {
		if v < w.baseDataID { //uint-underflow protection
			return fmt.Errorf("too small value: %d < %d, %s", v, w.baseDataID, w.fileName)
		}
		v = v - w.baseDataID
		if v > w.bitsPerBitmap {
			return fmt.Errorf("too big value: %d > %d, %s", v, w.bitsPerBitmap, w.fileName)
		}
		n := offset + v
		blkAt, bitAt := int(n/64), int(n%64)
		if blkAt > len(w.data) {
			return fmt.Errorf("too big value: %d, %d, max: %d", item, listOfValues, len(w.data))
		}
		w.data[blkAt] |= (1 << bitAt)
	}
	return nil
}

func (w *FixedSizeBitmapsWriter) Build() error {
	if err := w.m.Flush(); err != nil {
		return err
	}
	if err := w.fsync(); err != nil {
		return err
	}

	if err := w.m.Unmap(); err != nil {
		return err
	}
	w.m = nil

	if err := w.f.Close(); err != nil {
		return err
	}
	w.f = nil

	_ = dir.RemoveFile(w.indexFile)
	if err := os.Rename(w.tmpIdxFilePath, w.indexFile); err != nil {
		return err
	}
	return nil
}

func (w *FixedSizeBitmapsWriter) DisableFsync() { w.noFsync = true }

// fsync - other processes/goroutines must see only "fully-complete" (valid) files. No partial-writes.
// To achieve it: write to .tmp file then `rename` when file is ready.
// Machine may power-off right after `rename` - it means `fsync` must be before `rename`
func (w *FixedSizeBitmapsWriter) fsync() error {
	if w.noFsync {
		return nil
	}
	if err := w.f.Sync(); err != nil {
		w.logger.Warn("couldn't fsync", "err", err, "file", w.tmpIdxFilePath)
		return err
	}
	return nil
}
