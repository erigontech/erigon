/*
Copyright 2022 Erigon contributors

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

	http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
*/

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

	"github.com/c2h5oh/datasize"
	mmap2 "github.com/edsrzf/mmap-go"
	"github.com/ledgerwatch/log/v3"
)

type FixedSizeBitmaps struct {
	f                  *os.File
	filePath, fileName string

	data     []uint64
	metaData []byte
	amount   uint64
	version  uint8

	m             mmap2.MMap
	bitsPerBitmap int
	size          int
	modTime       time.Time
}

func OpenFixedSizeBitmaps(filePath string, bitsPerBitmap int) (*FixedSizeBitmaps, error) {
	_, fName := filepath.Split(filePath)
	idx := &FixedSizeBitmaps{
		filePath:      filePath,
		fileName:      fName,
		bitsPerBitmap: bitsPerBitmap,
	}

	var err error
	idx.f, err = os.Open(filePath)
	if err != nil {
		return nil, fmt.Errorf("OpenFile: %w", err)
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
	idx.amount = binary.BigEndian.Uint64(idx.metaData[1 : 8+1])

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
	if item > bm.amount {
		return nil, fmt.Errorf("too big item number: %d > %d", item, bm.amount)
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
				res = append(res, j)
			}
			j++
		}
		bitFrom = 0
	}

	return res, nil
}

func (bm *FixedSizeBitmaps) First2At(item, after uint64) (fst uint64, snd uint64, ok, ok2 bool, err error) {
	if item > bm.amount {
		return 0, 0, false, false, fmt.Errorf("too big item number: %d > %d", item, bm.amount)
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

	return
}

type FixedSizeBitmapsWriter struct {
	f *os.File

	indexFile, tmpIdxFilePath string
	data                      []uint64 // slice of correct size for the index to work with
	metaData                  []byte
	m                         mmap2.MMap

	version       uint8
	amount        uint64
	size          int
	bitsPerBitmap uint64

	logger  log.Logger
	noFsync bool // fsync is enabled by default, but tests can manually disable
}

const MetaHeaderSize = 64

func NewFixedSizeBitmapsWriter(indexFile string, bitsPerBitmap int, amount uint64, logger log.Logger) (*FixedSizeBitmapsWriter, error) {
	pageSize := os.Getpagesize()
	//TODO: use math.SafeMul()
	bytesAmount := MetaHeaderSize + (bitsPerBitmap*int(amount))/8
	size := (bytesAmount/pageSize + 1) * pageSize // must be page-size-aligned
	idx := &FixedSizeBitmapsWriter{
		indexFile:      indexFile,
		tmpIdxFilePath: indexFile + ".tmp",
		bitsPerBitmap:  uint64(bitsPerBitmap),
		size:           size,
		amount:         amount,
		version:        1,
		logger:         logger,
	}

	_ = os.Remove(idx.tmpIdxFilePath)

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
	binary.BigEndian.PutUint64(idx.metaData[1:], idx.amount)
	idx.amount = binary.BigEndian.Uint64(idx.metaData[1 : 8+1])

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
	if item > w.amount {
		return fmt.Errorf("too big item number: %d > %d", item, w.amount)
	}
	offset := item * w.bitsPerBitmap
	for _, v := range listOfValues {
		if v > w.bitsPerBitmap {
			return fmt.Errorf("too big value: %d > %d", v, w.bitsPerBitmap)
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

	_ = os.Remove(w.indexFile)
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
