// Copyright 2024 The Erigon Authors
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

package existence

import (
	"bufio"
	"fmt"
	"hash"
	"io"
	"os"
	"path/filepath"

	bloomfilter "github.com/holiman/bloomfilter/v2"

	"github.com/erigontech/erigon/common"
	"github.com/erigontech/erigon/common/dbg"
	"github.com/erigontech/erigon/common/dir"
	"github.com/erigontech/erigon/db/datastruct/fusefilter"
)

type Filter struct {
	filter             *bloomfilter.Filter
	fuseWriterSharded  *fusefilter.WriterSharded
	fuseReader         *fusefilter.Reader
	fuseReaderSharded  *fusefilter.ReaderSharded
	empty              bool
	FileName, FilePath string
}

func NewFilter(keysCount uint64, filePath string) (*Filter, error) {
	_, fileName := filepath.Split(filePath)
	e := &Filter{FilePath: filePath, FileName: fileName}
	if keysCount < 2 {
		e.empty = true
	} else {
		var err error
		e.fuseWriterSharded, err = fusefilter.NewWriterSharded(filePath)
		if err != nil {
			return nil, err
		}
		return e, nil
	}
	return e, nil
}

func (b *Filter) AddHash(hash uint64) error {
	if b.empty {
		return nil
	}
	return b.fuseWriterSharded.AddHash(hash)
}

func (b *Filter) ContainsHash(hashedKey uint64) bool {
	if b.empty {
		return true
	}
	if b.fuseReaderSharded != nil {
		return b.fuseReaderSharded.ContainsHash(hashedKey)
	}
	if b.fuseReader != nil {
		return b.fuseReader.ContainsHash(hashedKey)
	}
	return b.filter.ContainsHash(hashedKey)
}

func (b *Filter) Contains(v hash.Hash64) bool {
	if b.empty || b.filter == nil {
		return true
	}
	return b.filter.Contains(v)
}

func (b *Filter) Build() error {
	if b.empty {
		cf, err := os.Create(b.FilePath)
		if err != nil {
			return err
		}
		defer cf.Close()
		return nil
	}

	return b.fuseWriterSharded.Build()
}

func (b *Filter) DisableFsync() {
	if b.empty {
		return
	}
	b.fuseWriterSharded.DisableFsync()
}

// isBloomMagic checks the first 12 bytes for holiman/bloomfilter/v2's headerMagic:
// 8 zero bytes followed by "v02\n". A fuse file can never match: bytes 4-7 hold
// SegmentCount (v0) or a shard-size entry (v1), both structurally impossible to be zero.
func isBloomMagic(peek []byte) bool {
	return len(peek) >= 12 &&
		peek[0] == 0 && peek[1] == 0 && peek[2] == 0 && peek[3] == 0 &&
		peek[4] == 0 && peek[5] == 0 && peek[6] == 0 && peek[7] == 0 &&
		peek[8] == 'v' && peek[9] == '0' && peek[10] == '2' && peek[11] == '\n'
}

// OpenFilter opens a .kvei existence filter file
// the format is auto-detected from the file content (bloom vs fuse v0/v1).
func OpenFilter(filePath string, _ bool) (idx *Filter, err error) {
	_, fileName := filepath.Split(filePath)
	idx = &Filter{FilePath: filePath, FileName: fileName}
	var validationPassed = false
	defer func() {
		if rec := recover(); rec != nil {
			err = fmt.Errorf("incomplete file: %s, %+v, trace: %s", fileName, rec, dbg.Stack())
		}
		if err != nil || !validationPassed {
			idx.Close()
			idx = nil
		}
	}()

	exists, err := dir.FileExist(filePath)
	if err != nil {
		return nil, err
	}
	if !exists {
		return nil, fmt.Errorf("file doesn't exists: %s", fileName)
	}

	f, err := os.Open(filePath)
	if err != nil {
		return nil, err
	}
	defer f.Close()
	stat, err := f.Stat()
	if err != nil {
		return nil, err
	}
	idx.empty = stat.Size() == 0
	if idx.empty {
		validationPassed = true
		return idx, nil
	}

	var peek [12]byte
	if _, err = io.ReadFull(f, peek[:]); err != nil {
		return nil, fmt.Errorf("OpenFilter peek: %w, %s", err, fileName)
	}
	if _, err = f.Seek(0, io.SeekStart); err != nil {
		return nil, err
	}

	if isBloomMagic(peek[:]) {
		filter := new(bloomfilter.Filter)
		_, err = filter.UnmarshalFromReaderNoVerify(bufio.NewReaderSize(f, 1*1024*1024))
		if err != nil {
			return nil, fmt.Errorf("OpenFilter: %w, %s", err, fileName)
		}
		idx.filter = filter
		validationPassed = true
		return idx, nil
	}

	// fuse format: version byte determines monolithic (0) vs sharded (1)
	fuseVersion := peek[0]
	switch fuseVersion {
	case 0:
		idx.fuseReader, err = fusefilter.NewReader(filePath)
	case 1:
		idx.fuseReaderSharded, err = fusefilter.NewReaderSharded(filePath)
	default:
		return nil, fmt.Errorf("OpenFilter: unknown fuse version %d: %s", fuseVersion, fileName)
	}
	if err != nil {
		return nil, fmt.Errorf("OpenFilter: %w, %s", err, fileName)
	}
	validationPassed = true
	return idx, nil
}

func (b *Filter) ForceInMem() {
	if b == nil || b.empty {
		return
	}
	if b.fuseReaderSharded != nil {
		b.fuseReaderSharded.ForceInMem()
		return
	}
	if b.fuseReader != nil {
		b.fuseReader.ForceInMem()
	}
	// bloom filter is already heap-allocated by OpenFilter — no-op
}

func (b *Filter) MadvWillNeed() {
	if b == nil || b.empty {
		return
	}
	if b.fuseReaderSharded != nil {
		b.fuseReaderSharded.MadvWillNeed()
		return
	}
	if b.fuseReader != nil {
		b.fuseReader.MadvWillNeed()
	}
	// bloom filter is heap-allocated, no mmap to advise — no-op
}

func (b *Filter) MadvNormal() {
	if b == nil || b.empty {
		return
	}
	if b.fuseReaderSharded != nil {
		b.fuseReaderSharded.MadvNormal()
		return
	}
	if b.fuseReader != nil {
		b.fuseReader.MadvNormal()
	}
	// bloom filter is heap-allocated, no mmap to advise — no-op
}

func (b *Filter) MadvRandom() {
	if b == nil || b.empty {
		return
	}
	if b.fuseReaderSharded != nil {
		b.fuseReaderSharded.MadvRandom()
		return
	}
	if b.fuseReader != nil {
		b.fuseReader.MadvRandom()
	}
}

func (b *Filter) Close() {
	if b == nil {
		return
	}
	b.fuseReader.Close()
	b.fuseReaderSharded.Close()
}
