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
	"github.com/erigontech/erigon/common/log/v3"
	"github.com/erigontech/erigon/db/datastruct/fusefilter"
)

type Filter struct {
	filter             *bloomfilter.Filter
	fuseWriterSharded  *fusefilter.WriterSharded
	fuseReader         *fusefilter.Reader
	fuseReaderSharded  *fusefilter.ReaderSharded
	useFuse            bool
	empty              bool
	FileName, FilePath string
	noFsync            bool // fsync is enabled by default, but tests can manually disable
}

func NewFilter(keysCount uint64, filePath string, useFuse bool) (*Filter, error) {
	_, fileName := filepath.Split(filePath)
	e := &Filter{FilePath: filePath, FileName: fileName, useFuse: useFuse}
	if keysCount < 2 {
		e.empty = true
	} else {
		var err error
		if e.useFuse {
			e.fuseWriterSharded, err = fusefilter.NewWriterSharded(filePath)
			if err != nil {
				return nil, err
			}
			return e, nil
		}

		m := bloomfilter.OptimalM(keysCount, 0.01)
		e.filter, err = bloomfilter.New(m)
		if err != nil {
			return nil, fmt.Errorf("%w, %s", err, fileName)
		}
	}
	return e, nil
}

func (b *Filter) AddHash(hash uint64) error {
	if b.empty {
		return nil
	}
	if b.useFuse {
		return b.fuseWriterSharded.AddHash(hash)
	}
	b.filter.AddHash(hash)
	return nil
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
	if b.empty {
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

	if b.useFuse {
		return b.fuseWriterSharded.Build()
	}

	log.Trace("[agg] write file", "file", b.FileName)
	cf, err := dir.CreateTemp(b.FilePath)
	if err != nil {
		return err
	}
	defer cf.Close()

	if _, err := b.filter.WriteTo(cf); err != nil {
		return err
	}
	if err = b.fsync(cf); err != nil {
		return err
	}
	if err = cf.Close(); err != nil {
		return err
	}
	if err := os.Rename(cf.Name(), b.FilePath); err != nil {
		return err
	}
	return nil
}

func (b *Filter) DisableFsync() {
	if b.empty {
		return
	}
	b.noFsync = true
	if b.useFuse {
		b.fuseWriterSharded.DisableFsync()
	}
}

// fsync - other processes/goroutines must see only "fully-complete" (valid) files. No partial-writes.
// To achieve it: write to .tmp file then `rename` when file is ready.
// Machine may power-off right after `rename` - it means `fsync` must be before `rename`
func (b *Filter) fsync(f *os.File) error {
	if b.noFsync {
		return nil
	}
	if err := f.Sync(); err != nil {
		log.Warn("couldn't fsync", "err", err)
		return err
	}
	return nil
}

// isBloomMagic checks the first 12 bytes for holiman/bloomfilter/v2's headerMagic:
// 8 zero bytes followed by "v02\n". This disambiguates bloom files from fuse files
// (whose version byte and seed never accidentally match this pattern).
func isBloomMagic(peek []byte) bool {
	return len(peek) >= 12 &&
		peek[0] == 0 && peek[1] == 0 && peek[2] == 0 && peek[3] == 0 &&
		peek[4] == 0 && peek[5] == 0 && peek[6] == 0 && peek[7] == 0 &&
		peek[8] == 'v' && peek[9] == '0' && peek[10] == '2' && peek[11] == '\n'
}

// OpenFilter opens a .kvei existence filter file. The useFuse parameter is ignored;
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
		log.Warn("[existence] open bloom filter", "file", fileName, "size", common.ByteCount(uint64(stat.Size())))
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
	log.Warn("[existence] open fuse filter", "file", fileName, "version", fuseVersion, "size", common.ByteCount(uint64(stat.Size())))
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

func (b *Filter) Close() {
	if b == nil {
		return
	}
	b.fuseReader.Close()
	b.fuseReaderSharded.Close()
}
