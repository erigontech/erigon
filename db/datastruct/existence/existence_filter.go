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
	"fmt"
	"hash"
	"os"
	"path/filepath"

	"github.com/c2h5oh/datasize"
	bloomfilter "github.com/holiman/bloomfilter/v2"

	"github.com/erigontech/erigon/common/dbg"
	"github.com/erigontech/erigon/common/dir"
	"github.com/erigontech/erigon/common/log/v3"
	"github.com/erigontech/erigon/db/datastruct/fusefilter"
)

type Filter struct {
	filter             *bloomfilter.Filter // writer path; reader path uses mmapBloom instead
	mmapBloom          *mmapBloom          // reader path: mmap'd bits, no in-heap copy
	fuseWriter         *fusefilter.Writer
	fuseReader         *fusefilter.Reader
	useFuse            bool
	empty              bool
	FileName, FilePath string
	noFsync            bool // fsync is enabled by default, but tests can manually disable
}

func NewFilter(keysCount uint64, filePath string, useFuse bool) (*Filter, error) {
	//TODO: make filters compatible by usinig same seed/keys
	_, fileName := filepath.Split(filePath)
	e := &Filter{FilePath: filePath, FileName: fileName, useFuse: useFuse}
	if keysCount < 2 {
		e.empty = true
	} else {
		var err error
		if e.useFuse {
			e.fuseWriter, err = fusefilter.NewWriter(filePath)
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
		if err := b.fuseWriter.AddHash(hash); err != nil {
			return err
		}
		return nil
	}
	b.filter.AddHash(hash)
	return nil
}
func (b *Filter) ContainsHash(hashedKey uint64) bool {
	if b.empty {
		return true
	}
	if b.useFuse {
		return b.fuseReader.ContainsHash(hashedKey)
	}
	if b.mmapBloom != nil {
		return b.mmapBloom.ContainsHash(hashedKey)
	}
	return b.filter.ContainsHash(hashedKey)
}
func (b *Filter) Contains(v hash.Hash64) bool {
	if b.empty {
		return true
	}
	if b.useFuse {
		return b.fuseReader.ContainsHash(v.Sum64())
	}
	if b.mmapBloom != nil {
		return b.mmapBloom.ContainsHash(v.Sum64())
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
		return b.fuseWriter.Build()
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
		b.fuseWriter.DisableFsync()
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

func OpenFilter(filePath string, useFuse bool) (idx *Filter, err error) {
	_, fileName := filepath.Split(filePath)
	idx = &Filter{FilePath: filePath, FileName: fileName, useFuse: useFuse}
	var validationPassed = false
	defer func() {
		// recover from panic if one occurred. Set err to nil if no panic
		if rec := recover(); rec != nil {
			// do r with only the stack trace
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
	stat, err := f.Stat()
	if err != nil {
		f.Close()
		return nil, err
	}
	idx.empty = stat.Size() == 0
	if idx.empty {
		f.Close()
		validationPassed = true
		return idx, nil
	}

	if idx.useFuse {
		f.Close()
		idx.fuseReader, err = fusefilter.NewReader(filePath)
		if err != nil {
			return nil, fmt.Errorf("OpenFilter: %w, %s", err, fileName)
		}
		validationPassed = true
		return idx, nil
	}
	// Reader path: mmap the bloom file so the bit array lives in the OS page
	// cache instead of pinning multiple GB of Go heap across all .kvei files.
	f.Close()
	mb, err := openMmapBloom(filePath)
	if err != nil {
		return nil, fmt.Errorf("OpenFilter: %w, %s", err, fileName)
	}
	idx.mmapBloom = mb
	validationPassed = true
	return idx, nil
}

func (b *Filter) Close() {
	if b == nil {
		return
	}
	b.fuseReader.Close()
	if b.fuseWriter != nil {
		b.fuseWriter.Close()
		b.fuseWriter = nil
	}
	if b.mmapBloom != nil {
		_ = b.mmapBloom.Close()
		b.mmapBloom = nil
	}
}

// MadvWillNeed hints to the OS to prefetch the filter's mapped pages into the
// page cache. No-op for the in-heap writer path and empty filters.
func (b *Filter) MadvWillNeed() {
	if b == nil || b.empty {
		return
	}
	if b.useFuse {
		b.fuseReader.MadvWillNeed()
		return
	}
	b.mmapBloom.MadvWillNeed()
}

// MadvNormal resets the kernel readahead policy for the filter to its default.
func (b *Filter) MadvNormal() {
	if b == nil || b.empty {
		return
	}
	if b.useFuse {
		b.fuseReader.MadvNormal()
		return
	}
	b.mmapBloom.MadvNormal()
}

// ForceInMem pins the filter's bits in the Go heap, dropping the mmap, so they
// can't be evicted under memory pressure. Returns the bytes moved to the heap.
func (b *Filter) ForceInMem() datasize.ByteSize {
	if b == nil || b.empty {
		return 0
	}
	if b.useFuse {
		return b.fuseReader.ForceInMem()
	}
	return b.mmapBloom.ForceInMem()
}
