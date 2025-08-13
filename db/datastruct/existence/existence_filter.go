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
	"os"
	"path/filepath"

	bloomfilter "github.com/holiman/bloomfilter/v2"

	"github.com/erigontech/erigon-lib/common/dbg"
	"github.com/erigontech/erigon-lib/common/dir"
	"github.com/erigontech/erigon-lib/log/v3"
	"github.com/erigontech/erigon/db/datastruct/fusefilter"
)

type Filter struct {
	filter             *bloomfilter.Filter
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

func (b *Filter) AddHash(hash uint64) {
	if b.empty {
		return
	}
	if b.useFuse {
		if err := b.fuseWriter.AddHash(hash); err != nil {
			panic(err)
		}
		return
	}
	b.filter.AddHash(hash)
}
func (b *Filter) ContainsHash(hashedKey uint64) bool {
	if b.empty {
		return true
	}
	if b.useFuse {
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
		return b.fuseWriter.Build()
	}

	log.Trace("[agg] write file", "file", b.FileName)
	tmpFilePath := b.FilePath + ".tmp"
	cf, err := os.Create(tmpFilePath)
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
	if err := os.Rename(tmpFilePath, b.FilePath); err != nil {
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

	if idx.useFuse {
		idx.fuseReader, err = fusefilter.NewReader(filePath)
		if err != nil {
			return nil, fmt.Errorf("OpenFilter: %w, %s", err, fileName)
		}
		validationPassed = true
		return idx, nil
	}
	filter := new(bloomfilter.Filter)
	_, err = filter.UnmarshalFromReaderNoVerify(bufio.NewReaderSize(f, 1*1024*1024))
	if err != nil {
		return nil, fmt.Errorf("OpenFilter: %w, %s", err, fileName)
	}
	idx.filter = filter
	validationPassed = true
	return idx, nil
}

func (b *Filter) Close() {
	if b == nil {
		return
	}
	b.fuseReader.Close()
}
