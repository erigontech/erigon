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

package state

import (
	"fmt"
	"github.com/erigontech/erigon-lib/common/customfs"
	"github.com/spf13/afero"
	"hash"
	"path/filepath"

	"github.com/erigontech/erigon-lib/common/dbg"
	"github.com/erigontech/erigon-lib/common/dir"
	"github.com/erigontech/erigon-lib/log/v3"
	bloomfilter "github.com/holiman/bloomfilter/v2"
)

type ExistenceFilter struct {
	filter             *bloomfilter.Filter
	empty              bool
	FileName, FilePath string
	f                  afero.File
	noFsync            bool // fsync is enabled by default, but tests can manually disable
}

func NewExistenceFilter(keysCount uint64, filePath string) (*ExistenceFilter, error) {

	m := bloomfilter.OptimalM(keysCount, 0.01)
	//TODO: make filters compatible by usinig same seed/keys
	_, fileName := filepath.Split(filePath)
	e := &ExistenceFilter{FilePath: filePath, FileName: fileName}
	if keysCount < 2 {
		e.empty = true
	} else {
		var err error
		e.filter, err = bloomfilter.New(m)
		if err != nil {
			return nil, fmt.Errorf("%w, %s", err, fileName)
		}
	}
	return e, nil
}

func (b *ExistenceFilter) AddHash(hash uint64) {
	if b.empty {
		return
	}
	b.filter.AddHash(hash)
}
func (b *ExistenceFilter) ContainsHash(v uint64) bool {
	if b.empty {
		return true
	}
	return b.filter.ContainsHash(v)
}
func (b *ExistenceFilter) Contains(v hash.Hash64) bool {
	if b.empty {
		return true
	}
	return b.filter.Contains(v)
}
func (b *ExistenceFilter) Build() error {
	if b.empty {
		cf, err := customfs.CFS.Create(b.FilePath)
		if err != nil {
			return err
		}
		defer cf.Close()
		return nil
	}

	log.Trace("[agg] write file", "file", b.FileName)
	tmpFilePath := b.FilePath + ".tmp"
	cf, err := customfs.CFS.Create(tmpFilePath)
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
	if err := customfs.CFS.Rename(tmpFilePath, b.FilePath); err != nil {
		return err
	}
	return nil
}

func (b *ExistenceFilter) DisableFsync() { b.noFsync = true }

// fsync - other processes/goroutines must see only "fully-complete" (valid) files. No partial-writes.
// To achieve it: write to .tmp file then `rename` when file is ready.
// Machine may power-off right after `rename` - it means `fsync` must be before `rename`
func (b *ExistenceFilter) fsync(f afero.File) error {
	if b.noFsync {
		return nil
	}
	if err := f.Sync(); err != nil {
		log.Warn("couldn't fsync", "err", err)
		return err
	}
	return nil
}

func OpenExistenceFilter(filePath string) (exFilder *ExistenceFilter, err error) {
	var validationPassed = false
	_, fileName := filepath.Split(filePath)
	idx := &ExistenceFilter{FilePath: filePath, FileName: fileName}
	defer func() {
		// recover from panic if one occurred. Set err to nil if no panic
		if rec := recover(); rec != nil {
			// do r with only the stack trace
			err = fmt.Errorf("incomplete file: %s, %+v, trace: %s", filePath, rec, dbg.Stack())
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
	{
		ff, err := customfs.CFS.Open(filePath)
		if err != nil {
			return nil, err
		}
		defer ff.Close()
		stat, err := ff.Stat()
		if err != nil {
			return nil, err
		}
		idx.empty = stat.Size() == 0
	}

	if !idx.empty {
		var err error
		file, err := customfs.CFS.Open(filePath)
		if err != nil {
			return nil, err
		}
		idx.filter, _, err = bloomfilter.ReadFrom(file)
		if err != nil {
			return nil, fmt.Errorf("OpenExistenceFilter: %w, %s", err, fileName)
		}
	}
	return idx, nil
}
func (b *ExistenceFilter) Close() {
	if b == nil || b.f == nil {
		return
	}
	b.f.Close()
	b.f = nil
}
