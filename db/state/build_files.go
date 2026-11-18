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

package state

import (
	"os"
	"path/filepath"

	"github.com/erigontech/erigon/common/background"
	"github.com/erigontech/erigon/common/dir"
	"github.com/erigontech/erigon/db/datastruct/btindex"
	"github.com/erigontech/erigon/db/seg"
	"github.com/erigontech/erigon/db/state/statecfg"
)

// Completed snapshot files are immutable and may still be mapped by readers.
// Recovery builds missing components without replacing their completed siblings.
func buildMissingFile(path string, build func() error) error {
	exists, err := dir.FileExist(path)
	if err != nil || exists {
		return err
	}
	return build()
}

func (d *Domain) buildMissingBtIndex(btPath, filterPath string, data *seg.Reader, ps *background.ProgressSet) error {
	btExists, err := dir.FileExist(btPath)
	if err != nil {
		return err
	}
	filterExists := !d.Accessors.Has(statecfg.AccessorExistence)
	if !filterExists {
		filterExists, err = dir.FileExist(filterPath)
		if err != nil {
			return err
		}
	}
	if btExists && filterExists {
		return nil
	}

	// The builder produces both outputs. Send an already-complete sibling to
	// a temporary directory so its live mapping and path stay untouched.
	if btExists || (d.Accessors.Has(statecfg.AccessorExistence) && filterExists) {
		tmp, err := os.MkdirTemp(d.dirs.Tmp, "snapshot-accessors-")
		if err != nil {
			return err
		}
		defer dir.RemoveAll(tmp) //nolint:errcheck
		if btExists {
			btPath = filepath.Join(tmp, filepath.Base(btPath))
		}
		if filterExists {
			filterPath = filepath.Join(tmp, filepath.Base(filterPath))
		}
	}
	return btindex.BuildBtreeIndexWithDecompressor(btPath, filterPath, data, ps, d.dirs.Tmp, *d.salt.Load(), d.logger, d.noFsync, d.Accessors)
}
