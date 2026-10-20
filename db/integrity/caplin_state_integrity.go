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

package integrity

import (
	"context"
	"fmt"
	"path/filepath"
	"sort"

	"github.com/erigontech/erigon/common/length"
	"github.com/erigontech/erigon/common/log/v3"
	"github.com/erigontech/erigon/db/datadir"
	"github.com/erigontech/erigon/db/seg"
	"github.com/erigontech/erigon/db/snaptype"
)

// caplinDenseRootTables are the caplin state snapshot types that carry one 32-byte
// root per slot: process_slot writes block_roots/state_roots every slot, so an empty
// word is always a reconstruction gap, never valid state. A blank segment shadows the
// DB (snapshots take read precedence) and breaks historical-state reads for its range.
var caplinDenseRootTables = []string{"BlockRoot", "StateRoot"}

// CheckCaplinStateRoots verifies frozen caplin block_roots/state_roots snapshots hold a
// 32-byte root for every slot, catching ranges frozen before they were reconstructed.
func CheckCaplinStateRoots(ctx context.Context, dirs datadir.Dirs, failFast bool, logger log.Logger) error {
	var firstErr error
	for _, table := range caplinDenseRootTables {
		files, err := filepath.Glob(filepath.Join(dirs.SnapCaplin, "*-"+table+".seg"))
		if err != nil {
			return err
		}
		sort.Strings(files)
		for _, path := range files {
			if ctx.Err() != nil {
				return ctx.Err()
			}
			blank, total, firstBlankSlot, err := scanBlankRoots(ctx, path)
			if err != nil {
				return err
			}
			if blank == 0 {
				continue
			}
			err = fmt.Errorf("caplin %s snapshot %s: %d/%d slots have a missing or invalid root (first at slot %d)", table, filepath.Base(path), blank, total, firstBlankSlot)
			logger.Error("[integrity] CaplinStateRoots", "err", err)
			if failFast {
				return err
			}
			if firstErr == nil {
				firstErr = err
			}
		}
	}
	return firstErr
}

func scanBlankRoots(ctx context.Context, path string) (blank, total, firstBlankSlot uint64, err error) {
	var from uint64
	if info, _, ok := snaptype.ParseFileName(filepath.Dir(path), filepath.Base(path)); ok {
		from = info.From
	}
	d, err := seg.NewDecompressor(path)
	if err != nil {
		return 0, 0, 0, err
	}
	defer d.Close()
	g := d.MakeGetter()
	for g.HasNext() {
		if total%8192 == 0 && ctx.Err() != nil {
			return 0, 0, 0, ctx.Err()
		}
		w, _ := g.Next(nil)
		if len(w) != length.Hash {
			if blank == 0 {
				firstBlankSlot = from + total
			}
			blank++
		}
		total++
	}
	return blank, total, firstBlankSlot, nil
}
