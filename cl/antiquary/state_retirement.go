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

package antiquary

import (
	"context"
	"path/filepath"
	"slices"

	"github.com/erigontech/erigon/common/log/v3"
	"github.com/erigontech/erigon/db/datadir"
	"github.com/erigontech/erigon/db/kv"
	"github.com/erigontech/erigon/db/snapshotsync"
	"github.com/erigontech/erigon/db/snaptype"
)

type StateRetireResult struct {
	From            uint64
	To              uint64
	Files           int
	IncompleteTable string
	IncompleteSlot  uint64
	DatabaseChanged bool
}

type StateRetireHooks struct {
	Seed           func([]string) error
	RemoveOverlaps func([]string) error
	Prune          func(context.Context, uint64) error
}

func CaplinStateRetireRange(stateSnapshots *snapshotsync.CaplinStateSnapshots, progress uint64) (uint64, uint64) {
	if stateSnapshots == nil {
		return 0, 0
	}
	blocksPerStatefulFile := uint64(snaptype.CaplinMergeLimit * 5)
	from := stateSnapshots.BlocksAvailable() + 1
	if from+blocksPerStatefulFile+safetyMargin > progress || progress < safetyMargin+blocksPerStatefulFile {
		return 0, 0
	}
	to := (progress - safetyMargin - blocksPerStatefulFile) / blocksPerStatefulFile * blocksPerStatefulFile
	if from >= to {
		return 0, 0
	}
	return from, to
}

func RetireCaplinState(ctx context.Context, db kv.RwDB, stateSnapshots *snapshotsync.CaplinStateSnapshots, progress uint64, salt uint32, dirs datadir.Dirs, workers int, logger log.Logger, hooks StateRetireHooks) (StateRetireResult, error) {
	from, to := CaplinStateRetireRange(stateSnapshots, progress)
	result := StateRetireResult{From: from, To: to}
	coverageBefore := make(map[string]uint64)
	if from != to {
		for _, table := range stateSnapshots.TypeNames() {
			coverageBefore[table] = stateSnapshots.ContiguousCoverageEnd(table)
		}
	}
	if from != to {
		if err := stateSnapshots.DumpCaplinState(ctx, to, uint64(snaptype.CaplinMergeLimit*5), salt, dirs, workers, log.LvlInfo, logger); err != nil {
			return StateRetireResult{}, err
		}
		if err := stateSnapshots.OpenFolder(); err != nil {
			return StateRetireResult{}, err
		}
	}
	if hooks.Prune != nil && from != to {
		if err := hooks.Prune(ctx, progress); err != nil {
			return StateRetireResult{}, err
		}
	} else if hooks.Prune == nil {
		changed, err := pruneCaplinStateTablesChanged(ctx, db, stateSnapshots, progress, logger)
		if err != nil {
			return StateRetireResult{}, err
		}
		result.DatabaseChanged = changed
	}
	if from != to {
		for _, table := range stateSnapshots.TypeNames() {
			coverage := stateSnapshots.ContiguousCoverageEnd(table)
			if coverage < to && coverage <= coverageBefore[table] {
				result.IncompleteTable = table
				result.IncompleteSlot = coverage
				break
			}
		}
	}
	if to != 0 || hooks.Prune == nil {
		retireStateOverlapsAndSeed(ctx, stateSnapshots, to, hooks.Seed, hooks.RemoveOverlaps, logger)
	}
	result.Files = len(stateSnapshots.SegFileNames(from, to)) * 2
	return result, nil
}

func pruneCaplinStateTablesChanged(ctx context.Context, db kv.RwDB, stateSnapshots *snapshotsync.CaplinStateSnapshots, progress uint64, logger log.Logger) (bool, error) {
	tables := stateSnapshots.TypeNames()
	boundaryFn := func(table string) uint64 {
		return min(stateSnapshots.ContiguousCoverageEnd(table), progress)
	}
	_, changed, err := pruneStateTablesWithProgress(ctx, db, tables, boundaryFn, 0, statePruneBatchLimit, logger)
	return changed, err
}

func retireStateOverlapsAndSeed(ctx context.Context, stateSnapshots *snapshotsync.CaplinStateSnapshots, dumpedTo uint64, seed func([]string) error, remove func([]string) error, logger log.Logger) {
	dropped := map[string]struct{}{}
	var onDelete func([]string) error
	if remove != nil {
		onDelete = func(names []string) error {
			for _, name := range names {
				dropped[filepath.Base(name)] = struct{}{}
			}
			return remove(names)
		}
	}
	if err := stateSnapshots.RemoveOverlaps(onDelete); err != nil {
		logger.Warn("[Antiquary] Failed to remove overlaps", "err", err)
	}
	if dumpedTo == 0 || seed == nil {
		return
	}
	all := stateSnapshots.SegFileNames(0, dumpedTo)
	names := slices.DeleteFunc(all, func(name string) bool {
		_, ok := dropped[filepath.Base(name)]
		return ok
	})
	if len(names) == 0 {
		return
	}
	if err := seed(names); err != nil {
		logger.Warn("[Antiquary] Failed to add items to bittorrent", "err", err)
	}
}
