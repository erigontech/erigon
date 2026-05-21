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

package snapshotsync

import (
	"fmt"
	"os"
	"path/filepath"

	"github.com/erigontech/erigon/common/log/v3"
	"github.com/erigontech/erigon/db/snaptype"
	"github.com/erigontech/erigon/node/ethconfig"
)

// OpenRoSnapshotsWithOverrides builds an independent RoSnapshots over
// liveDir, then substitutes the block segment covering each override
// path's block range with a segment opened on that path. It is the
// block-file counterpart of (*state.Aggregator).BeginFilesRoWithOverrides
// — used to validate a staged (not-yet-live) block .seg file in the
// context of the otherwise-live block snapshots. See
// docs/plans/20260520-phase7-staged-adoption-design.md §7b-3.
//
// The returned RoSnapshots is a throwaway overlay: it owns its own
// DirtySegments (no sharing with the production RoSnapshots) and the
// caller must Close() it. The live snapshot directory is never written.
//
// Each override path must be a block segment (.seg) file whose index
// (.idx) sits alongside it in the same directory.
func OpenRoSnapshotsWithOverrides(cfg ethconfig.BlocksFreezing, liveDir string, overridePaths []string, types []snaptype.Type, alignMin bool, logger log.Logger) (*RoSnapshots, error) {
	s := NewRoSnapshots(cfg, liveDir, types, alignMin, logger)
	if err := s.OpenFolder(); err != nil {
		s.Close()
		return nil, err
	}
	for _, p := range overridePaths {
		if err := s.applyBlockOverride(p); err != nil {
			s.Close()
			return nil, fmt.Errorf("override %s: %w", filepath.Base(p), err)
		}
	}
	s.recalcVisibleFiles(s.alignMin)
	return s, nil
}

// applyBlockOverride opens the segment at path and swaps it into the
// dirty tree in place of the same-range live segment. The caller invokes
// recalcVisibleFiles once all overrides are applied.
func (s *RoSnapshots) applyBlockOverride(path string) error {
	dir, name := filepath.Split(path)
	dir = filepath.Clean(dir)
	f, isState, ok := snaptype.ParseFileName(dir, name)
	if !ok {
		return fmt.Errorf("not a parseable snapshot file name")
	}
	if isState {
		return fmt.Errorf("override must be a block segment file, got a state file")
	}
	if !s.HasType(f.Type) {
		return fmt.Errorf("snapshot type %s not tracked by this view", f.Type.Enum())
	}

	sn := &DirtySegment{segType: f.Type, version: f.Version, Range: Range{f.From, f.To}, frozen: s.snCfg.IsFrozen(f)}
	if err := sn.Open(dir); err != nil {
		return fmt.Errorf("open segment: %w", err)
	}
	entries, err := os.ReadDir(dir)
	if err != nil {
		sn.close()
		return fmt.Errorf("read staging dir: %w", err)
	}
	dirEntries := make([]string, 0, len(entries))
	for _, e := range entries {
		if !e.IsDir() {
			dirEntries = append(dirEntries, e.Name())
		}
	}
	if err := sn.OpenIdxIfNeed(dir, false, dirEntries); err != nil {
		sn.close()
		return fmt.Errorf("open segment index: %w", err)
	}

	s.dirtyLock.Lock()
	defer s.dirtyLock.Unlock()
	bt := s.dirty[f.Type.Enum()]
	if bt == nil {
		sn.close()
		return fmt.Errorf("no dirty tree for type %s", f.Type.Enum())
	}
	var replaced *DirtySegment
	bt.Walk(func(segs []*DirtySegment) bool {
		for _, existing := range segs {
			if existing.from == f.From && existing.to == f.To {
				replaced = existing
				return false
			}
		}
		return true
	})
	if replaced == nil {
		sn.close()
		return fmt.Errorf("no live %s segment covering blocks [%d, %d)", f.Type.Enum(), f.From, f.To)
	}
	bt.Delete(replaced)
	replaced.close()
	bt.Set(sn)
	return nil
}
