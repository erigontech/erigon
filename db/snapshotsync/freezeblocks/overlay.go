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

package freezeblocks

import (
	"github.com/erigontech/erigon/common/log/v3"
	"github.com/erigontech/erigon/db/snapshotsync"
	"github.com/erigontech/erigon/node/ethconfig"
)

// OpenRoSnapshotsWithOverrides builds an independent block-snapshots
// view over liveDir with the staged block segments in overridePaths
// substituted in. Unlike snapshotsync.OpenRoSnapshotsWithOverrides it
// returns a *freezeblocks.RoSnapshots — the concrete type
// NewBlockReader type-asserts for — so the result can back a
// BlockReader used to validate a staged block .seg file against the
// otherwise-live chain. See
// docs/plans/20260520-phase7-staged-adoption-design.md §7b-3.
//
// The returned view is a throwaway: the caller must Close() it. The
// live snapshot directory is never written.
func OpenRoSnapshotsWithOverrides(cfg ethconfig.BlocksFreezing, liveDir string, overridePaths []string, logger log.Logger) (*RoSnapshots, error) {
	s := NewRoSnapshots(cfg, liveDir, logger)
	if err := snapshotsync.ApplyBlockOverrides(&s.RoSnapshots, overridePaths); err != nil {
		s.Close()
		return nil, err
	}
	return s, nil
}
