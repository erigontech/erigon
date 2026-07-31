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
	"github.com/erigontech/erigon/db/snapshotsync/blocksnapshots"
	"github.com/erigontech/erigon/node/ethconfig"
)

// OpenRoSnapshotsWithOverrides builds an independent block-snapshots view
// over liveDir with the staged block segments in overridePaths substituted
// in. Returns the concrete *blocksnapshots.RoSnapshots so the result can
// back a BlockReader used to validate a staged .seg file against the
// otherwise-live chain. Throwaway view — caller must Close().
func OpenRoSnapshotsWithOverrides(cfg ethconfig.BlocksFreezing, liveDir string, overridePaths []string, logger log.Logger) (*blocksnapshots.RoSnapshots, error) {
	s := blocksnapshots.NewRoSnapshots(cfg, liveDir, logger)
	if err := snapshotsync.ApplyBlockOverrides(&s.BaseRoSnapshots, overridePaths); err != nil {
		s.Close()
		return nil, err
	}
	return s, nil
}
