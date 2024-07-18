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

package snap

import (
	"github.com/erigontech/erigon-lib/kv"
	"github.com/erigontech/erigon/eth/ethconfig"
)

var (
	blockSnapshotEnabledKey = []byte("blocksSnapshotEnabled")
)

func Enabled(tx kv.Getter) (bool, error) {
	return kv.GetBool(tx, kv.DatabaseInfo, blockSnapshotEnabledKey)
}

func EnsureNotChanged(tx kv.GetPut, cfg ethconfig.BlocksFreezing) (bool, bool, error) {
	ok, v, err := kv.EnsureNotChangedBool(tx, kv.DatabaseInfo, blockSnapshotEnabledKey, cfg.Enabled)
	if err != nil {
		return false, false, err
	}
	if !ok {
		return false, v, nil
	}
	return true, false, nil
}

// ForceSetFlags - if you know what you are doing
func ForceSetFlags(tx kv.GetPut, cfg ethconfig.BlocksFreezing) error {
	if cfg.Enabled {
		if err := tx.Put(kv.DatabaseInfo, blockSnapshotEnabledKey, []byte{1}); err != nil {
			return err
		}
	} else {
		if err := tx.Put(kv.DatabaseInfo, blockSnapshotEnabledKey, []byte{0}); err != nil {
			return err
		}
	}
	return nil
}
