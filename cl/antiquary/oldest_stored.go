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
	"encoding/binary"
	"fmt"

	"github.com/erigontech/erigon/cl/clparams"
	"github.com/erigontech/erigon/cl/persistence/base_encoding"
	"github.com/erigontech/erigon/cl/persistence/blob_storage"
	"github.com/erigontech/erigon/db/kv"
	"github.com/erigontech/erigon/db/snapshotsync"
	"github.com/erigontech/erigon/db/snapshotsync/freezeblocks"
)

type (
	StoredSlot         = clparams.StoredSlot
	OldestStoredResult = clparams.OldestStoredResult
)

func OldestStored(ctx context.Context, db kv.RoDB, snapshots *freezeblocks.CaplinSnapshots, stateSnapshots *snapshotsync.CaplinStateSnapshots, blobStorage blob_storage.BlobStorage, columnStorage blob_storage.DataColumnStorage) (clparams.OldestStoredResult, error) {
	result := clparams.OldestStoredResult{StateTables: make(map[string]clparams.StoredSlot)}
	var tx kv.Tx
	tables := make(map[string]struct{})
	if db != nil {
		var err error
		tx, err = db.BeginRo(ctx)
		if err != nil {
			return result, err
		}
		defer tx.Rollback()
		names, err := tx.ListTables()
		if err != nil {
			return result, err
		}
		for _, name := range names {
			tables[name] = struct{}{}
		}
	}

	if snapshots != nil {
		view := snapshots.View()
		result.Blocks = lowestVisibleSlot(view.BeaconBlocks())
		result.Sidecars = lowestVisibleSlot(view.BlobSidecars())
		view.Close()
	}

	blockRows, err := firstTableSlot(tx, tables, kv.BeaconBlocks, 8, func(key []byte) uint64 {
		return binary.BigEndian.Uint64(key)
	})
	if err != nil {
		return result, err
	}
	result.Blocks = lowerStoredSlot(result.Blocks, blockRows)

	result.Sidecars, err = addHotSlot(result.Sidecars, blobStorage)
	if err != nil {
		return result, err
	}
	result.Sidecars, err = addHotSlot(result.Sidecars, columnStorage)
	if err != nil {
		return result, err
	}

	stateView := stateSnapshots.View()
	if stateView != nil {
		defer stateView.Close()
	}
	stateNames := snapshotsync.MakeCaplinStateSnapshotsTypes(nil, nil).KeyValueGetters
	allStateTablesHold := true
	var latestStateSlot uint64
	for table := range stateNames {
		stored := StoredSlot{}
		if stateView != nil {
			stored = lowestVisibleSlot(stateView.VisibleSegments(table))
		}
		rows, err := firstTableSlot(tx, tables, table, 4, base_encoding.Decode64FromBytes4)
		if err != nil {
			return result, err
		}
		stored = lowerStoredSlot(stored, rows)
		result.StateTables[table] = stored
		if !stored.Holds {
			allStateTablesHold = false
			continue
		}
		if stored.Slot > latestStateSlot {
			latestStateSlot = stored.Slot
		}
	}
	result.States = StoredSlot{Slot: latestStateSlot, Holds: len(stateNames) > 0 && allStateTablesHold}

	return result, nil
}

func lowestVisibleSlot(segments []*snapshotsync.VisibleSegment) StoredSlot {
	if len(segments) == 0 {
		return StoredSlot{}
	}
	return StoredSlot{Slot: segments[0].From(), Holds: true}
}

func lowerStoredSlot(left, right StoredSlot) StoredSlot {
	if !right.Holds || left.Holds && left.Slot <= right.Slot {
		return left
	}
	return right
}

func addHotSlot(stored StoredSlot, source interface{ EarliestStoredSlot() (uint64, bool, error) }) (StoredSlot, error) {
	if source == nil {
		return stored, nil
	}
	slot, found, err := source.EarliestStoredSlot()
	if err != nil {
		return stored, err
	}
	if !found {
		return stored, nil
	}
	return lowerStoredSlot(stored, StoredSlot{Slot: slot, Holds: true}), nil
}

func firstTableSlot(tx kv.Tx, tables map[string]struct{}, table string, keyLength int, decode func([]byte) uint64) (StoredSlot, error) {
	if tx == nil {
		return StoredSlot{}, nil
	}
	if _, ok := tables[table]; !ok {
		return StoredSlot{}, nil
	}
	key, err := kv.FirstKey(tx, table)
	if err != nil {
		return StoredSlot{}, fmt.Errorf("read first %s key: %w", table, err)
	}
	if key == nil {
		return StoredSlot{}, nil
	}
	if len(key) < keyLength {
		return StoredSlot{}, fmt.Errorf("invalid %s key length: %d", table, len(key))
	}
	return StoredSlot{Slot: decode(key[:keyLength]), Holds: true}, nil
}
