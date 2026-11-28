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

package membatchwithdb

import "github.com/erigontech/erigon/db/kv"

type entry struct {
	k []byte
	v []byte
}

type MemoryDiff struct {
	diff              map[table][]entry // god.
	deletedEntries    map[string][]entry
	clearedTableNames []string
}

type table struct {
	name    string
	dupsort bool
}

func (m *MemoryDiff) Flush(tx kv.RwTx) error {
	// Obliterate buckets who are to be deleted
	for _, bucket := range m.clearedTableNames {
		if err := tx.ClearTable(bucket); err != nil {
			return err
		}
	}
	// Obliterate entries who are to be deleted
	for bucket, rows := range m.deletedEntries {
		if err := deleteRows(tx, bucket, rows); err != nil {
			return err
		}
	}
	// Iterate over each bucket and apply changes accordingly.
	for bucketInfo, bucketDiff := range m.diff {
		if err := func() error {
			if bucketInfo.dupsort {
				dbCursor, err := tx.RwCursorDupSort(bucketInfo.name)
				if err != nil {
					return err
				}
				defer dbCursor.Close()
				for _, entry := range bucketDiff {
					if err := dbCursor.Put(entry.k, entry.v); err != nil {
						return err
					}
				}
			} else {
				for _, entry := range bucketDiff {
					if err := tx.Put(bucketInfo.name, entry.k, entry.v); err != nil {
						return err
					}
				}
			}
			return nil
		}(); err != nil {
			return err
		}
	}
	return nil
}

// deleteRows applies tombstones to tx: a row with a nil value deletes the whole
// key, any other row deletes only that DupSort pair.
func deleteRows(tx kv.RwTx, bucket string, rows []entry) error {
	var pairs []entry
	for _, row := range rows {
		if row.v != nil {
			pairs = append(pairs, row)
			continue
		}
		if err := tx.Delete(bucket, row.k); err != nil {
			return err
		}
	}
	if len(pairs) == 0 {
		return nil
	}
	c, err := tx.RwCursorDupSort(bucket)
	if err != nil {
		return err
	}
	defer c.Close()
	for _, pair := range pairs {
		if err := c.DeleteExact(pair.k, pair.v); err != nil {
			return err
		}
	}
	return nil
}
