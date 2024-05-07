package membatchwithdb

import "github.com/ledgerwatch/erigon-lib/kv"

type entry struct {
	k []byte
	v []byte
}

type MemoryDiff struct {
	diff              map[table][]entry // god.
	deletedEntries    map[string][]string
	clearedTableNames []string
}

type table struct {
	name    string
	dupsort bool
}

func (m *MemoryDiff) Flush(tx kv.RwTx) error {
	// Obliterate buckets who are to be deleted
	for _, bucket := range m.clearedTableNames {
		if err := tx.ClearBucket(bucket); err != nil {
			return err
		}
	}
	// Obliterate entries who are to be deleted
	for bucket, keys := range m.deletedEntries {
		for _, key := range keys {
			if err := tx.Delete(bucket, []byte(key)); err != nil {
				return err
			}
		}
	}
	// Iterate over each bucket and apply changes accordingly.
	for bucketInfo, bucketDiff := range m.diff {
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
	}
	return nil
}
