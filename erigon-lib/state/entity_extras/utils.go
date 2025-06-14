package entity_extras

import (
	"bytes"

	"github.com/erigontech/erigon-lib/kv"
)

// toPrefix exclusive
func DeleteRangeFromTbl(tbl string, fromPrefix, toPrefix []byte, limit uint64, rwTx kv.RwTx) (delCount uint64, err error) {
	c, err := rwTx.RwCursor(tbl) // TODO: no dupsort tbl assumed
	if err != nil {
		return
	}

	defer c.Close()
	// bigendianess assumed (for key comparison)
	// imo this can be generalized if needed, by using key comparison functions, which mdbx provides.
	for k, _, err := c.Seek(fromPrefix); k != nil && (toPrefix == nil || bytes.Compare(k, toPrefix) < 0) && limit > 0; k, _, err = c.Next() {
		if err != nil {
			return delCount, err
		}

		if err := c.DeleteCurrent(); err != nil {
			return delCount, err
		}
		limit--
		delCount++
	}

	return
}
