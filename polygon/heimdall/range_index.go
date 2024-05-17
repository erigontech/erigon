package heimdall

import (
	"context"
	"encoding/binary"

	"github.com/c2h5oh/datasize"
	"github.com/ledgerwatch/log/v3"

	"github.com/ledgerwatch/erigon-lib/kv"
	"github.com/ledgerwatch/erigon-lib/kv/mdbx"
)

type RangeIndex struct {
	db kv.RwDB
}

const rangeIndexTableName = "Index"

func NewRangeIndex(ctx context.Context, tmpDir string, logger log.Logger) (*RangeIndex, error) {
	db, err := mdbx.NewMDBX(logger).
		InMem(tmpDir).
		WithTableCfg(func(_ kv.TableCfg) kv.TableCfg { return kv.TableCfg{rangeIndexTableName: {}} }).
		MapSize(1 * datasize.GB).
		Open(ctx)
	if err != nil {
		return nil, err
	}

	return &RangeIndex{db}, nil
}

func (i *RangeIndex) Close() {
	i.db.Close()
}

func rangeIndexKey(blockNum uint64) [8]byte {
	var key [8]byte
	binary.BigEndian.PutUint64(key[:], blockNum)
	return key
}

func rangeIndexValue(id uint64) [8]byte {
	var value [8]byte
	binary.BigEndian.PutUint64(value[:], id)
	return value
}

func rangeIndexValueParse(value []byte) uint64 {
	return binary.BigEndian.Uint64(value)
}

// Put a mapping from a range to an id.
func (i *RangeIndex) Put(ctx context.Context, r ClosedRange, id uint64) error {
	tx, err := i.db.BeginRw(ctx)
	if err != nil {
		return err
	}
	defer tx.Rollback()

	key := rangeIndexKey(r.End)
	value := rangeIndexValue(id)
	if err = tx.Put(rangeIndexTableName, key[:], value[:]); err != nil {
		return err
	}
	return tx.Commit()
}

// Lookup an id of a range by a blockNum within that range.
func (i *RangeIndex) Lookup(ctx context.Context, blockNum uint64) (uint64, error) {
	var id uint64
	err := i.db.View(ctx, func(tx kv.Tx) error {
		cursor, err := tx.Cursor(rangeIndexTableName)
		if err != nil {
			return err
		}
		defer cursor.Close()

		key := rangeIndexKey(blockNum)
		_, value, err := cursor.Seek(key[:])
		if err != nil {
			return err
		}
		// not found
		if value == nil {
			return nil
		}

		id = rangeIndexValueParse(value)
		return nil
	})
	return id, err
}
