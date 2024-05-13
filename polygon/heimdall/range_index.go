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

func NewRangeIndex(tmpDir string, logger log.Logger) (*RangeIndex, error) {
	db, err := mdbx.NewMDBX(logger).
		InMem(tmpDir).
		Label(kv.SentryDB).
		WithTableCfg(func(_ kv.TableCfg) kv.TableCfg { return kv.TableCfg{rangeIndexTableName: {}} }).
		MapSize(1 * datasize.GB).
		Open(context.Background())
	if err != nil {
		return nil, err
	}

	return &RangeIndex{db}, nil
}

func (i *RangeIndex) Close() {
	i.db.Close()
}

// Put a mapping from a range to an id.
func (i *RangeIndex) Put(r ClosedRange, id uint64) error {
	tx, err := i.db.BeginRw(context.Background())
	if err != nil {
		return err
	}
	defer tx.Rollback()

	var key [8]byte
	binary.BigEndian.PutUint64(key[:], r.End)

	var idBytes [8]byte
	binary.BigEndian.PutUint64(idBytes[:], id)

	if err = tx.Put(rangeIndexTableName, key[:], idBytes[:]); err != nil {
		return err
	}
	return tx.Commit()
}

// Lookup an id of a range by a blockNum within that range.
func (i *RangeIndex) Lookup(blockNum uint64) (uint64, error) {
	var id uint64
	err := i.db.View(context.Background(), func(tx kv.Tx) error {
		cursor, err := tx.Cursor(rangeIndexTableName)
		if err != nil {
			return err
		}

		var key [8]byte
		binary.BigEndian.PutUint64(key[:], blockNum)

		_, idBytes, err := cursor.Seek(key[:])
		if err != nil {
			return err
		}
		// not found
		if idBytes == nil {
			return nil
		}

		id = binary.BigEndian.Uint64(idBytes)
		return nil
	})
	return id, err
}
