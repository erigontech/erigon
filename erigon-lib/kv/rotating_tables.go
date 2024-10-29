package kv

import (
	"encoding/binary"

	"github.com/erigontech/erigon-lib/common/hexutility"
)

// RotatingTable - is partitioned Table with only 2 partitions: primary/secondary (hot/cold)
// - new writes must go to `primary`
// - do prune only `secondary`
// - rotate when prune of `secondary` is done
type RotatingTable string

func (table RotatingTable) Partitions(tx Getter) (primary string, secondary string, err error) {
	primaryID, secondaryID, err := table.PartitionsID(tx)
	if err != nil {
		return "", "", err
	}
	return RotatingTablePartitions[table][primaryID], RotatingTablePartitions[table][secondaryID], nil
}

func (table RotatingTable) GetOne(tx Getter, k []byte) (v []byte, err error) {
	primary, secondary, err := table.Partitions(tx)
	if err != nil {
		return nil, err
	}
	v, err = tx.GetOne(primary, k)
	if err != nil {
		return nil, err
	}
	if v != nil {
		return v, nil
	}
	v, err = tx.GetOne(secondary, k)
	if err != nil {
		return nil, err
	}
	if v != nil {
		return v, nil
	}
	return nil, nil
}
func (table RotatingTable) PartitionsID(tx Getter) (primary uint8, secondary uint8, err error) {
	val, err := tx.GetOne(TblPruningProgress, []byte(string(table)+"_primary"))
	if err != nil || len(val) < 1 {
		return 0, 1, err
	}
	if val[0] == 0 {
		return 0, 1, nil
	}
	return 1, 0, nil
}

func (table RotatingTable) Rotate(tx RwTx) (bool, error) {
	{ //assert secondary must be empty
		_, secondary, err := table.Partitions(tx)
		if err != nil {
			return false, err
		}
		cnt, err := tx.Count(secondary) // toto pre-alloc array of ID names
		if err != nil {
			return false, err
		}
		if cnt > 0 {
			return false, nil
		}
	}
	_, secondaryID, err := table.PartitionsID(tx)
	if err != nil {
		return false, err
	}

	err = putPrimaryPartition(tx, table, secondaryID)
	if err != nil {
		return false, err
	}
	return true, nil
}

func putPrimaryPartition(tx RwTx, table RotatingTable, newActivePartitionNum uint8) error {
	return tx.Put(TblPruningProgress, []byte(string(table)+"_primary"), []byte{newActivePartitionNum})
}

func (table RotatingTable) PutPrimaryPartitionMax(tx RwTx, _max uint64) error {
	return tx.Put(TblPruningProgress, []byte(string(table)+"_primary_max"), hexutility.EncodeTs(_max))
}
func (table RotatingTable) Max(tx RwTx) (primaryMax, secondaryMax uint64, err error) {
	fst, err := tx.GetOne(TblPruningProgress, []byte(string(table)+"_primary_max"))
	if err != nil {
		return 0, 0, err
	}
	if len(fst) == 8 {
		primaryMax = binary.BigEndian.Uint64(fst)
	}
	snd, err := tx.GetOne(TblPruningProgress, []byte(string(table)+"_secondary_max"))
	if err != nil {
		return 0, 0, err
	}
	if len(snd) == 8 {
		secondaryMax = binary.BigEndian.Uint64(snd)
	}
	return
}

type RotatingTablePartitionsList [2]string

var (
	RotatingTablePartitions = map[RotatingTable]RotatingTablePartitionsList{
		TxLookup: {txLookup0, txLookup1},
	}
)

var (
	TxLookup RotatingTable
)
