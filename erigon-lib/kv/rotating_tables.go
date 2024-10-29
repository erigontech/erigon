package kv

import (
	"encoding/binary"

	"github.com/erigontech/erigon-lib/common"
	"github.com/erigontech/erigon-lib/common/hexutility"
)

// RotatingTable - is partitioned Table with only 2 partitions: primary/secondary (hot/cold)
// - write new data only on `primary`
// - prune only on `secondary`: then all deletes become sequential
// - rotate partitions when prune of `secondary` is done
//
// Primary use-case: table with random updates (for example: `hash` -> `blockNumber`).
// Such table suffer from slow random writes and deletes.
// RollingTable will make writes and deletes cheaper - but `GetOne` may read from 2 tables.
type RotatingTable string

// Partitions - table names of partitions
func (table RotatingTable) Partitions(tx Getter) (primary string, secondary string, err error) {
	primaryID, secondaryID, err := table.partitionsID(tx)
	if err != nil {
		return "", "", err
	}
	return rotatingTablePartitions[table][primaryID], rotatingTablePartitions[table][secondaryID], nil
}

func (table RotatingTable) partitionsID(tx Getter) (primary uint8, secondary uint8, err error) {
	val, err := tx.GetOne(TblPruningProgress, []byte(string(table)+"_primary"))
	if err != nil || len(val) < 1 {
		return 0, 1, err
	}
	if val[0] == 0 {
		return 0, 1, nil
	}
	return 1, 0, nil
}

// GetOne - simple read from both partitions
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

// Rotate - switch primary and secondary partitions - may return `false` if secondary is not empty
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
	_, secondaryID, err := table.partitionsID(tx)
	if err != nil {
		return false, err
	}

	err = putPrimaryPartition(tx, table, secondaryID)
	if err != nil {
		return false, err
	}
	return true, nil
}

func (table RotatingTable) ClearTables(tx RwTx) error {
	primary, secondary, err := table.Partitions(tx)
	if err != nil {
		return err
	}
	if err := tx.ClearBucket(primary); err != nil {
		return err
	}
	if err := tx.ClearBucket(secondary); err != nil {
		return err
	}
	if err := table.PutPrimaryPartitionMax(tx, 0); err != nil {
		return err
	}
	if err := putPrimaryPartition(tx, table, 0); err != nil {
		return err
	}
	if err := swapVals(tx, TblPruningProgress, []byte(string(table)+"_primary_max"), []byte(string(table)+"_secondary_max")); err != nil {
		return err
	}
	return nil
}

func swapVals(tx RwTx, table string, k1, k2 []byte) error {
	v1, err := tx.GetOne(table, k1)
	if err != nil {
		return err
	}
	if v1 != nil {
		v1 = common.Copy(v1)
	}
	v2, err := tx.GetOne(table, k2)
	if err != nil {
		return err
	}
	if v2 != nil {
		v2 = common.Copy(v2)
	}

	if err := tx.Put(table, k2, v1); err != nil {
		return err
	}
	if err := tx.Put(table, k1, v2); err != nil {
		return err
	}
	return nil
}
func putPrimaryPartition(tx RwTx, table RotatingTable, newActivePartitionNum uint8) error {
	return tx.Put(TblPruningProgress, []byte(string(table)+"_primary"), []byte{newActivePartitionNum})
}

func (table RotatingTable) PutPrimaryPartitionMax(tx RwTx, _max uint64) error {
	return tx.Put(TblPruningProgress, []byte(string(table)+"_primary_max"), hexutility.EncodeTs(_max))
}
func (table RotatingTable) PartitionsMax(tx RwTx) (primaryMax, secondaryMax uint64, err error) {
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

type rotatingTablePartitionsList [2]string

var (
	rotatingTablePartitions = map[RotatingTable]rotatingTablePartitionsList{
		TxLookup: {txLookup0, txLookup1},
	}
)

var (
	TxLookup RotatingTable
)
