package kv

import (
	"encoding/binary"

	"github.com/erigontech/erigon-lib/common/hexutility"
)

type PartitionsID [2]string

type PartitionedTable string

func (table PartitionedTable) Partitions(tx Getter) (primary string, secondary string, err error) {
	primaryID, secondaryID, err := table.PartitionsID(tx)
	if err != nil {
		return "", "", err
	}
	return Partitions[table][primaryID], Partitions[table][secondaryID], nil
}

func (table PartitionedTable) GetOne(tx Getter, k []byte) (v []byte, err error) {
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
func (table PartitionedTable) PartitionsID(tx Getter) (primary uint8, secondary uint8, err error) {
	val, err := tx.GetOne(TblPruningProgress, []byte(string(table)+"_primary"))
	if err != nil || len(val) < 1 {
		return 0, 1, err
	}
	if val[0] == 0 {
		return 0, 1, nil
	}
	return 1, 0, nil
}

func (table PartitionedTable) Rotate(tx RwTx) (bool, error) {
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

func putPrimaryPartition(tx RwTx, table PartitionedTable, newActivePartitionNum uint8) error {
	return tx.Put(TblPruningProgress, []byte(string(table)+"_primary"), []byte{newActivePartitionNum})
}

func (table PartitionedTable) PutPrimaryPartitionMax(tx RwTx, _max uint64) error {
	return tx.Put(TblPruningProgress, []byte(string(table)+"_primary_max"), hexutility.EncodeTs(_max))
}
func (table PartitionedTable) Max(tx RwTx) (primaryMax, secondaryMax uint64, err error) {
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

var (
	TxLookupPartitions = PartitionsID{txLookup0, txLookup1}
)

var (
	Partitions = map[PartitionedTable]PartitionsID{
		TxLookup: TxLookupPartitions,
	}
)

var (
	TxLookup PartitionedTable
)
