package rawdbv3

import (
	"encoding/binary"

	"github.com/erigontech/erigon-lib/common/hexutility"
	"github.com/erigontech/erigon-lib/kv"
)

func Partitions(tx kv.Getter, table string) (primary string, secondary string, err error) {
	partitionID, err := primaryPartitionID(tx, table)
	if err != nil {
		return "", "", err
	}
	if partitionID == 0 {
		return kv.Partitions[table][0], kv.Partitions[table][1], nil
	}
	return kv.Partitions[table][1], kv.Partitions[table][0], nil
}

func ReadFromPartitions(tx kv.Getter, table string, k []byte) (v []byte, err error) {
	primaryPartition, secondaryPartition, err := Partitions(tx, table)
	data, err := tx.GetOne(primaryPartition, k)
	if err != nil {
		return nil, err
	}
	if data != nil {
		return v, nil
	}
	data, err = tx.GetOne(secondaryPartition, k)
	if err != nil {
		return nil, err
	}
	if data != nil {
		return v, nil
	}
	return nil, nil
}

func RotatePartitions(tx kv.RwTx, table string, partitions [2]string) (bool, error) {
	p, err := primaryPartitionID(tx, table)
	if err != nil {
		return false, err
	}
	secondaryPartitionNum := uint8(1)
	if p == 1 {
		secondaryPartitionNum = 0
	}
	secondaryPartition := partitions[secondaryPartitionNum]
	cnt, err := tx.Count(secondaryPartition) // toto pre-alloc array of primaryPartitionID names
	if err != nil {
		return false, err
	}
	if cnt > 0 {
		return false, nil
	}

	err = putPrimaryPartition(tx, table, secondaryPartitionNum)
	if err != nil {
		return false, err
	}
	return true, nil
}

func primaryPartitionID(tx kv.Getter, table string) (uint8, error) {
	val, err := tx.GetOne(kv.TblPruningProgress, []byte(string(table)+"_partition"))
	if err != nil || len(val) < 1 {
		return 0, err
	}
	return val[0], nil
}
func putPrimaryPartition(tx kv.RwTx, table string, newActivePartitionNum uint8) error {
	return tx.Put(kv.TblPruningProgress, []byte(string(table)+"_primary"), []byte{newActivePartitionNum})
}

func PutPrimaryPartitionMax(tx kv.RwTx, table string, _max uint64) error {
	return tx.Put(kv.TblPruningProgress, []byte(string(table)+"_primary_max"), hexutility.EncodeTs(_max))
}
func PartitionsMax(tx kv.RwTx, table string) (primaryMax, secondaryMax uint64, err error) {
	fst, err := tx.GetOne(kv.TblPruningProgress, []byte(string(table)+"_primary_max"))
	if err != nil {
		return 0, 0, err
	}
	if len(fst) == 8 {
		primaryMax = binary.BigEndian.Uint64(fst)
	}
	snd, err := tx.GetOne(kv.TblPruningProgress, []byte(string(table)+"_secondary_max"))
	if err != nil {
		return 0, 0, err
	}
	if len(snd) == 8 {
		secondaryMax = binary.BigEndian.Uint64(snd)
	}
	return
}
