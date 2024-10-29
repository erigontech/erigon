package partitions

import (
	"encoding/binary"

	"github.com/erigontech/erigon-lib/common/hexutility"
	"github.com/erigontech/erigon-lib/kv"
)

func Tables(tx kv.Getter, table string) (primary string, secondary string, err error) {
	primaryID, secondaryID, err := ID(tx, table)
	if err != nil {
		return "", "", err
	}
	return kv.Partitions[table][primaryID], kv.Partitions[table][secondaryID], nil
}

func ReadFromPartitions(tx kv.Getter, table string, k []byte) (v []byte, err error) {
	primary, secondary, err := Tables(tx, table)
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

func Rotate(tx kv.RwTx, table string) (bool, error) {
	{ //assert secondary must be empty
		_, secondary, err := Tables(tx, table)
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
	_, secondaryID, err := ID(tx, table)
	if err != nil {
		return false, err
	}

	err = putPrimaryPartition(tx, table, secondaryID)
	if err != nil {
		return false, err
	}
	return true, nil
}

func ID(tx kv.Getter, table string) (primary uint8, secondary uint8, err error) {
	val, err := tx.GetOne(kv.TblPruningProgress, []byte(string(table)+"_primary"))
	if err != nil || len(val) < 1 {
		return 0, 1, err
	}
	if val[0] == 0 {
		return 0, 1, nil
	}
	return 1, 0, nil
}
func putPrimaryPartition(tx kv.RwTx, table string, newActivePartitionNum uint8) error {
	return tx.Put(kv.TblPruningProgress, []byte(string(table)+"_primary"), []byte{newActivePartitionNum})
}

func PutPrimaryPartitionMax(tx kv.RwTx, table string, _max uint64) error {
	return tx.Put(kv.TblPruningProgress, []byte(string(table)+"_primary_max"), hexutility.EncodeTs(_max))
}
func Max(tx kv.RwTx, table string) (primaryMax, secondaryMax uint64, err error) {
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
