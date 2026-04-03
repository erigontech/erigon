package qmtree

import (
	"encoding/binary"
	"fmt"

	"github.com/erigontech/erigon/common"
	"github.com/erigontech/erigon/db/kv"
)

// MDBXStorage reads and writes qmtree entry components and KeyIndex data
// to MDBX tables. It replaces HPFile-based storage with standard Erigon DB
// tables that participate in the snapshot collation/pruning/torrent pipeline.
//
// Tables used:
//   - QMTreeEntries:  serialNum (8B BE) → pre(32B) || sc(32B) || trans(32B) = 96B value
//   - QMTreeKeyIndex: keyHash (32B) → txNum (8B BE)
//   - QMTreeMeta:     string key → value
type MDBXStorage struct{}

// Meta keys for QMTreeMeta table.
var (
	metaKeyNextSN   = []byte("nextSN")
	metaKeyPrevLeaf = []byte("prevLeaf")
)

// PutEntry writes a single entry's three hash components to QMTreeEntries.
func PutEntry(tx kv.RwTx, sn uint64, pre, stateChange, transition common.Hash) error {
	var key [8]byte
	binary.BigEndian.PutUint64(key[:], sn)

	var val [96]byte
	copy(val[0:32], pre[:])
	copy(val[32:64], stateChange[:])
	copy(val[64:96], transition[:])

	return tx.Put(kv.TblQMTreeEntries, key[:], val[:])
}

// GetEntry reads an entry's three hash components from QMTreeEntries.
func GetEntry(tx kv.Tx, sn uint64) (pre, stateChange, transition common.Hash, err error) {
	var key [8]byte
	binary.BigEndian.PutUint64(key[:], sn)

	val, err := tx.GetOne(kv.TblQMTreeEntries, key[:])
	if err != nil {
		return common.Hash{}, common.Hash{}, common.Hash{}, err
	}
	if val == nil {
		return common.Hash{}, common.Hash{}, common.Hash{}, fmt.Errorf("qmtree entry not found: sn=%d", sn)
	}
	if len(val) < 96 {
		return common.Hash{}, common.Hash{}, common.Hash{}, fmt.Errorf("qmtree entry too short: sn=%d len=%d", sn, len(val))
	}
	copy(pre[:], val[0:32])
	copy(stateChange[:], val[32:64])
	copy(transition[:], val[64:96])
	return
}

// PutKeyIndex writes a keyHash → txNum mapping to QMTreeKeyIndex.
func PutKeyIndex(tx kv.RwTx, keyHash common.Hash, txNum uint64) error {
	var val [8]byte
	binary.BigEndian.PutUint64(val[:], txNum)
	return tx.Put(kv.TblQMTreeKeyIndex, keyHash[:], val[:])
}

// GetKeyIndex reads the latest txNum for a keyHash from QMTreeKeyIndex.
func GetKeyIndex(tx kv.Tx, keyHash common.Hash) (txNum uint64, found bool, err error) {
	val, err := tx.GetOne(kv.TblQMTreeKeyIndex, keyHash[:])
	if err != nil {
		return 0, false, err
	}
	if val == nil {
		return 0, false, nil
	}
	return binary.BigEndian.Uint64(val), true, nil
}

// PutMeta writes a metadata value to QMTreeMeta.
func PutMeta(tx kv.RwTx, key string, val []byte) error {
	return tx.Put(kv.TblQMTreeMeta, []byte(key), val)
}

// GetMeta reads a metadata value from QMTreeMeta.
func GetMeta(tx kv.Tx, key string) ([]byte, error) {
	return tx.GetOne(kv.TblQMTreeMeta, []byte(key))
}

// PutNextSN writes the next serial number to QMTreeMeta.
func PutNextSN(tx kv.RwTx, nextSN uint64) error {
	var buf [8]byte
	binary.BigEndian.PutUint64(buf[:], nextSN)
	return PutMeta(tx, string(metaKeyNextSN), buf[:])
}

// GetNextSN reads the next serial number from QMTreeMeta.
func GetNextSN(tx kv.Tx) (uint64, error) {
	val, err := GetMeta(tx, string(metaKeyNextSN))
	if err != nil {
		return 0, err
	}
	if val == nil {
		return 0, nil
	}
	return binary.BigEndian.Uint64(val), nil
}

// PutPrevLeaf writes the previous leaf hash to QMTreeMeta.
func PutPrevLeaf(tx kv.RwTx, prevLeaf common.Hash) error {
	return PutMeta(tx, string(metaKeyPrevLeaf), prevLeaf[:])
}

// GetPrevLeaf reads the previous leaf hash from QMTreeMeta.
func GetPrevLeaf(tx kv.Tx) (common.Hash, error) {
	val, err := GetMeta(tx, string(metaKeyPrevLeaf))
	if err != nil {
		return common.Hash{}, err
	}
	if val == nil {
		return common.Hash{}, nil
	}
	var h common.Hash
	copy(h[:], val)
	return h, nil
}

// DeleteEntriesFrom deletes all entries with serialNum >= fromSN.
func DeleteEntriesFrom(tx kv.RwTx, fromSN uint64) error {
	var key [8]byte
	binary.BigEndian.PutUint64(key[:], fromSN)

	c, err := tx.Cursor(kv.TblQMTreeEntries)
	if err != nil {
		return err
	}
	defer c.Close()

	for k, _, err := c.Seek(key[:]); k != nil; k, _, err = c.Next() {
		if err != nil {
			return err
		}
		if err := tx.Delete(kv.TblQMTreeEntries, k); err != nil {
			return err
		}
	}
	return nil
}

// EntryCount returns the number of entries in QMTreeEntries by reading NextSN.
func EntryCount(tx kv.Tx) (uint64, error) {
	return GetNextSN(tx)
}
