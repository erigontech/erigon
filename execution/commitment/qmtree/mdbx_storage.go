package qmtree

import (
	"encoding/binary"
	"fmt"

	"github.com/erigontech/erigon/common"
	"github.com/erigontech/erigon/db/kv"
)

// MDBXStorage reads and writes qmtree entry components to MDBX tables.
// It replaces HPFile-based storage with standard Erigon DB tables that
// participate in the snapshot collation/pruning/torrent pipeline.
//
// Tables used:
//   - QMTreeEntries:  txNum (8B BE) → pre(32B) || sc(32B) || trans(32B) = 96B value
//   - QMTreeMeta:     string key → value
type MDBXStorage struct{}

// Meta keys for QMTreeMeta table.
var (
	metaKeyNextTxNum = []byte("nextTxNum")
	metaKeyPrevLeaf  = []byte("prevLeaf")
)

// PutEntry writes a single entry's three hash components to QMTreeEntries.
func PutEntry(tx kv.RwTx, txNum uint64, pre, stateChange, transition common.Hash) error {
	var key [8]byte
	binary.BigEndian.PutUint64(key[:], txNum)

	var val [96]byte
	copy(val[0:32], pre[:])
	copy(val[32:64], stateChange[:])
	copy(val[64:96], transition[:])

	return tx.Put(kv.TblQMTreeEntries, key[:], val[:])
}

// GetEntry reads an entry's three hash components from QMTreeEntries.
func GetEntry(tx kv.Tx, txNum uint64) (pre, stateChange, transition common.Hash, err error) {
	var key [8]byte
	binary.BigEndian.PutUint64(key[:], txNum)

	val, err := tx.GetOne(kv.TblQMTreeEntries, key[:])
	if err != nil {
		return common.Hash{}, common.Hash{}, common.Hash{}, err
	}
	if val == nil {
		return common.Hash{}, common.Hash{}, common.Hash{}, fmt.Errorf("qmtree entry not found: txNum=%d", txNum)
	}
	if len(val) < 96 {
		return common.Hash{}, common.Hash{}, common.Hash{}, fmt.Errorf("qmtree entry too short: txNum=%d len=%d", txNum, len(val))
	}
	copy(pre[:], val[0:32])
	copy(stateChange[:], val[32:64])
	copy(transition[:], val[64:96])
	return
}

// PutMeta writes a metadata value to QMTreeMeta.
func PutMeta(tx kv.RwTx, key string, val []byte) error {
	return tx.Put(kv.TblQMTreeMeta, []byte(key), val)
}

// GetMeta reads a metadata value from QMTreeMeta.
func GetMeta(tx kv.Tx, key string) ([]byte, error) {
	return tx.GetOne(kv.TblQMTreeMeta, []byte(key))
}

// PutNextTxNum writes the next txNum to QMTreeMeta.
func PutNextTxNum(tx kv.RwTx, nextTxNum uint64) error {
	var buf [8]byte
	binary.BigEndian.PutUint64(buf[:], nextTxNum)
	return PutMeta(tx, string(metaKeyNextTxNum), buf[:])
}

// GetNextTxNum reads the next txNum from QMTreeMeta.
func GetNextTxNum(tx kv.Tx) (uint64, error) {
	val, err := GetMeta(tx, string(metaKeyNextTxNum))
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

// DeleteEntriesFrom deletes all entries with txNum >= fromTxNum.
func DeleteEntriesFrom(tx kv.RwTx, fromTxNum uint64) error {
	var key [8]byte
	binary.BigEndian.PutUint64(key[:], fromTxNum)

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

// EntryCount returns the number of entries in QMTreeEntries.
func EntryCount(tx kv.Tx) (uint64, error) {
	return GetNextTxNum(tx)
}
