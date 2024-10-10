package rawdb

import (
	"encoding/binary"

	"github.com/ledgerwatch/erigon-lib/kv"
	"github.com/ledgerwatch/erigon/zk/hermez_db"
)

func GetLatestStateVersion(tx kv.Tx) (uint64, uint64, error) {
	c, err := tx.Cursor(hermez_db.PLAIN_STATE_VERSION)
	if err != nil {
		return 0, 0, err
	}
	defer c.Close()

	blockNumber, plainStateVersion, err := c.Last()
	if err != nil {
		return 0, 0, err
	}
	if len(blockNumber) == 0 || len(plainStateVersion) == 0 {
		return 0, 0, nil
	}

	return binary.BigEndian.Uint64(blockNumber), binary.BigEndian.Uint64(plainStateVersion), nil
}

func IncrementStateVersionByBlockNumberIfNeeded(tx kv.RwTx, blockNum uint64) (uint64, error) {
	plainStateVersionBlockNumber, plainStateVersion, err := GetLatestStateVersion(tx)
	if err != nil {
		return 0, err
	}
	if blockNum <= plainStateVersionBlockNumber {
		return 0, err
	}

	c, err := tx.RwCursor(hermez_db.PLAIN_STATE_VERSION)
	if err != nil {
		return 0, err
	}
	defer c.Close()

	newKBytes := make([]byte, 8)
	binary.BigEndian.PutUint64(newKBytes, blockNum)
	newVBytes := make([]byte, 8)
	binary.BigEndian.PutUint64(newVBytes, plainStateVersion+1)
	if err := c.Put(newKBytes, newVBytes); err != nil {
		return 0, err
	}
	return plainStateVersion + 1, nil
}

// delete all keys that are >= fromBlockNumber
// keys are sorted in accending order
func TruncateStateVersion(tx kv.RwTx, fromBlockNumber uint64) error {
	c, err := tx.RwCursor(hermez_db.PLAIN_STATE_VERSION)
	if err != nil {
		return err
	}
	defer c.Close()

	for k, _, err := c.Last(); k != nil; k, _, err = c.Prev() {
		if err != nil {
			return err
		}
		bn := binary.BigEndian.Uint64(k)
		if bn < fromBlockNumber {
			break
		}
		if err = c.DeleteCurrent(); err != nil {
			return err
		}
	}

	return nil
}
