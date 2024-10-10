package rawdb

import (
	"encoding/binary"

	"github.com/gateway-fm/cdk-erigon-lib/kv"
)

func GetStateVersion(tx kv.Tx) (uint64, error) {
	val, err := tx.GetOne(kv.Sequence, kv.PlainStateVersion)
	if err != nil {
		return 0, err
	}
	if len(val) == 0 {
		return 0, nil
	}
	return binary.BigEndian.Uint64(val), nil
}

func IncrementStateVersion(tx kv.RwTx) (uint64, error) {
	return tx.IncrementSequence(string(kv.PlainStateVersion), 1)
}
