package historyv2read

import (
	"encoding/binary"

	"github.com/ledgerwatch/erigon-lib/common/length"
	"github.com/ledgerwatch/erigon-lib/kv"
	"github.com/ledgerwatch/erigon-lib/kv/temporal/historyv2"
	"github.com/ledgerwatch/erigon/core/types/accounts"
)

const DefaultIncarnation = uint64(1)

func RestoreCodeHash(tx kv.Getter, key, v []byte) ([]byte, error) {
	var acc accounts.Account
	if err := acc.DecodeForStorage(v); err != nil {
		return nil, err
	}
	if acc.Incarnation > 0 && acc.IsEmptyCodeHash() {
		var codeHash []byte
		var err error

		prefix := make([]byte, length.Addr+length.BlockNum)
		copy(prefix, key)
		binary.BigEndian.PutUint64(prefix[length.Addr:], acc.Incarnation)

		codeHash, err = tx.GetOne(kv.PlainContractCode, prefix)
		if err != nil {
			return nil, err
		}
		if len(codeHash) > 0 {
			acc.CodeHash.SetBytes(codeHash)
		}
		v = make([]byte, acc.EncodingLengthForStorage())
		acc.EncodeForStorage(v)
	}
	return v, nil
}

func GetAsOf(tx kv.Tx, indexC kv.Cursor, changesC kv.CursorDupSort, storage bool, key []byte, timestamp uint64) ([]byte, error) {
	v, ok, err := historyv2.FindByHistory(indexC, changesC, storage, key, timestamp)
	if err != nil {
		return nil, err
	}
	if ok {
		//restore codehash
		if !storage {
			//restore codehash
			v, err = RestoreCodeHash(tx, key, v)
			if err != nil {
				return nil, err
			}
		}

		return v, nil
	}
	return tx.GetOne(kv.PlainState, key)
}
