package historyv2read

import (
	"encoding/binary"
	"fmt"

	"github.com/ledgerwatch/erigon-lib/common/length"
	"github.com/ledgerwatch/erigon-lib/kv"
	"github.com/ledgerwatch/erigon-lib/kv/temporal/historyv2"
	"github.com/ledgerwatch/erigon/core/state/temporal"
	"github.com/ledgerwatch/erigon/core/types/accounts"
)

const DefaultIncarnation = uint64(1)

func GetAsOfV3(tx kv.TemporalTx, storage bool, key []byte, timestamp uint64, histV3 bool) (v []byte, err error) {
	var ok bool
	if storage {
		v, ok, err = tx.HistoryGet(temporal.Storage, key, timestamp)
	} else {
		v, ok, err = tx.HistoryGet(temporal.Accounts, key, timestamp)
	}
	if err != nil {
		return nil, err
	}
	fmt.Printf("GetAsOfV3: %x, %d\n", key, timestamp)
	if !ok {
		return tx.GetOne(kv.PlainState, key)
	}

	if v == nil {
		return nil, nil
	}
	if storage {
		return v, nil
	}

	if histV3 {
		v, err = accounts.ConvertV3toV2(v)
		if err != nil {
			return nil, err
		}
		return v, nil
	}

	//restore codehash
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
		}

		return v, nil
	}
	return tx.GetOne(kv.PlainState, key)
}
