// Copyright 2024 The Erigon Authors
// This file is part of Erigon.
//
// Erigon is free software: you can redistribute it and/or modify
// it under the terms of the GNU Lesser General Public License as published by
// the Free Software Foundation, either version 3 of the License, or
// (at your option) any later version.
//
// Erigon is distributed in the hope that it will be useful,
// but WITHOUT ANY WARRANTY; without even the implied warranty of
// MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE. See the
// GNU Lesser General Public License for more details.
//
// You should have received a copy of the GNU Lesser General Public License
// along with Erigon. If not, see <http://www.gnu.org/licenses/>.

package historyv2read

import (
	"encoding/binary"

	libcommon "github.com/erigontech/erigon-lib/common"
	"github.com/erigontech/erigon-lib/common/length"
	"github.com/erigontech/erigon-lib/kv"
	"github.com/erigontech/erigon-lib/kv/temporal/historyv2"
	"github.com/erigontech/erigon/core/types/accounts"
)

const DefaultIncarnation = uint64(1)

func RestoreCodeHash(tx kv.Getter, key, v []byte, force *libcommon.Hash) ([]byte, error) {
	var acc accounts.Account
	if err := acc.DecodeForStorage(v); err != nil {
		return nil, err
	}
	if force != nil {
		acc.CodeHash = *force
		v = make([]byte, acc.EncodingLengthForStorage())
		acc.EncodeForStorage(v)
		return v, nil
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
			v = make([]byte, acc.EncodingLengthForStorage())
			acc.EncodeForStorage(v)
		}
	}
	return v, nil
}

func GetAsOf(tx kv.Tx, indexC kv.Cursor, changesC kv.CursorDupSort, storage bool, key []byte, timestamp uint64) (v []byte, fromHistory bool, err error) {
	v, ok, err := historyv2.FindByHistory(indexC, changesC, storage, key, timestamp)
	if err != nil {
		return nil, true, err
	}
	if ok {
		return v, true, nil
	}
	v, err = tx.GetOne(kv.PlainState, key)
	return v, false, err
}
