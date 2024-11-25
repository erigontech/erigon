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

package rawdb

import (
	"encoding/binary"

	"github.com/erigontech/erigon/erigon-lib/kv"
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
