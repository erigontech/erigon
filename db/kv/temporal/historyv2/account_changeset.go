// Copyright 2022 The Erigon Authors
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

package historyv2

import (
	"encoding/binary"
	"fmt"

	"github.com/erigontech/erigon-lib/common/length"
)

func DecodeAccounts(dbKey, dbValue []byte) (uint64, []byte, []byte, error) {
	blockN := binary.BigEndian.Uint64(dbKey)
	if len(dbValue) < length.Addr {
		return 0, nil, nil, fmt.Errorf("account changes purged for block %d", blockN)
	}
	k := dbValue[:length.Addr]
	v := dbValue[length.Addr:]
	return blockN, k, v, nil
}
