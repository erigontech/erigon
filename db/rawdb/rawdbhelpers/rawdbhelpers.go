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

package rawdbhelpers

import (
	"encoding/binary"

	"github.com/erigontech/erigon/db/kv"
)

func IdxStepsCountV3(tx kv.Tx, stepSize uint64) float64 {
	fst, _ := kv.FirstKey(tx, kv.TblAccountHistoryKeys)
	lst, _ := kv.LastKey(tx, kv.TblAccountHistoryKeys)
	if len(fst) > 0 && len(lst) > 0 {
		fstTxNum := binary.BigEndian.Uint64(fst)
		lstTxNum := binary.BigEndian.Uint64(lst)

		return float64(lstTxNum-fstTxNum) / float64(stepSize)
	}
	return 0
}

// IdxStepsInDB computes the step-range present in a DupSort table whose key is a txNum (big-endian uint64 prefix).
func IdxStepsInDB(tx kv.Tx, table string, stepSize uint64) float64 {
	fst, _ := kv.FirstKey(tx, table)
	lst, _ := kv.LastKey(tx, table)
	if len(fst) >= 8 && len(lst) >= 8 { // keys have a big-endian uint64 txNum prefix
		fstTxNum := binary.BigEndian.Uint64(fst)
		lstTxNum := binary.BigEndian.Uint64(lst)
		return float64(lstTxNum-fstTxNum) / float64(stepSize)
	}
	return 0
}
