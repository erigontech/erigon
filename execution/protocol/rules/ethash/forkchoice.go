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

package ethash

import (
	"bytes"
	"math/big"

	"github.com/erigontech/erigon/common"
)

// ShouldReorg reports whether a candidate header should replace the current
// canonical head per PoW total-difficulty fork choice.
//
// The new chain wins if its total difficulty is strictly higher. On equal
// total difficulty, the shorter chain wins (smaller block height); on equal
// total difficulty AND equal height, the lexicographically larger block hash
// wins. This matches geth's tie-break — see
// https://github.com/maticnetwork/bor/blob/master/core/forkchoice.go#L81.
func ShouldReorg(localTd *big.Int, localHeight uint64, localHash common.Hash, newTd *big.Int, newHeight uint64, newHash common.Hash) bool {
	if cmp := newTd.Cmp(localTd); cmp != 0 {
		return cmp > 0
	}
	if newHeight != localHeight {
		return newHeight < localHeight
	}
	return bytes.Compare(localHash.Bytes(), newHash.Bytes()) < 0
}
