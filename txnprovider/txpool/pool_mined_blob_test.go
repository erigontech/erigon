// Copyright 2026 The Erigon Authors
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

package txpool

import (
	"testing"
)

func TestDeleteMinedBlobTxnUpdatesMovedIndex(t *testing.T) {
	pool := &TxPool{
		minedBlobTxnsByBlock: make(map[uint64][]*metaTxn),
		minedBlobTxnsByHash:  make(map[string]*metaTxn),
	}
	hashes := []string{"A", "B", "C"}
	items := make([]*metaTxn, len(hashes))
	for i, hash := range hashes {
		items[i] = &metaTxn{
			minedBlockNum: 1,
			bestIndex:     i,
		}
		pool.minedBlobTxnsByHash[hash] = items[i]
	}
	pool.minedBlobTxnsByBlock[1] = items

	pool.deleteMinedBlobTxn(hashes[0])
	pool.deleteMinedBlobTxn(hashes[2])
	pool.deleteMinedBlobTxn(hashes[1])
	if len(pool.minedBlobTxnsByBlock[1]) != 0 {
		t.Fatal("expected all mined blob transactions to be deleted")
	}
}
