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

	"github.com/stretchr/testify/require"
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

func TestFinalizeMinedBlobsAfterDeletion(t *testing.T) {
	pool := &TxPool{
		minedBlobTxnsByBlock: make(map[uint64][]*metaTxn),
		minedBlobTxnsByHash:  make(map[string]*metaTxn),
	}
	txns := make([]*TxnSlot, 4)
	for i := range txns {
		txns[i] = newTestBlobTxnSlot(uint64(i), 1, 1, 1, 21000)
		txns[i].IDHash[0] = byte(i + 1)
		pool.lastSeenBlock.Store(uint64(i + 1))
		require.NoError(t, pool.processMinedFinalizedBlobs(txns[i:i+1], 0))
	}

	pool.deleteMinedBlobTxn(string(txns[1].IDHash[:]))
	pool.deleteMinedBlobTxn(string(txns[2].IDHash[:]))
	pool.lastSeenBlock.Store(5)
	require.NoError(t, pool.processMinedFinalizedBlobs(nil, 3))

	for i := range 3 {
		require.NotContains(t, pool.minedBlobTxnsByBlock, uint64(i+1))
		require.NotContains(t, pool.minedBlobTxnsByHash, string(txns[i].IDHash[:]))
	}
	require.Len(t, pool.minedBlobTxnsByBlock, 2)
	require.Len(t, pool.minedBlobTxnsByHash, 1)
	require.Same(t, txns[3], pool.minedBlobTxnsByBlock[4][0].TxnSlot)
	require.Same(t, pool.minedBlobTxnsByBlock[4][0], pool.minedBlobTxnsByHash[string(txns[3].IDHash[:])])
}
