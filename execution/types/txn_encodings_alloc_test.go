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

package types

import (
	"testing"

	"github.com/holiman/uint256"
	"github.com/stretchr/testify/require"

	"github.com/erigontech/erigon/common"
	"github.com/erigontech/erigon/common/race"
	"github.com/erigontech/erigon/common/u256"
	"github.com/erigontech/erigon/execution/rlp"
)

// A block read with hashes reuses the collection buffers of earlier blocks: what is left per
// block is the cached hash of each txn.
func TestTxnEncodingsReuseBuffers(t *testing.T) {
	to := common.HexToAddress("0x0000000000000000000000000000000000000001")
	const n = 64
	stored := make([][]byte, n)
	txs := make([]Transaction, n)
	for i := range n {
		txn := &DynamicFeeTransaction{
			CommonTx: CommonTx{Nonce: uint64(i), To: &to, GasLimit: 21000, Data: make([]byte, 50+i*10)},
			ChainID:  u256.Num1,
			TipCap:   *uint256.NewInt(1),
			FeeCap:   *uint256.NewInt(2),
		}
		var err error
		stored[i], err = rlp.EncodeToBytes(txn)
		require.NoError(t, err)
		txs[i], err = DecodeTransaction(stored[i])
		require.NoError(t, err)
	}
	readBlock := func() {
		e := NewTxnEncodings()
		defer e.Release()
		for _, s := range stored {
			if err := e.Add(s); err != nil {
				t.Fatal(err)
			}
		}
		e.CacheHashes(txs)
	}
	readBlock()
	allocs := testing.AllocsPerRun(20, readBlock)
	if !race.Enabled { // the race detector drops sync.Pool puts
		require.LessOrEqual(t, allocs, float64(n), "one cached hash per txn")
	}
}
