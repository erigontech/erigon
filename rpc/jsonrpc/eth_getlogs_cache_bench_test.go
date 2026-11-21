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

package jsonrpc

import (
	"encoding/binary"
	"math/big"
	"testing"

	"github.com/holiman/uint256"
	"github.com/stretchr/testify/require"

	"github.com/erigontech/erigon/common"
	"github.com/erigontech/erigon/execution/chain"
	"github.com/erigontech/erigon/execution/execmodule/execmoduletester"
	"github.com/erigontech/erigon/execution/tests/blockgen"
	"github.com/erigontech/erigon/execution/types"
	"github.com/erigontech/erigon/rpc/filters"
	"github.com/erigontech/erigon/rpc/jsonstream"
)

const benchLogsPerBlock = 40

// newGetLogsBenchAPI builds two blocks with benchLogsPerBlock logs each, so block 2 is the head.
func newGetLogsBenchAPI(b *testing.B) (*APIImpl, *execmoduletester.ExecModuleTester) {
	m := execmoduletester.New(b,
		execmoduletester.WithGenesisSpec(&types.Genesis{
			Config: chain.TestChainBerlinConfig,
			Alloc:  types.GenesisAlloc{testAddr: {Balance: big.NewInt(1_000_000_000_000)}},
		}),
		execmoduletester.WithKey(testKey),
	)
	signer := types.LatestSignerForChainID(nil)
	c, err := m.GenerateChain(2, func(_ int, block *blockgen.BlockGen) {
		for k := range benchLogsPerBlock {
			topic := common.Hash{byte(k)}
			logOnCreate := append(append([]byte{0x7f}, topic[:]...), 0x60, 0x00, 0x60, 0x00, 0xa1, 0x00) // PUSH32 topic PUSH1 0 PUSH1 0 LOG1 STOP
			txn, err := types.SignTx(types.NewContractCreation(block.TxNonce(testAddr), uint256.NewInt(0), 100_000, uint256.NewInt(1), logOnCreate), *signer, testKey)
			require.NoError(b, err)
			block.AddTx(txn)
		}
	})
	require.NoError(b, err)
	require.NoError(b, m.InsertChain(c))
	return newEthApiForTest(newBaseApiForTest(m), m.DB, nil, nil), m
}

func benchGetLogs(b *testing.B, api *APIImpl, m *execmoduletester.ExecModuleTester, crit func(i uint64) filters.FilterCriteria) {
	b.ReportAllocs()
	var i uint64
	for b.Loop() {
		res, err := api.GetLogs(m.Ctx, crit(i))
		if err != nil {
			b.Fatal(err)
		}
		if _, err := jsonstream.Marshal(res); err != nil {
			b.Fatal(err)
		}
		i++
	}
}

func singleBlock(n int64) filters.FilterCriteria {
	return filters.FilterCriteria{FromBlock: big.NewInt(n), ToBlock: big.NewInt(n)}
}

// BenchmarkGetLogsHeadSameFilter repeats one query on the head block.
func BenchmarkGetLogsHeadSameFilter(b *testing.B) {
	api, m := newGetLogsBenchAPI(b)
	benchGetLogs(b, api, m, func(uint64) filters.FilterCriteria { return singleBlock(2) })
}

// BenchmarkGetLogsHeadDistinctFilters queries the head block with a new filter every time.
func BenchmarkGetLogsHeadDistinctFilters(b *testing.B) {
	api, m := newGetLogsBenchAPI(b)
	benchGetLogs(b, api, m, func(i uint64) filters.FilterCriteria {
		crit := singleBlock(2)
		var topic common.Hash
		binary.BigEndian.PutUint64(topic[24:], i)
		crit.Topics = [][]common.Hash{{common.Hash{0x01}, topic}}
		return crit
	})
}

// BenchmarkGetLogsBelowHead repeats one query on the block below the head.
func BenchmarkGetLogsBelowHead(b *testing.B) {
	api, m := newGetLogsBenchAPI(b)
	benchGetLogs(b, api, m, func(uint64) filters.FilterCriteria { return singleBlock(1) })
}
