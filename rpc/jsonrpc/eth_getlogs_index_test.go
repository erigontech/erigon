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
	"fmt"
	"math/big"
	"slices"
	"testing"

	"github.com/holiman/uint256"
	"github.com/stretchr/testify/require"

	"github.com/erigontech/erigon/common"
	"github.com/erigontech/erigon/common/crypto"
	"github.com/erigontech/erigon/execution/chain"
	"github.com/erigontech/erigon/execution/execmodule/execmoduletester"
	"github.com/erigontech/erigon/execution/tests/blockgen"
	"github.com/erigontech/erigon/execution/types"
	"github.com/erigontech/erigon/rpc/filters"
)

// TestGetLogsIndexFiltersMatchFullScan checks that eth_getLogs and erigon_getLatestLogs, which
// select txs through the address and topic indexes, return the logs that a filter over all logs
// of the range returns.
func TestGetLogsIndexFiltersMatchFullScan(t *testing.T) {
	t.Parallel()
	key, err := crypto.GenerateKey()
	require.NoError(t, err)
	sender := crypto.PubkeyToAddress(key.PublicKey)

	// LOG2 with topic0 = calldata[0:32] and topic1 = calldata[32:64], no data.
	emitterCode := common.FromHex("0x60203560003560006000a200")
	emitters := []common.Address{{0xe1}, {0xe2}, {0xe3}}
	alloc := types.GenesisAlloc{sender: {Balance: new(big.Int).Exp(big.NewInt(10), big.NewInt(20), nil)}}
	for _, e := range emitters {
		alloc[e] = types.GenesisAccount{Code: emitterCode, Balance: new(big.Int)}
	}
	m := execmoduletester.New(t, execmoduletester.WithGenesisSpec(&types.Genesis{Config: chain.AllProtocolChanges, Alloc: alloc}), execmoduletester.WithKey(key))

	topics := []common.Hash{{0x0a}, {0x0b}, {0x0c}}
	signer := types.LatestSignerForChainID(m.ChainConfig.ChainID)
	gasPrice := uint256.NewInt(100 * common.GWei)
	nonce := uint64(0)
	pack, err := m.GenerateChain(20, func(i int, b *blockgen.BlockGen) {
		if i%5 == 4 {
			return
		}
		for j := range 3 {
			t0, t1 := topics[i%3], topics[(2*i+j)%3]
			data := slices.Concat(t0[:], t1[:])
			txn, err := types.SignTx(types.NewTransaction(nonce, emitters[(i+j)%3], new(uint256.Int), 100_000, gasPrice, data), *signer, key)
			require.NoError(t, err)
			b.AddTx(txn)
			nonce++
		}
	})
	require.NoError(t, err)
	require.NoError(t, m.InsertChain(pack))

	base := newBaseApiForTest(m)
	ethAPI := newEthApiForTest(base, m.DB, nil, nil)
	erigonAPI := NewErigonAPI(base, m.DB, nil)

	missingAddr, missingTopic := common.Address{0xff}, common.Hash{0xff}
	addrSets := [][]common.Address{
		nil,
		{emitters[0]},
		{emitters[0], emitters[1]},
		{emitters[1], missingAddr},
		{missingAddr},
		emitters,
	}
	topicSets := [][][]common.Hash{
		nil,
		{{topics[0]}},
		{{topics[0], topics[1]}},
		{{}, {topics[2]}},
		{{topics[0]}, {topics[2]}},
		{{topics[0], topics[1]}, {topics[1], topics[2]}},
		{{topics[2]}, {topics[2]}},
		{{missingTopic}},
		{{}, {topics[0], missingTopic}},
		{{topics[0]}, {}, {topics[1]}},
	}
	ranges := [][2]int64{{1, 20}, {6, 13}}

	for _, r := range ranges {
		all, err := ethAPI.GetLogs(m.Ctx, filters.FilterCriteria{FromBlock: big.NewInt(r[0]), ToBlock: big.NewInt(r[1])})
		require.NoError(t, err)
		require.NotEmpty(t, all)
		for ai, addrs := range addrSets {
			for ti, tops := range topicSets {
				t.Run(fmt.Sprintf("blocks %d-%d/addrs %d/topics %d", r[0], r[1], ai, ti), func(t *testing.T) {
					crit := filters.FilterCriteria{FromBlock: big.NewInt(r[0]), ToBlock: big.NewInt(r[1]), Addresses: addrs, Topics: tops}
					var want types.Logs
					for _, l := range all {
						if logMatches(l, addrs, tops) {
							want = append(want, l)
						}
					}

					got, err := ethAPI.GetLogs(m.Ctx, crit)
					require.NoError(t, err)
					require.Equal(t, logKeys(want, true), logKeys(got, true))

					latest, err := erigonAPI.GetLatestLogs(m.Ctx, crit, filters.LogFilterOptions{LogCount: 1000})
					require.NoError(t, err)
					wantLatest := logKeys(want, false)
					slices.Reverse(wantLatest)
					require.Equal(t, wantLatest, logKeys(latest, false))
				})
			}
		}
	}
}

func logMatches(l *types.Log, addrs []common.Address, topics [][]common.Hash) bool {
	if len(addrs) > 0 && !slices.Contains(addrs, l.Address) {
		return false
	}
	for i, alternatives := range topics {
		if len(alternatives) == 0 {
			continue
		}
		if i >= len(l.Topics) || !slices.Contains(alternatives, l.Topics[i]) {
			return false
		}
	}
	return true
}

// logKeys identifies each log by block, tx, address and topics; erigon_getLatestLogs numbers
// logs per tx, so withIndex leaves the log index out for it.
func logKeys(logs types.Logs, withIndex bool) []string {
	keys := make([]string, 0, len(logs))
	for _, l := range logs {
		k := fmt.Sprintf("%d/%d/%x/%x", l.BlockNumber, l.TxIndex, l.Address, l.Topics)
		if withIndex {
			k += fmt.Sprintf("/%d", l.Index)
		}
		keys = append(keys, k)
	}
	return keys
}
