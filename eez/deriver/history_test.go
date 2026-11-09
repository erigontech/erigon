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

package deriver

import (
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/erigontech/erigon/common"
	"github.com/erigontech/erigon/eez/config"
	"github.com/erigontech/erigon/eez/l1"
	"github.com/erigontech/erigon/eez/networks"
	"github.com/erigontech/erigon/eez/protocol"
)

// testBridge is L1 blocks 100 to 110, 5 s apart from time 1000, with
// mixHash i for block 100+i.
func testBridge() *bridge {
	b := &bridge{first: 100}
	for i := range uint64(11) {
		b.headers = append(b.headers, &l1.Header{Number: 100 + i, Hash: common.Hash{1, byte(i)}, Time: 1000 + 5*i, MixDigest: common.Hash{2, byte(i)}})
	}
	return b
}

func TestHistoryCutoff(t *testing.T) {
	x := *networks.Chiado699169.Config.L2.(*config.L2Config).RollupX
	s := newHistorySchedule(&x, 1000)
	for t2, want := range map[uint64]uint64{
		1001: 995, 1002: 995, 1003: 1000, 1007: 1000, 1008: 1005, 1012: 1005, 1013: 1010,
	} {
		require.Equal(t, want, s.cutoff(t2), "time %d", t2)
	}
}

func TestHistoryTransition(t *testing.T) {
	x := *networks.Chiado699169.Config.L2.(*config.L2Config).RollupX
	// Genesis at 1020 with L1 block 100, at 1000, as its reference: block 3
	// (time 1023) updates with cutoff 1020, block 8 with cutoff 1025.
	s := newHistorySchedule(&x, 1020)
	b := testBridge()
	start := historyState{endpoint: 100, hash: b.headers[0].Hash}
	update := func(numbers ...uint64) *protocol.HistoryTx {
		u := &protocol.HistoryTx{ChainID: 699169}
		for _, n := range numbers {
			u.Blocks = append(u.Blocks, protocol.HistoryBlock{Number: n, Hash: b.headers[n-100].Hash})
		}
		return u
	}

	origin, err := s.transition(1021, start, b, nil)
	require.NoError(t, err)
	require.Equal(t, b.headers[0], origin, "before the first update the reference is the origin")

	origin, err = s.transition(1023, start, b, update(100, 101, 102, 103, 104))
	require.NoError(t, err)
	require.Equal(t, b.headers[4], origin, "the last import, at the cutoff")
	after := start.apply(origin, update(104))
	require.Equal(t, historyState{imported: true, endpoint: 104, hash: b.headers[4].Hash}, after)

	origin, err = s.transition(1028, after, b, update(105))
	require.NoError(t, err)
	require.Equal(t, uint64(105), origin.Number)

	for name, tc := range map[string]struct {
		t      uint64
		state  historyState
		update *protocol.HistoryTx
	}{
		"off its block":              {1024, start, update(101)},
		"after the cutoff":           {1023, start, update(105)},
		"beyond the bridge":          {1028, after, &protocol.HistoryTx{Blocks: []protocol.HistoryBlock{{Number: 111, Hash: common.Hash{1, 11}}}}},
		"before the endpoint":        {1028, after, update(103)},
		"the endpoint again":         {1028, after, update(104)},
		"another hash":               {1023, start, &protocol.HistoryTx{Blocks: []protocol.HistoryBlock{{Number: 101, Hash: common.Hash{9}}}}},
		"endpoint off L1":            {1021, historyState{endpoint: 100, hash: common.Hash{9}}, nil},
		"genesis block":              {1020, start, nil},
		"endpoint before the bridge": {1021, historyState{endpoint: 99}, nil},
	} {
		_, err := s.transition(tc.t, tc.state, b, tc.update)
		require.ErrorIs(t, err, ErrHistory, name)
	}
}

func TestHistoryHashSlot(t *testing.T) {
	// keccak256(abi.encode(uint64(1), uint256(0))), the mapping slot of
	// imported block 1.
	require.Equal(t, common.HexToHash("0xada5013122d395ba3c54772283fb069b10426056ef8ca54750cb9bb552a59e7d"), historyHashSlot(1))
}
