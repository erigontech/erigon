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

package stagedsync

import (
	"testing"

	"github.com/holiman/uint256"
	"github.com/stretchr/testify/require"

	"github.com/erigontech/erigon/common"
	"github.com/erigontech/erigon/execution/state"
	"github.com/erigontech/erigon/execution/types/accounts"
)

func balBuilderTestIO(txs int) *state.VersionedIO {
	io := state.NewVersionedIO(txs)
	coinbase := accounts.InternAddress(common.Address{19: 0xcb})
	for tx := -1; tx < txs; tx++ {
		addr := accounts.InternAddress(common.Address{0: 1, 18: byte(tx >> 8), 19: byte(tx)})
		v := state.Version{TxIndex: tx}
		ws := &state.WriteSet{}
		ws.SetBalance(addr, &state.VersionedWrite[uint256.Int]{WriteHeader: state.WriteHeader{Address: addr, Path: state.BalancePath, Version: v}, Val: *uint256.NewInt(uint64(tx + 2))})
		ws.SetBalance(coinbase, &state.VersionedWrite[uint256.Int]{WriteHeader: state.WriteHeader{Address: coinbase, Path: state.BalancePath, Version: v}, Val: *uint256.NewInt(uint64(1000 + tx))})
		io.RecordWrites(v, ws)
	}
	return io
}

func TestAsyncBALBuilderMatchesSyncBuild(t *testing.T) {
	const txs = 300
	io := balBuilderTestIO(txs)
	want := state.NewBlockAccessListBuilder()
	want.AddRemaining(io)
	wantHash := want.Build().Hash()

	for _, stopAt := range []int{0, 1, 7, txs / 2, txs + 1} {
		a := startBALBuilder(io)
		for sealed := 1; sealed <= stopAt; sealed++ {
			a.seal(sealed)
		}
		a.close()
		b := a.builder()
		require.LessOrEqual(t, b.Next(), max(stopAt-1, -1))
		b.AddRemaining(io)
		require.Equal(t, wantHash, b.Build().Hash(), "sealed up to %d", stopAt)
	}
}

func TestAsyncBALBuilderNil(t *testing.T) {
	var a *asyncBALBuilder
	a.close()
	require.Nil(t, a.builder())
}
