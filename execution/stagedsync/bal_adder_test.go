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

	"github.com/stretchr/testify/require"

	"github.com/erigontech/erigon/common"
	"github.com/erigontech/erigon/execution/state"
	"github.com/erigontech/erigon/execution/types/accounts"
)

func TestBalAdderMatchesInlineBuild(t *testing.T) {
	const txs = 50
	io := state.NewVersionedIO(txs)
	for i := -1; i < txs; i++ {
		addr := accounts.InternAddress(common.Address{18: byte(i >> 8), 19: byte(i)})
		ws := &state.WriteSet{}
		ws.SetAddress(addr, &state.VersionedWrite[*accounts.Account]{
			WriteHeader: state.WriteHeader{Address: addr, Path: state.AddressPath},
			Val:         &accounts.Account{Nonce: uint64(i + 2)},
		})
		io.RecordWrites(state.Version{TxIndex: i}, ws)
	}

	want := state.NewBlockAccessListBuilder()
	want.AddRemaining(io)

	a := newBalAdder()
	for _, sealed := range []int{1, 7, 7, 30, txs + 1} {
		a.add(io, sealed)
	}
	got := a.finish()
	require.Equal(t, txs, got.Next())
	got.AddRemaining(io)
	require.Equal(t, want.Build(), got.Build())
}

func TestBalAdderFinishIsNilSafe(t *testing.T) {
	var a *balAdder
	require.Nil(t, a.finish())
}
