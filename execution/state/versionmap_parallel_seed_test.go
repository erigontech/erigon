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

package state

import (
	"testing"

	"github.com/holiman/uint256"
	"github.com/stretchr/testify/require"

	"github.com/erigontech/erigon/common"
	"github.com/erigontech/erigon/execution/types"
	"github.com/erigontech/erigon/execution/types/accounts"
)

func TestNewVersionMapParallelSeedMatchesSerial(t *testing.T) {
	const n = 4 * balChunkAccounts
	bal := make(types.BlockAccessList, n)
	for i := range bal {
		bal[i] = types.AccountChanges{
			Address:        common.Address{18: byte(i >> 8), 19: byte(i)},
			BalanceChanges: []*types.BalanceChange{{Index: uint32(i%7 + 1), Value: *uint256.NewInt(uint64(i + 1))}},
			NonceChanges:   []*types.NonceChange{{Index: uint32(i%5 + 1), Value: uint64(i)}},
			StorageChanges: []types.SlotChanges{{Slot: accounts.InternKey(common.Hash{31: byte(i)}), Changes: []*types.StorageChange{{Index: uint32(i%3 + 1), Value: *uint256.NewInt(uint64(i * 3))}}}},
		}
	}
	parallel := NewVersionMap(bal)
	parallel.SeedAddresses()
	serial := &VersionMap{HasBAL: true, bal: bal}
	serial.WriteChanges(bal)
	serial.seedAddresses(0, len(bal))

	for i := range bal {
		addr := accounts.InternAddress(bal[i].Address)
		for tx := 0; tx < 9; tx++ {
			pb, pr, pok := parallel.ReadBalance(addr, tx)
			sb, sr, sok := serial.ReadBalance(addr, tx)
			require.Equal(t, sb, pb)
			require.Equal(t, sr.Status(), pr.Status())
			require.Equal(t, sok, pok)
			pn, _, _ := parallel.ReadNonce(addr, tx)
			sn, _, _ := serial.ReadNonce(addr, tx)
			require.Equal(t, sn, pn)
			pa, _, _ := parallel.ReadAddress(addr, tx)
			sa, _, _ := serial.ReadAddress(addr, tx)
			require.Equal(t, sa, pa)
			pv, _, _, _ := parallel.ReadStorageLife(addr, bal[i].StorageChanges[0].Slot, tx)
			sv, _, _, _ := serial.ReadStorageLife(addr, bal[i].StorageChanges[0].Slot, tx)
			require.Equal(t, sv, pv)
		}
	}
}
