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

package t8ntool

import (
	"math/big"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/erigontech/erigon/common"
	"github.com/erigontech/erigon/db/datadir"
	"github.com/erigontech/erigon/db/kv/temporal/temporaltest"
	"github.com/erigontech/erigon/execution/chain"
	"github.com/erigontech/erigon/execution/state"
	"github.com/erigontech/erigon/execution/types"
	"github.com/erigontech/erigon/execution/types/accounts"
)

// The pre-state keeps every declared account, including an empty one EIP-161 would remove.
func TestMakePreStateWritesAlloc(t *testing.T) {
	db := temporaltest.NewTestDB(t, datadir.New(t.TempDir()))
	tx, err := db.BeginTemporalRw(t.Context())
	require.NoError(t, err)
	defer tx.Rollback()
	sd, err := newT8nSharedDomains(t.Context(), db, tx)
	require.NoError(t, err)
	defer sd.Close()

	eoa, empty, contract := common.HexToAddress("0x01"), common.HexToAddress("0x02"), common.HexToAddress("0x03")
	slot, slotVal := common.HexToHash("0x05"), common.HexToHash("0x07")
	alloc := types.GenesisAlloc{
		eoa:      {Balance: big.NewInt(100), Nonce: 3},
		empty:    {Balance: big.NewInt(0)},
		contract: {Balance: big.NewInt(1), Code: []byte{0x60, 0x00}, Storage: map[common.Hash]common.Hash{slot: slotVal}},
	}
	reader, _ := MakePreState(&chain.Rules{IsSpuriousDragon: true}, tx, sd, alloc, 0)
	// A fresh IBS proves the alloc reached the domains, not only the IBS that wrote it.
	ibs := state.NewWithVersionMap(reader, state.NewVersionMap(nil))

	eoaAddr := accounts.InternAddress(eoa)
	bal, err := ibs.GetBalance(eoaAddr)
	require.NoError(t, err)
	require.Equal(t, uint64(100), bal.Uint64())
	nonce, err := ibs.GetNonce(eoaAddr)
	require.NoError(t, err)
	require.Equal(t, uint64(3), nonce)

	exists, err := ibs.Exist(accounts.InternAddress(empty))
	require.NoError(t, err)
	require.True(t, exists)

	contractAddr := accounts.InternAddress(contract)
	code, err := ibs.GetCode(contractAddr)
	require.NoError(t, err)
	require.Equal(t, []byte{0x60, 0x00}, code)
	val, err := ibs.GetState(contractAddr, accounts.InternKey(slot))
	require.NoError(t, err)
	require.Equal(t, slotVal, common.Hash(val.Bytes32()))
	inc, err := ibs.GetIncarnation(contractAddr)
	require.NoError(t, err)
	require.Equal(t, uint64(state.FirstContractIncarnation), inc)
}
