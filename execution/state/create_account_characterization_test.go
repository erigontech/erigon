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

	"github.com/erigontech/erigon/common/crypto"
	"github.com/erigontech/erigon/execution/tracing"
	"github.com/erigontech/erigon/execution/types/accounts"
)

// Characterization of CreateAccount's EVM-observable semantics on the versioned path,
// pinned before the stateObject crutch is removed: the version map must carry enough
// lifecycle information (existence, carried balance, incarnation, self-destruct clear)
// to reproduce these outcomes without a transient stateObject. If a version-map-only
// rewrite breaks one of these, the map lacked that hint and the test says which.

func requireAccount(t *testing.T, ibs *IntraBlockState, addr accounts.Address, balance uint64, nonce uint64, codeHash accounts.CodeHash) {
	t.Helper()
	exist, err := ibs.Exist(addr)
	require.NoError(t, err)
	require.True(t, exist, "account exists")
	bal, err := ibs.GetBalance(addr)
	require.NoError(t, err)
	require.Equal(t, *uint256.NewInt(balance), bal, "balance")
	n, err := ibs.GetNonce(addr)
	require.NoError(t, err)
	require.Equal(t, nonce, n, "nonce")
	ch, err := ibs.GetCodeHash(addr)
	require.NoError(t, err)
	require.Equal(t, codeHash, ch, "code hash")
}

func TestCreateAccount_FreshEOA(t *testing.T) {
	t.Parallel()
	addr := accounts.InternAddress([20]byte{0xca, 0x01})
	ibs := versionedRevertIBS(&emptyReader{})

	snap := ibs.PushSnapshot()
	require.NoError(t, ibs.CreateAccount(addr, false))
	requireAccount(t, ibs, addr, 0, 0, accounts.EmptyCodeHash)
	inc, err := ibs.GetIncarnation(addr)
	require.NoError(t, err)
	require.Equal(t, uint64(0), inc, "a fresh non-contract create has incarnation 0")

	ibs.RevertToSnapshot(snap, nil)
	exist, err := ibs.Exist(addr)
	require.NoError(t, err)
	require.False(t, exist, "revert drops the fresh account")
}

func TestCreateAccount_FreshContractBumpsIncarnation(t *testing.T) {
	t.Parallel()
	addr := accounts.InternAddress([20]byte{0xca, 0x02})
	ibs := versionedRevertIBS(&emptyReader{})

	require.NoError(t, ibs.CreateAccount(addr, true))
	inc, err := ibs.GetIncarnation(addr)
	require.NoError(t, err)
	require.Equal(t, uint64(1), inc, "a fresh contract create bumps incarnation to 1")
	destructed, err := ibs.HasSelfdestructed(addr)
	require.NoError(t, err)
	require.False(t, destructed)
}

func TestCreateAccount_OverExistingEOACarriesBalance(t *testing.T) {
	t.Parallel()
	addr := accounts.InternAddress([20]byte{0xca, 0x03})
	ibs := versionedRevertIBS(committedAccountReader(addr, 7, 1000, nil))

	snap := ibs.PushSnapshot()
	require.NoError(t, ibs.CreateAccount(addr, true))
	// The balance carries over the create; nonce and code reset.
	requireAccount(t, ibs, addr, 1000, 0, accounts.EmptyCodeHash)

	ibs.RevertToSnapshot(snap, nil)
	requireAccount(t, ibs, addr, 1000, 7, accounts.EmptyCodeHash)
}

func TestCreateAccount_OverExistingContractResetsAndCarriesBalance(t *testing.T) {
	t.Parallel()
	addr := accounts.InternAddress([20]byte{0xca, 0x04})
	code := []byte{0x60, 0x01}
	acc := accounts.NewAccount()
	acc.Nonce, acc.Balance, acc.Incarnation = 3, *uint256.NewInt(500), 2
	acc.CodeHash = accounts.InternCodeHash(crypto.Keccak256Hash(code))
	ibs := versionedRevertIBS(&fieldReader{addr: addr, account: &acc, code: code})

	snap := ibs.PushSnapshot()
	require.NoError(t, ibs.CreateAccount(addr, true))
	// The balance carries; nonce and code reset to the fresh account's.
	requireAccount(t, ibs, addr, 500, 0, accounts.EmptyCodeHash)

	ibs.RevertToSnapshot(snap, nil)
	ch, err := ibs.GetCodeHash(addr)
	require.NoError(t, err)
	require.Equal(t, acc.CodeHash, ch, "revert restores the prior contract's code hash")
}

func TestCreateAccount_CarriesSameTxCreditToAbsentAddress(t *testing.T) {
	t.Parallel()
	addr := accounts.InternAddress([20]byte{0xca, 0x05})
	ibs := versionedRevertIBS(&emptyReader{})

	// The EVM credits funds to a not-yet-created address, then CREATE deploys there.
	require.NoError(t, ibs.AddBalance(addr, *uint256.NewInt(250), tracing.BalanceChangeUnspecified))
	require.NoError(t, ibs.CreateAccount(addr, true))
	requireAccount(t, ibs, addr, 250, 0, accounts.EmptyCodeHash)
}
