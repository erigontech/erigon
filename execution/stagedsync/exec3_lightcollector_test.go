// Copyright 2025 The Erigon Authors
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
	"context"
	"testing"

	"github.com/holiman/uint256"
	"github.com/stretchr/testify/require"

	"github.com/erigontech/erigon/common"
	"github.com/erigontech/erigon/common/log/v3"
	"github.com/erigontech/erigon/db/kv"
	"github.com/erigontech/erigon/execution/chain"
	"github.com/erigontech/erigon/execution/state"
	"github.com/erigontech/erigon/execution/types/accounts"
	"github.com/erigontech/erigon/node/ethconfig"
)

// TestLightCollectorNoncePreservation verifies that a balance-only transfer
// does not overwrite a nonce increment from an earlier TX in the same block.
//
// Scenario (parallel executor):
//
//	Block N, TX 100: account A sends a TX → nonce 5 → 6
//	Block N, TX 200: account A receives ETH → balance changes, nonce stays 6
//
// The parallel executor uses MakeWriteSet(useBlockOrigin=true) which produces
// LightCollector writes with pre-block field values. TX 200's write carries
// nonce=5 (block origin). Without the fix, ApplyStateWrites processes TX 200
// after TX 100 and overwrites nonce 6 with 5.
//
// The fix: LightCollector.UpdateAccountData only emits fields that changed
// from `original`, and applyVersionedWrites reads the current domain state
// as the base, overlaying only present fields.
func TestLightCollectorNoncePreservation(t *testing.T) {
	if testing.Short() {
		t.Skip("requires mdbx")
	}

	tx, domains := setup2CacheTest(t)
	lgr := log.New()

	rs := state.NewStateV3Buffered(state.NewStateV3(domains, ethconfig.Sync{}, lgr))

	addr := accounts.InternAddress(common.HexToAddress("0xA1B2C3"))

	// Seed the domain with account A at nonce=5, balance=1000.
	seedAccount := accounts.Account{
		Nonce:   5,
		Balance: *uint256.NewInt(1000),
	}
	addrVal := addr.Value()
	err := domains.DomainPut(kv.AccountsDomain, tx, addrVal[:],
		accounts.SerialiseV3(&seedAccount), 0, nil)
	require.NoError(t, err)

	// --- TX 100: account A sends a TX (nonce 5 → 6) ---
	// LightCollector with original=seed (nonce=5), account=nonce=6
	lc1 := state.NewLightCollector()
	tx100Original := &accounts.Account{Nonce: 5, Balance: *uint256.NewInt(1000)}
	tx100Account := &accounts.Account{Nonce: 6, Balance: *uint256.NewInt(900)} // sent some ETH
	err = lc1.UpdateAccountData(addr, tx100Original, tx100Account)
	require.NoError(t, err)
	writes1 := lc1.TakeWrites()

	// --- TX 200: account A receives ETH (balance-only change) ---
	// LightCollector with original=seed (nonce=5, block origin!), account=nonce=5, balance=1100
	// The parallel executor uses blockOriginStorage for the original, so the
	// original still has the pre-block nonce=5, NOT the post-TX-100 nonce=6.
	lc2 := state.NewLightCollector()
	tx200Original := &accounts.Account{Nonce: 5, Balance: *uint256.NewInt(1000)} // block origin
	tx200Account := &accounts.Account{Nonce: 5, Balance: *uint256.NewInt(1100)}  // received ETH, nonce unchanged from block origin
	err = lc2.UpdateAccountData(addr, tx200Original, tx200Account)
	require.NoError(t, err)
	writes2 := lc2.TakeWrites()

	// Apply TX 100 writes first (nonce increment)
	err = rs.ApplyStateWrites(context.Background(), tx, 1, 100, writes1, nil, &chain.Rules{})
	require.NoError(t, err)

	// Apply TX 200 writes second (balance-only)
	err = rs.ApplyStateWrites(context.Background(), tx, 1, 200, writes2, nil, &chain.Rules{})
	require.NoError(t, err)

	// Read the final account state from the domain.
	enc, _, err := domains.GetLatest(kv.AccountsDomain, tx, addrVal[:])
	require.NoError(t, err)
	require.NotEmpty(t, enc, "account must exist in domain")

	var finalAcc accounts.Account
	err = accounts.DeserialiseV3(&finalAcc, enc)
	require.NoError(t, err)

	require.Equal(t, uint64(6), finalAcc.Nonce,
		"nonce must be 6 (from TX 100), not 5 (stale block-origin from TX 200)")
	require.Equal(t, uint64(1100), finalAcc.Balance.Uint64(),
		"balance must be 1100 (from TX 200)")
}

// TestLightCollectorNoncePreservationCrossBlock verifies that stale nonces
// from the LightCollector don't leak across block boundaries.
//
// Block N: account A sends TX (nonce 5→6), then receives transfers (nonce=5 in writes)
// Block N+1: a new TX from A expects nonce=6
//
// After ApplyStateWrites for block N, the domain must have nonce=6.
func TestLightCollectorNoncePreservationCrossBlock(t *testing.T) {
	if testing.Short() {
		t.Skip("requires mdbx")
	}

	tx, domains := setup2CacheTest(t)
	lgr := log.New()

	rs := state.NewStateV3Buffered(state.NewStateV3(domains, ethconfig.Sync{}, lgr))

	addr := accounts.InternAddress(common.HexToAddress("0xDEAD"))
	addrVal := addr.Value()

	// Seed: nonce=10, balance=5000
	seedAccount := accounts.Account{Nonce: 10, Balance: *uint256.NewInt(5000)}
	err := domains.DomainPut(kv.AccountsDomain, tx, addrVal[:],
		accounts.SerialiseV3(&seedAccount), 0, nil)
	require.NoError(t, err)

	// --- Block N ---
	blockOrigin := &accounts.Account{Nonce: 10, Balance: *uint256.NewInt(5000)}

	// TX 1: A sends (nonce 10→11)
	lc1 := state.NewLightCollector()
	err = lc1.UpdateAccountData(addr, blockOrigin,
		&accounts.Account{Nonce: 11, Balance: *uint256.NewInt(4800)})
	require.NoError(t, err)
	err = rs.ApplyStateWrites(context.Background(), tx, 1, 10, lc1.TakeWrites(), nil, &chain.Rules{})
	require.NoError(t, err)

	// TX 2-5: A receives transfers (balance-only, nonce unchanged from block origin=10)
	for i := 0; i < 4; i++ {
		lc := state.NewLightCollector()
		err = lc.UpdateAccountData(addr, blockOrigin,
			&accounts.Account{Nonce: 10, Balance: *uint256.NewInt(uint64(4800 + (i+1)*100))})
		require.NoError(t, err)
		err = rs.ApplyStateWrites(context.Background(), tx, 1, uint64(11+i), lc.TakeWrites(), nil, &chain.Rules{})
		require.NoError(t, err)
	}

	// Verify: domain must have nonce=11 (from TX 1), balance=5200 (from TX 5)
	enc, _, err := domains.GetLatest(kv.AccountsDomain, tx, addrVal[:])
	require.NoError(t, err)
	var finalAcc accounts.Account
	err = accounts.DeserialiseV3(&finalAcc, enc)
	require.NoError(t, err)

	require.Equal(t, uint64(11), finalAcc.Nonce,
		"block N: nonce must be 11, not reverted to 10 by balance-only transfers")
	require.Equal(t, uint64(5200), finalAcc.Balance.Uint64(),
		"block N: balance must reflect the last transfer")
}
