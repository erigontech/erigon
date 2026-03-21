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
	err = rs.ApplyStateWrites(context.Background(), tx, 1, 100, writes1, nil, &chain.Rules{}, nil)
	require.NoError(t, err)

	// Apply TX 200 writes second (balance-only)
	err = rs.ApplyStateWrites(context.Background(), tx, 1, 200, writes2, nil, &chain.Rules{}, nil)
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
	err = rs.ApplyStateWrites(context.Background(), tx, 1, 10, lc1.TakeWrites(), nil, &chain.Rules{}, nil)
	require.NoError(t, err)

	// TX 2-5: A receives transfers (balance-only, nonce unchanged from block origin=10)
	for i := 0; i < 4; i++ {
		lc := state.NewLightCollector()
		err = lc.UpdateAccountData(addr, blockOrigin,
			&accounts.Account{Nonce: 10, Balance: *uint256.NewInt(uint64(4800 + (i+1)*100))})
		require.NoError(t, err)
		err = rs.ApplyStateWrites(context.Background(), tx, 1, uint64(11+i), lc.TakeWrites(), nil, &chain.Rules{}, nil)
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

// TestLightCollectorNewAccountCodeHash verifies that a balance-only write to
// an account that doesn't exist in the domain produces a correctly-serialized
// account with EmptyCodeHash (not zero CodeHash).
//
// applyVersionedWrites reads the domain base before overlaying fields from the
// write. For a new account, the domain returns empty bytes. The base must use
// accounts.NewAccount() (which sets CodeHash=EmptyCodeHash), not a zero-value
// Account (which has CodeHash=0x000...). Otherwise SerialiseV3 encodes 32
// zero bytes for CodeHash instead of omitting it, producing a different blob
// that causes receiptHash mismatches downstream.
func TestLightCollectorNewAccountCodeHash(t *testing.T) {
	if testing.Short() {
		t.Skip("requires mdbx")
	}

	tx, domains := setup2CacheTest(t)
	lgr := log.New()

	rs := state.NewStateV3Buffered(state.NewStateV3(domains, ethconfig.Sync{}, lgr))

	addr := accounts.InternAddress(common.HexToAddress("0xNEW1"))
	addrVal := addr.Value()

	// Do NOT seed the domain — addr is a brand-new account.

	// A transfer creates the account with balance only.
	// LightCollector emits balance (changed), nonce (unchanged from zero original),
	// incarnation (unchanged), codeHash (unchanged).
	// With partial writes: only balance is emitted.
	lc := state.NewLightCollector()
	original := &accounts.Account{} // zero — account didn't exist
	account := &accounts.Account{Balance: *uint256.NewInt(1000)}
	err := lc.UpdateAccountData(addr, original, account)
	require.NoError(t, err)

	err = rs.ApplyStateWrites(context.Background(), tx, 1, 1, lc.TakeWrites(), nil, &chain.Rules{}, nil)
	require.NoError(t, err)

	// Read back and verify CodeHash is EmptyCodeHash, not zero.
	enc, _, err := domains.GetLatest(kv.AccountsDomain, tx, addrVal[:])
	require.NoError(t, err)
	require.NotEmpty(t, enc)

	var finalAcc accounts.Account
	err = accounts.DeserialiseV3(&finalAcc, enc)
	require.NoError(t, err)

	require.Equal(t, uint64(1000), finalAcc.Balance.Uint64())
	require.Equal(t, uint64(0), finalAcc.Nonce)
	require.True(t, finalAcc.IsEmptyCodeHash(),
		"new account must have EmptyCodeHash, not zero CodeHash")

	// Verify round-trip: SerialiseV3(DeserialiseV3(enc)) == enc
	reEncoded := accounts.SerialiseV3(&finalAcc)
	require.Equal(t, enc, reEncoded,
		"account serialization must be idempotent (EmptyCodeHash serializes as 0-byte, not 32 zero bytes)")
}

// TestLightCollectorStorageReentrancyGuard verifies that the storage skip
// removal in LightCollector.WriteAccountStorage correctly handles the
// reentrancy guard pattern: TX A writes slot S=X, TX B SSTOREs S=V (same
// as block origin — the reentrancy guard reset). The domain should have X
// after applying both TXs (TX B's SSTORE(S,V) should be a DomainPut no-op
// since prevVal=X ≠ V means it WOULD write, but TX B's dirtyStorage has V
// which matches block origin → skip in old code. With skip removed, DomainPut
// writes V, reverting TX A's change).
//
// Actually: if TX B explicitly SSTOREs V, the correct final value IS V.
// The test verifies both the "unchanged" case (where DomainPut should skip)
// and the "revert" case (where DomainPut should write).
func TestLightCollectorStorageReentrancyGuard(t *testing.T) {
	if testing.Short() {
		t.Skip("requires mdbx")
	}

	tx, domains := setup2CacheTest(t)
	lgr := log.New()

	rs := state.NewStateV3Buffered(state.NewStateV3(domains, ethconfig.Sync{}, lgr))

	contract := accounts.InternAddress(common.HexToAddress("0xC0NTRACT"))
	contractVal := contract.Value()
	slotKey := accounts.InternKey(common.HexToHash("0xcb")) // reentrancy guard slot

	// Seed: contract exists with storage slot cb = 1
	seedAccount := accounts.Account{Nonce: 1, Balance: *uint256.NewInt(0)}
	err := domains.DomainPut(kv.AccountsDomain, tx, contractVal[:],
		accounts.SerialiseV3(&seedAccount), 0, nil)
	require.NoError(t, err)

	slotHash := slotKey.Value()
	composite := append(contractVal[:], slotHash[:]...)
	one := uint256.NewInt(1)
	err = domains.DomainPut(kv.StorageDomain, tx, composite, one.Bytes(), 0, nil)
	require.NoError(t, err)

	// Verify seed
	v, _, err := domains.GetLatest(kv.StorageDomain, tx, composite)
	require.NoError(t, err)
	require.Equal(t, one.Bytes(), v)

	// --- Case 1: TX A writes slot to different value, TX B writes same as block origin ---
	// TX A (txNum 10): SSTORE(cb, 2) — changes from 1 to 2
	lc1 := state.NewLightCollector()
	err = lc1.WriteAccountStorage(contract, 1, slotKey, *uint256.NewInt(1), *uint256.NewInt(2))
	require.NoError(t, err)
	err = rs.ApplyStateWrites(context.Background(), tx, 1, 10, lc1.TakeWrites(), nil, &chain.Rules{}, nil)
	require.NoError(t, err)

	// Verify domain has 2
	v, _, err = domains.GetLatest(kv.StorageDomain, tx, composite)
	require.NoError(t, err)
	require.Equal(t, uint256.NewInt(2).Bytes(), v, "after TX A: slot should be 2")

	// TX B (txNum 11): SSTORE(cb, 1) — reentrancy guard reset to block origin
	// LightCollector with original = blockOrigin = 1, value = 1
	// With skip: dropped. With skip removed: emitted.
	lc2 := state.NewLightCollector()
	err = lc2.WriteAccountStorage(contract, 1, slotKey, *uint256.NewInt(1), *uint256.NewInt(1))
	require.NoError(t, err)
	writes2 := lc2.TakeWrites()

	// With the skip, writes2 is empty (original == value).
	// Without the skip, writes2 has one entry.
	// Either way, ApplyStateWrites should handle it correctly.
	err = rs.ApplyStateWrites(context.Background(), tx, 1, 11, writes2, nil, &chain.Rules{}, nil)
	require.NoError(t, err)

	// TX B explicitly SSTORed 1. If the write was emitted, DomainPut reads
	// prevVal=2 (from TX A), sees 2≠1, writes 1. Domain has 1.
	// If the write was skipped (old code), domain keeps 2 from TX A.
	// The CORRECT value depends on whether TX B intended to revert:
	// - If TX B executed SSTORE(cb, 1), the correct value is 1.
	// - The LightCollector skip removal ensures this case is handled.
	v, _, err = domains.GetLatest(kv.StorageDomain, tx, composite)
	require.NoError(t, err)

	// With the pre-check in applyVersionedWrites (reading domain value before
	// calling DomainPut), the revert case is handled correctly: DomainPut is
	// only called when the domain value differs from the write value.
	// TX B's write (value=1) differs from domain (value=2 from TX A), so
	// the write proceeds and the domain gets 1 (TX B's explicit revert).
	require.Equal(t, one.Bytes(), v,
		"after TX B revert: slot should be 1 (TX B explicitly SSTORed the block-origin value)")
}

// TestLightCollectorStorageUnchangedSlot verifies that when a TX touches a
// storage slot but doesn't change its value from the block origin, and NO
// other TX in the block wrote to that slot, the domain value is preserved.
func TestLightCollectorStorageUnchangedSlot(t *testing.T) {
	if testing.Short() {
		t.Skip("requires mdbx")
	}

	tx, domains := setup2CacheTest(t)
	lgr := log.New()

	rs := state.NewStateV3Buffered(state.NewStateV3(domains, ethconfig.Sync{}, lgr))

	contract := accounts.InternAddress(common.HexToAddress("0xC0NTRACT2"))
	contractVal := contract.Value()
	slotKey := accounts.InternKey(common.HexToHash("0xcb"))

	// Seed: slot cb = 1
	seedAccount := accounts.Account{Nonce: 1}
	err := domains.DomainPut(kv.AccountsDomain, tx, contractVal[:],
		accounts.SerialiseV3(&seedAccount), 0, nil)
	require.NoError(t, err)

	slotHash := slotKey.Value()
	composite := append(contractVal[:], slotHash[:]...)
	one := uint256.NewInt(1)
	err = domains.DomainPut(kv.StorageDomain, tx, composite, one.Bytes(), 0, nil)
	require.NoError(t, err)

	// TX A (txNum 10): SSTORE(cb, 1) — same as block origin (reentrancy guard)
	// LightCollector: original = blockOrigin = 1, value = 1
	lc := state.NewLightCollector()
	err = lc.WriteAccountStorage(contract, 1, slotKey, *uint256.NewInt(1), *uint256.NewInt(1))
	require.NoError(t, err)
	writes := lc.TakeWrites()

	err = rs.ApplyStateWrites(context.Background(), tx, 1, 10, writes, nil, &chain.Rules{}, nil)
	require.NoError(t, err)

	// Domain should still have 1 — the write was a no-op (DomainPut skip).
	v, _, err := domains.GetLatest(kv.StorageDomain, tx, composite)
	require.NoError(t, err)
	require.Equal(t, one.Bytes(), v,
		"unchanged slot should preserve domain value")

	// Now verify a DIFFERENT slot on the same contract is unaffected
	slotKey2 := accounts.InternKey(common.HexToHash("0xfe"))
	slotHash2 := slotKey2.Value()
	composite2 := append(contractVal[:], slotHash2[:]...)
	thirtySeven := uint256.NewInt(37)
	err = domains.DomainPut(kv.StorageDomain, tx, composite2, thirtySeven.Bytes(), 0, nil)
	require.NoError(t, err)

	// TX B writes a different slot but also has the reentrancy guard
	lc2 := state.NewLightCollector()
	err = lc2.WriteAccountStorage(contract, 1, slotKey, *uint256.NewInt(1), *uint256.NewInt(1)) // guard
	require.NoError(t, err)
	err = lc2.WriteAccountStorage(contract, 1, slotKey2, *uint256.NewInt(37), *uint256.NewInt(42)) // actual change
	require.NoError(t, err)

	err = rs.ApplyStateWrites(context.Background(), tx, 1, 11, lc2.TakeWrites(), nil, &chain.Rules{}, nil)
	require.NoError(t, err)

	v, _, err = domains.GetLatest(kv.StorageDomain, tx, composite)
	require.NoError(t, err)
	require.Equal(t, one.Bytes(), v, "guard slot still 1")

	v2, _, err := domains.GetLatest(kv.StorageDomain, tx, composite2)
	require.NoError(t, err)
	require.Equal(t, uint256.NewInt(42).Bytes(), v2, "changed slot should be 42")
}
