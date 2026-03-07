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

package vm

import (
	"testing"

	"github.com/holiman/uint256"
	"github.com/stretchr/testify/require"

	"github.com/erigontech/erigon/common"
	"github.com/erigontech/erigon/execution/types/accounts"
)

// runTransitionSequence runs a complete simple ETH transfer transition and
// returns the hash. Used as a building block for determinism/difference tests.
func runTransitionSequence(t *testing.T, modify func(h *TransitionHasher)) [32]byte {
	t.Helper()
	h := GetTransitionHasher()
	defer PutTransitionHasher(h)
	h.Reset()

	txHash := common.HexToHash("0xaabbccdd")
	feeCap := uint256.NewInt(30_000_000_000)           // 30 gwei
	tipCap := uint256.NewInt(2_000_000_000)            // 2 gwei
	value := uint256.NewInt(1_000_000_000_000_000_000) // 1 ETH

	h.HashTxContext(txHash, 2, 42, 21000, feeCap, tipCap, value)
	h.HashIntrinsicGas(21000, 0)
	h.HashGasPurchase(uint256.NewInt(630_000_000_000_000), uint256.NewInt(0)) // 21000 * 30gwei
	h.HashNonceIncrement(43)

	// Access list: sorted addresses
	coinbase := accounts.InternAddress(common.HexToAddress("0x1000"))
	sender := accounts.InternAddress(common.HexToAddress("0x2000"))
	dest := accounts.InternAddress(common.HexToAddress("0x3000"))
	h.HashAccessWarm(coinbase)
	h.HashAccessWarm(sender)
	h.HashAccessWarm(dest)

	// Transfer
	h.HashTransfer(0, sender, dest, value)

	// Exec hash (empty — no code for ETH transfer)
	h.HashExecHash([32]byte{}, 0)

	// Gas accounting
	h.HashGasAccounting(21000, 0, 0, 0, 21000)

	// Fee distribution
	tipAmount := uint256.NewInt(42_000_000_000_000)   // 21000 * 2gwei
	burnAmount := uint256.NewInt(588_000_000_000_000) // 21000 * 28gwei
	burntContract := accounts.InternAddress(common.HexToAddress("0x4000"))
	h.HashFeeDistribution(coinbase, tipAmount, burntContract, burnAmount, uint256.NewInt(0))

	if modify != nil {
		modify(h)
	}

	return h.Finalize()
}

func TestTransitionHasher_Determinism(t *testing.T) {
	hash1 := runTransitionSequence(t, nil)
	hash2 := runTransitionSequence(t, nil)
	require.Equal(t, hash1, hash2, "same transition must produce identical hashes")
	require.NotEqual(t, [32]byte{}, hash1, "hash must not be zero")
}

func TestTransitionHasher_DifferentNonce(t *testing.T) {
	hash1 := runTransitionSequence(t, nil)

	h := GetTransitionHasher()
	defer PutTransitionHasher(h)
	h.Reset()

	txHash := common.HexToHash("0xaabbccdd")
	feeCap := uint256.NewInt(30_000_000_000)
	tipCap := uint256.NewInt(2_000_000_000)
	value := uint256.NewInt(1_000_000_000_000_000_000)

	// Same as runTransitionSequence but nonce=99 instead of 42
	h.HashTxContext(txHash, 2, 99, 21000, feeCap, tipCap, value)
	h.HashIntrinsicGas(21000, 0)
	h.HashGasPurchase(uint256.NewInt(630_000_000_000_000), uint256.NewInt(0))
	h.HashNonceIncrement(100) // different
	coinbase := accounts.InternAddress(common.HexToAddress("0x1000"))
	sender := accounts.InternAddress(common.HexToAddress("0x2000"))
	dest := accounts.InternAddress(common.HexToAddress("0x3000"))
	h.HashAccessWarm(coinbase)
	h.HashAccessWarm(sender)
	h.HashAccessWarm(dest)
	h.HashTransfer(0, sender, dest, value)
	h.HashExecHash([32]byte{}, 0)
	h.HashGasAccounting(21000, 0, 0, 0, 21000)
	tipAmount := uint256.NewInt(42_000_000_000_000)
	burnAmount := uint256.NewInt(588_000_000_000_000)
	burntContract := accounts.InternAddress(common.HexToAddress("0x4000"))
	h.HashFeeDistribution(coinbase, tipAmount, burntContract, burnAmount, uint256.NewInt(0))

	hash2 := h.Finalize()
	require.NotEqual(t, hash1, hash2, "different nonce must produce different hashes")
}

func TestTransitionHasher_DifferentExecHash(t *testing.T) {
	hash1 := runTransitionSequence(t, nil)

	// Build same transition but with non-empty exec hash
	h := GetTransitionHasher()
	defer PutTransitionHasher(h)
	h.Reset()

	txHash := common.HexToHash("0xaabbccdd")
	feeCap := uint256.NewInt(30_000_000_000)
	tipCap := uint256.NewInt(2_000_000_000)
	value := uint256.NewInt(1_000_000_000_000_000_000)

	h.HashTxContext(txHash, 2, 42, 21000, feeCap, tipCap, value)
	h.HashIntrinsicGas(21000, 0)
	h.HashGasPurchase(uint256.NewInt(630_000_000_000_000), uint256.NewInt(0))
	h.HashNonceIncrement(43)
	coinbase := accounts.InternAddress(common.HexToAddress("0x1000"))
	sender := accounts.InternAddress(common.HexToAddress("0x2000"))
	dest := accounts.InternAddress(common.HexToAddress("0x3000"))
	h.HashAccessWarm(coinbase)
	h.HashAccessWarm(sender)
	h.HashAccessWarm(dest)
	h.HashTransfer(0, sender, dest, value)

	// Different exec hash (simulating contract execution)
	execHash := common.HexToHash("0xdeadbeefdeadbeefdeadbeefdeadbeef")
	h.HashExecHash(execHash, 1000)

	h.HashGasAccounting(21000, 0, 0, 0, 21000)
	tipAmount := uint256.NewInt(42_000_000_000_000)
	burnAmount := uint256.NewInt(588_000_000_000_000)
	burntContract := accounts.InternAddress(common.HexToAddress("0x4000"))
	h.HashFeeDistribution(coinbase, tipAmount, burntContract, burnAmount, uint256.NewInt(0))

	hash2 := h.Finalize()
	require.NotEqual(t, hash1, hash2, "different exec hash must produce different transition hash")
}

func TestTransitionHasher_DifferentTip(t *testing.T) {
	hash1 := runTransitionSequence(t, nil)

	h := GetTransitionHasher()
	defer PutTransitionHasher(h)
	h.Reset()

	txHash := common.HexToHash("0xaabbccdd")
	feeCap := uint256.NewInt(30_000_000_000)
	tipCap := uint256.NewInt(2_000_000_000)
	value := uint256.NewInt(1_000_000_000_000_000_000)

	h.HashTxContext(txHash, 2, 42, 21000, feeCap, tipCap, value)
	h.HashIntrinsicGas(21000, 0)
	h.HashGasPurchase(uint256.NewInt(630_000_000_000_000), uint256.NewInt(0))
	h.HashNonceIncrement(43)
	coinbase := accounts.InternAddress(common.HexToAddress("0x1000"))
	sender := accounts.InternAddress(common.HexToAddress("0x2000"))
	dest := accounts.InternAddress(common.HexToAddress("0x3000"))
	h.HashAccessWarm(coinbase)
	h.HashAccessWarm(sender)
	h.HashAccessWarm(dest)
	h.HashTransfer(0, sender, dest, value)
	h.HashExecHash([32]byte{}, 0)
	h.HashGasAccounting(21000, 0, 0, 0, 21000)

	// Different tip amount
	tipAmount := uint256.NewInt(99_000_000_000_000) // different
	burnAmount := uint256.NewInt(588_000_000_000_000)
	burntContract := accounts.InternAddress(common.HexToAddress("0x4000"))
	h.HashFeeDistribution(coinbase, tipAmount, burntContract, burnAmount, uint256.NewInt(0))

	hash2 := h.Finalize()
	require.NotEqual(t, hash1, hash2, "different tip must produce different hashes")
}

func TestTransitionHasher_Empty(t *testing.T) {
	h := GetTransitionHasher()
	defer PutTransitionHasher(h)
	h.Reset()

	hash := h.Finalize()
	require.NotEqual(t, [32]byte{}, hash, "empty hash must be valid (keccak of empty)")
}

func TestTransitionHasher_AuthResult(t *testing.T) {
	h1 := GetTransitionHasher()
	h1.Reset()

	authority := accounts.InternAddress(common.HexToAddress("0xaa"))
	delegation := accounts.InternAddress(common.HexToAddress("0xbb"))

	h1.HashAuthResult(authority, 5, delegation, true)
	hash1 := h1.Finalize()
	PutTransitionHasher(h1)

	// Same auth, different acceptance
	h2 := GetTransitionHasher()
	h2.Reset()
	h2.HashAuthResult(authority, 5, delegation, false)
	hash2 := h2.Finalize()
	PutTransitionHasher(h2)

	require.NotEqual(t, hash1, hash2, "accepted vs rejected must differ")

	// Same auth, accepted again — determinism
	h3 := GetTransitionHasher()
	h3.Reset()
	h3.HashAuthResult(authority, 5, delegation, true)
	hash3 := h3.Finalize()
	PutTransitionHasher(h3)

	require.Equal(t, hash1, hash3, "same auth must produce same hash")
}

func TestTransitionHasher_AccessSlot(t *testing.T) {
	h1 := GetTransitionHasher()
	h1.Reset()

	addr := accounts.InternAddress(common.HexToAddress("0xcc"))
	slot1 := common.HexToHash("0x01")
	slot2 := common.HexToHash("0x02")

	h1.HashAccessSlot(addr, slot1)
	h1.HashAccessSlot(addr, slot2)
	hash1 := h1.Finalize()
	PutTransitionHasher(h1)

	// Reversed slot order — must differ
	h2 := GetTransitionHasher()
	h2.Reset()
	h2.HashAccessSlot(addr, slot2)
	h2.HashAccessSlot(addr, slot1)
	hash2 := h2.Finalize()
	PutTransitionHasher(h2)

	require.NotEqual(t, hash1, hash2, "different slot ordering must produce different hashes")
}

func TestTransitionHasher_Pool(t *testing.T) {
	h1 := GetTransitionHasher()
	h1.Reset()
	h1.HashIntrinsicGas(21000, 0)
	expected := h1.Finalize()
	PutTransitionHasher(h1)

	// Get from pool, reset, same record should give same hash
	h2 := GetTransitionHasher()
	h2.Reset()
	h2.HashIntrinsicGas(21000, 0)
	got := h2.Finalize()
	PutTransitionHasher(h2)

	require.Equal(t, expected, got, "pooled hasher must produce same result after Reset")
}

func TestTransitionHasher_GasAccounting(t *testing.T) {
	h1 := GetTransitionHasher()
	h1.Reset()
	h1.HashGasAccounting(100000, 5000, 2500, 52500, 100000)
	hash1 := h1.Finalize()
	PutTransitionHasher(h1)

	// Different refund
	h2 := GetTransitionHasher()
	h2.Reset()
	h2.HashGasAccounting(100000, 5000, 3000, 53000, 100000) // different effective_refund
	hash2 := h2.Finalize()
	PutTransitionHasher(h2)

	require.NotEqual(t, hash1, hash2, "different refund must produce different hashes")
}

func TestTransitionHasher_TransferDepth(t *testing.T) {
	from := accounts.InternAddress(common.HexToAddress("0x1111"))
	to := accounts.InternAddress(common.HexToAddress("0x2222"))
	value := uint256.NewInt(1000)

	h1 := GetTransitionHasher()
	h1.Reset()
	h1.HashTransfer(0, from, to, value)
	hash1 := h1.Finalize()
	PutTransitionHasher(h1)

	// Same transfer at different depth
	h2 := GetTransitionHasher()
	h2.Reset()
	h2.HashTransfer(1, from, to, value)
	hash2 := h2.Finalize()
	PutTransitionHasher(h2)

	require.NotEqual(t, hash1, hash2, "different call depth must produce different hashes")
}

func BenchmarkTransitionHasher_FullTransition(b *testing.B) {
	h := GetTransitionHasher()
	defer PutTransitionHasher(h)

	txHash := common.HexToHash("0xaabbccdd")
	feeCap := uint256.NewInt(30_000_000_000)
	tipCap := uint256.NewInt(2_000_000_000)
	value := uint256.NewInt(1_000_000_000_000_000_000)
	coinbase := accounts.InternAddress(common.HexToAddress("0x1000"))
	sender := accounts.InternAddress(common.HexToAddress("0x2000"))
	dest := accounts.InternAddress(common.HexToAddress("0x3000"))
	burntContract := accounts.InternAddress(common.HexToAddress("0x4000"))
	tipAmount := uint256.NewInt(42_000_000_000_000)
	burnAmount := uint256.NewInt(588_000_000_000_000)

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		h.Reset()
		h.HashTxContext(txHash, 2, 42, 21000, feeCap, tipCap, value)
		h.HashIntrinsicGas(21000, 0)
		h.HashGasPurchase(uint256.NewInt(630_000_000_000_000), uint256.NewInt(0))
		h.HashNonceIncrement(43)
		h.HashAccessWarm(coinbase)
		h.HashAccessWarm(sender)
		h.HashAccessWarm(dest)
		h.HashTransfer(0, sender, dest, value)
		h.HashExecHash([32]byte{}, 0)
		h.HashGasAccounting(21000, 0, 0, 0, 21000)
		h.HashFeeDistribution(coinbase, tipAmount, burntContract, burnAmount, uint256.NewInt(0))
		h.Finalize()
	}
}
