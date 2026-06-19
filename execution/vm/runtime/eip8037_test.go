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

// Opcode-level tests for EIP-8037 (multidimensional state-gas metering).
// They drive a single EVM frame via evm.Call or evm.Create and assert the
// state-gas accounting exposed by the returned MdGasUsage (State / Spilled).

package runtime

import (
	"math"
	"math/rand"
	"testing"

	"github.com/holiman/uint256"
	"github.com/stretchr/testify/require"

	"github.com/erigontech/erigon/common"
	"github.com/erigontech/erigon/execution/protocol/mdgas"
	"github.com/erigontech/erigon/execution/protocol/params"
	"github.com/erigontech/erigon/execution/state"
	"github.com/erigontech/erigon/execution/tests/testutil"
	"github.com/erigontech/erigon/execution/tracing"
	"github.com/erigontech/erigon/execution/types"
	"github.com/erigontech/erigon/execution/types/accounts"
	"github.com/erigontech/erigon/execution/vm"
)

// ---- constants ----

var (
	stateGasNewSlot    = int64(params.StateGasPerStorageSet) // 97,920
	stateGasNewAccount = int64(params.StateGasNewAccount)    // 183,600
)

// ---- helpers ----

// run8037 executes code at a contract address under Amsterdam rules and returns
// the call's return data and the resulting gas usage. setup mutates the
// pre-state before the call.
func run8037(t *testing.T, code []byte, gas mdgas.MdGas, value uint256.Int,
	setup func(db *state.IntraBlockState, self accounts.Address),
) ([]byte, mdgas.MdGasUsage, error) {
	t.Helper()

	db := testutil.TemporalDB(t)
	tx, domains := testutil.TemporalTxSD(t, db)
	statedb := state.New(state.NewReaderV3(domains.AsGetter(tx)))

	self := accounts.InternAddress(common.BytesToAddress([]byte("self")))
	statedb.CreateAccount(self, true)
	statedb.SetCode(self, code, tracing.CodeChangeUnspecified)
	if setup != nil {
		setup(statedb, self)
	}

	cfg := &Config{
		State:    statedb,
		GasLimit: gas.Regular + gas.State,
	}
	setDefaults(cfg)

	vmenv := NewEnv(cfg)
	rules := vmenv.ChainRules()
	statedb.Prepare(rules, cfg.Origin, cfg.Coinbase, self, vm.ActivePrecompiles(rules), nil, nil)

	ret, _, gasUsed, err := vmenv.Call(cfg.Origin, self, nil, gas, value, false)
	return ret, gasUsed, err
}

// hugeBudget returns a gas budget large enough that nothing runs out.
func hugeBudget() mdgas.MdGas {
	return mdgas.MdGas{Regular: math.MaxUint64 / 2, State: math.MaxUint64 / 2}
}

// sstore returns "PUSH val; PUSH slot; SSTORE" bytecode.
func sstore(slot, val byte) []byte {
	return []byte{byte(vm.PUSH1), val, byte(vm.PUSH1), slot, byte(vm.SSTORE)}
}

// setSlot commits an original (pre-tx) value into a storage slot.
func setSlot(slot, val byte) func(*state.IntraBlockState, accounts.Address) {
	return func(db *state.IntraBlockState, self accounts.Address) {
		db.SetState(self, accounts.InternKey(common.BytesToHash([]byte{slot})), *uint256.NewInt(uint64(val)))
	}
}


// fundSelf adds balance to the executing contract.
func fundSelf(wei uint64) func(*state.IntraBlockState, accounts.Address) {
	return func(db *state.IntraBlockState, self accounts.Address) {
		db.AddBalance(self, *uint256.NewInt(wei), tracing.BalanceChangeUnspecified)
	}
}

// callCode builds bytecode that CALLs `to` forwarding `value` wei and all gas,
// followed by `tail`.
func callCode(to accounts.Address, value byte, tail []byte) []byte {
	addr := to.Value()
	b := []byte{
		byte(vm.PUSH1), 0x00, byte(vm.PUSH1), 0x00,
		byte(vm.PUSH1), 0x00, byte(vm.PUSH1), 0x00,
		byte(vm.PUSH1), value, byte(vm.PUSH20),
	}
	b = append(b, addr[:]...)
	b = append(b, byte(vm.GAS), byte(vm.CALL)) // GAS; CALL
	return append(b, tail...)
}

// deployCode builds bytecode that MSTOREs init and runs CREATE with the given value.
func deployCode(init []byte, value byte) []byte {
	word := make([]byte, 32)
	copy(word[32-len(init):], init)
	off, sz := byte(32-len(init)), byte(len(init))
	b := append([]byte{byte(vm.PUSH32)}, word...) // PUSH32 init-word
	b = append(b, byte(vm.PUSH1), 0x00, byte(vm.MSTORE))
	b = append(b, byte(vm.PUSH1), sz, byte(vm.PUSH1), off, byte(vm.PUSH1), value, byte(vm.CREATE))
	return append(b, byte(vm.STOP))
}

// deployCode2 builds bytecode that MSTOREs init and runs CREATE2 with the given value and salt=0.
func deployCode2(init []byte, value byte) []byte {
	word := make([]byte, 32)
	copy(word[32-len(init):], init)
	off, sz := byte(32-len(init)), byte(len(init))
	b := append([]byte{byte(vm.PUSH32)}, word...) // PUSH32 init-word
	b = append(b, byte(vm.PUSH1), 0x00, byte(vm.MSTORE))
	// PUSH1 salt=0; PUSH1 size; PUSH1 offset; PUSH1 value; CREATE2
	b = append(b, byte(vm.PUSH1), 0x00, byte(vm.PUSH1), sz, byte(vm.PUSH1), off, byte(vm.PUSH1), value, byte(vm.CREATE2))
	return append(b, byte(vm.STOP))
}

var (
	freshAddr   = accounts.InternAddress(common.BytesToAddress([]byte("fresh-target")))
	existAddr   = accounts.InternAddress(common.BytesToAddress([]byte("exist-target")))
	balanceAddr = accounts.InternAddress(common.BytesToAddress([]byte("balance-only")))
	childAddr   = accounts.InternAddress(common.BytesToAddress([]byte("child-frame")))
	revertInit  = []byte{byte(vm.PUSH1), 0x00, byte(vm.PUSH1), 0x00, byte(vm.REVERT)}
	invalidInit = []byte{byte(vm.INVALID)}
	deploy3Init = []byte{byte(vm.PUSH1), 0x03, byte(vm.PUSH1), 0x00, byte(vm.RETURN)} // return 3 bytes
	deploy0Init = []byte{byte(vm.PUSH1), 0x00, byte(vm.PUSH1), 0x00, byte(vm.RETURN)} // return 0 bytes
	stop        = []byte{byte(vm.STOP)}
	revertTail  = []byte{byte(vm.PUSH1), 0x00, byte(vm.PUSH1), 0x00, byte(vm.REVERT)}
	invalidTail = []byte{byte(vm.INVALID)}

	stateDeposit = int64(3 * params.CostPerStateByte)

	// Addresses for halt-frame fuzz tests.
	haltOKChild    = accounts.InternAddress(common.BytesToAddress([]byte("child-ok")))
	haltBadChild   = accounts.InternAddress(common.BytesToAddress([]byte("child-halt")))
	haltGrandchild = accounts.InternAddress(common.BytesToAddress([]byte("grandchild")))
)

func concat(parts ...[]byte) []byte {
	var b []byte
	for _, p := range parts {
		b = append(b, p...)
	}
	return b
}

// ============================ SSTORE state-gas ============================

// Ensures a brand-new storage slot is charged one storage-creation state gas unit.
func TestSStoreNewSlot(t *testing.T) {
	t.Parallel()
	_, res, err := run8037(t, sstore(0, 1), hugeBudget(), uint256.Int{}, nil)
	require.NoError(t, err)
	require.Equal(t, stateGasNewSlot, res.State, "new slot must charge storage-creation state gas")
}

// Ensures creating then clearing a slot in the same tx results in zero net state gas.
func TestSStoreClearSameTx(t *testing.T) {
	t.Parallel()
	code := append(sstore(0, 1), sstore(0, 0)...)
	_, res, err := run8037(t, code, hugeBudget(), uint256.Int{}, nil)
	require.NoError(t, err)
	require.Equal(t, int64(0), res.State, "0->x->0 must refill net state gas to zero")
}

// Ensures clearing a slot that was non-zero at tx start generates a state-gas
// refund. The signed State field goes negative: the slot was created before this
// frame and removing it credits the frame.
func TestSStoreClearOriginalNonzero(t *testing.T) {
	t.Parallel()
	_, res, err := run8037(t, sstore(0, 0), hugeBudget(), uint256.Int{}, setSlot(0, 1))
	require.NoError(t, err)
	require.Equal(t, -stateGasNewSlot, res.State, "clearing a pre-existing slot must generate a state-gas refund")
}

// Ensures clearing then restoring the original value has zero net state gas.
func TestSStoreRestoreOriginal(t *testing.T) {
	t.Parallel()
	code := append(sstore(0, 0), sstore(0, 1)...)
	_, res, err := run8037(t, code, hugeBudget(), uint256.Int{}, setSlot(0, 1))
	require.NoError(t, err)
	require.Equal(t, int64(0), res.State, "clearing then restoring original should not adjust state gas")
}

// Ensures overwriting an existing slot with another value does not charge state gas.
func TestSStoreOverwriteExisting(t *testing.T) {
	t.Parallel()
	_, res, err := run8037(t, sstore(0, 2), hugeBudget(), uint256.Int{}, setSlot(0, 1))
	require.NoError(t, err)
	require.Equal(t, int64(0), res.State, "overwriting existing slot should not charge state gas")
}

// Ensures that when the reservoir is smaller than the new-slot charge, the excess
// spills into regular gas.
func TestSStoreSpillsIntoRegular(t *testing.T) {
	t.Parallel()
	_, res, err := run8037(t, sstore(0, 1), mdgas.MdGas{Regular: 1_000_000, State: 100}, uint256.Int{}, nil)
	require.NoError(t, err)
	want := uint64(stateGasNewSlot) - 100
	require.Equal(t, want, res.Spilled, "deficit must spill into regular gas")
}

// ====================== CALL new-account state-gas =======================

// Ensures CALL with value to a non-existent account charges one account creation.
func TestCallValueToNewAccount(t *testing.T) {
	t.Parallel()
	_, res, err := run8037(t, callCode(freshAddr, 1, stop), hugeBudget(), uint256.Int{}, fundSelf(10))
	require.NoError(t, err)
	require.Equal(t, stateGasNewAccount, res.State, "CALL with value to empty account must charge account creation")
}

// Ensures CALL with value to an existing account does not charge state gas.
func TestCallValueToExistingAccount(t *testing.T) {
	t.Parallel()
	setup := func(db *state.IntraBlockState, self accounts.Address) {
		db.CreateAccount(existAddr, true)
		db.SetCode(existAddr, stop, tracing.CodeChangeUnspecified)
		db.AddBalance(self, *uint256.NewInt(10), tracing.BalanceChangeUnspecified)
	}
	_, res, err := run8037(t, callCode(existAddr, 1, stop), hugeBudget(), uint256.Int{}, setup)
	require.NoError(t, err)
	require.Equal(t, int64(0), res.State, "CALL to existing account should not charge state gas")
}

// Ensures CALL with zero value creates no account, so nothing is charged.
func TestCallZeroValueToNewAccount(t *testing.T) {
	t.Parallel()
	_, res, err := run8037(t, callCode(freshAddr, 0, stop), hugeBudget(), uint256.Int{}, nil)
	require.NoError(t, err)
	require.Equal(t, int64(0), res.State, "zero-value CALL should not charge state gas")
}

// EIP-8037: "If the operation is unsuccessful before entering the call frame
// (e.g., due to insufficient balance or due to the stack depth), ... the
// charged state-gas is refilled in LIFO order." When a CALL with value targets
// a fresh (empty) address but the caller lacks balance, the new-account state
// gas charged at the dynamic gas phase must be refunded, resulting in zero net
// state gas usage.
func TestCallInsufficientBalanceRefundsCharge(t *testing.T) {
	t.Parallel()
	// self has no balance, so the value transfer fails at CanTransfer.
	_, res, err := run8037(t, callCode(freshAddr, 1, stop), hugeBudget(), uint256.Int{}, nil)
	require.NoError(t, err)
	require.Equal(t, int64(0), res.State,
		"CALL new-account state gas must be refilled when the call fails before entering the child")
}

// Ensures state gas charged inside a child frame is refilled at the frame boundary
// when the child reverts.
func TestCallChildRevertRefillsStateGas(t *testing.T) {
	t.Parallel()
	// Child SSTOREs a new slot then REVERTs. State changes are rolled back,
	// so the parent must see zero net state gas from the child.
	childCode := concat(sstore(0, 1), revertTail)
	setup := func(db *state.IntraBlockState, self accounts.Address) {
		db.CreateAccount(childAddr, true)
		db.SetCode(childAddr, childCode, tracing.CodeChangeUnspecified)
	}
	_, res, err := run8037(t, callCode(childAddr, 0, stop), hugeBudget(), uint256.Int{}, setup)
	require.NoError(t, err)
	require.Equal(t, int64(0), res.State, "reverted child state gas should be refilled at frame boundary")
}

// Ensures state gas charged inside a child frame is refilled at the frame boundary
// when the child halts exceptionally.
func TestCallChildHaltRefillsStateGas(t *testing.T) {
	t.Parallel()
	childCode := concat(sstore(0, 1), invalidTail)
	setup := func(db *state.IntraBlockState, self accounts.Address) {
		db.CreateAccount(childAddr, true)
		db.SetCode(childAddr, childCode, tracing.CodeChangeUnspecified)
	}
	_, res, err := run8037(t, callCode(childAddr, 0, stop), hugeBudget(), uint256.Int{}, setup)
	require.NoError(t, err)
	require.Equal(t, int64(0), res.State, "halted child state gas should be refilled at frame boundary")
}

// ===================== CREATE / CREATE2 state-gas =========================

// Ensures CREATE to a fresh address charges account creation plus code deposit.
func TestCreateNewAccount(t *testing.T) {
	t.Parallel()
	_, res, err := run8037(t, deployCode(deploy3Init, 0), hugeBudget(), uint256.Int{}, nil)
	require.NoError(t, err)
	want := stateGasNewAccount + stateDeposit
	require.Equal(t, want, res.State, "new CREATE must charge account creation + code deposit")
}

// Ensures CREATE whose init code reverts refills the account charge and deposits nothing.
func TestCreateInitRevertRefill(t *testing.T) {
	t.Parallel()
	_, res, err := run8037(t, deployCode(revertInit, 0), hugeBudget(), uint256.Int{}, nil)
	require.NoError(t, err)
	require.Equal(t, int64(0), res.State, "reverted CREATE must refill all state gas")
}

// Ensures CREATE whose init code halts exceptionally refills the account charge.
func TestCreateInitHaltRefill(t *testing.T) {
	t.Parallel()
	_, res, err := run8037(t, deployCode(invalidInit, 0), hugeBudget(), uint256.Int{}, nil)
	require.NoError(t, err)
	require.Equal(t, int64(0), res.State, "halted CREATE must refill all state gas")
}

// Ensures CREATE with value exceeding balance fails before the frame and is refilled.
func TestCreateInsufficientBalanceRefill(t *testing.T) {
	t.Parallel()
	// self has no balance; CREATE forwards value 1.
	_, res, err := run8037(t, deployCode(deploy3Init, 1), hugeBudget(), uint256.Int{}, nil)
	require.NoError(t, err)
	require.Equal(t, int64(0), res.State, "insufficient-balance CREATE must refill state gas")
}

// Ensures CREATE onto a pre-existing (balance-only) leaf refills the account
// portion; only the code deposit is charged. Per the EIP-8037 spec: "Creating a
// contract at such an address updates the existing leaf rather than adding a new
// one, so only the code-deposit cost (L × CPSB) applies and the
// STATE_BYTES_PER_NEW_ACCOUNT × CPSB account-creation charge does not."
//
// Protocol invariant: account-creation state gas is refunded when the target
// already has a state trie leaf (EIP-161 existence).
// Bug prevented: failing to refund the unconditional pre-charge on pre-existing
// targets would overcharge every CREATE to a funded address.
func TestCreatePreexistingTarget(t *testing.T) {
	t.Parallel()
	self := accounts.InternAddress(common.BytesToAddress([]byte("self")))
	setup := func(db *state.IntraBlockState, _ accounts.Address) {
		// Derive the CREATE target: CreateAddress(self, nonce=0).
		// The test contract's nonce is 0; CREATE increments it to 1.
		derived := accounts.InternAddress(types.CreateAddress(self.Value(), 0))
		db.AddBalance(derived, *uint256.NewInt(1), tracing.BalanceChangeUnspecified)
	}
	_, res, err := run8037(t, deployCode(deploy3Init, 0), hugeBudget(), uint256.Int{}, setup)
	require.NoError(t, err)
	// Only code-deposit state gas (3 bytes × CPSB), not account creation.
	require.Equal(t, stateDeposit, res.State,
		"CREATE to pre-existing target must charge only code deposit, not account creation")
}

// Ensures CREATE2 charges account creation plus code deposit identically to
// CREATE. EIP-8037 applies the same unconditional-charge-with-refill model to
// both opcodes.
//
// Protocol invariant: CREATE and CREATE2 have identical state-gas semantics.
// Bug prevented: a missing or misrouted state-gas charge on the CREATE2 path
// would cause divergence from CREATE behavior.
func TestCreate2SameSemantics(t *testing.T) {
	t.Parallel()
	_, res, err := run8037(t, deployCode2(deploy3Init, 0), hugeBudget(), uint256.Int{}, nil)
	require.NoError(t, err)
	want := stateGasNewAccount + stateDeposit
	require.Equal(t, want, res.State,
		"CREATE2 must charge account creation + code deposit identically to CREATE")
}

// Ensures the code-deposit portion is charged per byte independently of the
// account charge: the delta between a 3-byte and 0-byte deploy is exactly
// 3 × CPSB.
//
// Protocol invariant: GAS_CODE_DEPOSIT = CPSB per byte, separate from the
// account-creation charge.
// Bug prevented: conflating code deposit with account creation would cause
// incorrect total state gas.
func TestCreateCodeDepositChargedSeparately(t *testing.T) {
	t.Parallel()
	_, res3, err := run8037(t, deployCode(deploy3Init, 0), hugeBudget(), uint256.Int{}, nil)
	require.NoError(t, err)
	_, res0, err := run8037(t, deployCode(deploy0Init, 0), hugeBudget(), uint256.Int{}, nil)
	require.NoError(t, err)
	got := res3.State - res0.State
	require.Equal(t, stateDeposit, got,
		"code-deposit delta (3-byte vs 0-byte) must equal 3 × CPSB")
}

// ===================== Reservoir / gas_left mechanics =====================

// Ensures state gas is drawn from the reservoir first: a charge within reservoir
// size does not spill into regular gas.
func TestReservoirDrawnFirst(t *testing.T) {
	t.Parallel()
	_, res, err := run8037(t, sstore(0, 1), mdgas.MdGas{Regular: 1_000_000, State: 200_000}, uint256.Int{}, nil)
	require.NoError(t, err)
	require.Equal(t, uint64(0), res.Spilled, "charge within reservoir must not spill")
}

// Ensures that borrowed regular gas is repaid before the reservoir when a
// state-gas refund is credited (LIFO order).
func TestLIFORefillOrder(t *testing.T) {
	t.Parallel()
	// With zero reservoir, 0->x->0 SSTORE borrows from regular for the charge,
	// then the refund repays the borrow and leaves the reservoir at 0.
	code := append(sstore(0, 1), sstore(0, 0)...)
	_, res, err := run8037(t, code, mdgas.MdGas{Regular: 1_000_000, State: 0}, uint256.Int{}, nil)
	require.NoError(t, err)
	require.Equal(t, uint64(0), res.Spilled, "LIFO refill must repay borrowed regular gas")
	require.Equal(t, int64(0), res.State, "net state gas must be zero after 0->x->0")
}

// ==================== Halting frame terminal state =========================

// Ensures that a frame terminated by INVALID consumes all regular gas but
// restores the state reservoir, leaving zero net state gas.
func TestHaltFrameTerminalState(t *testing.T) {
	t.Parallel()
	// Top frame: new slot, then calls a child that halts, then INVALID itself.
	haltChild := accounts.InternAddress(common.BytesToAddress([]byte("halt-child")))
	top := concat(
		sstore(0, 1),                  // self: new slot
		callCode(haltChild, 0, nil),   // child halts (contained)
		callCode(freshAddr, 1, nil),   // new-account charge
		[]byte{byte(vm.INVALID)},      // this frame halts
	)
	initial := mdgas.MdGas{Regular: 2_000_000, State: 300_000}

	setup := func(db *state.IntraBlockState, self accounts.Address) {
		db.AddBalance(self, *uint256.NewInt(1000), tracing.BalanceChangeUnspecified)
		db.CreateAccount(haltChild, true)
		db.SetCode(haltChild, concat(sstore(3, 3), invalidTail), tracing.CodeChangeUnspecified)
	}

	_, res, err := run8037(t, top, initial, uint256.Int{}, setup)
	// Expect an error (the frame halted exceptionally).
	require.Error(t, err)
	require.NotEqual(t, vm.ErrExecutionReverted, err, "expected exceptional halt, not revert")
	// State gas must be fully refilled on halt.
	require.Equal(t, int64(0), res.State, "halted frame must have zero net state gas")
}

// =================== SSTORE reentrancy sentry ===========================

// EIP-8037: the SSTORE reentrancy sentry (2300 gas) checks gas_left only;
// the state-gas reservoir is excluded. Even with a massive reservoir, if
// regular gas is at the sentry threshold, the SSTORE must fail.
//
// Protocol invariant: the sentry prevents re-entrancy griefing and must
// operate solely on gas_left regardless of the multi-dimensional budget.
// Bug prevented: if the sentry summed regular + reservoir, a contract with
// low regular gas but a huge reservoir could pass the sentry and execute an
// SSTORE, violating the protection guarantee.
func TestSStoreStipendExcludesReservoir(t *testing.T) {
	t.Parallel()
	// Noop write (slot already == 1, writing 1 again) so only the sentry gate matters.
	// PUSH1(1) costs 3 gas + PUSH1(0) costs 3 gas = 6 gas consumed before SSTORE.
	// Sentry threshold: regular gas <= 2300 → fail.
	// With 2306 regular gas: 2306 - 6 = 2300, fails the sentry (<=).
	_, _, err := run8037(t, sstore(0, 1), mdgas.MdGas{Regular: 2306, State: math.MaxUint64 / 2}, uint256.Int{}, setSlot(0, 1))
	require.Error(t, err, "expected sentry failure with regular gas at the limit")

	// With 2307 regular gas: 2307 - 6 = 2301, passes the sentry (> 2300).
	_, _, err = run8037(t, sstore(0, 1), mdgas.MdGas{Regular: 2307, State: math.MaxUint64 / 2}, uint256.Int{}, setSlot(0, 1))
	require.NoError(t, err, "one more regular gas above the sentry must succeed")
}

// =================== GAS opcode excludes reservoir ========================

// EIP-8037: "The GAS (0x5a) opcode returns gas_left, which excludes the
// reservoir." The GAS opcode must return only the regular gas remaining, not
// regular + state_gas_reservoir.
//
// Protocol invariant: smart contracts that rely on gasleft() for stipend
// checks or forwarding decisions must not see the reservoir.
// Bug prevented: if GAS returned regular + reservoir, the 63/64 forwarding
// rule would forward too much gas, or contracts that check gasleft() < N
// would behave incorrectly.
func TestGasOpcodeExcludesReservoir(t *testing.T) {
	t.Parallel()
	// Bytecode: GAS; PUSH1 0; MSTORE; PUSH1 0x20; PUSH1 0; RETURN
	// Reads gas_left, stores it in memory, returns the 32-byte word.
	code := []byte{
		byte(vm.GAS),                                           // push gas_left
		byte(vm.PUSH1), 0x00, byte(vm.MSTORE),                  // MSTORE at 0
		byte(vm.PUSH1), 0x20, byte(vm.PUSH1), 0x00, byte(vm.RETURN), // RETURN 32 bytes
	}
	regular := uint64(1_000_000)
	reservoir := uint64(500_000)
	ret, _, err := run8037(t, code, mdgas.MdGas{Regular: regular, State: reservoir}, uint256.Int{}, nil)
	require.NoError(t, err)
	require.Len(t, ret, 32, "expected 32-byte return")

	got := new(uint256.Int).SetBytes(ret).Uint64()
	// GAS opcode itself costs 2 gas (GasQuickStep). At the point GAS executes,
	// the regular gas has only been decremented by the GAS opcode's constant cost.
	expected := regular - vm.GasQuickStep
	require.Equal(t, expected, got,
		"GAS opcode must return gas_left only; reservoir (%d) must be excluded", reservoir)
}

// =================== Balance-only account ================================

// EIP-161: an account with a non-zero balance but no code and nonce == 0 is
// NOT empty. Therefore, a value CALL to such an account must NOT charge the
// new-account state gas — the account already exists.
//
// Protocol invariant: the CALL new-account charge is conditional on the
// Empty() predicate (EIP-161), not the Exist() predicate.
// Bug prevented: if the implementation used Exist() instead of Empty(), or
// if Empty() incorrectly returned true for balance-only accounts, every value
// CALL to a balance-bearing EOA would be overcharged.
func TestCallBalanceOnlyAccountIsExistent(t *testing.T) {
	t.Parallel()
	setup := func(db *state.IntraBlockState, self accounts.Address) {
		// Give balanceAddr a balance but no code or nonce — not empty per EIP-161.
		db.AddBalance(balanceAddr, *uint256.NewInt(1), tracing.BalanceChangeUnspecified)
		db.AddBalance(self, *uint256.NewInt(10), tracing.BalanceChangeUnspecified)
	}
	_, res, err := run8037(t, callCode(balanceAddr, 1, stop), hugeBudget(), uint256.Int{}, setup)
	require.NoError(t, err)
	require.Equal(t, int64(0), res.State,
		"value CALL to balance-only account must not charge account creation state gas")
}

// =================== Halt-frame terminal state fuzz =======================

// haltFrameFuzzSetup is a run8037 setup that funds self and deploys a 3-level
// child set: a success child that itself calls a grandchild (which SSTOREs a
// new slot), and a halting child (SSTOREs a new slot then INVALID).
func haltFrameFuzzSetup(db *state.IntraBlockState, self accounts.Address) {
	db.AddBalance(self, *uint256.NewInt(1000), tracing.BalanceChangeUnspecified)
	db.CreateAccount(haltGrandchild, true)
	db.SetCode(haltGrandchild, concat(sstore(5, 5), stop), tracing.CodeChangeUnspecified)
	db.CreateAccount(haltOKChild, true)
	db.SetCode(haltOKChild, concat(sstore(1, 1), callCode(haltGrandchild, 0, nil), stop), tracing.CodeChangeUnspecified)
	db.CreateAccount(haltBadChild, true)
	db.SetCode(haltBadChild, concat(sstore(3, 3), invalidTail), tracing.CodeChangeUnspecified)
}

// Fuzz: arbitrary sequences of state writes, child calls (success / halting)
// and new-account calls, always terminated by INVALID. However the descendants
// behave, a halted frame's terminal budget must always have zero net state gas.
//
// Protocol invariant: EIP-8037 requires that on exceptional halt, ALL state gas
// charged in the frame (and descendants) is refilled to the reservoir. The
// frame's gas_left is consumed, but the reservoir is made whole.
// Bug prevented: a leak in the LIFO refill path that doesn't fully restore the
// reservoir under complex nesting of successes, reverts, and halts.
func TestHaltFrameTerminalStateFuzz(t *testing.T) {
	t.Parallel()
	rng := rand.New(rand.NewSource(80371))
	steps := [][]byte{
		sstore(1, 1),                  // new slot charge
		callCode(haltOKChild, 0, nil), // child success (with grandchild)
		callCode(haltBadChild, 0, nil), // descendant halts (contained)
		callCode(freshAddr, 1, nil),    // new-account charge
	}
	for trial := 0; trial < 400; trial++ {
		var code []byte
		nSteps := 1 + rng.Intn(8)
		for i := 0; i < nSteps; i++ {
			code = append(code, steps[rng.Intn(len(steps))]...)
		}
		code = append(code, byte(vm.INVALID)) // halt
		initial := mdgas.MdGas{Regular: 3_000_000, State: uint64(rng.Intn(400_000))}
		_, res, err := run8037(t, code, initial, uint256.Int{}, haltFrameFuzzSetup)
		require.Error(t, err, "trial %d: expected exceptional halt", trial)
		require.NotEqual(t, vm.ErrExecutionReverted, err,
			"trial %d: expected exceptional halt, not revert", trial)
		require.Equal(t, int64(0), res.State,
			"trial %d: halted frame must have zero net state gas (got %d)", trial, res.State)
	}
}

// =================== LIFO vector conservation fuzz ========================

// Fuzz: arbitrary sequences of SSTOREs (charge + refund) around the reservoir/
// spill boundary. The net state gas after a charge-then-refund cycle must be
// zero, and the spillover must be fully repaid.
//
// Protocol invariant: the LIFO refill mechanism correctly conserves gas across
// arbitrary charge/refund sequences. The reservoir is restored to its initial
// value, and borrowed regular gas is fully repaid.
// Bug prevented: an off-by-one in RefundState or creditStateGasRefund that
// causes gas leakage or underflow under specific spill/refund orderings.
func TestLIFOVectorFuzz(t *testing.T) {
	t.Parallel()
	rng := rand.New(rand.NewSource(8037))
	for trial := 0; trial < 200; trial++ {
		// Build a sequence of SSTORE operations that creates new slots
		// (charging state gas) then clears them back to zero (refunding).
		// Each slot 1..N is set to 1 (charged) then set to 0 (refunded).
		nSlots := 1 + rng.Intn(8)
		var code []byte
		// Phase 1: create slots 1..N (each charges state gas).
		for s := 1; s <= nSlots; s++ {
			code = append(code, sstore(byte(s), 1)...)
		}
		// Phase 2: clear slots N..1 in reverse (LIFO refund).
		for s := nSlots; s >= 1; s-- {
			code = append(code, sstore(byte(s), 0)...)
		}
		code = append(code, byte(vm.STOP))

		// Vary the reservoir size: sometimes it covers all charges,
		// sometimes none, sometimes partially.
		reservoir := uint64(rng.Intn(int(stateGasNewSlot) * (nSlots + 1)))
		gas := mdgas.MdGas{Regular: 10_000_000, State: reservoir}

		_, res, err := run8037(t, code, gas, uint256.Int{}, nil)
		require.NoError(t, err, "trial %d: unexpected error", trial)
		require.Equal(t, int64(0), res.State,
			"trial %d: net state gas must be zero after full charge-then-refund cycle (got %d, reservoir=%d, slots=%d)",
			trial, res.State, reservoir, nSlots)
		require.Equal(t, uint64(0), res.Spilled,
			"trial %d: spilled must be zero after full refund (got %d, reservoir=%d, slots=%d)",
			trial, res.Spilled, reservoir, nSlots)
	}
}
