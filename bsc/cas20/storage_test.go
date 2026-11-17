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

package cas20

import (
	"strings"
	"testing"

	"github.com/holiman/uint256"

	"github.com/erigontech/erigon/common"
	"github.com/erigontech/erigon/common/crypto"
	"github.com/erigontech/erigon/execution/protocol/params"
	"github.com/erigontech/erigon/execution/tracing"
)

func strOf(v string, _ bool) string { return v }

func newTestStorage(t *testing.T) cas20Storage {
	t.Helper()
	statedb := newTestStateDB(t)
	return newUnmeteredCAS20Storage(statedb, cas20Addr(cas20VariantAsset, 1))
}

func TestCAS20StorageScalars(t *testing.T) {
	s := newTestStorage(t)
	v := uint256.NewInt(123456789)
	s.setTotalSupply(v)
	s.setSupplyCap(uint256.NewInt(1_000_000))
	s.setAdminCount(uint256.NewInt(2))
	s.setPaused(uint256.NewInt(0b101))

	if got := s.totalSupply(); got.Cmp(v) != 0 {
		t.Errorf("totalSupply = %v, want %v", got, v)
	}
	if got := s.supplyCap().Uint64(); got != 1_000_000 {
		t.Errorf("supplyCap = %d", got)
	}
	if got := s.adminCount().Uint64(); got != 2 {
		t.Errorf("adminCount = %d", got)
	}
	if got := s.paused().Uint64(); got != 0b101 {
		t.Errorf("paused = %b", got)
	}
}

func TestCAS20StorageMappings(t *testing.T) {
	s := newTestStorage(t)
	alice := common.HexToAddress("0xa11ce")
	bob := common.HexToAddress("0xb0b")

	s.setBalance(alice, uint256.NewInt(500))
	s.setAllowance(alice, bob, uint256.NewInt(42))
	s.setNonce(alice, uint256.NewInt(7))

	if got := s.balanceOf(alice).Uint64(); got != 500 {
		t.Errorf("balanceOf = %d", got)
	}
	if got := s.balanceOf(bob).Uint64(); got != 0 {
		t.Errorf("unset balance = %d, want 0", got)
	}
	if got := s.allowance(alice, bob).Uint64(); got != 42 {
		t.Errorf("allowance = %d", got)
	}
	if got := s.allowance(bob, alice).Uint64(); got != 0 {
		t.Errorf("reversed allowance = %d, want 0", got)
	}
	if got := s.nonce(alice).Uint64(); got != 7 {
		t.Errorf("nonce = %d", got)
	}

	// Raw slots, spelled out from Solidity's rules: a nested mapping with swapped
	// keys is self-consistent, so only the slot the value lands in tells them apart.
	pad := func(b []byte) []byte { return hashBytes(common.BytesToHash(b)) }
	solMap := func(key, base []byte) []byte { return crypto.Keccak256(pad(key), pad(base)) }

	balSlot := solMap(alice[:], hashBytes(slotAt(cas20SlotBalances)))
	if got := s.getWord(common.BytesToHash(balSlot)); new(uint256.Int).SetBytes(got[:]).Uint64() != 500 {
		t.Errorf("balance is not at keccak256(alice ++ balancesBase)")
	}

	inner := solMap(alice[:], hashBytes(slotAt(cas20SlotAllowances)))
	allowSlot := solMap(bob[:], inner)
	if got := s.getWord(common.BytesToHash(allowSlot)); new(uint256.Int).SetBytes(got[:]).Uint64() != 42 {
		t.Error("allowance is not at keccak256(spender ++ keccak256(owner ++ base)) — nesting order is wrong")
	}
	swappedInner := solMap(bob[:], hashBytes(slotAt(cas20SlotAllowances)))
	if got := s.getWord(common.BytesToHash(solMap(alice[:], swappedInner))); got != (common.Hash{}) {
		t.Error("allowance also landed under the swapped nesting order")
	}

	s.setRole(roleMint, alice, true)
	roleInner := solMap(roleMint[:], hashBytes(slotAt(cas20SlotRoles)))
	if got := s.getWord(common.BytesToHash(solMap(alice[:], roleInner))); got == (common.Hash{}) {
		t.Error("role is not at keccak256(account ++ keccak256(role ++ base))")
	}
}

func TestCAS20StorageRoles(t *testing.T) {
	s := newTestStorage(t)
	mintRole := keccakHash([]byte("MINT_ROLE"))
	alice := common.HexToAddress("0xa11ce")

	if s.hasRole(mintRole, alice) {
		t.Fatal("role should be unset initially")
	}
	s.setRole(mintRole, alice, true)
	if !s.hasRole(mintRole, alice) {
		t.Error("role should be set")
	}
	s.setRole(mintRole, alice, false)
	if s.hasRole(mintRole, alice) {
		t.Error("role should be cleared")
	}

	s.setRoleAdmin(mintRole, common.Hash{}) // DEFAULT_ADMIN
	if got := s.roleAdmin(mintRole); got != (common.Hash{}) {
		t.Errorf("roleAdmin = %s", got.Hex())
	}
}

func TestCAS20StoragePackedPolicies(t *testing.T) {
	s := newTestStorage(t)
	s.setTransferSenderPolicy(0x1111111111111111)
	s.setTransferReceiverPolicy(0x2222222222222222)
	s.setTransferExecutorPolicy(0x3333333333333333)
	s.setMintReceiverPolicy(0x4444444444444444)

	if got := s.transferSenderPolicy(); got != 0x1111111111111111 {
		t.Errorf("sender = %#x", got)
	}
	if got := s.transferReceiverPolicy(); got != 0x2222222222222222 {
		t.Errorf("receiver = %#x", got)
	}
	if got := s.transferExecutorPolicy(); got != 0x3333333333333333 {
		t.Errorf("executor = %#x", got)
	}
	if got := s.mintReceiverPolicy(); got != 0x4444444444444444 {
		t.Errorf("mintReceiver = %#x", got)
	}

	word := s.getWord(slotAt(cas20SlotTransferPolicies))
	wantWord := "0x" + "0000000000000000" + "3333333333333333" + "2222222222222222" + "1111111111111111"
	if word.Hex() != wantWord {
		t.Errorf("packed slot 9 = %s, want %s", word.Hex(), wantWord)
	}

	s.setTransferReceiverPolicy(0xdeadbeefdeadbeef)
	if got := s.transferSenderPolicy(); got != 0x1111111111111111 {
		t.Errorf("sender disturbed: %#x", got)
	}
	if got := s.transferExecutorPolicy(); got != 0x3333333333333333 {
		t.Errorf("executor disturbed: %#x", got)
	}
	if got := s.transferReceiverPolicy(); got != 0xdeadbeefdeadbeef {
		t.Errorf("receiver = %#x", got)
	}
}

func TestCAS20StorageGas(t *testing.T) {
	statedb := newTestStateDB(t)
	token := cas20Addr(cas20VariantAsset, 1)
	gas := NewGasBudget(1_000_000)
	ctx := &PrecompileContext{StateDB: statedb, Self: token, gas: &gas}
	s := newMeteredCAS20Storage(ctx)

	alice := common.HexToAddress("0xa11ce")
	charged := func(fn func()) uint64 {
		before := gas.Execution
		fn()
		return before - gas.Execution
	}

	// Every access also pays the 64-byte keccak that derives the slot.
	const keccak64 = params.Keccak256Gas + 2*params.Keccak256WordGas
	var (
		cold  = params.ColdSloadCostEIP2929
		warm  = params.WarmStorageReadCostEIP2929
		set   = params.SstoreSetGasEIP2200
		reset = params.SstoreResetGasEIP2200 - params.ColdSloadCostEIP2929
	)

	if c := charged(func() { s.setBalance(alice, uint256.NewInt(500)) }); c != keccak64+cold+set {
		t.Errorf("cold set write charged %d, want %d", c, keccak64+cold+set)
	}
	if c := charged(func() { _ = s.balanceOf(alice) }); c != keccak64+warm {
		t.Errorf("warm read charged %d, want %d", c, keccak64+warm)
	}
	// A dirty update at the warm price, not a reset: the slot was zero at the start
	// of the transaction. TestCAS20StorageRefunds covers reset.
	if c := charged(func() { s.setBalance(alice, uint256.NewInt(600)) }); c != keccak64+warm {
		t.Errorf("dirty update charged %d, want %d", c, keccak64+warm)
	}
	if c := charged(func() { s.setBalance(alice, uint256.NewInt(600)) }); c != keccak64+warm {
		t.Errorf("warm no-op write charged %d, want %d", c, keccak64+warm)
	}
	_ = reset

	if got, want := 1_000_000-gas.Execution, 4*keccak64+cold+set+warm+warm+warm; got != want {
		t.Errorf("gas used = %d, want %d", got, want)
	}
	if ctx.OutOfGas() {
		t.Error("should not be out of gas")
	}

	free := newUnmeteredCAS20Storage(statedb, token)
	if c := charged(func() { _ = free.balanceOf(alice) }); c != 0 {
		t.Errorf("unmetered read charged %d, want 0", c)
	}
}

func TestCAS20StorageGasOutOfGas(t *testing.T) {
	statedb := newTestStateDB(t)
	token := cas20Addr(cas20VariantAsset, 1)
	gas := NewGasBudget(100) // less than one cold access
	ctx := &PrecompileContext{StateDB: statedb, Self: token, gas: &gas}
	s := newMeteredCAS20Storage(ctx)

	_ = s.getWord(slotAt(cas20SlotTotalSupply)) // cold read needs 2100 > 100
	if !ctx.OutOfGas() {
		t.Fatal("expected out of gas")
	}
	if gas.Execution != 0 {
		t.Errorf("budget should be exhausted, got %d", gas.Execution)
	}
}

func TestCAS20StorageStrings(t *testing.T) {
	s := newTestStorage(t)

	s.setName("USD Coin")
	if got := strOf(s.name()); got != "USD Coin" {
		t.Errorf("name = %q", got)
	}
	if w := s.getWord(slotAt(cas20SlotName)); w[31] != byte(len("USD Coin")*2) {
		t.Errorf("short string length byte = %d, want %d", w[31], len("USD Coin")*2)
	}

	short31 := strings.Repeat("a", 31)
	s.setSymbol(short31)
	if got := strOf(s.symbol()); got != short31 {
		t.Errorf("31-byte string round-trip failed")
	}

	long := strings.Repeat("x", 100)
	s.setContractURI(long)
	if got := strOf(s.contractURI()); got != long {
		t.Errorf("long string round-trip failed: len %d", len(got))
	}
	if w := s.getWord(slotAt(cas20SlotContractURI)); w[31]&1 != 1 {
		t.Error("long string should set the low bit of the length slot")
	}
}

// Reads are length-bounded and would look correct either way; a leftover tail word
// diverges the state root from a Solidity reference.
func TestCAS20StorageStringShrink(t *testing.T) {
	s := newTestStorage(t)
	tailSlot := func(i uint64) common.Hash {
		base := new(uint256.Int).SetBytes(crypto.Keccak256(hashBytes(slotAt(cas20SlotName))))
		return common.Hash(base.AddUint64(base, i).Bytes32())
	}

	s.setName(strings.Repeat("x", 100))
	for i := range uint64(4) {
		if s.getWord(tailSlot(i)) == (common.Hash{}) {
			t.Fatalf("tail slot %d empty after writing a 100-byte name", i)
		}
	}

	s.setName(strings.Repeat("y", 40))
	if got := strOf(s.name()); got != strings.Repeat("y", 40) {
		t.Errorf("name = %q, want 40 y's", got)
	}
	for i := uint64(2); i < 4; i++ {
		if got := s.getWord(tailSlot(i)); got != (common.Hash{}) {
			t.Errorf("tail slot %d = %x after shrink, want cleared", i, got)
		}
	}

	s.setName("USD")
	if got := strOf(s.name()); got != "USD" {
		t.Errorf("name = %q, want USD", got)
	}
	for i := range uint64(4) {
		if got := s.getWord(tailSlot(i)); got != (common.Hash{}) {
			t.Errorf("tail slot %d = %x after shrink to short, want cleared", i, got)
		}
	}
}

// Every directed transition between the lengths where the encoding changes shape.
func TestCAS20StringBoundaryMatrix(t *testing.T) {
	lengths := []int{0, 1, 31, 32, 33, 64, 65}
	// The largest case needs 3 chunks; scanning 8 proves nothing lingers past the end.
	const scan = 8

	for _, from := range lengths {
		for _, to := range lengths {
			s := newTestStorage(t)
			slot := slotAt(cas20SlotName)
			dataSlot := func(i uint64) common.Hash {
				base := new(uint256.Int).SetBytes(crypto.Keccak256(slot[:]))
				return common.Hash(base.AddUint64(base, i).Bytes32())
			}
			before, after := strings.Repeat("a", from), strings.Repeat("b", to)

			s.setName(before)
			s.setName(after)

			if got := strOf(s.name()); got != after {
				t.Errorf("%d->%d: name = %q (len %d), want len %d", from, to, got, len(got), to)
			}
			word := s.getWord(slot)
			wantLong := to >= 32
			if isLong := word[31]&1 == 1; isLong != wantLong {
				t.Errorf("%d->%d: length slot long-marker = %v, want %v", from, to, isLong, wantLong)
			}
			if !wantLong && int(word[31]) != to*2 {
				t.Errorf("%d->%d: inline length byte = %d, want %d", from, to, word[31], to*2)
			}
			wantChunks := 0
			if wantLong {
				wantChunks = (to + 31) / 32
			}
			for i := range uint64(scan) {
				occupied := s.getWord(dataSlot(i)) != (common.Hash{})
				if want := i < uint64(wantChunks); occupied != want {
					t.Errorf("%d->%d: data slot %d occupied = %v, want %v (stale tail not released)",
						from, to, i, occupied, want)
				}
			}
		}
	}
}

// A long string's data root is a runtime keccak exactly as a mapping slot is.
func TestCAS20LongStringGas(t *testing.T) {
	statedb := newTestStateDB(t)
	token := cas20Addr(cas20VariantAsset, 1)
	gas := NewGasBudget(10_000_000)
	ctx := &PrecompileContext{StateDB: statedb, Self: token, gas: &gas}
	s := newMeteredCAS20Storage(ctx)

	charged := func(fn func()) uint64 {
		before := gas.Execution
		fn()
		return before - gas.Execution
	}
	const (
		keccak32 = params.Keccak256Gas + params.Keccak256WordGas // 32-byte preimage
		cold     = params.ColdSloadCostEIP2929
		warm     = params.WarmStorageReadCostEIP2929
		set      = params.SstoreSetGasEIP2200
	)

	if c := charged(func() { s.setName("USD Coin") }); c != cold+set {
		t.Errorf("short-string write charged %d, want %d", c, cold+set)
	}
	if c := charged(func() { _ = strOf(s.name()) }); c != warm {
		t.Errorf("short-string read charged %d, want %d", c, warm)
	}

	// Old length read, length slot rewritten, one data-root keccak, four cold sets.
	long := strings.Repeat("x", 100)
	if c := charged(func() { s.setName(long) }); c != 2*warm+keccak32+4*(cold+set) {
		t.Errorf("long-string write charged %d, want %d", c, 2*warm+keccak32+4*(cold+set))
	}
	if c := charged(func() { _ = strOf(s.name()) }); c != warm+keccak32+4*warm {
		t.Errorf("long-string read charged %d, want %d", c, warm+keccak32+4*warm)
	}

	// The data root must be derived once for the whole release.
	if c := charged(func() { s.setName("USD") }); c != 2*warm+keccak32+4*warm {
		t.Errorf("shrink-to-short charged %d, want %d", c, 2*warm+keccak32+4*warm)
	}
	if got := strOf(s.name()); got != "USD" {
		t.Errorf("name = %q, want USD", got)
	}
}

// The budget is shared by pointer, so only the flag can go missing, and it is the
// spawner's dispatcher that checks it.
func TestCAS20SpawnedContextPropagatesOutOfGas(t *testing.T) {
	statedb := newTestStateDB(t)
	gas := NewGasBudget(100)
	parent := &PrecompileContext{
		StateDB: statedb, Self: cas20Addr(cas20VariantAsset, 1),
		Caller: cas20Alice, DirectCall: true, gas: &gas,
	}
	child := parent.spawnBootstrap(cas20Addr(cas20VariantAsset, 2), cas20Alice)

	if parent.OutOfGas() || child.OutOfGas() {
		t.Fatal("fresh contexts must not report out of gas")
	}
	child.chargeGas(1_000_000) // more than the shared budget holds

	if !child.OutOfGas() {
		t.Error("child does not report out of gas after an unaffordable charge")
	}
	if !parent.OutOfGas() {
		t.Error("spawner does not see the child's exhaustion — it would report success over an empty budget")
	}
	if got := parent.gasLeft(); got != 0 {
		t.Errorf("shared budget = %d, want 0", got)
	}

	// One shared cell, not a copy: a context spawned after exhaustion starts exhausted.
	if later := parent.spawnBootstrap(cas20Addr(cas20VariantAsset, 3), cas20Alice); !later.OutOfGas() {
		t.Error("a context spawned from an exhausted frame does not start exhausted")
	}
	gas2 := NewGasBudget(100)
	p2 := &PrecompileContext{StateDB: statedb, Self: cas20Addr(cas20VariantAsset, 4), gas: &gas2}
	c2 := p2.spawnBootstrap(cas20Addr(cas20VariantAsset, 5), cas20Alice)
	p2.chargeGas(1_000_000)
	if !c2.OutOfGas() {
		t.Error("child spawned before the spawner's exhaustion does not observe it")
	}
}

// A bootstrap is the same EVM frame with a different Self, so both contexts
// spend the same gas budget.
func TestCAS20SpawnedContextSharesGasBudget(t *testing.T) {
	statedb := newTestStateDB(t)
	gas := NewGasBudget(10_000_000)
	parent := &PrecompileContext{
		StateDB: statedb, Self: cas20Addr(cas20VariantAsset, 1),
		Caller: cas20Alice, DirectCall: true, gas: &gas,
	}
	child := parent.spawnBootstrap(cas20Addr(cas20VariantAsset, 2), cas20Alice)

	before := parent.gas.Execution
	parent.chargeGas(700)
	child.chargeGas(300)

	if got := before - parent.gas.Execution; got != 1000 {
		t.Errorf("spawner gas used = %d, want 1000 — the child's charges are missing", got)
	}
	if got := before - child.gas.Execution; got != 1000 {
		t.Errorf("child gas used = %d, want 1000 — the budget is not shared", got)
	}
}

// The sentry refuses while gas remains, so unlike an unaffordable charge nothing
// downstream would catch it: the spawner would report success over a write that
// never landed.
func TestCAS20SpawnedContextPropagatesSentryRefusal(t *testing.T) {
	statedb := newTestStateDB(t)
	gas := NewGasBudget(params.SstoreSentryGasEIP2200)
	parent := &PrecompileContext{
		StateDB: statedb, Self: cas20Addr(cas20VariantAsset, 1),
		Caller: cas20Alice, DirectCall: true, gas: &gas,
	}
	token := cas20Addr(cas20VariantAsset, 2)
	child := parent.spawnBootstrap(token, cas20Alice)

	cas20Storage{state: statedb, token: token, ctx: child}.
		setWord(common.Hash{31: 7}, common.Hash{31: 1})

	if !child.OutOfGas() {
		t.Error("child does not report out of gas after a sentry-refused write")
	}
	if !parent.OutOfGas() {
		t.Error("spawner does not see the sentry refusal — it would report success over a skipped write")
	}
	if got := statedb.GetState(token, common.Hash{31: 7}); got != (common.Hash{}) {
		t.Errorf("refused write landed anyway: slot = %x", got)
	}
	if parent.gasLeft() == 0 {
		t.Error("sentry refusal should not itself drain the budget; the test would prove nothing")
	}
}

func TestCAS20StorageRefunds(t *testing.T) {
	clearing := params.SstoreClearsScheduleRefundEIP3529

	newCtx := func() (*testState, cas20Storage, *PrecompileContext) {
		statedb := newTestStateDB(t)
		token := cas20Addr(cas20VariantAsset, 1)
		// Without the sentinel Finalise below would reap the account, storage included.
		statedb.SetCode(token, CAS20MarkerCode, tracing.CodeChangeContractCreation)
		gas := NewGasBudget(10_000_000)
		ctx := &PrecompileContext{StateDB: statedb, Self: token, gas: &gas}
		return statedb, newMeteredCAS20Storage(ctx), ctx
	}
	slot := slotAt(cas20SlotTotalSupply)
	one := common.Hash{31: 1}
	two := common.Hash{31: 2}

	statedb, s, _ := newCtx()
	s.state.SetState(s.token, slot, one)
	statedb.Finalise(true) // commit, so `original` is non-zero
	s.setWord(slot, common.Hash{})
	if got := statedb.GetRefund(); got != clearing {
		t.Errorf("clear refund = %d, want %d", got, clearing)
	}

	s.setWord(slot, two)
	if got := statedb.GetRefund(); got != 0 {
		t.Errorf("refund after recreate = %d, want 0", got)
	}

	statedb, s, _ = newCtx()
	s.state.SetState(s.token, slot, one)
	statedb.Finalise(true)
	s.setWord(slot, two)
	s.setWord(slot, one)
	want := (params.SstoreResetGasEIP2200 - params.ColdSloadCostEIP2929) - params.WarmStorageReadCostEIP2929
	if got := statedb.GetRefund(); got != want {
		t.Errorf("reset-to-original refund = %d, want %d", got, want)
	}
}

func TestCAS20SstoreSentry(t *testing.T) {
	statedb := newTestStateDB(t)
	token := cas20Addr(cas20VariantAsset, 1)
	slot := slotAt(cas20SlotTotalSupply)

	// Warm and dirty, so the write itself would be cheap; exactly the stipend left.
	gas := NewGasBudget(params.SstoreSentryGasEIP2200)
	ctx := &PrecompileContext{StateDB: statedb, Self: token, gas: &gas}
	s := newMeteredCAS20Storage(ctx)
	statedb.AddSlotToAccessList(token, slot)

	s.setWord(slot, common.Hash{31: 7})
	if !ctx.OutOfGas() {
		t.Fatal("write at the stipend boundary must trip the sentry")
	}
	if got := statedb.GetState(token, slot); got != (common.Hash{}) {
		t.Fatalf("refused write still mutated state: %x", got)
	}

	gas2 := NewGasBudget(params.SstoreSentryGasEIP2200 + 1 + params.SstoreSetGasEIP2200)
	ctx2 := &PrecompileContext{StateDB: statedb, Self: token, gas: &gas2}
	newMeteredCAS20Storage(ctx2).setWord(slot, common.Hash{31: 7})
	if ctx2.OutOfGas() {
		t.Fatal("write above the stipend must be allowed")
	}
	if got := statedb.GetState(token, slot); got != (common.Hash{31: 7}) {
		t.Fatalf("state = %x, want 7", got)
	}
}

// BEP-702 3.14: a transfer must cost at least what bytecode pays for the same
// accesses, plus the derivation and dispatch work on top.
func TestCAS20GasNeverCheaperThanBytecode(t *testing.T) {
	_, evm := newCAS20EVM(t)
	creator := common.HexToAddress("0xfee")
	call := func(caller, to common.Address, input []byte) ([]byte, error) {
		ret, _, err := evm.Call(caller, to, input, NewGasBudget(5_000_000), uint256.NewInt(0))
		return ret, err
	}
	initCalls := [][]byte{
		cas20Call(selGrantRole, roleMint, addrKey(creator)),
		cas20Call(selMint, addrKey(cas20Alice), u256hash(1000)),
	}
	ret, err := call(creator, CAS20FactoryAddress, encodeCreateCAS20(cas20VariantAsset, common.HexToHash("0x9a"), creator, initCalls))
	if err != nil {
		t.Fatalf("createCAS20: %v", err)
	}
	token := common.BytesToAddress(ret)

	const budget = 1_000_000
	measure := func(to common.Address, amount uint64) uint64 {
		t.Helper()
		in := cas20Call(selTransfer, addrKey(to), u256hash(amount))
		if _, _, err := evm.Call(cas20Alice, token, in, NewGasBudget(budget), uint256.NewInt(0)); err != nil {
			t.Fatalf("warming transfer: %v", err)
		}
		_, left, err := evm.Call(cas20Alice, token, in, NewGasBudget(budget), uint256.NewInt(0))
		if err != nil {
			t.Fatalf("measured transfer: %v", err)
		}
		return uint64(budget) - left.Execution
	}

	keccak64 := params.Keccak256Gas + 2*params.Keccak256WordGas
	floor := 4*params.WarmStorageReadCostEIP2929 + 2*keccak64

	ordinary := measure(cas20Bob, 10)
	if ordinary < floor {
		t.Fatalf("warm transfer charged %d, below the bytecode floor %d", ordinary, floor)
	}

	// Self and zero-value transfers must cost exactly the ordinary shape: the floor
	// above is too loose to notice two skipped writes, and skipping them is
	// precisely the optimisation a later reader would reach for.
	for _, tc := range []struct {
		what   string
		to     common.Address
		amount uint64
	}{
		{"self-transfer", cas20Alice, 10},
		{"zero-value transfer", cas20Bob, 0},
		{"zero-value self-transfer", cas20Alice, 0},
	} {
		if charged := measure(tc.to, tc.amount); charged != ordinary {
			t.Errorf("%s charged %d, want %d — the same accesses bytecode would perform",
				tc.what, charged, ordinary)
		}
	}
}
