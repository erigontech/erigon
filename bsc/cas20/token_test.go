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
	"bytes"
	"errors"
	"testing"

	"github.com/erigontech/erigon/execution/vm"

	"github.com/holiman/uint256"

	"github.com/erigontech/erigon/common"
)

func u256hash(n uint64) common.Hash { return common.Hash(uint256.NewInt(n).Bytes32()) }

var (
	cas20Alice = common.HexToAddress("0xa11ce")
	cas20Bob   = common.HexToAddress("0xb0b")
	cas20Carol = common.HexToAddress("0xca401")
)

func TestCAS20TokenDispatch(t *testing.T) {
	statedb := newTestStateDB(t)
	token := cas20Addr(cas20VariantAsset, 1)

	view := newUnmeteredCAS20Storage(statedb, token)
	view.setName("Test Token")
	view.setSymbol("TT")
	view.setTotalSupply(uint256.NewInt(1000))
	view.setBalance(cas20Alice, uint256.NewInt(1000))

	run := func(caller common.Address, ro bool, input []byte) ([]byte, error) {
		gas := NewGasBudget(1_000_000)
		ctx := &PrecompileContext{StateDB: statedb, Self: token, Caller: caller, DirectCall: true, ReadOnly: ro, gas: &gas}
		return newCAS20Token(ctx, 18).dispatch(input)
	}
	readU := func(input []byte) uint64 {
		t.Helper()
		ret, err := run(cas20Alice, true, input)
		if err != nil {
			t.Fatalf("view call err: %v", err)
		}
		return new(uint256.Int).SetBytes(ret).Uint64()
	}
	wantU := func(label string, got, want uint64) {
		t.Helper()
		if got != want {
			t.Fatalf("%s = %d, want %d", label, got, want)
		}
	}

	if ret, err := run(cas20Alice, true, cas20Call(selName)); err != nil || string(bytes.TrimRight(ret[64:], "\x00")) != "Test Token" {
		t.Fatalf("name() = %q, err %v", ret, err)
	}
	wantU("decimals", readU(cas20Call(selDecimals)), 18)
	wantU("totalSupply", readU(cas20Call(selTotalSupply)), 1000)
	wantU("balanceOf(alice)", readU(cas20Call(selBalanceOf, addrKey(cas20Alice))), 1000)

	if ret, err := run(cas20Alice, false, cas20Call(selTransfer, addrKey(cas20Bob), u256hash(100))); err != nil || !bytes.Equal(ret, encBool(true)) {
		t.Fatalf("transfer ret %x err %v", ret, err)
	}
	wantU("balanceOf(alice)", readU(cas20Call(selBalanceOf, addrKey(cas20Alice))), 900)
	wantU("balanceOf(bob)", readU(cas20Call(selBalanceOf, addrKey(cas20Bob))), 100)

	if _, err := run(cas20Alice, false, cas20Call(selApprove, addrKey(cas20Carol), u256hash(50))); err != nil {
		t.Fatalf("approve err %v", err)
	}
	wantU("allowance", readU(cas20Call(selAllowance, addrKey(cas20Alice), addrKey(cas20Carol))), 50)
	if _, err := run(cas20Carol, false, cas20Call(selTransferFrom, addrKey(cas20Alice), addrKey(cas20Bob), u256hash(30))); err != nil {
		t.Fatalf("transferFrom err %v", err)
	}
	wantU("balanceOf(alice)", readU(cas20Call(selBalanceOf, addrKey(cas20Alice))), 870)
	wantU("balanceOf(bob)", readU(cas20Call(selBalanceOf, addrKey(cas20Bob))), 130)
	wantU("allowance", readU(cas20Call(selAllowance, addrKey(cas20Alice), addrKey(cas20Carol))), 20)

	if _, err := run(cas20Carol, false, cas20Call(selTransferFrom, addrKey(cas20Alice), addrKey(cas20Bob), u256hash(100))); !errors.Is(err, vm.ErrExecutionReverted) {
		t.Fatalf("over-allowance err = %v, want revert", err)
	}
	if _, err := run(cas20Bob, false, cas20Call(selTransfer, addrKey(cas20Alice), u256hash(1e9))); !errors.Is(err, vm.ErrExecutionReverted) {
		t.Fatalf("over-balance err = %v, want revert", err)
	}
	if _, err := run(cas20Alice, true, cas20Call(selTransfer, addrKey(cas20Bob), u256hash(1))); !errors.Is(err, vm.ErrWriteProtection) {
		t.Fatalf("readonly transfer err = %v, want write protection", err)
	}
	if _, err := run(cas20Alice, true, []byte{0xde, 0xad, 0xbe, 0xef}); !errors.Is(err, vm.ErrExecutionReverted) {
		t.Fatalf("unknown selector err = %v, want revert", err)
	}
}

func TestCAS20TokenPauseBlocksTransfer(t *testing.T) {
	statedb := newTestStateDB(t)
	token := cas20Addr(cas20VariantAsset, 1)
	view := newUnmeteredCAS20Storage(statedb, token)
	view.setBalance(cas20Alice, uint256.NewInt(100))
	view.setPaused(uint256.NewInt(1 << cas20PauseTransfer))

	gas := NewGasBudget(1_000_000)
	ctx := &PrecompileContext{StateDB: statedb, Self: token, Caller: cas20Alice, DirectCall: true, gas: &gas}
	if _, err := newCAS20Token(ctx, 18).dispatch(cas20Call(selTransfer, addrKey(cas20Bob), u256hash(1))); !errors.Is(err, vm.ErrExecutionReverted) {
		t.Fatalf("paused transfer err = %v, want revert", err)
	}
}

func TestCAS20EndToEndTransfer(t *testing.T) {
	statedb := newTestStateDB(t)
	token := cas20Addr(cas20VariantAsset, 1)
	statedb.SetCode(token, CAS20MarkerCode, 0)
	newUnmeteredCAS20Storage(statedb, token).setBalance(cas20Alice, uint256.NewInt(1000))

	cfg := *cas20TestChainConfig()

	txHash := common.HexToHash("0x1234")
	statedb.SetTxContext(txHash, 0)

	bc := cas20BlockContext(1)
	evm := newTestEVM(bc, statedb, &cfg)
	if !evm.cas20Enabled() {
		t.Fatal("CAS20 must be enabled for the precompile to resolve")
	}

	input := cas20Call(selTransfer, addrKey(cas20Bob), u256hash(250))
	ret, _, err := evm.Call(cas20Alice, token, input, NewGasBudget(1_000_000), uint256.NewInt(0))
	if err != nil {
		t.Fatalf("evm.Call transfer err: %v", err)
	}
	if !bytes.Equal(ret, encBool(true)) {
		t.Fatalf("transfer returned %x, want true", ret)
	}

	view := newUnmeteredCAS20Storage(statedb, token)
	if got := view.balanceOf(cas20Alice).Uint64(); got != 750 {
		t.Errorf("alice balance = %d, want 750", got)
	}
	if got := view.balanceOf(cas20Bob).Uint64(); got != 250 {
		t.Errorf("bob balance = %d, want 250", got)
	}

	logs := statedb.GetLogs(txHash, 1, common.Hash{}, 1)
	if len(logs) != 1 {
		t.Fatalf("got %d logs, want 1", len(logs))
	}
	log := logs[0]
	if log.Address != token || log.Topics[0] != cas20TopicTransfer {
		t.Errorf("unexpected log: addr %s topic %s", log.Address.Hex(), log.Topics[0].Hex())
	}
	if log.Topics[1] != addrKey(cas20Alice) || log.Topics[2] != addrKey(cas20Bob) {
		t.Errorf("Transfer topics from/to mismatch")
	}
}

// Which malformed encodings are refused and which are not is a decision, not an accident.
func TestCAS20CalldataStrictnessProfile(t *testing.T) {
	statedb, evm := newCAS20EVM(t)
	creator := common.HexToAddress("0xdec0de")
	ret, _, err := evm.Call(creator, CAS20FactoryAddress,
		encodeCreateCAS20(cas20VariantAsset, common.HexToHash("0x57"), creator, [][]byte{
			cas20Call(selGrantRole, roleMint, addrKey(creator)),
			cas20Call(selMint, addrKey(cas20Alice), u256hash(1000)),
		}), NewGasBudget(9_000_000), uint256.NewInt(0))
	if err != nil {
		t.Fatalf("createCAS20: %v", err)
	}
	token := common.BytesToAddress(ret)
	call := func(caller common.Address, input []byte) ([]byte, error) {
		r, _, e := evm.Call(caller, token, input, NewGasBudget(1_000_000), uint256.NewInt(0))
		return r, e
	}

	short := cas20Call(selTransfer, addrKey(cas20Bob), u256hash(1))
	if _, err := call(cas20Alice, short[:len(short)-1]); !errors.Is(err, vm.ErrExecutionReverted) {
		t.Errorf("truncated transfer args: err = %v, want a revert", err)
	}
	dirty := addrKey(cas20Bob)
	dirty[0] = 0x01
	if _, err := call(cas20Alice, cas20Call(selTransfer, dirty, u256hash(1))); !errors.Is(err, vm.ErrExecutionReverted) {
		t.Errorf("dirty address high bits: err = %v, want a revert", err)
	}

	// Accepted, as Solidity accepts it: a whole extra word after a complete argument list.
	before := newUnmeteredCAS20Storage(statedb, token).balanceOf(cas20Bob).Uint64()
	trailing := append(cas20Call(selTransfer, addrKey(cas20Bob), u256hash(7)), hashBytes(u256hash(0xdead))...)
	if r, err := call(cas20Alice, trailing); err != nil || !bytes.Equal(r, encBool(true)) {
		t.Fatalf("transfer with trailing data: ret %x err %v, want success — Solidity ignores "+
			"extra calldata, so refusing it would diverge from the contract this replaces", r, err)
	}
	if got := newUnmeteredCAS20Storage(statedb, token).balanceOf(cas20Bob).Uint64(); got != before+7 {
		t.Errorf("bob's balance = %d, want %d; the trailing word changed how the args decoded",
			got, before+7)
	}
}

func TestCAS20TransferFromSelfSpendsAllowance(t *testing.T) {
	statedb, evm := newCAS20EVM(t)
	admin := cas20TestCaller

	// A deny-all executor policy, so the self shortcut is observable.
	ret, _, err := evm.Call(admin, CAS20PolicyRegistryAddress,
		cas20Call(selCreatePolicy, addrKey(admin), u256hash(cas20PolicyAllowlist)),
		NewGasBudget(9_000_000), uint256.NewInt(0))
	if err != nil {
		t.Fatalf("createPolicy: %v", err)
	}
	empty := new(uint256.Int).SetBytes(ret).Uint64()

	ret, _, err = evm.Call(admin, CAS20FactoryAddress,
		encodeCreateCAS20(cas20VariantAsset, common.HexToHash("0x5e1f"), admin, [][]byte{
			cas20Call(selGrantRole, roleMint, addrKey(admin)),
			cas20Call(selMint, addrKey(cas20Alice), u256hash(1000)),
			cas20Call(selUpdatePolicy, scopeTransferExecutor, wU64(empty)),
		}), NewGasBudget(9_000_000), uint256.NewInt(0))
	if err != nil {
		t.Fatalf("createCAS20: %v", err)
	}
	token := common.BytesToAddress(ret)
	view := newUnmeteredCAS20Storage(statedb, token)
	send := func(caller common.Address, input []byte) ([]byte, error) {
		r, _, e := evm.Call(caller, token, input, NewGasBudget(1_000_000), uint256.NewInt(0))
		return r, e
	}

	out, err := send(cas20Alice, cas20Call(selTransferFrom, addrKey(cas20Alice), addrKey(cas20Bob), u256hash(40)))
	if !errors.Is(err, vm.ErrExecutionReverted) {
		t.Fatalf("self transferFrom without an approval: err = %v, want a revert", err)
	}
	if len(out) < 4 || [4]byte(out[:4]) != errSelInsufficientAllow {
		t.Errorf("revert = %x, want InsufficientAllowance", out[:min(4, len(out))])
	}

	if _, err := send(cas20Alice, cas20Call(selApprove, addrKey(cas20Alice), u256hash(100))); err != nil {
		t.Fatalf("self approve: %v", err)
	}
	if out, err := send(cas20Alice, cas20Call(selTransferFrom, addrKey(cas20Alice), addrKey(cas20Bob), u256hash(40))); err != nil ||
		!bytes.Equal(out, encBool(true)) {
		t.Fatalf("self transferFrom with an approval: ret %x err %v", out, err)
	}
	if got := view.allowance(cas20Alice, cas20Alice).Uint64(); got != 60 {
		t.Errorf("self-allowance = %d, want 60 — it must be spent like any other", got)
	}
	if got := view.balanceOf(cas20Bob).Uint64(); got != 40 {
		t.Errorf("bob's balance = %d, want 40", got)
	}

	// The contrast: a third party with a full allowance is still refused by the executor policy.
	if _, err := send(admin, cas20Call(selApprove, addrKey(cas20Carol), u256hash(100))); err != nil {
		t.Fatalf("approve carol: %v", err)
	}
	if _, err := send(cas20Carol, cas20Call(selTransferFrom, addrKey(admin), addrKey(cas20Bob), u256hash(1))); !errors.Is(err, vm.ErrExecutionReverted) {
		t.Errorf("a delegated transfer under a deny-all executor policy err = %v, want a revert", err)
	}
}
