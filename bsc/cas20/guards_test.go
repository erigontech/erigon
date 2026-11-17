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

	"github.com/erigontech/erigon/execution/protocol/mdgas"
	"github.com/erigontech/erigon/execution/vm"

	"github.com/holiman/uint256"

	"github.com/erigontech/erigon/common"
)

// A non-empty bootstrap bundle, so the ReadOnly flag spawnBootstrap carries is covered too.
func TestCAS20CreateRejectsStaticCall(t *testing.T) {
	statedb, evm := newCAS20EVM(t)
	caller := common.HexToAddress("0xc4ea70")
	salt := common.HexToHash("0x51a71c")

	minter := common.HexToAddress("0x33333")
	bundle := [][]byte{cas20Call(selGrantRole, roleMint, addrKey(minter))}
	input := encodeCreateCAS20(cas20VariantAsset, salt, caller, bundle)
	budget := NewGasBudget(5_000_000)
	ret, left, err := evm.StaticCall(caller, CAS20FactoryAddress, input, budget)
	if !errors.Is(err, vm.ErrExecutionReverted) {
		t.Fatalf("STATICCALL createCAS20 gave %v, want a revert", err)
	}
	wantData, _ := finishCAS20(nil, revCAS20("StaticCallNotAllowed()", errSelStaticCallDenied))
	if !bytes.Equal(ret, wantData) {
		t.Errorf("returndata = %x, want StaticCallNotAllowed() = %x", ret, wantData)
	}
	if left.Execution == 0 {
		t.Error("the whole budget was consumed; a revert refunds what it did not spend")
	}

	addr := cas20DeriveAddress(cas20VariantAsset, caller, salt)
	if code := statedb.GetCode(addr); len(code) != 0 {
		t.Errorf("code at %s after a refused STATICCALL: %x", addr.Hex(), code)
	}
	if newUnmeteredCAS20Storage(statedb, addr).hasRole(roleMint, minter) {
		t.Error("the bundle's grantRole took effect under STATICCALL")
	}

	for _, tc := range []struct {
		name  string
		to    common.Address
		input []byte
	}{
		{"activate", CAS20ActivationRegistryAddress, cas20Call(selActivate, common.HexToHash("0xf2"))},
		{"deactivate", CAS20ActivationRegistryAddress, cas20Call(selDeactivate, featureCAS20Asset)},
		{"updateParam", CAS20ActivationRegistryAddress, encodeSetAdmin(common.HexToAddress("0xad4152"))},
		{"createPolicy", CAS20PolicyRegistryAddress, cas20Call(selCreatePolicy, addrKey(caller), u256hash(cas20PolicyBlocklist))},
	} {
		caller := cas20TestCaller
		if tc.name == "updateParam" {
			caller = CAS20GovHubAddress // the one caller authorization would admit
		}
		if _, _, err := evm.StaticCall(caller, tc.to, tc.input, NewGasBudget(5_000_000)); err == nil {
			t.Errorf("%s succeeded under STATICCALL", tc.name)
		}
	}
}

// An inflated adminCount makes the sole-admin protection see two admins where
// there is one, so the last becomes revocable.
func TestCAS20AdminCountGuards(t *testing.T) {
	admin := common.HexToAddress("0xad4149")
	second := common.HexToAddress("0x5ec0")

	statedb, token, run := newTokenWithEVM(t, 1, func(s cas20Storage) {
		s.setRole(roleDefaultAdmin, admin, true)
		s.setAdminCount(uint256.NewInt(1))
	})
	view := func() *uint256.Int { return newUnmeteredCAS20Storage(statedb, token).adminCount() }

	if _, err := run(admin, cas20Call(selGrantRole, roleDefaultAdmin, addrKey(admin))); err != nil {
		t.Fatalf("re-granting DEFAULT_ADMIN to its holder: %v", err)
	}
	if got := view(); got.Uint64() != 1 {
		t.Errorf("adminCount = %s after a duplicate grant, want 1", got)
	}

	if _, err := run(admin, cas20Call(selGrantRole, roleDefaultAdmin, addrKey(second))); err != nil {
		t.Fatalf("granting DEFAULT_ADMIN to a new account: %v", err)
	}
	if got := view(); got.Uint64() != 2 {
		t.Fatalf("adminCount = %s after a real grant, want 2", got)
	}

	third := common.HexToAddress("0x07")
	if _, err := run(admin, cas20Call(selRevokeRole, roleDefaultAdmin, addrKey(third))); err != nil {
		t.Fatalf("revoking DEFAULT_ADMIN from a non-holder: %v", err)
	}
	if got := view(); got.Uint64() != 2 {
		t.Errorf("adminCount = %s after revoking a non-holder, want 2", got)
	}
}

func TestCAS20AnnounceKeepsInnerRoleChecks(t *testing.T) {
	statedb, evm := newCAS20EVM(t)
	creator := common.HexToAddress("0xc4ea70")
	operator := common.HexToAddress("0x09e4a704")
	salt := common.HexToHash("0x0e2")

	call := func(caller, to common.Address, input []byte) ([]byte, error) {
		ret, _, err := evm.Call(caller, to, input, NewGasBudget(5_000_000), uint256.NewInt(0))
		return ret, err
	}

	ret, err := call(creator, CAS20FactoryAddress, encodeCreateCAS20(cas20VariantAsset, salt, creator,
		[][]byte{cas20Call(selGrantRole, roleOperator, addrKey(operator))}))
	if err != nil {
		t.Fatalf("createCAS20: %v", err)
	}
	token := common.BytesToAddress(ret)

	// Positive control, so a failure below cannot be blamed on the setup.
	if _, err := call(operator, token, encodeAnnounce(
		[][]byte{cas20Call(selUpdateMultiplier, u256hash(2_000_000_000_000_000_000))}, "2026-Q1-NAV")); err != nil {
		t.Fatalf("an OPERATOR_ROLE holder could not announce updateMultiplier: %v", err)
	}

	inner := encodeBatchMint([]common.Address{cas20Alice}, []uint64{1000})
	if _, err := call(operator, token, encodeAnnounce([][]byte{inner}, "2026-Q2-NAV")); err == nil {
		t.Fatal("an announcer without MINT_ROLE ran batchMint inside its announcement")
	}
	if newUnmeteredCAS20Storage(statedb, token).balanceOf(cas20Alice).Sign() != 0 {
		t.Error("batchMint took effect despite the announcement failing")
	}
}

// Through the EVM's own entry points, so the DirectCall flag evm.go sets is
// covered rather than assumed.
func TestCAS20NonDirectCallPlumbing(t *testing.T) {
	_, evm := newCAS20EVM(t)
	creator := common.HexToAddress("0xc4ea70")
	ret, _, err := evm.Call(creator, CAS20FactoryAddress,
		encodeCreateCAS20(cas20VariantAsset, common.HexToHash("0x9a11"), creator, nil),
		NewGasBudget(5_000_000), uint256.NewInt(0))
	if err != nil {
		t.Fatalf("createCAS20: %v", err)
	}
	token := common.BytesToAddress(ret)

	caller := common.HexToAddress("0xca11e5")
	origin := common.HexToAddress("0x0416019")

	wantDelegate, _ := finishCAS20(nil, revCAS20("DelegateCallNotAllowed()", errSelDelegateCallDenied))
	for _, tc := range []struct {
		name string
		run  func() ([]byte, mdgas.MdGas, error)
	}{
		{"CALLCODE", func() ([]byte, mdgas.MdGas, error) {
			return evm.CallCode(caller, token, cas20Call(selTotalSupply), NewGasBudget(100_000), uint256.NewInt(7))
		}},
		{"DELEGATECALL", func() ([]byte, mdgas.MdGas, error) {
			return evm.DelegateCall(origin, caller, token, cas20Call(selTotalSupply), NewGasBudget(100_000), uint256.NewInt(0))
		}},
	} {
		ret, left, err := tc.run()
		if !errors.Is(err, vm.ErrExecutionReverted) {
			t.Errorf("%s err = %v, want a revert", tc.name, err)
		}
		if !bytes.Equal(ret, wantDelegate) {
			t.Errorf("%s returndata = %x, want %x", tc.name, ret, wantDelegate)
		}
		if left.Execution == 0 {
			t.Errorf("%s consumed the whole budget; a revert refunds the rest", tc.name)
		}
	}
}

// The backstop under the hand-written ReadOnly guards: a missing one costs a
// revert rather than consensus.
func TestCAS20MeteringRefusesWritesInStaticFrames(t *testing.T) {
	statedb, evm := newCAS20EVM(t)
	ret, _, err := evm.Call(cas20TestCaller, CAS20FactoryAddress,
		encodeCreateCAS20(cas20VariantAsset, common.HexToHash("0x5711"), cas20TestCaller, nil),
		NewGasBudget(9_000_000), uint256.NewInt(0))
	if err != nil {
		t.Fatalf("createCAS20: %v", err)
	}
	token := common.BytesToAddress(ret)
	// Reaching the handler directly with readOnly set is the state of a frame whose
	// own guard is missing.
	tok := cas20Token{
		ctx: &PrecompileContext{
			evm: evm.EVM, StateDB: statedb, Self: token, Caller: cas20Alice,
			ReadOnly: true, DirectCall: true, Value: uint256.NewInt(0), gas: &mdgas.MdGas{Execution: 5_000_000},
		},
	}
	tok.s = newMeteredCAS20StorageAt(tok.ctx, token)

	if ok := tok.s.setWord(tok.s.allowanceSlot(cas20Alice, cas20Bob), u256hash(4242)); ok {
		t.Error("a metered write reported success in a read-only frame")
	}
	if got := statedb.GetState(token, tok.s.allowanceSlot(cas20Alice, cas20Bob)); got != (common.Hash{}) {
		t.Errorf("the allowance slot holds %x, want empty — a read-only frame wrote state", got)
	}
	if !tok.ctx.writeProtectionViolated() {
		t.Fatal("the refusal was not recorded, so the exit cannot report it")
	}
	if tok.ctx.OutOfGas() {
		t.Error("the frame was marked out of gas; a static write is a protection failure, not exhaustion")
	}

	_, err = finishCAS20Metered(tok.ctx, nil, nil)
	if !errors.Is(err, vm.ErrExecutionReverted) {
		t.Fatalf("exit err = %v, want the StaticCallNotAllowed revert", err)
	}

	before := len(statedb.Logs())
	if tok.ctx.chargeCodeWrite(tok.ctx.Self, CAS20MarkerCode) {
		t.Error("chargeCodeWrite reported success in a read-only frame")
	}
	if tok.ctx.AddLog([]common.Hash{cas20TopicApproval}, nil) {
		t.Error("AddLog reported success in a read-only frame")
	}
	if n := len(statedb.Logs()) - before; n != 0 {
		t.Errorf("a read-only frame emitted %d log(s)", n)
	}
}

// One guard stands for every registry write selector, so a selector added outside
// it must fail here.
func TestCAS20PolicyWritesAllRefuseStaticFrames(t *testing.T) {
	statedb, evm := newCAS20EVM(t)
	caller := cas20TestCaller
	admin := addrKey(caller)
	list := u256hash(cas20PolicyBlocklist)
	id := u256hash(uint64(cas20PolicyBlocklist)<<56 | 2)

	for _, tc := range []struct {
		name  string
		input []byte
	}{
		{"createPolicy", cas20Call(selCreatePolicy, admin, list)},
		{"createPolicyWithAccounts", encodeCreatePolicyWithAccounts(caller, cas20PolicyBlocklist, []common.Address{caller})},
		{"updateAllowlist", encodeUpdateList(selUpdateAllowlist, uint64(cas20PolicyAllowlist)<<56|2, true, []common.Address{caller})},
		{"updateBlocklist", encodeUpdateList(selUpdateBlocklist, uint64(cas20PolicyBlocklist)<<56|2, true, []common.Address{caller})},
		{"stageUpdateAdmin", cas20Call(selStageUpdateAdmin, id, admin)},
		{"finalizeUpdateAdmin", cas20Call(selFinalizeUpdateAdmin, id)},
		{"renounceAdmin", cas20Call(selRenounceAdmin, id)},
		{"createCompositePolicy", selCreateComposite[:]},
		{"updateComposite", selUpdateComposite[:]},
	} {
		before := statedb.IntermediateRoot(true)
		_, _, err := evm.StaticCall(caller, CAS20PolicyRegistryAddress, tc.input, NewGasBudget(5_000_000))
		if err == nil {
			t.Errorf("%s succeeded under STATICCALL", tc.name)
		}
		if after := statedb.IntermediateRoot(true); after != before {
			t.Errorf("%s changed state under STATICCALL: %x -> %x", tc.name, before, after)
		}
	}

	for _, sel := range [][4]byte{
		selCreatePolicy, selCreatePolicyWithAccounts, selUpdateAllowlist, selUpdateBlocklist,
		selStageUpdateAdmin, selFinalizeUpdateAdmin, selRenounceAdmin,
		selCreateComposite, selUpdateComposite,
	} {
		if _, _, err := evm.StaticCall(caller, CAS20PolicyRegistryAddress, sel[:], NewGasBudget(5_000_000)); err == nil {
			t.Errorf("selector %x accepted a static frame", sel)
		}
	}
}

// Every case compares the returndata: a bare revert cannot be told from Panic(0x21)
// by the error alone.
func TestCAS20MalformedArgsRevertEmpty(t *testing.T) {
	_, evm := newCAS20EVM(t)
	caller := cas20TestCaller

	assertEmpty := func(what string, ret []byte, err error) {
		t.Helper()
		if !errors.Is(err, vm.ErrExecutionReverted) {
			t.Errorf("%s: err = %v, want a revert", what, err)
			return
		}
		if len(ret) != 0 {
			t.Errorf("%s: returndata = %x, want empty. A decode failure is revert(0,0), "+
				"never Panic(0x21)", what, ret)
		}
	}

	outOfEnum := u256hash(uint64(cas20PolicyIntersect) + 1)
	for _, tc := range []struct {
		name  string
		input []byte
	}{
		{
			"createPolicy, policyType past the enum",
			cas20Call(selCreatePolicy, addrKey(caller), outOfEnum),
		},
		{
			"createPolicyWithAccounts, policyType past the enum",
			encodeCreatePolicyWithAccountsRaw(addrKey(caller), outOfEnum, nil),
		},
		{
			"createCompositePolicy, policyType past the enum",
			encodeComposite(selCreateComposite,
				[]common.Hash{addrKey(caller), outOfEnum}, []uint64{cas20PolicyAlwaysAllow}),
		},
	} {
		ret, _, err := evm.Call(caller, CAS20PolicyRegistryAddress, tc.input,
			NewGasBudget(5_000_000), uint256.NewInt(0))
		assertEmpty(tc.name, ret, err)
	}

	// A live policy, so the bool fails on its own encoding rather than on the policy's absence.
	ret, _, err := evm.Call(caller, CAS20PolicyRegistryAddress,
		cas20Call(selCreatePolicy, addrKey(caller), u256hash(cas20PolicyAllowlist)),
		NewGasBudget(5_000_000), uint256.NewInt(0))
	if err != nil {
		t.Fatalf("createPolicy: %v", err)
	}
	id := new(uint256.Int).SetBytes(ret).Uint64()
	ret, _, err = evm.Call(caller, CAS20PolicyRegistryAddress,
		encodeUpdateListRaw(selUpdateAllowlist, id, u256hash(2), []common.Hash{addrKey(cas20Alice)}),
		NewGasBudget(5_000_000), uint256.NewInt(0))
	assertEmpty("updateAllowlist, allowed = 2", ret, err)

	// Through evm.Call, not a bare dispatch: revert data is materialized on the way
	// out of the precompile, so an emptiness assertion over dispatch would hold vacuously.
	initCalls := [][]byte{cas20Call(selGrantRole, rolePause, addrKey(caller))}
	ret, _, err = evm.Call(caller, CAS20FactoryAddress,
		encodeCreateCAS20(cas20VariantAsset, common.HexToHash("0x9174d5"), caller, initCalls),
		NewGasBudget(9_000_000), uint256.NewInt(0))
	if err != nil {
		t.Fatalf("createCAS20: %v", err)
	}
	asset := common.BytesToAddress(ret)
	onToken := func(input []byte) ([]byte, error) {
		r, _, e := evm.Call(caller, asset, input, NewGasBudget(5_000_000), uint256.NewInt(0))
		return r, e
	}

	badFeature := uint64(cas20PauseSeize) + 1
	assertEmpty2 := func(what string, input []byte) {
		t.Helper()
		r, e := onToken(input)
		assertEmpty(what, r, e)
	}
	assertEmpty2("isPaused, feature past the enum", cas20Call(selIsPaused, u256hash(badFeature)))
	assertEmpty2("pause, element past the enum", cas20CallU8Array(selPause, byte(badFeature)))

	if _, err := onToken(cas20Call(selIsPaused, u256hash(uint64(cas20PauseSeize)))); err != nil {
		t.Errorf("isPaused with a valid feature: %v", err)
	}
	if _, err := onToken(cas20CallU8Array(selPause, byte(cas20PauseTransfer))); err != nil {
		t.Errorf("pause with a valid feature: %v", err)
	}

	dirtyID := common.Hash{}
	copy(dirtyID[:4], selIsPaused[:])
	dirtyID[31] = 1 // a nonzero byte outside the bytes4 value
	assertEmpty2("supportsInterface, bytes4 with nonzero padding",
		cas20Call(selSupportsInterface, dirtyID))

	clean := common.Hash{}
	copy(clean[:4], selIsPaused[:])
	if ret, err := onToken(cas20Call(selSupportsInterface, clean)); err != nil {
		t.Errorf("supportsInterface with a clean bytes4: %v", err)
	} else if !bytes.Equal(ret, encBool(false)) {
		t.Errorf("supportsInterface(unknown id) = %x, want false", ret)
	}
}

// Solidity decodes before any modifier runs, so an undecodable payload is reported
// as such whoever sends it and whatever the token's state.
func TestCAS20DecodePrecedesBusinessChecks(t *testing.T) {
	_, evm := newCAS20EVM(t)
	creator := cas20TestCaller
	// Both business checks would fire if they ran first.
	ret, _, err := evm.Call(creator, CAS20FactoryAddress,
		encodeCreateCAS20(cas20VariantAsset, common.HexToHash("0xde0de"), creator,
			[][]byte{
				cas20Call(selGrantRole, rolePause, addrKey(creator)),
				cas20CallU8Array(selPause, byte(cas20PauseMint)),
			}),
		NewGasBudget(9_000_000), uint256.NewInt(0))
	if err != nil {
		t.Fatalf("createCAS20: %v", err)
	}
	token := common.BytesToAddress(ret)

	stranger := common.HexToAddress("0x57")
	for _, tc := range []struct {
		name string
		sel  [4]byte
	}{
		{"announce, no role and no arguments", selAnnounce},
		{"batchMint, mint paused and no arguments", selBatchMint},
	} {
		ret, _, err := evm.Call(stranger, token, tc.sel[:], NewGasBudget(5_000_000), uint256.NewInt(0))
		if !errors.Is(err, vm.ErrExecutionReverted) {
			t.Errorf("%s: err = %v, want a revert", tc.name, err)
			continue
		}
		if len(ret) != 0 {
			t.Errorf("%s: returndata = %x, want empty. Arguments are decoded before "+
				"the caller or the token's state is looked at", tc.name, ret)
		}
	}

	if ret, _, _ := evm.Call(stranger, token,
		encodeAnnounceWith(nil, "id", "d", "u"), NewGasBudget(5_000_000), uint256.NewInt(0)); len(ret) < 4 ||
		[4]byte(ret[:4]) != errSelACUnauthorized {
		t.Errorf("announce with valid args: revert data %x, want AccessControlUnauthorizedAccount", ret)
	}
	emptyPair := append([]byte{}, selBatchMint[:]...)
	emptyPair = append(emptyPair, hashBytes(u256hash(0x40))...) // offset to recipients
	emptyPair = append(emptyPair, hashBytes(u256hash(0x60))...) // offset to amounts
	emptyPair = append(emptyPair, hashBytes(u256hash(0))...)    // len(recipients)
	emptyPair = append(emptyPair, hashBytes(u256hash(0))...)    // len(amounts)
	if ret, _, _ := evm.Call(stranger, token, emptyPair,
		NewGasBudget(5_000_000), uint256.NewInt(0)); len(ret) < 4 ||
		[4]byte(ret[:4]) != errSelContractPaused {
		t.Errorf("batchMint with valid args: revert data %x, want ContractPaused", ret)
	}
}
