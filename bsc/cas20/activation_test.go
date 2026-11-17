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
	"github.com/erigontech/erigon/execution/tracing"
)

func encodeSetAdmin(a common.Address) []byte {
	return encodeUpdateParamRaw(cas20ParamAdmin, a[:])
}

func encodeUpdateParamRaw(key string, value []byte) []byte {
	out := append([]byte{}, selUpdateParam[:]...)
	out = append(out, hashBytes(u256hash(0x40))...)
	out = append(out, hashBytes(u256hash(0x40+32+32*wordsOf(len(key))))...)
	out = append(out, hashBytes(u256hash(uint64(len(key))))...)
	out = append(out, padRight32([]byte(key))...)
	out = append(out, hashBytes(u256hash(uint64(len(value))))...)
	out = append(out, padRight32(value)...)
	return out
}

func wordsOf(n int) uint64 {
	if n == 0 {
		return 0
	}
	return uint64((n + 31) / 32)
}

func padRight32(b []byte) []byte {
	if len(b) == 0 {
		return nil
	}
	out := make([]byte, 32*((len(b)+31)/32))
	copy(out, b)
	return out
}

// Spelled out so a rename has to change the tests too.
const (
	featureNameAsset      = "bsc.cas20_asset"
	featureNameStablecoin = "bsc.cas20_stablecoin"
	featureNamePolicy     = "bsc.policy_registry"
)

func TestCAS20ActivationRegistry(t *testing.T) {
	_, evm := newCAS20EVM(t)
	reg := CAS20ActivationRegistryAddress
	gov := CAS20GovHubAddress
	admin := common.HexToAddress("0xad4149")
	stranger := common.HexToAddress("0x5747a9e")

	call := func(caller common.Address, input []byte) ([]byte, error) {
		ret, _, err := evm.Call(caller, reg, input, NewGasBudget(1_000_000), uint256.NewInt(0))
		return ret, err
	}
	feature := common.HexToHash("0xf1") // not seeded, so it starts inactive

	if ret, err := call(stranger, cas20Call(selIsActivated, feature)); err != nil {
		t.Fatalf("isActivated: %v", err)
	} else if !bytes.Equal(ret, encBool(false)) {
		t.Errorf("isActivated = %x, want false", ret)
	}
	if _, err := call(stranger, cas20Call(selCheckActivated, feature)); !errors.Is(err, vm.ErrExecutionReverted) {
		t.Errorf("checkActivated on an inactive feature: err = %v, want a revert", err)
	}

	if _, err := call(stranger, encodeSetAdmin(admin)); !errors.Is(err, vm.ErrExecutionReverted) {
		t.Fatalf("updateParam from a stranger: err = %v, want a revert", err)
	}
	if _, err := call(gov, encodeSetAdmin(admin)); err != nil {
		t.Fatalf("governance appointing the admin: %v", err)
	}
	if ret, err := call(stranger, cas20Call(selActivationAdm)); err != nil || !bytes.Equal(ret, hashBytes(addrKey(admin))) {
		t.Fatalf("admin() = %x err %v, want %s", ret, err, admin.Hex())
	}
	if _, err := call(gov, cas20Call(selActivate, feature)); !errors.Is(err, vm.ErrExecutionReverted) {
		t.Error("governance holds the appointment, not the switch; activate from GovHub must fail")
	}
	if _, err := call(stranger, cas20Call(selActivate, feature)); !errors.Is(err, vm.ErrExecutionReverted) {
		t.Fatal("activate from a stranger must fail")
	}
	if _, err := call(admin, cas20Call(selActivate, feature)); err != nil {
		t.Fatalf("activate: %v", err)
	}

	if _, err := call(admin, cas20Call(selActivate, feature)); !errors.Is(err, vm.ErrExecutionReverted) {
		t.Error("activating an active feature should report AlreadyActivated")
	}
	if _, err := call(admin, cas20Call(selDeactivate, feature)); err != nil {
		t.Fatalf("deactivate: %v", err)
	}
	if _, err := call(admin, cas20Call(selDeactivate, feature)); !errors.Is(err, vm.ErrExecutionReverted) {
		t.Error("deactivating an inactive feature should report FeatureNotActivated")
	}

	// Rotation is what makes a compromised admin key recoverable without a fork.
	next := common.HexToAddress("0xad4150")
	if _, err := call(gov, encodeSetAdmin(next)); err != nil {
		t.Fatalf("rotate: %v", err)
	}
	if _, err := call(admin, cas20Call(selActivate, feature)); !errors.Is(err, vm.ErrExecutionReverted) {
		t.Error("the replaced admin must no longer hold the switch")
	}
	if _, err := call(next, cas20Call(selActivate, feature)); err != nil {
		t.Errorf("the new admin cannot use the switch: %v", err)
	}
}

// Deactivation stops creation and policy writes, and nothing on a token that
// already exists (BEP-702 3.15).
func TestCAS20ActivationGates(t *testing.T) {
	statedb, evm := newCAS20EVM(t)
	creator := common.HexToAddress("0xc4ea70")

	call := func(caller, to common.Address, input []byte) ([]byte, error) {
		ret, _, err := evm.Call(caller, to, input, NewGasBudget(5_000_000), uint256.NewInt(0))
		return ret, err
	}

	initCalls := [][]byte{
		cas20Call(selGrantRole, roleMint, addrKey(creator)),
		cas20Call(selMint, addrKey(cas20Alice), u256hash(1000)),
	}
	ret, err := call(creator, CAS20FactoryAddress, encodeCreateCAS20(cas20VariantAsset, common.HexToHash("0xa1"), creator, initCalls))
	if err != nil {
		t.Fatalf("createCAS20 while activated: %v", err)
	}
	token := common.BytesToAddress(ret)

	if _, err := call(cas20TestCaller, CAS20ActivationRegistryAddress, cas20Call(selDeactivate, featureCAS20Asset)); err != nil {
		t.Fatalf("deactivate: %v", err)
	}

	if _, err := call(creator, CAS20FactoryAddress, encodeCreateCAS20(cas20VariantAsset, common.HexToHash("0xa2"), creator, nil)); !errors.Is(err, vm.ErrExecutionReverted) {
		t.Fatalf("createCAS20 while deactivated err = %v, want FeatureNotActivated", err)
	}
	if _, err := call(creator, CAS20FactoryAddress, encodeCreateCAS20(cas20VariantStablecoin, common.HexToHash("0xa3"), creator, nil)); err != nil {
		t.Fatalf("stablecoin creation must be unaffected: %v", err)
	}

	if _, err := call(cas20Alice, token, cas20Call(selTransfer, addrKey(cas20Bob), u256hash(10))); err != nil {
		t.Fatalf("transfer on a live token must not be gated: %v", err)
	}
	view := newUnmeteredCAS20Storage(statedb, token)
	if got := view.balanceOf(cas20Bob).Uint64(); got != 10 {
		t.Fatalf("bob balance = %d, want 10", got)
	}

	if _, err := call(cas20TestCaller, CAS20ActivationRegistryAddress, cas20Call(selDeactivate, featurePolicyRegistry)); err != nil {
		t.Fatalf("deactivate policy registry: %v", err)
	}
	if _, err := call(creator, CAS20PolicyRegistryAddress, cas20Call(selIsAuthorized, u256hash(0), addrKey(cas20Alice))); err != nil {
		t.Fatalf("isAuthorized must never be gated: %v", err)
	}
	if _, err := call(creator, CAS20PolicyRegistryAddress, cas20Call(selCreatePolicy, addrKey(creator), u256hash(cas20PolicyBlocklist))); !errors.Is(err, vm.ErrExecutionReverted) {
		t.Fatalf("createPolicy while deactivated err = %v, want FeatureNotActivated", err)
	}
}

// An address that already carries code is occupied, whoever put it there (BEP-702 3.4, 3.16).
func TestCAS20NeverOverwritesForeignCode(t *testing.T) {
	statedb, evm := newCAS20EVM(t)
	creator := common.HexToAddress("0xc0de")
	foreign := []byte{0x60, 0x00, 0x60, 0x00, 0xf3} // a plausible runtime stub

	call := func(caller, to common.Address, input []byte) ([]byte, error) {
		ret, _, err := evm.Call(caller, to, input, NewGasBudget(5_000_000), uint256.NewInt(0))
		return ret, err
	}

	salt := common.HexToHash("0xf01e")
	predicted := cas20DeriveAddress(cas20VariantAsset, creator, salt)
	statedb.SetCode(predicted, foreign, tracing.CodeChangeContractCreation)
	foreignHash := statedb.GetCodeHash(predicted)

	if _, err := call(creator, CAS20FactoryAddress, encodeCreateCAS20(cas20VariantAsset, salt, creator, nil)); !errors.Is(err, vm.ErrExecutionReverted) {
		t.Fatalf("createCAS20 at an occupied address err = %v, want TokenAlreadyExists", err)
	}
	if got := statedb.GetCodeHash(predicted); got != foreignHash {
		t.Fatalf("foreign code was overwritten: hash %x, want %x", got, foreignHash)
	}

	reg := CAS20PolicyRegistryAddress
	statedb.SetCode(reg, foreign, tracing.CodeChangeContractCreation)
	seedCAS20Sentinel(statedb, reg)
	if got := statedb.GetCodeHash(reg); got != foreignHash {
		t.Fatalf("registry foreign code was overwritten: hash %x, want %x", got, foreignHash)
	}
}

// GovHub swallows a target's revert into an event, so a key accepted by accident
// would read as a governance action that changed nothing.
func TestCAS20ParamKeySpaceIsClosed(t *testing.T) {
	_, evm := newCAS20EVM(t)
	gov := CAS20GovHubAddress
	call := func(input []byte) error {
		_, _, err := evm.Call(gov, CAS20ActivationRegistryAddress, input, NewGasBudget(1_000_000), uint256.NewInt(0))
		return err
	}
	addr := addrBytes(common.HexToAddress("0xad4151"))

	for _, tc := range []struct {
		name   string
		key    string
		value  []byte
		accept bool
	}{
		{"the one key", cas20ParamAdmin, addr, true},
		{"miscased", "Admin", addr, false},
		{"an unknown key", "bsc.something_later", addr, false},
		{"an empty key", "", addr, false},
		{"a padded address", cas20ParamAdmin, hashBytes(common.HexToHash("0xad4151")), false},
		{"a short value", cas20ParamAdmin, addr[:19], false},
		{"the zero address", cas20ParamAdmin, make([]byte, 20), false},
	} {
		err := call(encodeUpdateParamRaw(tc.key, tc.value))
		if tc.accept && err != nil {
			t.Errorf("%s: err = %v, want accepted", tc.name, err)
		}
		if !tc.accept && !errors.Is(err, vm.ErrExecutionReverted) {
			t.Errorf("%s: err = %v, want a revert", tc.name, err)
		}
	}
}

// Between the fork and governance's first appointment the admin slot is empty;
// equality alone would let the zero address hold the switch.
func TestCAS20ActivationAdminMustBeSet(t *testing.T) {
	statedb := newTestStateDB(t)
	cfg := *cas20TestChainConfig()
	evm := newTestEVM(cas20BlockContext(1), statedb, &cfg)
	if _, ok := evm.precompile(CAS20ActivationRegistryAddress); !ok {
		t.Fatal("the activation registry does not resolve; the fork gate is in the way")
	}
	reg := cas20Storage{state: statedb, token: CAS20ActivationRegistryAddress}
	if got := reg.getWord(actSlot(actSlotAdmin)); got != (common.Hash{}) {
		t.Fatalf("the admin slot holds %x, want empty for this test to mean anything", got)
	}

	feature := common.HexToHash("0xf3")
	for _, caller := range []common.Address{{}, common.HexToAddress("0x57")} {
		_, _, err := evm.Call(caller, CAS20ActivationRegistryAddress,
			cas20Call(selActivate, feature), NewGasBudget(5_000_000), uint256.NewInt(0))
		if !errors.Is(err, vm.ErrExecutionReverted) {
			t.Errorf("activate from %s against an empty admin slot: err = %v, want a revert",
				caller.Hex(), err)
		}
	}
	if got := reg.getWord(mappingSlot(actSlot(actSlotFeatures), feature)); got != (common.Hash{}) {
		t.Errorf("the feature reads %x, want off", got)
	}
}

// A feature id is keccak256 of its name, so a rename silently returns an
// activated feature to inactive. Every other test uses the Go variables.
func TestCAS20FeatureNamesArePinned(t *testing.T) {
	for _, tc := range []struct {
		name string
		id   common.Hash
	}{
		{featureNameAsset, featureCAS20Asset},
		{featureNameStablecoin, featureCAS20Stablecoin},
		{featureNamePolicy, featurePolicyRegistry},
	} {
		if got := keccakHash([]byte(tc.name)); got != tc.id {
			t.Errorf("keccak256(%q) = %x, want %x", tc.name, got, tc.id)
		}
	}

	// The policy registry's feature name and its storage namespace are the same
	// string for two unrelated derivations; a rename must not carry one into the other.
	if featureNamePolicy != cas20PolicyNamespace {
		t.Errorf("the policy feature name %q and namespace %q have diverged; if that is "+
			"deliberate, both values changed and every integrator's activate() call with it",
			featureNamePolicy, cas20PolicyNamespace)
	}
	for _, tc := range []struct{ feature, namespace string }{
		{featureNameAsset, cas20AssetNamespace},
		{featureNameStablecoin, cas20StablecoinNamespace},
	} {
		if tc.feature == tc.namespace {
			t.Errorf("feature %q now equals its storage namespace; they are independent "+
				"derivations and a namespace rename must not move the feature id", tc.feature)
		}
	}
}

// The negative control comes first: without it the seeded case would prove only
// that a byte was written.
func TestCAS20SeededSentinelSurvivesClearing(t *testing.T) {
	slot, value := common.HexToHash("0x01"), common.HexToHash("0x2a")
	registries := []common.Address{CAS20ActivationRegistryAddress, CAS20PolicyRegistryAddress}

	bare := newTestStateDB(t)
	for _, addr := range registries {
		bare.SetState(addr, slot, value)
	}
	bare.Finalise(true)
	for _, addr := range registries {
		if bare.Exist(addr) || bare.GetState(addr, slot) != (common.Hash{}) {
			t.Fatalf("%s with storage and no code survived a clearing pass; the hazard this test "+
				"guards against is not present in this StateDB", addr.Hex())
		}
	}

	seeded := newTestStateDB(t)
	seedCAS20Activation(seeded)
	for _, addr := range registries {
		seeded.SetState(addr, slot, value)
	}
	seeded.Finalise(true)
	for _, addr := range registries {
		if got := seeded.GetCodeHash(addr); got != cas20MarkerCodeHash {
			t.Fatalf("%s after a clearing pass: code hash %x, want the sentinel %x", addr.Hex(), got, cas20MarkerCodeHash)
		}
		if got := seeded.GetState(addr, slot); got != value {
			t.Fatalf("%s lost its storage across a clearing pass despite the sentinel: %x", addr.Hex(), got)
		}
	}
}
