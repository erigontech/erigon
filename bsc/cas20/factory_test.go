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

func rightPad32(b []byte) []byte {
	out := make([]byte, (len(b)+31)/32*32)
	copy(out, b)
	return out
}

func cas20AssetParams(name, symbol string, admin common.Address, decimals byte) []byte {
	return abiEncodeStruct(
		abiWord(wU8(cas20ParamsVersion)),
		abiString(name),
		abiString(symbol),
		abiWord(addrKey(admin)),
		abiWord(wU8(decimals)),
	)
}

func cas20StablecoinParams(name, symbol string, admin common.Address, currency string) []byte {
	return abiEncodeStruct(
		abiWord(wU8(cas20ParamsVersion)),
		abiString(name),
		abiString(symbol),
		abiWord(addrKey(admin)),
		abiString(currency),
	)
}

func encodeCreateCAS20(variant byte, salt common.Hash, admin common.Address, calls [][]byte) []byte {
	params := cas20AssetParams("Test Token", "TT", admin, 18)
	if variant == cas20VariantStablecoin {
		params = cas20StablecoinParams("Test Stable", "TS", admin, "USD")
	}
	return encodeCreateCAS20WithParams(variant, salt, params, calls)
}

func encodeCreateCAS20WithParams(variant byte, salt common.Hash, params []byte, calls [][]byte) []byte {
	elems := make([][]byte, len(calls))
	for i, c := range calls {
		elems[i] = append(hashBytes(u256hash(uint64(len(c)))), rightPad32(c)...)
	}
	arr := append([]byte{}, hashBytes(u256hash(uint64(len(calls))))...)
	cur := uint64(len(calls) * 32) // element offsets are relative to just after the length word
	for _, e := range elems {
		arr = append(arr, hashBytes(u256hash(cur))...)
		cur += uint64(len(e))
	}
	for _, e := range elems {
		arr = append(arr, e...)
	}

	out := append([]byte{}, selCreateCAS20[:]...)
	return append(out, encodeTuple(
		abiWord(u256hash(uint64(variant))),
		abiWord(salt),
		abiBytes(params),
		abiPart{dynamic: true, tail: arr},
	)...)
}

func TestCAS20Factory(t *testing.T) {
	statedb := newTestStateDB(t)
	cfg := *cas20TestChainConfig()
	bc := cas20BlockContext(1)
	seedActivation(statedb, cas20TestCaller)
	evm := newTestEVM(bc, statedb, &cfg)

	creator := common.HexToAddress("0xc4ea70")
	minter := common.HexToAddress("0x33333")
	salt := common.HexToHash("0x01")

	call := func(caller, to common.Address, input []byte) ([]byte, error) {
		ret, _, err := evm.Call(caller, to, input, NewGasBudget(5_000_000), uint256.NewInt(0))
		return ret, err
	}

	predicted, err := call(creator, CAS20FactoryAddress, cas20Call(selGetCAS20Address, u256hash(cas20VariantAsset), addrKey(creator), salt))
	if err != nil {
		t.Fatalf("getCAS20Address: %v", err)
	}
	want := cas20DeriveAddress(cas20VariantAsset, creator, salt)
	if common.BytesToAddress(predicted) != want {
		t.Fatalf("getCAS20Address = %s, want %s", common.BytesToAddress(predicted).Hex(), want.Hex())
	}

	initCalls := [][]byte{
		cas20Call(selGrantRole, roleMint, addrKey(minter)),
		cas20Call(selMint, addrKey(cas20Alice), u256hash(1000)),
	}
	ret, err := call(creator, CAS20FactoryAddress, encodeCreateCAS20(cas20VariantAsset, salt, creator, initCalls))
	if err != nil {
		t.Fatalf("createCAS20: %v", err)
	}
	token := common.BytesToAddress(ret)
	if token != want {
		t.Fatalf("createCAS20 returned %s, want %s", token.Hex(), want.Hex())
	}

	if r, _ := call(creator, CAS20FactoryAddress, cas20Call(selIsCAS20Initialized, addrKey(token))); !bytes.Equal(r, encBool(true)) {
		t.Fatal("token should be initialized")
	}

	view := newUnmeteredCAS20Storage(statedb, token)
	if view.totalSupply().Uint64() != 1000 || view.balanceOf(cas20Alice).Uint64() != 1000 {
		t.Fatalf("supply %d aliceBal %d, want 1000/1000", view.totalSupply().Uint64(), view.balanceOf(cas20Alice).Uint64())
	}
	if !view.hasRole(roleDefaultAdmin, creator) || view.adminCount().Uint64() != 1 {
		t.Fatal("creator should be sole DEFAULT_ADMIN")
	}
	if !view.hasRole(roleMint, minter) {
		t.Fatal("minter should hold MINT_ROLE")
	}

	if r, err := call(cas20Alice, token, cas20Call(selTransfer, addrKey(cas20Bob), u256hash(400))); err != nil || !bytes.Equal(r, encBool(true)) {
		t.Fatalf("transfer via created token: ret %x err %v", r, err)
	}
	if view.balanceOf(cas20Bob).Uint64() != 400 {
		t.Fatalf("bob balance %d, want 400", view.balanceOf(cas20Bob).Uint64())
	}

	if _, err := call(creator, CAS20FactoryAddress, encodeCreateCAS20(cas20VariantAsset, salt, creator, nil)); !errors.Is(err, vm.ErrExecutionReverted) {
		t.Fatalf("duplicate createCAS20 err = %v, want revert", err)
	}
}

func TestCAS20FactoryOwnerless(t *testing.T) {
	statedb := newTestStateDB(t)
	cfg := *cas20TestChainConfig()
	bc := cas20BlockContext(1)
	seedActivation(statedb, cas20TestCaller)
	evm := newTestEVM(bc, statedb, &cfg)
	creator := common.HexToAddress("0xc4ea70")
	salt := common.HexToHash("0x02")

	initCalls := [][]byte{cas20Call(selGrantRole, roleMint, addrKey(creator))}
	ret, _, err := evm.Call(creator, CAS20FactoryAddress,
		encodeCreateCAS20(cas20VariantStablecoin, salt, common.Address{}, initCalls),
		NewGasBudget(5_000_000), uint256.NewInt(0))
	if err != nil {
		t.Fatalf("createCAS20 ownerless: %v", err)
	}
	token := common.BytesToAddress(ret)
	view := newUnmeteredCAS20Storage(statedb, token)
	if !view.adminCount().IsZero() {
		t.Fatalf("adminCount = %d, want 0 (ownerless)", view.adminCount().Uint64())
	}
	if !view.hasRole(roleMint, creator) {
		t.Fatal("bootstrap should have granted MINT despite ownerless")
	}
	if _, _, err := evm.Call(creator, token, cas20Call(selGrantRole, roleBurn, addrKey(creator)),
		NewGasBudget(1_000_000), uint256.NewInt(0)); !errors.Is(err, vm.ErrExecutionReverted) {
		t.Fatalf("grant on ownerless token err = %v, want revert", err)
	}
}

func TestCAS20CreateParams(t *testing.T) {
	statedb, evm := newCAS20EVM(t)
	creator := common.HexToAddress("0xc4ea70")
	call := func(input []byte) ([]byte, error) {
		ret, _, err := evm.Call(creator, CAS20FactoryAddress, input, NewGasBudget(5_000_000), uint256.NewInt(0))
		return ret, err
	}
	salt := u256hash

	// Also invalid downstream, so the version is what must be reported.
	bad := abiEncodeStruct(abiWord(wU8(2)), abiString("N"), abiString("S"), abiWord(addrKey(creator)), abiWord(wU8(3)))
	ret, err := call(encodeCreateCAS20WithParams(cas20VariantAsset, salt(1), bad, nil))
	if !errors.Is(err, vm.ErrExecutionReverted) {
		t.Fatalf("bad version err = %v, want revert", err)
	}
	want := append(append([]byte{}, errSelUnsupportedVersion[:]...), hashBytes(wU8(2))...)
	want = append(want, hashBytes(wU8(cas20VariantAsset))...)
	if !bytes.Equal(ret, want) {
		t.Fatalf("revert data = %x, want UnsupportedVersion(2, ASSET) = %x", ret, want)
	}

	for _, d := range []byte{5, 19} {
		p := cas20AssetParams("N", "S", creator, d)
		ret, err := call(encodeCreateCAS20WithParams(cas20VariantAsset, salt(uint64(d)), p, nil))
		if !errors.Is(err, vm.ErrExecutionReverted) {
			t.Fatalf("decimals %d err = %v, want revert", d, err)
		}
		want := append(append([]byte{}, errSelInvalidDecimals[:]...), hashBytes(wU8(d))...)
		if !bytes.Equal(ret, want) {
			t.Fatalf("decimals %d revert data = %x, want InvalidDecimals", d, ret)
		}
	}

	if _, err := call(encodeCreateCAS20WithParams(cas20VariantStablecoin, salt(20), cas20StablecoinParams("N", "S", creator, ""), nil)); !errors.Is(err, vm.ErrExecutionReverted) {
		t.Fatalf("empty currency err = %v, want MissingRequiredField", err)
	}
	if _, err := call(encodeCreateCAS20WithParams(cas20VariantStablecoin, salt(21), cas20StablecoinParams("N", "S", creator, "usd"), nil)); !errors.Is(err, vm.ErrExecutionReverted) {
		t.Fatalf("lowercase currency err = %v, want InvalidCurrency", err)
	}

	ret, err = call(encodeCreateCAS20WithParams(cas20VariantAsset, salt(30), cas20AssetParams("Gold Fund", "GLD", creator, 8), nil))
	if err != nil {
		t.Fatalf("createCAS20 asset: %v", err)
	}
	asset := common.BytesToAddress(ret)
	view := newUnmeteredCAS20Storage(statedb, asset)
	if got := strOf(view.name()); got != "Gold Fund" {
		t.Fatalf("name = %q, want Gold Fund", got)
	}
	if got := strOf(view.symbol()); got != "GLD" {
		t.Fatalf("symbol = %q, want GLD", got)
	}
	dec, _, err := evm.Call(creator, asset, cas20Call(selDecimals), NewGasBudget(200_000), uint256.NewInt(0))
	if err != nil || !bytes.Equal(dec, hashBytes(u256hash(8))) {
		t.Fatalf("decimals() = %x (err %v), want 8", dec, err)
	}

	ret, err = call(encodeCreateCAS20WithParams(cas20VariantStablecoin, salt(31), cas20StablecoinParams("Euro Coin", "EURC", creator, "EUR"), nil))
	if err != nil {
		t.Fatalf("createCAS20 stablecoin: %v", err)
	}
	stable := common.BytesToAddress(ret)
	cur, _, err := evm.Call(creator, stable, cas20Call(selCurrency), NewGasBudget(200_000), uint256.NewInt(0))
	if err != nil || !bytes.Equal(cur, encString("EUR")) {
		t.Fatalf("currency() = %x (err %v), want EUR", cur, err)
	}
	dec, _, err = evm.Call(creator, stable, cas20Call(selDecimals), NewGasBudget(200_000), uint256.NewInt(0))
	if err != nil || !bytes.Equal(dec, hashBytes(u256hash(6))) {
		t.Fatalf("stablecoin decimals() = %x (err %v), want 6", dec, err)
	}
}

func TestCAS20CreatedEvent(t *testing.T) {
	statedb, evm := newCAS20EVM(t)
	creator := common.HexToAddress("0xe7e17")
	statedb.SetTxContext(common.HexToHash("0xbeef"), 0)

	params := cas20StablecoinParams("Dollar Coin", "USDX", creator, "USD")
	ret, _, err := evm.Call(creator, CAS20FactoryAddress,
		encodeCreateCAS20WithParams(cas20VariantStablecoin, common.HexToHash("0x77"), params, nil),
		NewGasBudget(5_000_000), uint256.NewInt(0))
	if err != nil {
		t.Fatalf("createCAS20: %v", err)
	}
	token := common.BytesToAddress(ret)

	logs := statedb.Logs()
	if len(logs) == 0 || len(logs[0].Topics) == 0 || logs[0].Topics[0] != cas20TopicCAS20Created {
		t.Fatal("CAS20Created must be the first log of the creation")
	}
	created := logs[0]
	if len(logs) < 2 || logs[1].Topics[0] != cas20TopicRoleGranted {
		t.Fatal("the initial admin's RoleGranted must follow CAS20Created")
	}
	if logs[1].Topics[3] != addrKey(CAS20FactoryAddress) {
		t.Fatalf("RoleGranted sender = %x, want the factory", logs[1].Topics[3])
	}
	if created.Address != CAS20FactoryAddress {
		t.Fatalf("emitted by %x, want the factory %x", created.Address, CAS20FactoryAddress)
	}
	if len(created.Topics) != 3 {
		t.Fatalf("topics = %d, want 3 (signature, token, variant)", len(created.Topics))
	}
	if created.Topics[1] != addrKey(token) {
		t.Fatalf("indexed token = %x, want %x", created.Topics[1], addrKey(token))
	}
	if created.Topics[2] != wU8(cas20VariantStablecoin) {
		t.Fatalf("indexed variant = %x, want STABLECOIN", created.Topics[2])
	}

	wantParams := abiEncodeStruct(abiWord(wU8(cas20ParamsVersion)), abiString("USD"))
	wantData := encodeTuple(
		abiString("Dollar Coin"), abiString("USDX"), abiWord(wU8(6)), abiBytes(wantParams),
	)
	if !bytes.Equal(created.Data, wantData) {
		t.Fatalf("event data = %x\nwant                = %x", created.Data, wantData)
	}
}

// A hand-written vector, independent of the helper every other params test both
// builds its input with and measures against.
func TestCAS20CreateParamsCanonicalEncoding(t *testing.T) {
	statedb, evm := newCAS20EVM(t)
	admin := common.HexToAddress("0xad3111")

	word := func(v uint64) []byte { return hashBytes(u256hash(v)) }
	var blob []byte
	blob = append(blob, word(0x20)...)                // w0 outer offset
	blob = append(blob, word(1)...)                   // w1 version
	blob = append(blob, word(0xa0)...)                // w2 name offset
	blob = append(blob, word(0xe0)...)                // w3 symbol offset
	blob = append(blob, hashBytes(addrKey(admin))...) // w4 initialAdmin
	blob = append(blob, word(18)...)                  // w5 decimals
	blob = append(blob, word(1)...)                   // w6 name length
	blob = append(blob, rightPad32([]byte("A"))...)
	blob = append(blob, word(1)...) // w8 symbol length
	blob = append(blob, rightPad32([]byte("B"))...)

	if got := cas20AssetParams("A", "B", admin, 18); !bytes.Equal(got, blob) {
		t.Fatalf("cas20AssetParams disagrees with the canonical encoding:\n got %x\nwant %x", got, blob)
	}

	ret, _, err := evm.Call(admin, CAS20FactoryAddress,
		encodeCreateCAS20WithParams(cas20VariantAsset, common.HexToHash("0xcafe"), blob, nil),
		NewGasBudget(5_000_000), uint256.NewInt(0))
	if err != nil {
		t.Fatalf("createCAS20 with a canonically encoded blob: %v", err)
	}
	view := newUnmeteredCAS20Storage(statedb, common.BytesToAddress(ret))
	if strOf(view.name()) != "A" || strOf(view.symbol()) != "B" {
		t.Fatalf("name/symbol = %q/%q, want A/B", strOf(view.name()), strOf(view.symbol()))
	}
}

func TestCAS20CreateParamsRejectsMalformed(t *testing.T) {
	_, evm := newCAS20EVM(t)
	admin := common.HexToAddress("0xad3111")
	call := func(salt common.Hash, blob []byte) error {
		_, _, err := evm.Call(admin, CAS20FactoryAddress,
			encodeCreateCAS20WithParams(cas20VariantAsset, salt, blob, nil),
			NewGasBudget(5_000_000), uint256.NewInt(0))
		return err
	}

	dirty := cas20AssetParams("A", "B", admin, 18)
	dirty[32] = 0xff // first byte of the version word, inside the struct
	if err := call(common.HexToHash("0x1"), dirty); !errors.Is(err, vm.ErrExecutionReverted) {
		t.Fatalf("dirty version word err = %v, want revert", err)
	}

	bad := cas20AssetParams("A", "B", admin, 18)
	copy(bad[:32], hashBytes(u256hash(uint64(len(bad)+32))))
	if err := call(common.HexToHash("0x2"), bad); !errors.Is(err, vm.ErrExecutionReverted) {
		t.Fatalf("out-of-range offset err = %v, want revert", err)
	}

	// The name length word: outer offset (1 word) + 5 struct head words = word 6.
	dirtyLen := cas20AssetParams("A", "B", admin, 18)
	dirtyLen[6*32] = 0x01 // high byte of the length word
	ret, _, err := evm.Call(admin, CAS20FactoryAddress,
		encodeCreateCAS20WithParams(cas20VariantAsset, common.HexToHash("0x3"), dirtyLen, nil),
		NewGasBudget(5_000_000), uint256.NewInt(0))
	if !errors.Is(err, vm.ErrExecutionReverted) {
		t.Fatalf("dirty length word err = %v, want revert", err)
	}
	if len(ret) != 0 {
		t.Fatalf("malformed encoding revert data = %x, want empty", ret)
	}
}

func TestCAS20FieldValidationPrecedesOccupancy(t *testing.T) {
	_, evm := newCAS20EVM(t)
	creator := common.HexToAddress("0x0")
	call := func(salt common.Hash, currency string) ([]byte, error) {
		params := cas20StablecoinParams("N", "S", creator, currency)
		ret, _, err := evm.Call(creator, CAS20FactoryAddress,
			encodeCreateCAS20WithParams(cas20VariantStablecoin, salt, params, nil),
			NewGasBudget(5_000_000), uint256.NewInt(0))
		return ret, err
	}
	salt := common.HexToHash("0x5a17")

	if _, err := call(salt, "USD"); err != nil {
		t.Fatalf("first create: %v", err)
	}
	ret, err := call(salt, "usd")
	if !errors.Is(err, vm.ErrExecutionReverted) {
		t.Fatalf("duplicate salt with a bad currency: err = %v, want revert", err)
	}
	if len(ret) < 4 || [4]byte(ret[:4]) != errSelInvalidCurrency {
		t.Fatalf("revert selector = %x, want InvalidCurrency %x", ret[:min(4, len(ret))], errSelInvalidCurrency)
	}
	ret, err = call(salt, "")
	if !errors.Is(err, vm.ErrExecutionReverted) {
		t.Fatalf("empty currency: err = %v, want a revert", err)
	}
	if len(ret) < 4 || [4]byte(ret[:4]) != errSelMissingField {
		t.Fatalf("empty currency: selector = %x, want MissingRequiredField %x",
			ret[:min(4, len(ret))], errSelMissingField)
	}
	// Shows the cases above had two failing conditions rather than one.
	ret, err = call(salt, "EUR")
	if !errors.Is(err, vm.ErrExecutionReverted) {
		t.Fatalf("valid currency, duplicate salt: err = %v, want a revert", err)
	}
	if len(ret) < 4 || [4]byte(ret[:4]) != errSelTokenExists {
		t.Fatalf("valid currency, duplicate salt: selector = %x, want TokenAlreadyExists %x",
			ret[:min(4, len(ret))], errSelTokenExists)
	}
}

// Solidity's external decoder rejects an out-of-range enum with empty returndata;
// Panic(0x21) belongs to an internal uint-to-enum cast.
func TestCAS20OutOfEnumVariantRevertsEmpty(t *testing.T) {
	_, evm := newCAS20EVM(t)
	caller := common.HexToAddress("0xca11e4")

	for _, tc := range []struct {
		name  string
		input []byte
	}{
		{"createCAS20", encodeCreateCAS20WithParams(0x02, common.HexToHash("0xb0a1"),
			cas20AssetParams("T", "T", caller, 18), nil)},
		{"getCAS20Address", cas20Call(selGetCAS20Address, u256hash(2), addrKey(caller), common.Hash{})},
	} {
		ret, _, err := evm.Call(caller, CAS20FactoryAddress, tc.input,
			NewGasBudget(5_000_000), uint256.NewInt(0))
		if !errors.Is(err, vm.ErrExecutionReverted) {
			t.Errorf("%s with variant 2: err = %v, want a revert", tc.name, err)
		}
		if len(ret) != 0 {
			t.Errorf("%s with variant 2: returndata = %x, want empty. Both entry points must "+
				"decode the variant identically", tc.name, ret)
		}
	}

	ret, _, err := evm.Call(caller, CAS20FactoryAddress,
		cas20Call(selGetCAS20Address, u256hash(uint64(cas20VariantStablecoin)), addrKey(caller), common.Hash{}),
		NewGasBudget(5_000_000), uint256.NewInt(0))
	if err != nil {
		t.Fatalf("getCAS20Address for the stablecoin variant: %v", err)
	}
	if want := addrKey(cas20DeriveAddress(cas20VariantStablecoin, caller, common.Hash{})); !bytes.Equal(ret, want[:]) {
		t.Errorf("getCAS20Address = %x, want %x", ret, want)
	}
}

func TestCAS20BootstrapReachesTheVariant(t *testing.T) {
	_, evm := newCAS20EVM(t)
	creator := common.HexToAddress("0xc4ea70")
	call := func(input []byte) ([]byte, error) {
		ret, _, err := evm.Call(creator, common.Address{}, input, NewGasBudget(9_000_000), uint256.NewInt(0))
		return ret, err
	}
	_ = call

	at := func(to common.Address, input []byte) ([]byte, error) {
		ret, _, err := evm.Call(creator, to, input, NewGasBudget(9_000_000), uint256.NewInt(0))
		return ret, err
	}
	u := func(ret []byte, err error) uint64 {
		t.Helper()
		if err != nil {
			t.Fatalf("call: %v", err)
		}
		return new(uint256.Int).SetBytes(ret).Uint64()
	}

	const oneAndAHalf = 1_500_000_000_000_000_000
	ret, err := at(CAS20FactoryAddress, encodeCreateCAS20(cas20VariantAsset,
		common.HexToHash("0xb007"), creator, [][]byte{
			cas20Call(selGrantRole, roleMint, addrKey(creator)),
			cas20Call(selGrantRole, roleOperator, addrKey(creator)),
			cas20Call(selGrantRole, roleMetadata, addrKey(creator)),
			cas20Call(selUpdateMultiplier, u256hash(oneAndAHalf)),
			encodeBatchMint([]common.Address{cas20Alice, cas20Bob}, []uint64{10, 20}),
			encodeStringCall(selUpdateExtraMetadata, "issuer", "acme"),
		}))
	if err != nil {
		t.Fatalf("createCAS20 with Asset-only initCalls: %v", err)
	}
	token := common.BytesToAddress(ret)

	if got := u(at(token, cas20Call(selMultiplier))); got != oneAndAHalf {
		t.Errorf("multiplier = %d, want %d — updateMultiplier did not run", got, oneAndAHalf)
	}
	if got := u(at(token, cas20Call(selBalanceOf, addrKey(cas20Alice)))); got != 10 {
		t.Errorf("balanceOf(alice) = %d, want 10 — batchMint did not run", got)
	}
	if got := u(at(token, cas20Call(selScaledBalanceOf, addrKey(cas20Bob)))); got != 30 {
		t.Errorf("scaledBalanceOf(bob) = %d, want 30 (raw 20 at 1.5x)", got)
	}
	if got, err := at(token, encodeStringCall(selExtraMetadata, "issuer")); err != nil {
		t.Errorf("extraMetadata: %v", err)
	} else if s := decodeString(t, got); s != "acme" {
		t.Errorf("extraMetadata(issuer) = %q, want %q", s, "acme")
	}

	if got := u(at(token, cas20Call(selTotalSupply))); got != 30 {
		t.Errorf("totalSupply = %d, want 30", got)
	}
}
