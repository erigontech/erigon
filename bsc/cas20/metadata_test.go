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
	"math/big"
	"strings"
	"testing"

	"github.com/erigontech/erigon/execution/vm"

	"github.com/holiman/uint256"

	"github.com/erigontech/erigon/common"
	"github.com/erigontech/erigon/execution/abi"
)

func cas20CallString(sel [4]byte, s string) []byte {
	return append(append([]byte{}, sel[:]...), encString(s)...)
}

// decodeString follows the head offset and requires word padding, as a real ABI
// consumer would; a laxer helper would hide the encoder bugs these tests exist for.
func decodeString(t *testing.T, ret []byte) string {
	t.Helper()
	if len(ret) < 64 {
		t.Fatalf("string return too short: %x", ret)
	}
	off := new(uint256.Int).SetBytes(ret[0:32])
	if !off.Eq(uint256.NewInt(32)) {
		t.Fatalf("string head offset = %s, want 32: %x", off, ret)
	}
	n := new(uint256.Int).SetBytes(ret[32:64]).Uint64()
	if uint64(len(ret)) != 64+(n+31)/32*32 {
		t.Fatalf("string tail is %d bytes, want %d for a %d-byte string: %x",
			len(ret)-64, (n+31)/32*32, n, ret)
	}
	return string(ret[64 : 64+n])
}

func TestCAS20MetadataUpdates(t *testing.T) {
	admin := common.HexToAddress("0xad4149")
	editor := common.HexToAddress("0xed17")

	statedb, token, run := newTokenWithEVM(t, 1, func(s cas20Storage) {
		s.setName("Test Token")
		s.setSymbol("TT")
		s.setRole(roleDefaultAdmin, admin, true)
		s.setAdminCount(uint256.NewInt(1))
	})
	view := newUnmeteredCAS20Storage(statedb, token)

	for _, input := range [][]byte{
		cas20CallString(selUpdateName, "Hijacked"),
		cas20CallString(selUpdateSymbol, "HJK"),
		cas20CallString(selUpdateContractURI, "ipfs://hijacked"),
	} {
		if _, err := run(editor, input); !errors.Is(err, vm.ErrExecutionReverted) {
			t.Fatalf("unauthorized metadata write err = %v, want revert", err)
		}
	}
	if got := strOf(view.name()); got != "Test Token" {
		t.Fatalf("name after refused update = %q, want unchanged", got)
	}

	if _, err := run(admin, cas20Call(selGrantRole, roleMetadata, addrKey(editor))); err != nil {
		t.Fatalf("grant METADATA_ROLE: %v", err)
	}

	before, err := run(editor, cas20Call(selDomainSeparator))
	if err != nil {
		t.Fatalf("DOMAIN_SEPARATOR: %v", err)
	}

	if _, err := run(editor, cas20CallString(selUpdateName, "Renamed Token")); err != nil {
		t.Fatalf("updateName: %v", err)
	}
	if _, err := run(editor, cas20CallString(selUpdateSymbol, "RNT")); err != nil {
		t.Fatalf("updateSymbol: %v", err)
	}
	if _, err := run(editor, cas20CallString(selUpdateContractURI, "ipfs://cid")); err != nil {
		t.Fatalf("updateContractURI: %v", err)
	}

	if got := strOf(view.name()); got != "Renamed Token" {
		t.Errorf("name = %q, want Renamed Token", got)
	}
	if got := strOf(view.symbol()); got != "RNT" {
		t.Errorf("symbol = %q, want RNT", got)
	}
	ret, err := run(editor, cas20Call(selContractURI))
	if err != nil {
		t.Fatalf("contractURI: %v", err)
	}
	if got := decodeString(t, ret); got != "ipfs://cid" {
		t.Errorf("contractURI() = %q, want ipfs://cid", got)
	}

	after, err := run(editor, cas20Call(selDomainSeparator))
	if err != nil {
		t.Fatalf("DOMAIN_SEPARATOR: %v", err)
	}
	if bytes.Equal(before, after) {
		t.Error("DOMAIN_SEPARATOR unchanged after updateName — outstanding permits would stay valid")
	}

	for _, c := range []struct {
		what  string
		input []byte
	}{
		{"updateName", cas20CallString(selUpdateName, "Static")},
		{"updateSymbol", cas20CallString(selUpdateSymbol, "STC")},
		{"updateContractURI", cas20CallString(selUpdateContractURI, "ipfs://static")},
	} {
		gas := NewGasBudget(1_000_000)
		roCtx := &PrecompileContext{StateDB: statedb, Self: token, Caller: editor, DirectCall: true, ReadOnly: true, gas: &gas}
		if _, err := newCAS20Token(roCtx, 18).dispatch(c.input); !errors.Is(err, vm.ErrWriteProtection) {
			t.Errorf("read-only %s err = %v, want write protection", c.what, err)
		}
	}
}

func TestCAS20MetadataDuringBootstrap(t *testing.T) {
	statedb, evm := newCAS20EVM(t)
	creator := common.HexToAddress("0xdec0de")

	input := encodeCreateCAS20(cas20VariantAsset, common.HexToHash("0xc0"), creator,
		[][]byte{cas20CallString(selUpdateName, "Configured")})
	ret, _, err := evm.Call(creator, CAS20FactoryAddress, input, NewGasBudget(5_000_000), uint256.NewInt(0))
	if err != nil {
		t.Fatalf("createCAS20 with updateName init call: %v", err)
	}
	token := common.BytesToAddress(ret)

	view := newUnmeteredCAS20Storage(statedb, token)
	if got := strOf(view.name()); got != "Configured" {
		t.Errorf("name = %q, want Configured", got)
	}
	if view.hasRole(roleMetadata, creator) {
		t.Error("creator holds METADATA_ROLE after bootstrap — the bypass leaked into stored state")
	}
	gas := NewGasBudget(1_000_000)
	ctx := &PrecompileContext{evm: evm.EVM, StateDB: statedb, Self: token, Caller: creator, DirectCall: true, gas: &gas}
	if _, err := newCAS20Token(ctx, 18).dispatch(cas20CallString(selUpdateName, "After")); !errors.Is(err, vm.ErrExecutionReverted) {
		t.Errorf("post-creation updateName err = %v, want revert", err)
	}
}

func TestCAS20MetadataEvents(t *testing.T) {
	editor := common.HexToAddress("0xed17")
	statedb, token, run := newTokenWithEVM(t, 1, func(s cas20Storage) {
		s.setRole(roleMetadata, editor, true)
	})
	txHash := common.HexToHash("0xbeef")
	statedb.SetTxContext(txHash, 0)

	// Long enough to spill past the single-word case.
	longName := strings.Repeat("Renamed ", 6) // 48 bytes
	for _, c := range []struct {
		what  string
		input []byte
	}{
		{"updateName", cas20CallString(selUpdateName, longName)},
		{"updateSymbol", cas20CallString(selUpdateSymbol, "RNT")},
		{"updateContractURI", cas20CallString(selUpdateContractURI, "ipfs://cid")},
	} {
		if _, err := run(editor, c.input); err != nil {
			t.Fatalf("%s: %v", c.what, err)
		}
	}

	logs := statedb.GetLogs(txHash, 1, common.Hash{}, 1)
	wantTopics := []common.Hash{
		cas20TopicNameUpdated, cas20TopicEIP712DomainChanged,
		cas20TopicSymbolUpdated, cas20TopicContractURIUpdated,
	}
	if len(logs) != len(wantTopics) {
		t.Fatalf("got %d logs, want %d (NameUpdated, EIP712DomainChanged, SymbolUpdated, ContractURIUpdated)",
			len(logs), len(wantTopics))
	}
	wantIndexed := map[common.Hash]bool{cas20TopicNameUpdated: true, cas20TopicSymbolUpdated: true}
	for i, want := range wantTopics {
		if logs[i].Address != token {
			t.Errorf("log %d address = %s, want the token", i, logs[i].Address.Hex())
		}
		topics := 1
		if wantIndexed[want] {
			topics = 2
		}
		if len(logs[i].Topics) != topics || logs[i].Topics[0] != want {
			t.Errorf("log %d topics = %v, want %d starting with %s", i, logs[i].Topics, topics, want.Hex())
			continue
		}
		if topics == 2 && logs[i].Topics[1] != addrKey(editor) {
			t.Errorf("log %d updater = %s, want %s", i, logs[i].Topics[1].Hex(), addrKey(editor).Hex())
		}
	}
	if got := decodeString(t, logs[0].Data); got != longName {
		t.Errorf("NameUpdated data = %q, want %q", got, longName)
	}
	if len(logs[1].Data) != 0 {
		t.Errorf("EIP712DomainChanged data = %x, want empty", logs[1].Data)
	}
	if got := decodeString(t, logs[2].Data); got != "RNT" {
		t.Errorf("SymbolUpdated data = %q, want RNT", got)
	}
	if len(logs[3].Data) != 0 {
		t.Errorf("ContractURIUpdated data = %x, want empty", logs[3].Data)
	}
}

// The uint8[] expectations are written out word by word, not produced by the encoder under test.
func TestCAS20PausedFeaturesAndSupplyCap(t *testing.T) {
	admin := common.HexToAddress("0xad4149")
	statedb, _, run := newTokenWithEVM(t, 1, func(s cas20Storage) {
		s.setRole(roleDefaultAdmin, admin, true)
		s.setRole(rolePause, admin, true)
		s.setRole(roleUnpause, admin, true)
		s.setAdminCount(uint256.NewInt(1))
		s.setSupplyCap(uint256.NewInt(1000))
	})
	txHash := common.HexToHash("0xfeed")
	statedb.SetTxContext(txHash, 0)

	ret, err := run(admin, cas20Call(selSupplyCap))
	if err != nil {
		t.Fatalf("supplyCap: %v", err)
	}
	if got := new(uint256.Int).SetBytes(ret).Uint64(); got != 1000 {
		t.Errorf("supplyCap() = %d, want 1000", got)
	}

	ret, err = run(admin, cas20Call(selPausedFeatures))
	if err != nil {
		t.Fatalf("pausedFeatures: %v", err)
	}
	wantEmpty := append(hashBytes(u256hash(0x20)), hashBytes(u256hash(0))...)
	if !bytes.Equal(ret, wantEmpty) {
		t.Errorf("pausedFeatures() = %x, want %x", ret, wantEmpty)
	}

	// SEIZE pins the scan's upper bound; the report is ordered by id, not by request.
	if _, err := run(admin, cas20CallU8Array(selPause, byte(cas20PauseSeize), byte(cas20PauseBurn), byte(cas20PauseTransfer))); err != nil {
		t.Fatalf("pause: %v", err)
	}
	ret, err = run(admin, cas20Call(selPausedFeatures))
	if err != nil {
		t.Fatalf("pausedFeatures: %v", err)
	}
	want := append([]byte{}, hashBytes(u256hash(0x20))...)
	want = append(want, hashBytes(u256hash(3))...)
	want = append(want, hashBytes(u256hash(uint64(cas20PauseTransfer)))...)
	want = append(want, hashBytes(u256hash(uint64(cas20PauseBurn)))...)
	want = append(want, hashBytes(u256hash(uint64(cas20PauseSeize)))...)
	if !bytes.Equal(ret, want) {
		t.Errorf("pausedFeatures() = %x, want %x", ret, want)
	}

	if _, err := run(admin, cas20CallU8Array(selUnpause, byte(cas20PauseSeize))); err != nil {
		t.Fatalf("unpause: %v", err)
	}
	ret, err = run(admin, cas20Call(selPausedFeatures))
	if err != nil {
		t.Fatalf("pausedFeatures: %v", err)
	}
	want = append([]byte{}, hashBytes(u256hash(0x20))...)
	want = append(want, hashBytes(u256hash(2))...)
	want = append(want, hashBytes(u256hash(uint64(cas20PauseTransfer)))...)
	want = append(want, hashBytes(u256hash(uint64(cas20PauseBurn)))...)
	if !bytes.Equal(ret, want) {
		t.Errorf("pausedFeatures() after unpause = %x, want %x", ret, want)
	}

	if _, err := run(admin, cas20Call(selUpdateSupplyCap, u256hash(5000))); err != nil {
		t.Fatalf("updateSupplyCap: %v", err)
	}
	ret, err = run(admin, cas20Call(selSupplyCap))
	if err != nil {
		t.Fatalf("supplyCap: %v", err)
	}
	if got := new(uint256.Int).SetBytes(ret).Uint64(); got != 5000 {
		t.Errorf("supplyCap() after update = %d, want 5000", got)
	}

	if _, err := run(admin, cas20Call(selUpdatePolicy, scopeTransferSender, wU64(cas20PolicyAlwaysBlock))); err != nil {
		t.Fatalf("updatePolicy: %v", err)
	}
	ret, err = run(admin, cas20Call(selPolicyId, scopeTransferSender))
	if err != nil {
		t.Fatalf("policyId: %v", err)
	}
	if got := new(uint256.Int).SetBytes(ret).Uint64(); got != cas20PolicyAlwaysBlock {
		t.Errorf("policyId(TRANSFER_SENDER) = %d, want %d", got, cas20PolicyAlwaysBlock)
	}

	logs := statedb.GetLogs(txHash, 1, common.Hash{}, 1)
	if len(logs) != 4 {
		t.Fatalf("got %d logs, want 4 (Paused, Unpaused, SupplyCapUpdated, PolicyUpdated)", len(logs))
	}
	// The payload echoes the requested order: a record of the action, not of state.
	if len(logs[0].Topics) != 2 || logs[0].Topics[0] != cas20TopicPaused || logs[0].Topics[1] != addrKey(admin) {
		t.Errorf("Paused topics = %v, want [Paused, admin]", logs[0].Topics)
	}
	wantPaused := append([]byte{}, hashBytes(u256hash(0x20))...)
	wantPaused = append(wantPaused, hashBytes(u256hash(3))...)
	wantPaused = append(wantPaused, hashBytes(u256hash(uint64(cas20PauseSeize)))...)
	wantPaused = append(wantPaused, hashBytes(u256hash(uint64(cas20PauseBurn)))...)
	wantPaused = append(wantPaused, hashBytes(u256hash(uint64(cas20PauseTransfer)))...)
	if !bytes.Equal(logs[0].Data, wantPaused) {
		t.Errorf("Paused data = %x, want %x", logs[0].Data, wantPaused)
	}
	if len(logs[1].Topics) != 2 || logs[1].Topics[0] != cas20TopicUnpaused || logs[1].Topics[1] != addrKey(admin) {
		t.Errorf("Unpaused topics = %v, want [Unpaused, admin]", logs[1].Topics)
	}
	wantUnpaused := append([]byte{}, hashBytes(u256hash(0x20))...)
	wantUnpaused = append(wantUnpaused, hashBytes(u256hash(1))...)
	wantUnpaused = append(wantUnpaused, hashBytes(u256hash(uint64(cas20PauseSeize)))...)
	if !bytes.Equal(logs[1].Data, wantUnpaused) {
		t.Errorf("Unpaused data = %x, want %x", logs[1].Data, wantUnpaused)
	}
	if len(logs[2].Topics) != 2 || logs[2].Topics[0] != cas20TopicSupplyCapUpdated ||
		logs[2].Topics[1] != addrKey(admin) {
		t.Errorf("SupplyCapUpdated topics = %v, want [sig, updater %s]", logs[2].Topics, admin.Hex())
	}
	wantCap := append(hashBytes(u256hash(1000)), hashBytes(u256hash(5000))...)
	if !bytes.Equal(logs[2].Data, wantCap) {
		t.Errorf("SupplyCapUpdated data = %x, want %x", logs[2].Data, wantCap)
	}
	if len(logs[3].Topics) != 2 || logs[3].Topics[0] != cas20TopicPolicyUpdated || logs[3].Topics[1] != scopeTransferSender {
		t.Errorf("PolicyUpdated topics = %v, want [PolicyUpdated, TRANSFER_SENDER]", logs[3].Topics)
	}
	wantPolicy := append(hashBytes(wU64(0)), hashBytes(wU64(cas20PolicyAlwaysBlock))...)
	if !bytes.Equal(logs[3].Data, wantPolicy) {
		t.Errorf("PolicyUpdated data = %x, want %x (unbound -> ALWAYS_BLOCK)", logs[3].Data, wantPolicy)
	}
}

// A hand-built vector: a head/tail mistake would be invisible to an expectation
// produced by the same encoder.
func TestCAS20EIP712DomainEncoding(t *testing.T) {
	_, token, run := newTokenWithEVM(t, 1, func(s cas20Storage) {
		s.setName("Tok")
	})

	ret, err := run(cas20Alice, cas20Call(selEIP712Domain))
	if err != nil {
		t.Fatalf("eip712Domain: %v", err)
	}

	chainID, _ := cas20TestChainConfig().ChainID, true
	pad := func(s string) []byte {
		out := make([]byte, 32)
		copy(out, s)
		return out
	}
	// Head 7 words; tails at 224 (name), 288 (version), 352 (extensions).
	var want []byte
	want = append(want, hashBytes(common.Hash{0: 0x0f})...) // bytes1 fields, left-aligned
	want = append(want, hashBytes(u256hash(224))...)        // -> name
	want = append(want, hashBytes(u256hash(288))...)        // -> version
	want = append(want, hashBytes(common.Hash(chainID.Bytes32()))...)
	want = append(want, hashBytes(addrKey(token))...)
	want = append(want, hashBytes(common.Hash{})...) // salt: unused
	want = append(want, hashBytes(u256hash(352))...) // -> extensions
	want = append(want, hashBytes(u256hash(3))...)   // len("Tok")
	want = append(want, pad("Tok")...)               //
	want = append(want, hashBytes(u256hash(1))...)   // len("1")
	want = append(want, pad(cas20EIP712Version)...)  //
	want = append(want, hashBytes(u256hash(0))...)   // extensions: empty

	if !bytes.Equal(ret, want) {
		t.Fatalf("eip712Domain() =\n%x\nwant\n%x", ret, want)
	}
}

// Against the abi package's packer: the hand-built vectors pin the layout, this pins conformance.
func TestCAS20ABIEncodingOracle(t *testing.T) {
	mustType := func(s string) abi.Type {
		t.Helper()
		ty, err := abi.NewType(s, "", nil)
		if err != nil {
			t.Fatalf("NewType(%s): %v", s, err)
		}
		return ty
	}

	// A name past 32 bytes moves the later tails; a fixed-size assumption survives the short case.
	domainArgs := abi.Arguments{
		{Type: mustType("bytes1")},
		{Type: mustType("string")},
		{Type: mustType("string")},
		{Type: mustType("uint256")},
		{Type: mustType("address")},
		{Type: mustType("bytes32")},
		{Type: mustType("uint256[]")},
	}
	for _, name := range []string{"Tok", strings.Repeat("Long Name ", 7)} {
		_, token, run := newTokenWithEVM(t, 1, func(s cas20Storage) { s.setName(name) })
		got, err := run(cas20Alice, cas20Call(selEIP712Domain))
		if err != nil {
			t.Fatalf("eip712Domain(%d-byte name): %v", len(name), err)
		}
		want, err := domainArgs.Pack([1]byte{0x0f}, name, cas20EIP712Version,
			cas20TestChainConfig().ChainID.ToBig(), token, [32]byte{}, []*big.Int{})
		if err != nil {
			t.Fatalf("pack domain: %v", err)
		}
		if !bytes.Equal(got, want) {
			t.Errorf("eip712Domain(%d-byte name)\n got = %x\nwant = %x", len(name), got, want)
		}
	}

	admin := common.HexToAddress("0xad4149")
	_, _, runPause := newTokenWithEVM(t, 1, func(s cas20Storage) { s.setRole(rolePause, admin, true) })
	if _, err := runPause(admin, cas20CallU8Array(selPause, byte(cas20PauseSeize), byte(cas20PauseTransfer))); err != nil {
		t.Fatalf("pause: %v", err)
	}
	got, err := runPause(admin, cas20Call(selPausedFeatures))
	if err != nil {
		t.Fatalf("pausedFeatures: %v", err)
	}
	arrayArgs := abi.Arguments{{Type: mustType("uint8[]")}}
	want, err := arrayArgs.Pack([]uint8{uint8(cas20PauseTransfer), uint8(cas20PauseSeize)})
	if err != nil {
		t.Fatalf("pack uint8[]: %v", err)
	}
	if !bytes.Equal(got, want) {
		t.Errorf("pausedFeatures()\n got = %x\nwant = %x", got, want)
	}
}
