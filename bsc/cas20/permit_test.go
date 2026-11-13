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
	"crypto/ecdsa"
	"errors"
	"testing"

	"github.com/erigontech/erigon/execution/vm"
	"github.com/erigontech/erigon/execution/vm/evmtypes"

	"github.com/holiman/uint256"

	"github.com/erigontech/erigon/common"
	"github.com/erigontech/erigon/common/crypto"
)

func newTokenWithEVM(t *testing.T, now uint64, seed func(cas20Storage)) (*testState, common.Address, func(caller common.Address, input []byte) ([]byte, error)) {
	t.Helper()
	statedb := newTestStateDB(t)
	token := cas20Addr(cas20VariantAsset, 1)
	if seed != nil {
		seed(newUnmeteredCAS20Storage(statedb, token))
	}
	bc := cas20BlockContext(now)
	evm := newTestEVM(bc, statedb, cas20TestChainConfig())
	run := func(caller common.Address, input []byte) ([]byte, error) {
		gas := NewGasBudget(2_000_000)
		ctx := &PrecompileContext{evm: evm.EVM, StateDB: statedb, Self: token, Caller: caller, DirectCall: true, gas: &gas}
		return newCAS20Token(ctx, 18).dispatch(input)
	}
	return statedb, token, run
}

func TestCAS20Permit(t *testing.T) {
	const now = 100
	statedb, token, run := newTokenWithEVM(t, now, func(s cas20Storage) {
		s.setName("Test Token")
	})
	view := newUnmeteredCAS20Storage(statedb, token)

	evm := newTestEVM(evmtypes.BlockContext{BlockNumber: 1, Time: now}, statedb, cas20TestChainConfig())
	// A real budget: a read whose charge fails returns zero, and a 1-gas context
	// would sign against an empty name.
	gas := NewGasBudget(2_000_000)
	domTok := newCAS20Token(&PrecompileContext{evm: evm.EVM, StateDB: statedb, Self: token, gas: &gas}, 18)

	sign := func(key *ecdsa.PrivateKey, owner, spender common.Address, value, deadline, nonce uint64) (byte, common.Hash, common.Hash) {
		sh := append([]byte{}, cas20PermitTypehash[:]...)
		sh = append(sh, hashBytes(addrKey(owner))...)
		sh = append(sh, hashBytes(addrKey(spender))...)
		sh = append(sh, hashBytes(u256hash(value))...)
		sh = append(sh, hashBytes(u256hash(nonce))...)
		sh = append(sh, hashBytes(u256hash(deadline))...)
		dom, paid := domTok.domainSeparator()
		if !paid {
			t.Fatal("the reference domain separator could not be paid for")
		}
		digest := crypto.Keccak256([]byte{0x19, 0x01}, dom[:], crypto.Keccak256(sh))
		sig, err := crypto.Sign(digest, key)
		if err != nil {
			t.Fatal(err)
		}
		return sig[64] + 27, common.BytesToHash(sig[0:32]), common.BytesToHash(sig[32:64])
	}
	permitCall := func(owner, spender common.Address, value, deadline uint64, v byte, r, s common.Hash) []byte {
		return cas20Call(selPermit, addrKey(owner), addrKey(spender), u256hash(value), u256hash(deadline), u256hash(uint64(v)), r, s)
	}

	key, _ := crypto.GenerateKey()
	owner := crypto.PubkeyToAddress(key.PublicKey)
	spender := cas20Carol
	relayer := cas20Bob // anyone may submit

	v, r, s := sign(key, owner, spender, 777, 200, 0)
	// EIP-2612's permit returns nothing, so a caller decoding a bool must fail here.
	if ret, err := run(relayer, permitCall(owner, spender, 777, 200, v, r, s)); err != nil || len(ret) != 0 {
		t.Fatalf("permit ret %x err %v, want empty returndata", ret, err)
	}
	if got := view.allowance(owner, spender).Uint64(); got != 777 {
		t.Fatalf("allowance = %d, want 777", got)
	}
	if got := view.nonce(owner).Uint64(); got != 1 {
		t.Fatalf("nonce = %d, want 1", got)
	}

	if _, err := run(relayer, permitCall(owner, spender, 777, 200, v, r, s)); !errors.Is(err, vm.ErrExecutionReverted) {
		t.Fatalf("replay err = %v, want revert", err)
	}

	v, r, s = sign(key, owner, spender, 1, 50, 1) // deadline 50 < now 100
	if _, err := run(relayer, permitCall(owner, spender, 1, 50, v, r, s)); !errors.Is(err, vm.ErrExecutionReverted) {
		t.Fatalf("expired err = %v, want revert", err)
	}

	other, _ := crypto.GenerateKey()
	v, r, s = sign(other, owner, spender, 5, 200, 1)
	if _, err := run(relayer, permitCall(owner, spender, 5, 200, v, r, s)); !errors.Is(err, vm.ErrExecutionReverted) {
		t.Fatalf("wrong-signer err = %v, want revert", err)
	}

	want, paid := domTok.domainSeparator()
	if !paid {
		t.Fatal("the reference domain separator could not be paid for")
	}
	if ret, err := run(relayer, cas20Call(selDomainSeparator)); err != nil || !bytes.Equal(ret, want[:]) {
		t.Fatalf("DOMAIN_SEPARATOR mismatch: %x err %v", ret, err)
	}
	if ret, _ := run(relayer, cas20Call(selNonces, addrKey(owner))); new(uint256.Int).SetBytes(ret).Uint64() != 1 {
		t.Fatalf("nonces(owner) = %x, want 1", ret)
	}
}

func TestCAS20Memo(t *testing.T) {
	statedb, _, run := newTokenWithEVM(t, 1, func(s cas20Storage) {
		s.setBalance(cas20Alice, uint256.NewInt(1000))
	})
	txHash := common.HexToHash("0xabc")
	statedb.SetTxContext(txHash, 0)

	memo := common.HexToHash("0xdeadbeef")
	ret, err := run(cas20Alice, cas20Call(selTransferWithMemo, addrKey(cas20Bob), u256hash(100), memo))
	if err != nil || !bytes.Equal(ret, encBool(true)) {
		t.Fatalf("transferWithMemo ret %x err %v", ret, err)
	}

	logs := statedb.GetLogs(txHash, 1, common.Hash{}, 1)
	if len(logs) != 2 {
		t.Fatalf("got %d logs, want 2 (Transfer + Memo)", len(logs))
	}
	if logs[0].Topics[0] != cas20TopicTransfer {
		t.Errorf("first log should be Transfer, got %s", logs[0].Topics[0].Hex())
	}
	if logs[1].Topics[0] != cas20TopicMemo || logs[1].Topics[1] != addrKey(cas20Alice) || logs[1].Topics[2] != memo {
		t.Errorf("second log should be Memo(alice, memo)")
	}
}
