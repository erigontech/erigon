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

func TestCAS20TransferWithMemoFormat(t *testing.T) {
	statedb, evm := newCAS20EVM(t)
	creator := common.HexToAddress("0xdec0de")
	call := func(caller common.Address, to common.Address, input []byte) ([]byte, error) {
		ret, _, err := evm.Call(caller, to, input, NewGasBudget(3_000_000), uint256.NewInt(0))
		return ret, err
	}
	sel := func(ret []byte, want [4]byte) bool { return len(ret) >= 4 && [4]byte(ret[:4]) == want }

	ret, err := call(creator, CAS20FactoryAddress, encodeCreateCAS20(cas20VariantStablecoin, common.HexToHash("0x3e0"), creator, [][]byte{
		cas20Call(selGrantRole, roleMint, addrKey(creator)),
		cas20Call(selGrantRole, rolePause, addrKey(creator)),
		cas20Call(selMint, addrKey(cas20Alice), u256hash(1000)),
	}))
	if err != nil {
		t.Fatalf("createCAS20: %v", err)
	}
	token := common.BytesToAddress(ret)
	view := newUnmeteredCAS20Storage(statedb, token)
	memo, format := common.HexToHash("0x1234"), common.HexToHash("0x5c4e")

	// Transfer, Memo, MemoFormatDeclared, in that order; the declarer is the caller.
	statedb.SetTxContext(common.HexToHash("0x7b"), 0)
	if ret, err := call(cas20Alice, token, cas20Call(selTransferWithMemoFormat, addrKey(cas20Bob), u256hash(40), memo, format)); err != nil || !bytes.Equal(ret, encBool(true)) {
		t.Fatalf("transferWithMemoFormat: ret %x err %v", ret, err)
	}
	if view.balanceOf(cas20Bob).Uint64() != 40 {
		t.Errorf("bob = %d, want 40", view.balanceOf(cas20Bob).Uint64())
	}
	logs := statedb.GetLogs(common.HexToHash("0x7b"), 1, common.Hash{}, 1)
	if len(logs) != 3 || logs[0].Topics[0] != cas20TopicTransfer || logs[1].Topics[0] != cas20TopicMemo || logs[2].Topics[0] != cas20TopicMemoFormatDeclared {
		t.Fatalf("logs = %+v, want Transfer, Memo, MemoFormatDeclared", logs)
	}
	if d := logs[2]; len(d.Topics) != 4 || d.Topics[1] != addrKey(cas20Alice) || d.Topics[2] != memo || d.Topics[3] != format || len(d.Data) != 0 {
		t.Errorf("MemoFormatDeclared = topics %v data %x, want (alice, memo, format) and no data", d.Topics, d.Data)
	}

	// A zero format is what the plain memo methods already mean.
	if ret, err := call(cas20Alice, token, cas20Call(selTransferWithMemoFormat, addrKey(cas20Bob), u256hash(1), memo, common.Hash{})); !errors.Is(err, vm.ErrExecutionReverted) || !sel(ret, errSelInvalidFormatId) {
		t.Errorf("zero format: ret %x err %v, want InvalidFormatId", ret, err)
	}
	// After that, the transfer's own sequence applies unchanged.
	if _, err := call(creator, token, cas20CallU8Array(selPause, byte(cas20PauseTransfer))); err != nil {
		t.Fatal(err)
	}
	if ret, _ := call(cas20Alice, token, cas20Call(selTransferWithMemoFormat, addrKey(cas20Bob), u256hash(1), memo, format)); !sel(ret, errSelContractPaused) {
		t.Errorf("paused: ret %x, want ContractPaused", ret)
	}
	if ret, _ := call(cas20Alice, token, cas20Call(selTransferWithMemoFormat, addrKey(cas20Bob), u256hash(1), memo, common.Hash{})); !sel(ret, errSelInvalidFormatId) {
		t.Errorf("paused + zero format: ret %x, want InvalidFormatId first", ret)
	}
	view.setPaused(new(uint256.Int))

	// A static frame is refused.
	if ret, _, _ := evm.StaticCall(cas20Alice, token, cas20Call(selTransferWithMemoFormat, addrKey(cas20Bob), u256hash(1), memo, format), NewGasBudget(1_000_000)); !sel(ret, errSelStaticCallDenied) {
		t.Errorf("STATICCALL: ret %x, want StaticCallNotAllowed", ret)
	}

	// transferFrom spends the allowance as its memo counterpart does.
	if _, err := call(cas20Alice, token, cas20Call(selApprove, addrKey(cas20Carol), u256hash(10))); err != nil {
		t.Fatal(err)
	}
	statedb.SetTxContext(common.HexToHash("0x7c"), 0)
	if _, err := call(cas20Carol, token, cas20Call(selTransferFromWithMemoFormat, addrKey(cas20Alice), addrKey(cas20Bob), u256hash(6), memo, format)); err != nil {
		t.Fatalf("transferFromWithMemoFormat: %v", err)
	}
	if view.allowance(cas20Alice, cas20Carol).Uint64() != 4 || view.balanceOf(cas20Bob).Uint64() != 46 {
		t.Errorf("allowance %d bob %d, want 4 and 46", view.allowance(cas20Alice, cas20Carol).Uint64(), view.balanceOf(cas20Bob).Uint64())
	}
	logs = statedb.GetLogs(common.HexToHash("0x7c"), 1, common.Hash{}, 1)
	if len(logs) != 3 || logs[2].Topics[1] != addrKey(cas20Carol) {
		t.Errorf("declarer of a transferFrom = %v, want carol, the spender", logs[2].Topics)
	}

	// The transferFrom form is transferFrom whoever calls it: a holder moving its own
	// balance spends its self-approval, as through transferFromWithMemo.
	if ret, _ := call(cas20Alice, token, cas20Call(selTransferFromWithMemoFormat, addrKey(cas20Alice), addrKey(cas20Bob), u256hash(6), memo, format)); !sel(ret, errSelInsufficientAllow) {
		t.Errorf("self transferFromWithMemoFormat without an approval: ret %x, want InsufficientAllowance", ret)
	}
	if _, err := call(cas20Alice, token, cas20Call(selApprove, addrKey(cas20Alice), u256hash(10))); err != nil {
		t.Fatal(err)
	}
	if _, err := call(cas20Alice, token, cas20Call(selTransferFromWithMemoFormat, addrKey(cas20Alice), addrKey(cas20Bob), u256hash(6), memo, format)); err != nil {
		t.Fatalf("self transferFromWithMemoFormat with an approval: %v", err)
	}
	if view.allowance(cas20Alice, cas20Alice).Uint64() != 4 {
		t.Errorf("self-allowance = %d, want 4 — it must be spent like any other", view.allowance(cas20Alice, cas20Alice).Uint64())
	}
}
