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
	"github.com/holiman/uint256"

	"github.com/erigontech/erigon/common"
	"github.com/erigontech/erigon/common/crypto"
	"github.com/erigontech/erigon/execution/vm"
)

type cas20Token struct {
	ctx      *PrecompileContext
	s        cas20Storage
	decimals uint8

	// privileged marks the factory's bootstrap frame: role and transfer-side
	// policy gates are skipped there, MINT_RECEIVER and the renounce freeze are not.
	privileged bool

	// inAnnounce travels by value into announce's internal calls, so a nested
	// announce sees it and reverts.
	inAnnounce bool
}

func newCAS20Token(ctx *PrecompileContext, decimals uint8) cas20Token {
	return cas20Token{ctx: ctx, s: newMeteredCAS20Storage(ctx), decimals: decimals}
}

func newCAS20TokenBootstrap(ctx *PrecompileContext, decimals uint8) cas20Token {
	t := newCAS20Token(ctx, decimals)
	t.privileged = true
	return t
}

// pause feature bits in the paused bitmask (slot 11).
const (
	cas20PauseTransfer = 0
	cas20PauseMint     = 1
	cas20PauseBurn     = 2
	cas20PauseSeize    = 3
)

func selector(sig string) (s [4]byte) {
	copy(s[:], crypto.Keccak256([]byte(sig)))
	return s
}

var (
	selName         = selector("name()")
	selSymbol       = selector("symbol()")
	selDecimals     = selector("decimals()")
	selTotalSupply  = selector("totalSupply()")
	selBalanceOf    = selector("balanceOf(address)")
	selAllowance    = selector("allowance(address,address)")
	selApprove      = selector("approve(address,uint256)")
	selTransfer     = selector("transfer(address,uint256)")
	selTransferFrom = selector("transferFrom(address,address,uint256)")

	cas20TopicTransfer = eventTopic("Transfer(address,address,uint256)")
	cas20TopicApproval = eventTopic("Approval(address,address,uint256)")

	maxU256 = new(uint256.Int).Not(new(uint256.Int))
)

func (t cas20Token) dispatch(input []byte) ([]byte, error) {
	if len(input) < 4 {
		return nil, vm.ErrExecutionReverted
	}
	var sel [4]byte
	copy(sel[:], input[:4])
	args := input[4:]

	switch sel {
	case selName:
		v, ok := t.s.name()
		if !ok {
			return nil, vm.ErrOutOfGas
		}
		return encString(v), nil
	case selSymbol:
		v, ok := t.s.symbol()
		if !ok {
			return nil, vm.ErrOutOfGas
		}
		return encString(v), nil
	case selDecimals:
		return encU256(uint256.NewInt(uint64(t.decimals))), nil
	case selTotalSupply:
		return encU256(t.s.totalSupply()), nil
	case selBalanceOf:
		a, err := readAddress(args, 0)
		if err != nil {
			return nil, err
		}
		return encU256(t.s.balanceOf(a)), nil
	case selAllowance:
		owner, err := readAddress(args, 0)
		if err != nil {
			return nil, err
		}
		spender, err := readAddress(args, 1)
		if err != nil {
			return nil, err
		}
		return encU256(t.s.allowance(owner, spender)), nil
	case selApprove:
		spender, err := readAddress(args, 0)
		if err != nil {
			return nil, err
		}
		amount, err := readU256(args, 1)
		if err != nil {
			return nil, err
		}
		return t.approve(t.ctx.Caller, spender, amount)
	case selTransfer:
		to, err := readAddress(args, 0)
		if err != nil {
			return nil, err
		}
		amount, err := readU256(args, 1)
		if err != nil {
			return nil, err
		}
		return t.transfer(t.ctx.Caller, to, amount)
	case selTransferFrom:
		from, err := readAddress(args, 0)
		if err != nil {
			return nil, err
		}
		to, err := readAddress(args, 1)
		if err != nil {
			return nil, err
		}
		amount, err := readU256(args, 2)
		if err != nil {
			return nil, err
		}
		return t.transferFrom(t.ctx.Caller, from, to, amount)
	}
	if ret, err, ok := t.dispatchAdmin(sel, args); ok {
		return ret, err
	}
	if ret, err, ok := t.dispatchMetadata(sel, args); ok {
		return ret, err
	}
	if ret, err, ok := t.dispatchPermitMemo(sel, args); ok {
		return ret, err
	}
	if ret, err, ok := t.dispatchMemoFormat(sel, args); ok {
		return ret, err
	}
	return nil, vm.ErrExecutionReverted
}

// --- ERC-20 core ------------------------------------------------------------

func (t cas20Token) approve(owner, spender common.Address, amount *uint256.Int) ([]byte, error) {
	if t.ctx.ReadOnly {
		return nil, vm.ErrWriteProtection
	}
	// owner is msg.sender, so this only trips in a frame with no caller; the check is declared anyway.
	if owner == (common.Address{}) {
		return nil, revCAS20("InvalidApprover(address)", errSelInvalidApprover, addrKey(owner))
	}
	if spender == (common.Address{}) {
		return nil, revCAS20("InvalidSpender(address)", errSelInvalidSpender, addrKey(spender))
	}
	// Neither the pause features (BEP-702 3.9) nor the policy scopes (3.8) name approve.
	t.s.setAllowance(owner, spender, amount)
	if !t.emit(cas20TopicApproval, owner, spender, amount) {
		return nil, vm.ErrOutOfGas
	}
	return encBool(true), nil
}

func (t cas20Token) transfer(from, to common.Address, amount *uint256.Int) ([]byte, error) {
	if t.ctx.ReadOnly {
		return nil, vm.ErrWriteProtection
	}
	if t.isPaused(cas20PauseTransfer) {
		return nil, revCAS20("ContractPaused(uint8)", errSelContractPaused, wU8(cas20PauseTransfer))
	}
	if err := t.move(from, to, amount); err != nil {
		return nil, err
	}
	if !t.emit(cas20TopicTransfer, from, to, amount) {
		return nil, vm.ErrOutOfGas
	}
	return encBool(true), nil
}

func (t cas20Token) transferFrom(spender, from, to common.Address, amount *uint256.Int) ([]byte, error) {
	if t.ctx.ReadOnly {
		return nil, vm.ErrWriteProtection
	}
	if t.isPaused(cas20PauseTransfer) {
		return nil, revCAS20("ContractPaused(uint8)", errSelContractPaused, wU8(cas20PauseTransfer))
	}
	// Zero-address checks before the allowance: a bad receiver is reported as such
	// whatever the allowance is. move repeats them for the direct path.
	if to == (common.Address{}) {
		return nil, revCAS20("InvalidReceiver(address)", errSelInvalidReceiver, addrKey(to))
	}
	if from == (common.Address{}) {
		return nil, revCAS20("InvalidSender(address)", errSelInvalidSender, addrKey(from))
	}
	slot := t.s.allowanceSlot(from, spender)
	allowed := t.s.getU256At(slot)
	infinite := allowed.Eq(maxU256)
	if !infinite && allowed.Lt(amount) {
		return nil, revCAS20("InsufficientAllowance(address,uint256,uint256)", errSelInsufficientAllow,
			addrKey(spender), wU256(allowed), wU256(amount))
	}
	// After the allowance: an unauthorized executor with too little allowance is told about the allowance.
	if !t.privileged && spender != from {
		if _, _, executor := t.s.transferPolicies(); !t.policyAllows(executor, spender) {
			return nil, revCAS20("PolicyForbids(bytes32,uint64)", errSelPolicyForbids,
				scopeTransferExecutor, wU64(executor))
		}
	}
	if !infinite {
		t.s.setU256At(slot, new(uint256.Int).Sub(allowed, amount))
	}
	if err := t.move(from, to, amount); err != nil {
		return nil, err
	}
	if !t.emit(cas20TopicTransfer, from, to, amount) {
		return nil, vm.ErrOutOfGas
	}
	return encBool(true), nil
}

func (t cas20Token) policyAllows(id uint64, account common.Address) bool {
	if id == 0 {
		return true
	}
	return newPolicyReg(t.ctx).isAuthorized(id, account)
}

func (t cas20Token) move(from, to common.Address, amount *uint256.Int) error {
	if to == (common.Address{}) {
		return revCAS20("InvalidReceiver(address)", errSelInvalidReceiver, addrKey(to))
	}
	if from == (common.Address{}) {
		return revCAS20("InvalidSender(address)", errSelInvalidSender, addrKey(from))
	}
	if !t.privileged {
		sender, receiver, _ := t.s.transferPolicies()
		if !t.policyAllows(sender, from) {
			return revCAS20("PolicyForbids(bytes32,uint64)", errSelPolicyForbids,
				scopeTransferSender, wU64(sender))
		}
		if !t.policyAllows(receiver, to) {
			return revCAS20("PolicyForbids(bytes32,uint64)", errSelPolicyForbids,
				scopeTransferReceiver, wU64(receiver))
		}
	}
	// Both writes happen even when from == to or amount is zero: bytecode would
	// perform them, and skipping them would underprice the native token (BEP-702 3.14).
	fromSlot := t.s.balanceSlot(from)
	bal := t.s.getU256At(fromSlot)
	if bal.Lt(amount) {
		return revCAS20("InsufficientBalance(address,uint256,uint256)", errSelInsufficientBalance,
			addrKey(from), wU256(bal), wU256(amount))
	}
	t.s.setU256At(fromSlot, new(uint256.Int).Sub(bal, amount))
	toSlot := t.s.balanceSlot(to)
	t.s.setU256At(toSlot, new(uint256.Int).Add(t.s.getU256At(toSlot), amount))
	return nil
}

func (t cas20Token) isPaused(bit uint) bool {
	return new(uint256.Int).Rsh(t.s.paused(), bit).Uint64()&1 == 1
}

func (t cas20Token) emit(topic0 common.Hash, a, b common.Address, value *uint256.Int) bool {
	v := value.Bytes32()
	return t.ctx.AddLog([]common.Hash{topic0, addrKey(a), addrKey(b)}, v[:])
}
