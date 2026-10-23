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

package vm

import (
	"errors"
	"fmt"
	"math"

	"github.com/holiman/uint256"

	"github.com/erigontech/erigon/common"
	"github.com/erigontech/erigon/execution/protocol/mdgas"
	"github.com/erigontech/erigon/execution/protocol/params"
	"github.com/erigontech/erigon/execution/tracing"
	"github.com/erigontech/erigon/execution/types"
	"github.com/erigontech/erigon/execution/types/accounts"
)

var FrameEntryPoint = accounts.InternAddress(common.HexToAddress("0xaa"))

var ErrFrameContext = errors.New("opcode requires a frame transaction")

func opApprove(pc uint64, evm *EVM, call *CallContext) (uint64, []byte, error) {
	if evm.FrameContext == nil {
		return pc, nil, ErrFrameContext
	}
	offset, length := call.Stack.pop2Uint64()
	scope := call.Stack.pop()
	if !scope.IsUint64() {
		return pc, nil, ErrExecutionReverted
	}
	if err := evm.ApproveFrame(call.Address(), scope.Uint64()); err != nil {
		return pc, nil, err
	}
	return pc, evm.output(&call.Memory, offset, length), errStopToken
}

func (evm *EVM) ApproveFrame(address accounts.Address, scope uint64) error {
	ctx := evm.FrameContext
	if ctx == nil {
		return ErrFrameContext
	}
	tx := ctx.Transaction
	frame := &tx.Frames[ctx.Index]
	target := accounts.InternAddress(frame.ResolvedTarget(tx.SenderAddress))
	sender := accounts.InternAddress(tx.SenderAddress)
	if address != target || scope == 0 || scope & ^uint64(frame.Flags&types.FrameApproveBoth) != 0 {
		return ErrExecutionReverted
	}
	approved, payer := ctx.SenderApproved, ctx.Payer
	if scope&types.FrameApproveExecution != 0 {
		if approved || target != sender {
			return ErrExecutionReverted
		}
		approved = true
	}
	if scope&types.FrameApprovePayment != 0 {
		if !payer.IsNil() || !approved {
			return ErrExecutionReverted
		}
		balance, err := evm.intraBlockState.GetBalance(target)
		if err != nil {
			return fmt.Errorf("%w: %w", ErrIntraBlockStateFailed, err)
		}
		if balance.Lt(&ctx.MaxCost) {
			return ErrExecutionReverted
		}
		if err := evm.consumeFrameNonce(); err != nil {
			return err
		}
		if err := evm.intraBlockState.SubBalance(target, ctx.MaxCost, tracing.BalanceDecreaseGasBuy); err != nil {
			return fmt.Errorf("%w: %w", ErrIntraBlockStateFailed, err)
		}
		payer = target
	}
	ctx.SetApproval(approved, payer)
	return nil
}

func (evm *EVM) consumeFrameNonce() error {
	ctx := evm.FrameContext
	tx := ctx.Transaction
	ibs := evm.intraBlockState
	if tx.UsesLegacyNonce() {
		sender := accounts.InternAddress(tx.SenderAddress)
		nonce, err := ibs.GetNonce(sender)
		if err != nil {
			return fmt.Errorf("%w: %w", ErrIntraBlockStateFailed, err)
		}
		if nonce == math.MaxUint64 {
			return ErrNonceUintOverflow
		}
		empty, err := ibs.Empty(sender)
		if err != nil {
			return fmt.Errorf("%w: %w", ErrIntraBlockStateFailed, err)
		}
		if empty && !ctx.ChargeStateGas(params.StateGasNewAccount) {
			return ErrOutOfGas
		}
		if err := ibs.SetNonce(sender, nonce+1, tracing.NonceChangeEoACall); err != nil {
			return fmt.Errorf("%w: %w", ErrIntraBlockStateFailed, err)
		}
		return nil
	}
	manager := accounts.InternAddress(types.FrameNonceManager)
	var slots [types.FrameMaxNonceKeys]accounts.StorageKey
	var stateGas uint64
	for i, key := range tx.NonceKeys {
		slots[i] = accounts.InternKey(types.FrameNonceSlot(tx.SenderAddress, key))
		sequence, err := ibs.GetState(manager, slots[i])
		if err != nil {
			return fmt.Errorf("%w: %w", ErrIntraBlockStateFailed, err)
		}
		if sequence.IsZero() {
			stateGas += params.StateGasPerStorageSet
		}
	}
	if !ctx.ChargeStateGas(stateGas) {
		return ErrOutOfGas
	}
	sequence := *uint256.NewInt(tx.Nonce + 1)
	for _, slot := range slots[:len(tx.NonceKeys)] {
		if err := ibs.SetState(manager, slot, sequence); err != nil {
			return fmt.Errorf("%w: %w", ErrIntraBlockStateFailed, err)
		}
	}
	return nil
}

func (evm *EVM) ExecuteTransactionFrame() (ret []byte, executionLeft uint64, err error) {
	ctx := evm.FrameContext
	tx := ctx.Transaction
	frame := &tx.Frames[ctx.Index]
	target := accounts.InternAddress(frame.ResolvedTarget(tx.SenderAddress))
	gas := frame.Limits
	accessCost := coldAccountAccessCost(evm.chainRules)
	if evm.intraBlockState.AddressInAccessList(target) {
		accessCost = params.WarmStorageReadCostEIP2929
	}
	if gas.Execution < accessCost {
		return nil, 0, ErrOutOfGas
	}
	gas.Execution -= accessCost
	evm.intraBlockState.AddAddressToAccessList(target)
	evm.intraBlockState.MarkAddressAccess(target, false)
	caller := FrameEntryPoint
	if frame.Mode == types.FrameSender {
		caller = accounts.InternAddress(tx.SenderAddress)
	}
	evm.Origin = caller
	if !frame.Value.IsZero() {
		canTransfer, err := evm.Context.CanTransfer(evm.intraBlockState, caller, frame.Value)
		if err != nil {
			return nil, gas.Execution, fmt.Errorf("%w: %w", ErrIntraBlockStateFailed, err)
		}
		if !canTransfer {
			return nil, gas.Execution, ErrExecutionReverted
		}
		empty, err := evm.intraBlockState.Empty(target)
		if err != nil {
			return nil, gas.Execution, fmt.Errorf("%w: %w", ErrIntraBlockStateFailed, err)
		}
		if empty && !ctx.ChargeStateGas(params.StateGasNewAccount) {
			return nil, 0, ErrOutOfGas
		}
	}
	_, precompile := evm.precompile(target)
	if !precompile {
		codeHash, err := evm.intraBlockState.GetCodeHash(target)
		if err != nil {
			return nil, gas.Execution, fmt.Errorf("%w: %w", ErrIntraBlockStateFailed, err)
		}
		if frame.Mode == types.FrameVerify && codeHash.IsEmpty() {
			scope := frame.Flags & types.FrameApproveBoth
			index := 1
			if scope&types.FrameApproveExecution != 0 {
				index = 0
			}
			if scope == 0 || index >= len(tx.Signatures) {
				return nil, gas.Execution, ErrExecutionReverted
			}
			sig := &tx.Signatures[index]
			if sig.Scheme != types.FrameSignatureSecp256k1 || len(sig.Message) != 0 || sig.ResolvedSigner(tx.SenderAddress) != target.Value() {
				return nil, gas.Execution, ErrExecutionReverted
			}
			err = evm.ApproveFrame(target, uint64(scope))
			if errors.Is(err, ErrOutOfGas) {
				return nil, 0, err
			}
			return nil, gas.Execution, err
		}
		delegated, ok, err := evm.intraBlockState.GetDelegatedDesignation(target)
		if err != nil {
			return nil, gas.Execution, fmt.Errorf("%w: %w", ErrIntraBlockStateFailed, err)
		}
		if ok {
			cost := coldAccountAccessCost(evm.chainRules)
			if evm.intraBlockState.AddressInAccessList(delegated) {
				cost = params.WarmStorageReadCostEIP2929
			}
			if gas.Execution < cost {
				return nil, 0, ErrOutOfGas
			}
			gas.Execution -= cost
			evm.intraBlockState.AddAddressToAccessList(delegated)
		}
	}
	gas.State = ctx.StateGasLeft()
	var left mdgas.MdGas
	if frame.Mode == types.FrameVerify {
		ret, left, _, err = evm.StaticCall(caller, target, frame.Data, gas)
	} else {
		ret, left, _, err = evm.Call(caller, target, frame.Data, gas, frame.Value, false)
	}
	return ret, left.Execution, err
}
