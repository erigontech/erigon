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

// FrameEntryPoint supplies the protocol caller for DEFAULT and VERIFY frames.
// Frames enter their targets directly, without executing an entry-point contract.
var FrameEntryPoint = accounts.InternAddress(common.HexToAddress("0xaa"))

var ErrFrameContext = errors.New("opcode requires a frame transaction")

var ErrFrameSkipped = errors.New("frame skipped")

func opApprove(pc uint64, evm *EVM, call *CallContext) (uint64, []byte, error) {
	if evm.FrameContext == nil {
		return pc, nil, ErrFrameContext
	}
	if evm.FrameContext.Transaction.Frames[evm.FrameContext.Index].Mode == types.FramePostTx {
		return pc, nil, ErrWriteProtection
	}
	offset, length := call.Stack.pop2Uint64()
	scope := call.Stack.pop()
	if !scope.IsUint64() {
		return pc, nil, ErrExecutionReverted
	}
	if err := evm.approveFrame(call.Address(), scope.Uint64()); err != nil {
		return pc, nil, err
	}
	// Successful approval also returns from the current EVM call.
	return pc, evm.output(&call.Memory, offset, length), errStopToken
}

// approveFrame is shared by the APPROVE opcode and default EOA verification so
// both paths enforce the same approval scopes and payment side effects.
func (evm *EVM) approveFrame(address accounts.Address, scope uint64) error {
	ctx := evm.FrameContext
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
		if !ctx.GasBailout {
			balance, err := evm.intraBlockState.GetBalance(target)
			if err != nil {
				return fmt.Errorf("%w: %w", ErrIntraBlockStateFailed, err)
			}
			if balance.Lt(&ctx.MaxCost) {
				return ErrExecutionReverted
			}
		}
		// Payment approval consumes the nonce set exactly once. The state journal
		// makes the nonce updates, fee debit and approval context revert together.
		if err := evm.consumeFrameNonce(); err != nil {
			return err
		}
		if !ctx.GasBailout {
			if err := evm.intraBlockState.SubBalance(target, ctx.MaxCost, tracing.BalanceDecreaseGasBuy); err != nil {
				return fmt.Errorf("%w: %w", ErrIntraBlockStateFailed, err)
			}
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
		// Earlier frames may have changed the account nonce. Increment its current
		// value rather than assigning the transaction's sequence plus one.
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
	// Keyed-nonce access is protocol bookkeeping, prepaid in intrinsic gas.
	// It records normal state accesses but must not warm the address or slots.
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
			ctx.RecordStorageCharge(manager, slots[i])
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

// ExecuteTransactionFrame enters one frame with its caller and gas budgets.
// Nested calls share its state-gas pool; the protocol executor owns frame rollback
// and transaction-level sequencing, receipts and fee settlement.
func (evm *EVM) ExecuteTransactionFrame() (ret []byte, executionLeft uint64, err error) {
	ctx := evm.FrameContext
	tx := ctx.Transaction
	frame := &tx.Frames[ctx.Index]
	target := accounts.InternAddress(frame.ResolvedTarget(tx.SenderAddress))
	caller := FrameEntryPoint
	if frame.Mode == types.FrameSender {
		caller = accounts.InternAddress(tx.SenderAddress)
	}
	entered := false
	if tracer := evm.Config().Tracer; tracer.HasEnterHook() || tracer.HasExitHook() {
		// Default verification and failures before the EVM call still need a frame entry.
		defer func() {
			if entered {
				return
			}
			typ := CALL
			if frame.Mode == types.FrameVerify || frame.Mode == types.FramePostTx {
				typ = STATICCALL
			}
			evm.captureBegin(0, typ, caller, target, false, frame.Data, frame.Limits, frame.Value, nil)
			evm.captureEnd(0, typ, caller, target, mdgas.MdGas{Execution: executionLeft, State: ctx.StateGasLeft()}, mdgas.MdGasUsage{}, ret, err)
		}()
	}
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
	evm.Origin = caller
	var bailout bool
	if !frame.Value.IsZero() {
		canTransfer, err := evm.Context.CanTransfer(evm.intraBlockState, caller, frame.Value)
		if err != nil {
			return nil, gas.Execution, fmt.Errorf("%w: %w", ErrIntraBlockStateFailed, err)
		}
		if !canTransfer {
			if !ctx.GasBailout {
				return nil, gas.Execution, ErrExecutionReverted
			}
			bailout = true
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
			// Signature validity was checked before execution. Default EOA code
			// still checks that the required signature authorizes this target and scope.
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
			err = evm.approveFrame(target, uint64(scope))
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
	entered = true
	if frame.Mode == types.FrameVerify || frame.Mode == types.FramePostTx {
		ret, left, _, err = evm.StaticCall(caller, target, frame.Data, gas)
	} else {
		ret, left, _, err = evm.Call(caller, target, frame.Data, gas, frame.Value, bailout)
	}
	return ret, left.Execution, err
}

func (evm *EVM) TraceSkippedFrame(index int) {
	tracer := evm.Config().Tracer
	if !tracer.HasEnterHook() && !tracer.HasExitHook() {
		return
	}
	tx := evm.FrameContext.Transaction
	frame := &tx.Frames[index]
	caller, typ := FrameEntryPoint, CALL
	if frame.Mode == types.FrameSender {
		caller = accounts.InternAddress(tx.SenderAddress)
	} else if frame.Mode == types.FrameVerify || frame.Mode == types.FramePostTx {
		typ = STATICCALL
	}
	tracer.EmitEnter(1, byte(typ), caller, accounts.InternAddress(frame.ResolvedTarget(tx.SenderAddress)), false, frame.Data, frame.Limits, frame.Value, nil)
	tracer.EmitExit(1, nil, mdgas.MdGasUsage{}, ErrFrameSkipped, true)
}
