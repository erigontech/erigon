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

package protocol

import (
	"bytes"
	"encoding/binary"
	"errors"
	"fmt"
	"maps"
	"math"
	"slices"

	"github.com/holiman/uint256"

	"github.com/erigontech/erigon/common"
	"github.com/erigontech/erigon/execution/protocol/mdgas"
	"github.com/erigontech/erigon/execution/tracing"
	"github.com/erigontech/erigon/execution/types"
	"github.com/erigontech/erigon/execution/types/accounts"
	"github.com/erigontech/erigon/execution/vm"
)

// FrameValidation describes a successful pool simulation at one state snapshot.
// It supplies payer reservations and dependencies for invalidating that result;
// it does not guarantee that the transaction's execution frames will succeed.
type FrameValidation struct {
	Payer          common.Address
	MaxCost        uint256.Int
	PayerBalance   uint256.Int
	Paymaster      bool
	Deadline       uint64
	Dependencies   []common.Address
	CreationNonces []FrameCreationNonce
}

// FrameCreationNonce bounds the creator nonce that preserves validation. CREATE
// uses an exact nonce; CREATE2 can use a range bounded by nonce overflow.
type FrameCreationNonce struct {
	Address  common.Address
	MinNonce uint64
	MaxNonce uint64
}

// frameValidationTracer enforces pool-only restrictions through EVM hooks, so
// simulation shares the consensus executor without restricting mined transactions.
type frameValidationTracer struct {
	FrameValidation
	evm          *vm.EVM
	end          int
	deploy       int
	err          error
	dependencies map[common.Address]struct{}
	creations    map[common.Address]frameCreationTrace
}

type frameCreationTrace struct {
	count uint64
	exact bool
}

var frameExpiryCode = common.FromHex("60083614600a575f5ffd5b5f3560c01c4211601657005b5f5ffd")

// ValidateFrameTransactionPrefix checks public-pool structure and gas budgets
// without verifying signatures or accessing state.
func ValidateFrameTransactionPrefix(tx *types.FrameTransaction) error {
	_, err := frameValidationPrefix(tx)
	return err
}

// ValidateFrameTransaction applies public-mempool policy to the validation prefix,
// stopping after payment approval and reverting all simulated state changes.
func ValidateFrameTransaction(evm *vm.EVM, tx *types.FrameTransaction) (*FrameValidation, error) {
	validation, err := frameValidationPrefix(tx)
	if err != nil {
		return nil, err
	}
	msg, err := tx.AsMessage(types.Signer{}, &evm.Context.BaseFee, evm.ChainRules())
	if err != nil {
		return nil, err
	}
	if tx.Frames[0].IsExpiryVerifier() && validation.Deadline < evm.Context.Time {
		return nil, errors.New("expired frame transaction")
	}
	config, txContext := evm.Config(), evm.TxContext
	defer evm.ResetBetweenBlocks(evm.Context, txContext, evm.IntraBlockState(), config, evm.ChainRules())
	validation.evm = evm
	validation.dependencies = map[common.Address]struct{}{tx.SenderAddress: {}}
	if !tx.UsesLegacyNonce() {
		// Keyed nonces depend on the manager account as well as their selected storage slots.
		validation.dependencies[types.FrameNonceManager] = struct{}{}
	}
	config.Tracer = &tracing.Hooks{OnOpcodeV2: validation.onOpcode, OnEnterV2: validation.onEnter}
	evm.ResetBetweenBlocks(evm.Context, NewEVMTxContext(msg), evm.IntraBlockState(), config, evm.ChainRules())
	st := NewTxnExecutor(evm, msg, NewGasPool(msg.Gas(), tx.GetBlobGas()))
	if _, err := st.executeFrameTransaction(tx, true, false, validation); err != nil {
		return nil, err
	}
	validation.Dependencies = make([]common.Address, 0, len(validation.dependencies))
	validation.Dependencies = slices.AppendSeq(validation.Dependencies, maps.Keys(validation.dependencies))
	// Simulation has reverted state, so these bounds use the pre-prefix nonce.
	for address, creation := range validation.creations {
		nonce, err := evm.IntraBlockState().GetNonce(accounts.InternAddress(address))
		if err != nil {
			return nil, err
		}
		// Counting reverted attempts too only narrows the safe CREATE2 range.
		dependency := FrameCreationNonce{Address: address, MaxNonce: math.MaxUint64 - creation.count}
		if creation.exact || nonce > dependency.MaxNonce {
			// A prefix may ignore a failed creation. Preserve its overflow outcome too.
			dependency.MinNonce, dependency.MaxNonce = nonce, nonce
		}
		validation.CreationNonces = append(validation.CreationNonces, dependency)
	}
	return &validation.FrameValidation, nil
}

// frameValidationPrefix recognizes optional expiry and deployment frames followed by
// sender approval and, if separate, payer approval. Restricting this prefix
// bounds unpaid validation work; execution frames after it are not simulated.
func frameValidationPrefix(tx *types.FrameTransaction) (*frameValidationTracer, error) {
	invalid := errors.New("unrecognized frame validation prefix")
	if len(tx.Frames) == 0 {
		return nil, invalid
	}
	v := &frameValidationTracer{deploy: -1}
	i := 0
	if f := &tx.Frames[0]; f.IsExpiryVerifier() {
		if len(f.Data) != 8 || f.Flags != 0 || f.Limits.State != 0 {
			return nil, invalid
		}
		v.Deadline = binary.BigEndian.Uint64(f.Data)
		i++
	}
	if i < len(tx.Frames) && tx.Frames[i].Mode == types.FrameDefault && tx.Frames[i].Flags == 0 {
		v.deploy = i
		i++
	}
	if i >= len(tx.Frames) {
		return nil, invalid
	}
	verify := &tx.Frames[i]
	if verify.Mode != types.FrameVerify || verify.ResolvedTarget(tx.SenderAddress) != tx.SenderAddress {
		return nil, invalid
	}
	switch verify.Flags {
	case types.FrameApproveBoth:
	case types.FrameApproveExecution:
		i++
		if i >= len(tx.Frames) || tx.Frames[i].Mode != types.FrameVerify || tx.Frames[i].Flags != types.FrameApprovePayment {
			return nil, invalid
		}
	default:
		return nil, invalid
	}
	v.end = i
	var execution, state uint64
	for _, sig := range tx.Signatures {
		execution += sig.VerificationGas()
		if execution > 100_000 {
			return nil, errors.New("frame signature validation exceeds gas limit")
		}
	}
	for j, frame := range tx.Frames {
		if j > i {
			if frame.Mode == types.FrameVerify {
				return nil, invalid
			}
			continue
		}
		if frame.Limits.Execution > 100_000-execution || frame.Limits.State > 500_000-state {
			return nil, errors.New("frame validation exceeds gas limit")
		}
		execution += frame.Limits.Execution
		state += frame.Limits.State
	}
	return v, nil
}

func (v *frameValidationTracer) fail(err error) {
	if v.err == nil {
		v.err = err
		v.evm.Cancel()
	}
}

// checkTarget records code dependencies, including a sender's delegated code.
// A change to either account can invalidate an otherwise unchanged sender's approval.
func (v *frameValidationTracer) checkTarget(address accounts.Address, top bool) {
	if v.err != nil {
		return
	}
	v.dependencies[address.Value()] = struct{}{}
	if _, ok := vm.Precompiles(v.evm.ChainRules())[address]; ok {
		return
	}
	ctx := v.evm.FrameContext
	sender := address.Value() == ctx.Transaction.SenderAddress
	delegated, ok, err := v.evm.IntraBlockState().GetDelegatedDesignation(address)
	if err != nil {
		v.fail(err)
		return
	}
	if ok {
		if !top || !sender {
			v.fail(errors.New("delegated validation target"))
			return
		}
		v.dependencies[delegated.Value()] = struct{}{}
	}
	codeHash, err := v.evm.IntraBlockState().GetCodeHash(address)
	if err != nil {
		v.fail(err)
	} else if codeHash.IsEmpty() && !sender && !(top && ctx.Transaction.Frames[ctx.Index].Mode == types.FrameVerify) {
		v.fail(errors.New("validation target has no code"))
	}
}

func (v *frameValidationTracer) beforeFrame() error {
	ctx := v.evm.FrameContext
	f := &ctx.Transaction.Frames[ctx.Index]
	target := accounts.InternAddress(f.ResolvedTarget(ctx.Transaction.SenderAddress))
	v.checkTarget(target, true)
	if ctx.Index == v.end {
		// APPROVE debits MaxCost. Reservations need the payer's balance before
		// that debit so the same transaction is not counted twice.
		var err error
		v.PayerBalance, err = v.evm.IntraBlockState().GetBalance(target)
		if err != nil {
			return err
		}
		if f.Flags == types.FrameApprovePayment {
			hash, err := v.evm.IntraBlockState().GetCodeHash(target)
			if err != nil {
				return err
			}
			v.Paymaster = !hash.IsEmpty()
		}
	}
	return v.err
}

func (v *frameValidationTracer) afterFrame() error {
	if v.err != nil {
		return v.err
	}
	ctx := v.evm.FrameContext
	f := &ctx.Transaction.Frames[ctx.Index]
	if f.Flags&types.FrameApproveExecution != 0 && !ctx.SenderApproved || f.Flags&types.FrameApprovePayment != 0 && ctx.Payer.IsNil() {
		return errors.New("validation frame did not approve its scope")
	}
	if ctx.Index == v.deploy {
		hash, err := v.evm.IntraBlockState().GetCodeHash(accounts.InternAddress(ctx.Transaction.SenderAddress))
		if err != nil {
			return err
		}
		if hash.IsEmpty() {
			return errors.New("deployment frame did not install sender code")
		}
	}
	v.Payer, v.MaxCost = ctx.Payer.Value(), ctx.MaxCost
	return nil
}

func (v *frameValidationTracer) onOpcode(pc uint64, op byte, gas mdgas.MdGas, cost mdgas.MdGasCost, scope tracing.OpContext, rData []byte, depth int, opErr error) {
	if v.err != nil || opErr != nil {
		return
	}
	ctx := v.evm.FrameContext
	deploy := ctx.Index == v.deploy
	stack := scope.StackData()
	banned := false
	switch vm.OpCode(op) {
	case vm.GASPRICE, vm.BLOCKHASH, vm.COINBASE, vm.NUMBER, vm.DIFFICULTY, vm.GASLIMIT, vm.BASEFEE, vm.BLOBBASEFEE, vm.SLOTNUM, vm.INVALID, vm.SELFDESTRUCT, vm.BALANCE, vm.SELFBALANCE:
		banned = true
	case vm.TIMESTAMP:
		frame := &ctx.Transaction.Frames[ctx.Index]
		banned = ctx.Index != 0 || !frame.IsExpiryVerifier() || scope.Address().Value() != types.FrameExpiryVerifier || !bytes.Equal(scope.Code(), frameExpiryCode)
	case vm.GAS:
		code := scope.Code()
		banned = pc+1 >= uint64(len(code))
		if !banned {
			next := vm.OpCode(code[pc+1])
			banned = next != vm.CALL && next != vm.CALLCODE && next != vm.DELEGATECALL && next != vm.STATICCALL
		}
	case vm.CREATE, vm.CREATE2:
		banned = !deploy
		// Helper balances are not allowed dependencies under EIP-8141's pool policy:
		// https://eips.ethereum.org/EIPS/eip-8141#policy-summary
		if len(stack) > 0 && !stack[len(stack)-1].IsZero() {
			v.fail(errors.New("value transfer during frame validation"))
			return
		}
		if !banned {
			if v.creations == nil {
				v.creations = make(map[common.Address]frameCreationTrace)
			}
			address := scope.Address().Value()
			creation := v.creations[address]
			creation.count++
			creation.exact = creation.exact || vm.OpCode(op) == vm.CREATE
			v.creations[address] = creation
		}
	case vm.SSTORE:
		banned = !deploy || scope.Address().Value() != ctx.Transaction.SenderAddress
	case vm.SLOAD:
		banned = scope.Address().Value() != ctx.Transaction.SenderAddress
	case vm.EXTCODESIZE, vm.EXTCODEHASH, vm.EXTCODECOPY:
		if len(stack) > 0 {
			address := accounts.InternAddress(common.Address(stack[len(stack)-1].Bytes20()))
			if vm.OpCode(op) == vm.EXTCODEHASH {
				// Empty and funded precompiles have different EXTCODEHASH results,
				// exposing a helper balance forbidden by the pool policy:
				// https://eips.ethereum.org/EIPS/eip-8141#policy-summary
				// Proposed clarification: https://github.com/ethereum/EIPs/pull/12596
				_, banned = vm.Precompiles(v.evm.ChainRules())[address]
			}
			v.checkTarget(address, false)
		}
	case vm.CALL, vm.CALLCODE, vm.DELEGATECALL, vm.STATICCALL:
		if len(stack) > 1 {
			v.checkTarget(accounts.InternAddress(common.Address(stack[len(stack)-2].Bytes20())), false)
		}
	case vm.APPROVE:
		if len(stack) >= 3 {
			banned = !stack[len(stack)-3].IsUint64() || stack[len(stack)-3].Uint64() != uint64(ctx.Transaction.Frames[ctx.Index].Flags)
		}
	}
	if banned {
		v.fail(fmt.Errorf("opcode %s is not allowed in frame validation", vm.OpCode(op)))
	}
}

func (v *frameValidationTracer) onEnter(depth int, typ byte, from, to accounts.Address, precompile bool, input []byte, gas mdgas.MdGas, value uint256.Int, code []byte) {
	ctx := v.evm.FrameContext
	if typ == byte(vm.CREATE) || typ == byte(vm.CREATE2) {
		if ctx.Index != v.deploy || to.Value() != ctx.Transaction.SenderAddress {
			v.fail(errors.New("validation may only create the sender account"))
		}
	} else if !value.IsZero() {
		v.fail(errors.New("value transfer during frame validation"))
	}
}
