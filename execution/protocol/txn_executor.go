// Copyright 2014 The go-ethereum Authors
// (original work)
// Copyright 2024 The Erigon Authors
// (modifications)
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
	"errors"
	"fmt"
	"slices"

	"github.com/holiman/uint256"

	"github.com/erigontech/erigon/common"
	"github.com/erigontech/erigon/common/dbg"
	"github.com/erigontech/erigon/common/log/v3"
	"github.com/erigontech/erigon/common/math"
	"github.com/erigontech/erigon/common/u256"
	"github.com/erigontech/erigon/execution/protocol/mdgas"
	"github.com/erigontech/erigon/execution/protocol/params"
	"github.com/erigontech/erigon/execution/protocol/rules"
	"github.com/erigontech/erigon/execution/state"
	"github.com/erigontech/erigon/execution/tracing"
	"github.com/erigontech/erigon/execution/types"
	"github.com/erigontech/erigon/execution/types/accounts"
	"github.com/erigontech/erigon/execution/vm"
	"github.com/erigontech/erigon/execution/vm/evmtypes"
)

/*
TxnExecutor applies a single transaction to the current world state.

 1. Compute intrinsic gas and run pre-execution validation (preCheck): nonce,
    sender eligibility, block gas availability, fee caps, transaction gas caps,
    SetCode prerequisites, affordability, intrinsic gas, and initcode size.
    This phase does not mutate state or reserve block gas.
 2. Reserve blob gas and, unless gas bailout is enabled, debit the precomputed
    gas and blob fees (buyGas)
 3. Increment sender nonce
 4. Execute: if contract creation, run initcode and store result as code;
    otherwise, call the recipient
 5. Refund unused gas to the sender; deduct gas used from the block pool
 6. Pay tips to coinbase, burn base fee
*/

var ErrTxnExecutionFailed = errors.New("txn execution failed")

type ErrExecAbortError struct {
	DependencyTxIndex int
	OriginError       error
}

// ErrExecPanic is a recovered non-dependency panic during transaction execution.
// It is an operational failure, not evidence that the block is invalid.
type ErrExecPanic struct {
	message string
}

func (e *ErrExecPanic) Error() string {
	return e.message
}

func (e ErrExecAbortError) Error() string {
	if e.DependencyTxIndex >= 0 {
		return fmt.Sprintf("execution aborted due to dependency %d", e.DependencyTxIndex)
	} else {
		if e.OriginError != nil {
			return e.OriginError.Error()
		}
		return "execution aborted"
	}
}

// IsError reports whether the abort carries an execution error rather than only
// a speculative dependency. Dependency aborts raised by state.ErrDependency
// carry no OriginError and are retried; DependencyTxIndex is scheduling
// metadata, not the classifier. An OriginError must be validated against settled
// input before it can be attributed to block data rather than stale state.
func (e ErrExecAbortError) IsError() bool {
	return e.OriginError != nil
}

// nonceError formats lazily: under parallel execution a nonce mismatch is a
// routine re-execution signal whose text is discarded.
type nonceError struct {
	err        error // ErrNonceTooHigh or ErrNonceTooLow
	from       accounts.Address
	txNonce    uint64
	stateNonce uint64
}

func (e *nonceError) Error() string {
	return fmt.Sprintf("%s: address %v, tx: %d state: %d", e.err, e.from, e.txNonce, e.stateNonce)
}

func (e *nonceError) Unwrap() error { return e.err }

type TxnExecutor struct {
	gp                    *GasPool
	msg                   Message
	gasRemaining          mdgas.MdGas
	blockExecutionGasUsed uint64 // Per-tx execution gas for block-level accounting (pre-Amsterdam: same as block gas)
	blockStateGasUsed     uint64 // Per-tx state gas for block-level Bottleneck (EIP-8037)
	txnGasUsed            uint64
	txnGasUsedB4Refunds   uint64 // txnGasUsed before refunds
	gasPrice              *uint256.Int
	feeCap                *uint256.Int
	tipCap                *uint256.Int
	value                 uint256.Int
	data                  []byte
	state                 *state.IntraBlockState
	evm                   *vm.EVM

	// If true, fee burning and tipping won't happen during transition. Instead, their values will be included in the
	// ExecutionResult, which caller can use the values to update the balance of burner and coinbase account.
	// This is useful during parallel txn execution, where the common account read/write should be minimized.
	noFeeBurnAndTip bool
}

type runtimeGasAccounting struct {
	auth     mdgas.MdGasUsage
	topLevel mdgas.MdGasUsage
	frame    mdgas.MdGasUsage
}

func (g runtimeGasAccounting) total() mdgas.MdGasUsage {
	return mdgas.MdGasUsage{
		Execution:  g.auth.Execution + g.topLevel.Execution + g.frame.Execution,
		State:      g.auth.State + g.topLevel.State + g.frame.State,
		StateSpill: g.auth.StateSpill + g.topLevel.StateSpill + g.frame.StateSpill,
	}
}

func (g *runtimeGasAccounting) consumeAllExecutionGas(execution uint64) {
	*g = runtimeGasAccounting{frame: mdgas.MdGasUsage{Execution: execution}}
}

func (g *runtimeGasAccounting) refillTopLevelState(gasRemaining *mdgas.MdGas, restoreState bool, vmerr error) {
	RefillTopLevelGas(gasRemaining, &g.topLevel, restoreState, vmerr)
}

func (g *runtimeGasAccounting) finishFrame(gas, gasRemaining mdgas.MdGas, vmerr error) {
	if vmerr == nil {
		return
	}
	g.frame.State = 0
	g.frame.Execution = gas.Total() - gasRemaining.Total()
}

// Message represents a message sent to a contract.
type Message interface {
	From() accounts.Address
	To() accounts.Address

	GasPrice() *uint256.Int
	FeeCap() *uint256.Int
	TipCap() *uint256.Int
	Gas() uint64
	CheckGas() bool
	BlobGas() uint64
	MaxFeePerBlobGas() *uint256.Int
	Value() *uint256.Int

	Nonce() uint64
	CheckNonce() bool
	CheckTransaction() bool
	Data() []byte
	AccessList() types.AccessList
	BlobHashes() []common.Hash
	Authorizations() []types.Authorization

	IsFree() bool // service transactions on Gnosis are exempt from EIP-1559 mandatory fees
	SetIsFree(bool)
}

// NewTxnExecutor initialises and returns a new transaction executor.
func NewTxnExecutor(evm *vm.EVM, msg Message, gp *GasPool) *TxnExecutor {
	return &TxnExecutor{
		gp:       gp,
		evm:      evm,
		msg:      msg,
		gasPrice: msg.GasPrice(),
		feeCap:   msg.FeeCap(),
		tipCap:   msg.TipCap(),
		value:    *msg.Value(),
		data:     msg.Data(),
		state:    evm.IntraBlockState(),
	}
}

// ApplyMessage computes the new state by applying the given message
// against the old state within the environment.
//
// ApplyMessage returns the bytes returned by any EVM execution (if it took place),
// the gas used (which includes gas refunds) and an error if it failed. An error always
// indicates a core error meaning that the message would always fail for that particular
// state and would never be accepted within a block.
// `refunds` is false when it is not required to apply gas refunds
// `gasBailout` is true when it is not required to fail transaction if the balance is not enough to pay gas.
// for trace_call to replicate OE/Parity behaviour
func ApplyMessage(evm *vm.EVM, msg Message, gp *GasPool, refunds bool, gasBailout bool, engine rules.EngineReader) (*evmtypes.ExecutionResult, error) {
	return applyMessage(evm, msg, gp, refunds, gasBailout, false, engine)
}

func applyMessage(evm *vm.EVM, msg Message, gp *GasPool, refunds bool, gasBailout bool, noFeeBurnAndTip bool, engine rules.EngineReader) (
	*evmtypes.ExecutionResult, error) {
	// Only zero-gas transactions may be service ones
	if msg.FeeCap().IsZero() && !msg.IsFree() && engine != nil {
		blockContext := evm.Context
		blockContext.Coinbase = params.SystemAddress
		syscall := func(contract accounts.Address, data []byte) ([]byte, error) {
			ret, err := SysCallContractWithBlockContext(contract, data, evm.ChainConfig(), evm.IntraBlockState(), blockContext, true, evm.Config())
			return ret, err
		}
		msg.SetIsFree(engine.IsServiceTransaction(msg.From(), syscall))
	}
	st := NewTxnExecutor(evm, msg, gp)
	st.noFeeBurnAndTip = noFeeBurnAndTip
	return st.Execute(refunds, gasBailout)
}

func ApplyMessageNoFeeBurnOrTip(evm *vm.EVM, msg Message, gp *GasPool, refunds bool, gasBailout bool, engine rules.EngineReader) (*evmtypes.ExecutionResult, error) {
	return applyMessage(evm, msg, gp, refunds, gasBailout, true, engine)
}

func ApplyFrame(evm *vm.EVM, msg Message, gp *GasPool) (*evmtypes.ExecutionResult, error) {
	return NewTxnExecutor(evm, msg, gp).ApplyFrame()
}

// to returns the recipient of the message.
func (st *TxnExecutor) to() accounts.Address {
	if st.msg == nil || st.msg.To().IsNil() /* contract creation */ {
		return accounts.ZeroAddress
	}
	return st.msg.To()
}

// upfrontTxnFees holds the gas and blob fees computed by preCheck for buyGas.
type upfrontTxnFees struct {
	gasVal     uint256.Int
	blobGasVal uint256.Int
}

// buyGas reserves blob gas and, unless gas bailout is enabled, debits the
// precomputed gas and blob fees. It assumes preCheck has validated the
// transaction.
func (st *TxnExecutor) buyGas(fees upfrontTxnFees, gasBailout bool) error {
	if st.evm.ChainRules().IsCancun {
		if err := st.gp.SubBlobGas(st.msg.BlobGas()); err != nil {
			return err
		}
	}

	if !gasBailout {
		if err := st.state.SubBalance(st.msg.From(), fees.gasVal, tracing.BalanceDecreaseGasBuy); err != nil {
			return err
		}
		if err := st.state.SubBalance(st.msg.From(), fees.blobGasVal, tracing.BalanceDecreaseGasBuy); err != nil {
			return err
		}
	}

	if st.evm.Config().Tracer != nil && st.evm.Config().Tracer.OnGasChange != nil {
		st.evm.Config().Tracer.OnGasChange(0, st.msg.Gas(), tracing.GasChangeTxInitialBalance)
	}

	return nil
}

func CheckEip1559TxGasFeeCap(from accounts.Address, feeCap, tipCap, baseFee *uint256.Int, isFree bool) error {
	if feeCap.Lt(tipCap) {
		return fmt.Errorf("%w: address %v, tipCap: %s, feeCap: %s", ErrTipAboveFeeCap, from, tipCap, feeCap)
	}
	if baseFee != nil && feeCap.Lt(baseFee) && !isFree {
		return fmt.Errorf("%w: address %v, feeCap: %s baseFee: %s", ErrFeeCapTooLow, from, feeCap, baseFee)
	}
	return nil
}

// preCheck validates the transaction and computes the fees for buyGas without
// mutating state or reserving block gas.
// DESCRIBED: docs/programmers_guide/guide.md#nonce
func (st *TxnExecutor) preCheck(gasBailout bool, intrinsicGasResult mdgas.IntrinsicGasCalcResult) (upfrontTxnFees, error) {
	rules := st.evm.ChainRules()
	from := st.msg.From()

	if rules.IsOsaka && len(st.msg.BlobHashes()) > params.MaxBlobsPerTxn {
		return upfrontTxnFees{}, fmt.Errorf("%w: address %v, blobs: %d", ErrTooManyBlobs, from, len(st.msg.BlobHashes()))
	}

	// Make sure this transaction's nonce is correct.
	if st.msg.CheckNonce() {
		stNonce, err := st.state.GetNonce(from)
		if err != nil {
			return upfrontTxnFees{}, fmt.Errorf("%w: %w", ErrTxnExecutionFailed, err)
		}
		if msgNonce := st.msg.Nonce(); stNonce < msgNonce {
			return upfrontTxnFees{}, &nonceError{err: ErrNonceTooHigh, from: from, txNonce: msgNonce, stateNonce: stNonce}
		} else if stNonce > msgNonce {
			return upfrontTxnFees{}, &nonceError{err: ErrNonceTooLow, from: from, txNonce: msgNonce, stateNonce: stNonce}
		} else if _, overflow := math.SafeAdd(stNonce, 1); overflow {
			return upfrontTxnFees{}, fmt.Errorf("%w: address %v, nonce: %d", ErrNonceMax,
				from, stNonce)
		}
	}

	if st.msg.CheckTransaction() {
		// Make sure the sender is an EOA (EIP-3607)
		codeHash, err := st.state.GetCodeHash(from)
		if err != nil {
			return upfrontTxnFees{}, fmt.Errorf("%w: %w", ErrTxnExecutionFailed, err)
		}
		if !codeHash.IsEmpty() {
			// common.Hash{} means that the sender is not in the state.
			// Historically there were transactions with 0 gas price and non-existing sender,
			// so we have to allow that.

			// eip-7702 allows tx origination from accounts having delegated designation code.
			_, ok, err := st.state.GetDelegatedDesignation(from)
			if err != nil {
				return upfrontTxnFees{}, fmt.Errorf("%w: %w", ErrTxnExecutionFailed, err)
			}
			if !ok {
				return upfrontTxnFees{}, fmt.Errorf("%w: address %v, codehash: %s", ErrSenderNoEOA,
					from, codeHash)
			}
		}
	}

	gas := st.msg.Gas()
	blobGas := st.msg.BlobGas()
	executionContribution, stateContribution := InclusionContributions(gas, rules.IsAmsterdam)
	if err := CheckBlockGasInclusion(st.gp, executionContribution, stateContribution, blobGas); err != nil {
		return upfrontTxnFees{}, err
	}

	if rules.IsLondon {
		// Skip the checks if gas fields are zero and baseFee was explicitly disabled (eth_call)
		skipCheck := st.evm.Config().NoBaseFee && st.feeCap.IsZero() && st.tipCap.IsZero()
		if !skipCheck {
			if err := CheckEip1559TxGasFeeCap(from, st.feeCap, st.tipCap, &st.evm.Context.BaseFee, st.msg.IsFree()); err != nil {
				return upfrontTxnFees{}, err
			}
		}
	}
	// EIP-4844.
	var maxFeePerBlobGas uint256.Int
	hasBlobGas := rules.IsCancun && blobGas > 0
	if hasBlobGas {
		blobGasPrice := st.evm.Context.BlobBaseFee
		if feeCap := st.msg.MaxFeePerBlobGas(); feeCap != nil {
			maxFeePerBlobGas.Set(feeCap)
		}
		skipBlobCheck := st.evm.Config().NoBaseFee && maxFeePerBlobGas.IsZero()
		if !skipBlobCheck && blobGasPrice.Cmp(&maxFeePerBlobGas) > 0 {
			return upfrontTxnFees{}, fmt.Errorf("%w: address %v, maxFeePerBlobGas: %s < blobGasPrice: %s",
				ErrMaxFeePerBlobGas, from, maxFeePerBlobGas.String(), blobGasPrice.String())
		}
	}

	// EIP-7825.
	requiredIntrinsicGas := max(intrinsicGasResult.ExecutionGas, intrinsicGasResult.FloorGasCost)
	if st.msg.CheckGas() && rules.IsOsaka {
		if rules.IsAmsterdam {
			// EIP-8037: TX_MAX_GAS_LIMIT applies to the execution gas dimension only.
			if requiredIntrinsicGas > params.MaxTxnGasLimit {
				return upfrontTxnFees{}, fmt.Errorf("%w: execution gas cap %d exceeds TX_MAX_GAS_LIMIT %d",
					ErrIntrinsicGas, requiredIntrinsicGas, params.MaxTxnGasLimit)
			}
		} else if gas > params.MaxTxnGasLimit {
			return upfrontTxnFees{}, fmt.Errorf("%w: address %v, gas limit %d", ErrGasLimitTooHigh, from, gas)
		}
	}

	// Match geth's EIP-7702 prerequisite precedence: after fee caps, before
	// affordability and intrinsic gas.
	if err := validateSetCodePrerequisites(st.msg.Authorizations(), st.msg.To().IsNil(), rules.IsPrague); err != nil {
		return upfrontTxnFees{}, err
	}

	var (
		fees     upfrontTxnFees
		overflow bool
	)
	fees.gasVal, overflow = u256.MulOverflow(u256.U64(gas), *st.gasPrice)
	if overflow {
		return upfrontTxnFees{}, fmt.Errorf("%w: address %v", ErrInsufficientFunds, from)
	}

	if hasBlobGas {
		fees.blobGasVal, overflow = u256.MulOverflow(st.evm.Context.BlobBaseFee, u256.U64(blobGas))
		if overflow {
			return upfrontTxnFees{}, fmt.Errorf("%w: overflow converting blob gas: %s", ErrInsufficientFunds, fees.blobGasVal.String())
		}
	}

	if !gasBailout {
		balanceCheck := fees.gasVal
		if st.feeCap != nil {
			balanceCheck, overflow = u256.MulOverflow(u256.U64(gas), *st.feeCap)
			if overflow {
				return upfrontTxnFees{}, fmt.Errorf("%w: address %v", ErrInsufficientFunds, from)
			}
			balanceCheck, overflow = u256.AddOverflow(balanceCheck, st.value)
			if overflow {
				return upfrontTxnFees{}, fmt.Errorf("%w: address %v", ErrInsufficientFunds, from)
			}
			if hasBlobGas {
				maxBlobFee, overflow := u256.MulOverflow(maxFeePerBlobGas, u256.U64(blobGas))
				if overflow {
					return upfrontTxnFees{}, fmt.Errorf("%w: address %v", ErrInsufficientFunds, from)
				}
				balanceCheck, overflow = u256.AddOverflow(balanceCheck, maxBlobFee)
				if overflow {
					return upfrontTxnFees{}, fmt.Errorf("%w: address %v", ErrInsufficientFunds, from)
				}
			}
		}
		balance, err := st.state.GetBalance(from)
		if err != nil {
			return upfrontTxnFees{}, err
		}
		if balance.Cmp(&balanceCheck) < 0 {
			return upfrontTxnFees{}, fmt.Errorf("%w: address %v have %s want %s", ErrInsufficientFunds, from, balance.String(), balanceCheck.String())
		}
	}

	if gas < requiredIntrinsicGas {
		return upfrontTxnFees{}, fmt.Errorf("%w: have %d, want %d", ErrIntrinsicGas, gas, requiredIntrinsicGas)
	}

	if st.msg.To().IsNil() {
		vmConfig := st.evm.Config()
		if err := vm.CheckMaxInitCodeSize(uint64(len(st.data)), vmConfig.HasEip3860(rules), rules.IsAmsterdam); err != nil {
			return upfrontTxnFees{}, err
		}
	}

	return fees, nil
}

// ApplyFrame is similar to Execute but without gas accounting, for use in RIP-7560 transactions
func (st *TxnExecutor) ApplyFrame() (*evmtypes.ExecutionResult, error) {
	coinbase := st.evm.Context.Coinbase
	senderInitBalance, err := st.state.GetBalance(st.msg.From())
	if err != nil {
		return nil, fmt.Errorf("%w: %w", ErrTxnExecutionFailed, err)
	}
	coinbaseInitBalance, err := st.state.GetBalance(coinbase)
	if err != nil {
		return nil, fmt.Errorf("%w: %w", ErrTxnExecutionFailed, err)
	}

	msg := st.msg
	sender := msg.From()
	contractCreation := msg.To().IsNil()
	rules := st.evm.ChainRules()
	vmConfig := st.evm.Config()
	isEIP3860 := vmConfig.HasEip3860(rules)
	accessTuples := slices.Clone[types.AccessList](msg.AccessList())

	auths := msg.Authorizations()
	intrinsicGasResult, overflow := st.calcIntrinsicGas(contractCreation, auths, accessTuples)
	if overflow {
		return nil, ErrGasUintOverflow
	}
	intrinsicGas := intrinsicGasResult.ExecutionGas
	if st.msg.Gas() < intrinsicGas {
		return nil, fmt.Errorf("%w: have %d, want %d", ErrIntrinsicGas, st.msg.Gas(), intrinsicGas)
	}

	if contractCreation {
		if err := vm.CheckMaxInitCodeSize(uint64(len(st.data)), isEIP3860, rules.IsAmsterdam); err != nil {
			return nil, err
		}
	}

	// Match the execution-spec error precedence: intrinsic gas and initcode size
	// are checked before SetCode prerequisites. All validation completes before
	// verifyAuthorities can update account code or nonces.
	if err := validateSetCodePrerequisites(auths, contractCreation, rules.IsPrague); err != nil {
		return nil, err
	}
	st.gasRemaining = mdgas.SplitTxnGasLimit(st.msg.Gas(), intrinsicGas, rules)
	st.state.Prepare(rules, msg.From(), coinbase, msg.To(), vm.ActivePrecompiles(rules), accessTuples)
	var (
		gasUsed         runtimeGasAccounting
		runtimeGas      mdgas.MdGas
		runtimeSnapshot = -1
	)
	if rules.IsAmsterdam {
		runtimeGas = st.gasRemaining
		runtimeSnapshot = st.state.PushSnapshot()
		defer st.state.PopSnapshot(runtimeSnapshot)
	}
	st.gasRemaining, _, err = st.verifyAuthorities(auths, rules.ChainID, st.gasRemaining)
	if err == nil && !contractCreation {
		st.gasRemaining, gasUsed.topLevel, err = st.prepareTopLevelCall(st.gasRemaining)
	}
	if err != nil {
		if !rules.IsAmsterdam {
			return nil, err
		}
		st.state.RevertToSnapshot(runtimeSnapshot, err)
		if errors.Is(err, vm.ErrRuntimeOutOfGas) {
			st.gasRemaining = mdgas.MdGas{State: runtimeGas.State}
			st.traceRuntimeFailure(vm.CALL, st.to(), runtimeGas, st.gasRemaining, err)
			return &evmtypes.ExecutionResult{Err: err}, nil
		}
		return nil, err
	}
	var (
		ret   []byte
		vmerr error // vm errors do not affect consensus and are therefore not assigned to err
	)

	ret, st.gasRemaining, _, vmerr = st.evm.Call(sender, st.to(), st.data, st.gasRemaining, st.value, false)
	if !contractCreation {
		gasUsed.refillTopLevelState(&st.gasRemaining, vmConfig.RestoreState, vmerr)
	}

	result := &evmtypes.ExecutionResult{
		ReceiptGasUsed:        st.txnGasUsed,
		BlockExecutionGasUsed: st.blockExecutionGasUsed,
		BlockStateGasUsed:     st.blockStateGasUsed,
		Err:                   vmerr,
		Reverted:              errors.Is(vmerr, vm.ErrExecutionReverted),
		ReturnData:            ret,
		SenderInitBalance:     senderInitBalance,
		CoinbaseInitBalance:   coinbaseInitBalance,
	}

	if st.evm.Context.PostApplyMessage != nil {
		st.evm.Context.PostApplyMessage(st.state, msg.From(), coinbase, result, rules)
	}

	return result, nil
}

// Execute will transition the state by applying the current message and
// returning the evm execution result with following fields.
//
//   - used gas:
//     total gas used (including gas being refunded)
//   - returndata:
//     the returned data from evm
//   - concrete execution error:
//     various **EVM** error which aborts the execution,
//     e.g. ErrOutOfGas, ErrExecutionReverted
//
// However if any consensus issue encountered, return the error directly with
// nil evm execution result.
func (st *TxnExecutor) Execute(refunds bool, gasBailout bool) (result *evmtypes.ExecutionResult, err error) {
	if st.evm.IntraBlockState().IsVersioned() {
		defer func() {
			if r := recover(); r != nil {
				panicErr, isError := r.(error)
				if isError && errors.Is(panicErr, state.ErrDependency) {
					err = ErrExecAbortError{DependencyTxIndex: st.evm.IntraBlockState().DepTxIndex()}
					return
				}
				stack := dbg.Stack()
				log.Debug("Recovered from transition exec failure.", "Error:", r, "stack", stack)
				err = &ErrExecPanic{message: fmt.Sprintf("transition exec panic: %v at: %s", r, stack)}
			}
		}()
	}

	coinbase := st.evm.Context.Coinbase
	senderInitBalance, err := st.state.GetBalance(st.msg.From())
	if err != nil {
		return nil, fmt.Errorf("%w: %w", ErrTxnExecutionFailed, err)
	}

	var coinbaseInitBalance uint256.Int
	if !st.noFeeBurnAndTip {
		coinbaseInitBalance, err = st.state.GetBalance(coinbase)
		if err != nil {
			return nil, fmt.Errorf("%w: %w", ErrTxnExecutionFailed, err)
		}
	}
	msg := st.msg
	sender := msg.From()
	contractCreation := msg.To().IsNil()
	accessTuples := slices.Clone[types.AccessList](msg.AccessList())
	auths := msg.Authorizations()

	intrinsicGasResult, overflow := st.calcIntrinsicGas(contractCreation, auths, accessTuples)
	if overflow {
		return nil, ErrGasUintOverflow
	}

	// Complete pre-execution validation before buyGas or nonce mutation so a
	// rejected transaction leaves sender state and gas pools unchanged.
	fees, err := st.preCheck(gasBailout, intrinsicGasResult)
	if err != nil {
		return nil, err
	}
	if err := st.buyGas(fees, gasBailout); err != nil {
		return nil, err
	}

	rules := st.evm.ChainRules()
	vmConfig := st.evm.Config()

	if !contractCreation {
		// Increment the nonce for the next transaction
		nonce, err := st.state.GetNonce(sender)
		if err != nil {
			return nil, fmt.Errorf("%w: %w", ErrTxnExecutionFailed, err)
		}
		st.state.SetNonce(msg.From(), nonce+1, tracing.NonceChangeEoACall)
	}

	intrinsicGas := intrinsicGasResult.ExecutionGas
	st.gasRemaining = mdgas.SplitTxnGasLimit(st.msg.Gas(), intrinsicGas, rules)

	if t := st.evm.Config().Tracer; t != nil && t.OnGasChange != nil {
		t.OnGasChange(st.msg.Gas(), st.gasRemaining.Total(), tracing.GasChangeTxIntrinsicGas)
	}

	var bailout bool
	// Gas bailout (for trace_call) should only be applied if there is not sufficient balance to perform value transfer
	if gasBailout {
		canTransfer, err := st.evm.Context.CanTransfer(st.state, msg.From(), *msg.Value())
		if err != nil {
			return nil, fmt.Errorf("%w: %w", ErrTxnExecutionFailed, err)
		}
		if !msg.Value().IsZero() && !canTransfer {
			bailout = true
		}
	}

	var (
		ret             []byte
		vmerr           error
		gasUsed         runtimeGasAccounting
		runtimeGas      mdgas.MdGas
		runtimeSnapshot = -1
		createAddress   accounts.Address
		createNonce     uint64
	)
	st.state.Prepare(rules, msg.From(), coinbase, msg.To(), vm.ActivePrecompiles(rules), accessTuples)
	if rules.IsAmsterdam {
		runtimeGas = st.gasRemaining
		runtimeSnapshot = st.state.PushSnapshot()
		defer st.state.PopSnapshot(runtimeSnapshot)
	}
	st.gasRemaining, gasUsed.auth, vmerr = st.verifyAuthorities(auths, rules.ChainID, st.gasRemaining)
	if vmerr == nil {
		if contractCreation {
			createNonce, vmerr = st.state.GetNonce(sender)
			if vmerr != nil {
				vmerr = fmt.Errorf("%w: %w", ErrTxnExecutionFailed, vmerr)
			}
			if vmerr == nil && createNonce+1 >= createNonce {
				createAddress = accounts.InternAddress(types.CreateAddress(sender.Value(), createNonce))
				st.gasRemaining, gasUsed.topLevel, vmerr = st.prepareTopLevelCreate(createAddress, st.gasRemaining)
			}
		} else {
			st.gasRemaining, gasUsed.topLevel, vmerr = st.prepareTopLevelCall(st.gasRemaining)
		}
	}
	if vmerr != nil {
		if !rules.IsAmsterdam {
			return nil, vmerr
		}
		st.state.RevertToSnapshot(runtimeSnapshot, vmerr)
		if !errors.Is(vmerr, vm.ErrRuntimeOutOfGas) {
			return nil, vmerr
		}
		if contractCreation {
			st.state.SetNonce(sender, createNonce+1, tracing.NonceChangeContractCreator)
		}
		st.gasRemaining = mdgas.MdGas{State: runtimeGas.State}
		gasUsed.consumeAllExecutionGas(runtimeGas.Execution)
		typ, destination := vm.CALL, st.to()
		if contractCreation {
			typ, destination = vm.CREATE, createAddress
		}
		st.traceRuntimeFailure(typ, destination, runtimeGas, st.gasRemaining, vmerr)
	} else {
		frameGas := st.gasRemaining
		if contractCreation {
			ret, _, st.gasRemaining, gasUsed.frame, vmerr = st.evm.Create(sender, st.data, st.gasRemaining, st.value, nil, bailout)
		} else {
			ret, st.gasRemaining, gasUsed.frame, vmerr = st.evm.Call(sender, st.to(), st.data, st.gasRemaining, st.value, bailout)
		}
		gasUsed.finishFrame(frameGas, st.gasRemaining, vmerr)
		gasUsed.refillTopLevelState(&st.gasRemaining, vmConfig.RestoreState, vmerr)
	}

	totalGasUsed := gasUsed.total()
	switch {
	case refunds && !gasBailout:
		refundQuotient := params.RefundQuotient
		if rules.IsLondon {
			refundQuotient = params.RefundQuotientEIP3529
		}
		switch {
		case rules.IsAmsterdam:
			combined := totalGasUsed.PlusIntrinsic(intrinsicGas)
			st.blockStateGasUsed = combined.StateClamped()
			st.blockExecutionGasUsed = max(combined.Execution, intrinsicGasResult.FloorGasCost)
			st.txnGasUsedB4Refunds = combined.Total()
			refund := min(st.txnGasUsedB4Refunds/refundQuotient, st.state.GetRefund())
			st.txnGasUsed = max(intrinsicGasResult.FloorGasCost, st.txnGasUsedB4Refunds-refund)
		case rules.IsPrague:
			st.txnGasUsedB4Refunds = intrinsicGas + totalGasUsed.Execution
			refund := min(st.txnGasUsedB4Refunds/refundQuotient, st.state.GetRefund())
			st.txnGasUsed = max(intrinsicGasResult.FloorGasCost, st.txnGasUsedB4Refunds-refund)
			st.blockExecutionGasUsed = st.txnGasUsed
		default:
			st.txnGasUsedB4Refunds = intrinsicGas + totalGasUsed.Execution
			refund := min(st.txnGasUsedB4Refunds/refundQuotient, st.state.GetRefund())
			st.txnGasUsed = st.txnGasUsedB4Refunds - refund
			st.blockExecutionGasUsed = st.txnGasUsed
		}
		st.refundGas()
	case rules.IsAmsterdam:
		combined := totalGasUsed.PlusIntrinsic(intrinsicGas)
		st.blockStateGasUsed = combined.StateClamped()
		st.blockExecutionGasUsed = max(combined.Execution, intrinsicGasResult.FloorGasCost)
		st.txnGasUsedB4Refunds = combined.Total()
		st.txnGasUsed = max(st.txnGasUsedB4Refunds, intrinsicGasResult.FloorGasCost)
	default:
		// No-refund path: gasBailout (trace_call) or !refunds.
		// Don't apply Prague floor or refunds — just record raw gas used.
		st.txnGasUsedB4Refunds = intrinsicGas + totalGasUsed.Execution
		st.txnGasUsed = st.txnGasUsedB4Refunds
		st.blockExecutionGasUsed = st.msg.Gas() // match pre-refactor: consume full gas limit from pool
	}
	// EIP-8037: deduct the actual per-dimension usage from the block pool.
	// Pre-Amsterdam only the execution dimension exists.
	if err := st.gp.ConsumeExecution(st.blockExecutionGasUsed); err != nil {
		return nil, fmt.Errorf("%w: %w", ErrTxnExecutionFailed, err)
	}
	if rules.IsAmsterdam {
		if err := st.gp.ConsumeState(st.blockStateGasUsed); err != nil {
			return nil, fmt.Errorf("%w: %w", ErrTxnExecutionFailed, err)
		}
	}

	effectiveTip := *st.gasPrice
	if rules.IsLondon {
		if st.feeCap.Gt(&st.evm.Context.BaseFee) {
			effectiveTip = u256.Min(*st.tipCap, u256.Sub(*st.feeCap, st.evm.Context.BaseFee))
		} else {
			effectiveTip = u256.Num0
		}
	}

	tipAmount := u256.Mul(u256.U64(st.txnGasUsed), effectiveTip) // gasUsed * effectiveTip = how much goes to the block producer (miner, validator)

	if !st.noFeeBurnAndTip {
		if err := st.state.AddBalance(coinbase, tipAmount, tracing.BalanceIncreaseRewardTransactionFee); err != nil {
			return nil, fmt.Errorf("%w: %w", ErrTxnExecutionFailed, err)
		}
	}

	var burnAmount uint256.Int
	var burntContractAddress accounts.Address

	if !msg.IsFree() && rules.IsLondon {
		burntContractAddress = st.evm.ChainConfig().GetBurntContract(st.evm.Context.BlockNumber)
		if !burntContractAddress.IsNil() {
			burnAmount = u256.Mul(u256.U64(st.txnGasUsed), st.evm.Context.BaseFee)

			if rules.IsAura && rules.IsPrague {
				// https://github.com/gnosischain/specs/blob/master/network-upgrades/pectra.md#eip-4844-pectra
				burnAmount = u256.Add(burnAmount, fees.blobGasVal)
			}

			if !st.noFeeBurnAndTip {
				st.state.AddBalance(burntContractAddress, burnAmount, tracing.BalanceChangeUnspecified)
			}
		}
	}

	if dbg.TraceGas || st.state.Trace() || dbg.TraceAccount(st.msg.From().Handle()) {
		fmt.Printf("%d (%d.%d) Fees %x: tipped: %s, burnt: %s, price: %d, gas: %d\n", st.state.BlockNumber(), st.state.TxIndex(), st.state.Incarnation(), st.msg.From(), tipAmount.String(), burnAmount.String(), st.gasPrice, st.txnGasUsed)
	}

	result = &evmtypes.ExecutionResult{
		ReceiptGasUsed:        st.txnGasUsed,
		BlockExecutionGasUsed: st.blockExecutionGasUsed,
		BlockStateGasUsed:     st.blockStateGasUsed,
		MaxGasUsed:            max(st.txnGasUsedB4Refunds, intrinsicGasResult.FloorGasCost),
		Err:                   vmerr,
		Reverted:              errors.Is(vmerr, vm.ErrExecutionReverted),
		ReturnData:            ret,
		SenderInitBalance:     senderInitBalance,
		CoinbaseInitBalance:   coinbaseInitBalance,
		FeeTipped:             tipAmount,
		FeeBurnt:              burnAmount,
	}

	result.BurntContractAddress = burntContractAddress

	if st.evm.Context.PostApplyMessage != nil {
		st.evm.Context.PostApplyMessage(st.state, msg.From(), coinbase, result, rules)
	}

	return result, nil
}

func validateSetCodePrerequisites(auths []types.Authorization, contractCreation, isPrague bool) error {
	if auths == nil {
		return nil
	}
	if !isPrague {
		return errors.New("SetCode transaction not allowed before Prague fork")
	}
	if contractCreation {
		return errors.New("contract creation not allowed with type4 txs")
	}
	if len(auths) == 0 {
		return errors.New("SetCode transaction must have at least one authorization")
	}
	return nil
}

func (st *TxnExecutor) traceRuntimeFailure(typ vm.OpCode, destination accounts.Address, startGas, gasRemaining mdgas.MdGas, err error) {
	TraceTopLevelFailure(st.evm, typ, st.msg.From(), destination, st.data, startGas, gasRemaining, st.value, err)
}

func (st *TxnExecutor) prepareTopLevelCall(gasRemaining mdgas.MdGas) (mdgas.MdGas, mdgas.MdGasUsage, error) {
	gasRemaining, gasUsed, err := PrepareTopLevelCall(st.evm, st.to(), st.value, gasRemaining)
	if err != nil && !errors.Is(err, vm.ErrRuntimeOutOfGas) {
		err = fmt.Errorf("%w: %w", ErrTxnExecutionFailed, err)
	}
	return gasRemaining, gasUsed, err
}

func (st *TxnExecutor) prepareTopLevelCreate(destination accounts.Address, gasRemaining mdgas.MdGas) (mdgas.MdGas, mdgas.MdGasUsage, error) {
	gasRemaining, gasUsed, err := PrepareTopLevelCreate(st.evm, destination, gasRemaining)
	if err != nil && !errors.Is(err, vm.ErrRuntimeOutOfGas) {
		err = fmt.Errorf("%w: %w", ErrTxnExecutionFailed, err)
	}
	return gasRemaining, gasUsed, err
}

// verifyAuthorities applies the EIP-7702 authorization list, mutating state;
// callers must first validate the list with validateSetCodePrerequisites.
func (st *TxnExecutor) verifyAuthorities(auths []types.Authorization, chainID *uint256.Int, gasRemaining mdgas.MdGas) (mdgas.MdGas, mdgas.MdGasUsage, error) {
	var gasUsed mdgas.MdGasUsage
	if auths == nil {
		return gasRemaining, gasUsed, nil
	}
	isAmsterdam := st.evm.ChainRules().IsAmsterdam
	writtenAccounts := map[accounts.Address]struct{}{st.msg.From(): {}}
	if !st.msg.Value().IsZero() {
		writtenAccounts[st.msg.To()] = struct{}{}
	}
	preTxDelegates := make(map[accounts.Address]bool)
	delegationSetFor := make(map[accounts.Address]bool)
	for i := range auths {
		auth := &auths[i]

		// 1. chainId check
		if !auth.ChainID.IsZero() && !auth.ChainID.Eq(chainID) {
			log.Debug("invalid chainID, skipping", "chainId", auth.ChainID, "authIndex", i)
			continue
		}

		// 2. authority recover
		recovered, err := auth.RecoverSigner()
		if err != nil {
			log.Trace("authority recover failed, skipping", "err", err, "authIndex", i)
			continue
		}
		authority := accounts.InternAddress(recovered)

		// 3. add authority account to accesses_addresses
		st.state.AddAddressToAccessList(authority)
		st.state.MarkAddressAccess(authority, false)

		// 4. authority code should be empty or already delegated
		codeHash, err := st.state.GetCodeHash(authority)
		if err != nil {
			return gasRemaining, gasUsed, fmt.Errorf("%w: %w", ErrTxnExecutionFailed, err)
		}
		hasDelegation := !codeHash.IsEmpty()
		if hasDelegation {
			_, ok, err := st.state.GetDelegatedDesignation(authority)
			if err != nil {
				return gasRemaining, gasUsed, fmt.Errorf("%w: %w", ErrTxnExecutionFailed, err)
			}
			if !ok {
				log.Debug("authority code is not empty or not delegated, skipping", "authIndex", i)
				continue
			}
		}

		// 5. nonce check
		authorityNonce, err := st.state.GetNonce(authority)
		if err != nil {
			return gasRemaining, gasUsed, fmt.Errorf("%w: %w", ErrTxnExecutionFailed, err)
		}
		if authorityNonce != auth.Nonce {
			log.Trace("invalid nonce, skipping", "authIndex", i)
			continue
		}

		exists, err := st.state.Exist(authority)
		if err != nil {
			return gasRemaining, gasUsed, fmt.Errorf("%w: %w", ErrTxnExecutionFailed, err)
		}
		if isAmsterdam {
			if !exists && !mdgas.Consume(&gasRemaining, &gasUsed, params.StateGasNewAccount, mdgas.StateGas) {
				return gasRemaining, gasUsed, vm.ErrRuntimeOutOfGas
			}
			if _, written := writtenAccounts[authority]; !written {
				if !mdgas.Consume(&gasRemaining, &gasUsed, params.AccountWriteCostEIP8038, mdgas.ExecutionGas) {
					return gasRemaining, gasUsed, vm.ErrRuntimeOutOfGas
				}
				writtenAccounts[authority] = struct{}{}
			}
			preTxDelegated, seen := preTxDelegates[authority]
			if !seen {
				preTxDelegated = hasDelegation
				preTxDelegates[authority] = preTxDelegated
			}
			if auth.Address != (common.Address{}) {
				if !preTxDelegated && !delegationSetFor[authority] {
					if !mdgas.Consume(&gasRemaining, &gasUsed, params.StateGasAuthBase, mdgas.StateGas) {
						return gasRemaining, gasUsed, vm.ErrRuntimeOutOfGas
					}
				}
				delegationSetFor[authority] = true
			}
		} else if exists {
			st.state.AddRefund(params.PerEmptyAccountCost - params.PerAuthBaseCost)
		}

		// 7. set authority code
		if auth.Address == (common.Address{}) {
			if err := st.state.SetCode(authority, nil, tracing.CodeChangeAuthorizationClear); err != nil {
				return gasRemaining, gasUsed, fmt.Errorf("%w: %w", ErrTxnExecutionFailed, err)
			}
		} else {
			if err := st.state.SetCode(authority, types.AddressToDelegation(accounts.InternAddress(auth.Address)), tracing.CodeChangeAuthorization); err != nil {
				return gasRemaining, gasUsed, fmt.Errorf("%w: %w", ErrTxnExecutionFailed, err)
			}
		}

		// 8. increase the nonce of authority
		if err := st.state.SetNonce(authority, authorityNonce+1, tracing.NonceChangeAuthorization); err != nil {
			return gasRemaining, gasUsed, fmt.Errorf("%w: %w", ErrTxnExecutionFailed, err)
		}
	}

	return gasRemaining, gasUsed, nil
}

func (st *TxnExecutor) refundGas() {
	// Return ETH for remaining gas, exchanged at the original rate.
	remaining := u256.Mul(u256.U64(st.msg.Gas()-st.txnGasUsed), *st.gasPrice)
	if dbg.TraceGas || st.state.Trace() || dbg.TraceAccount(st.msg.From().Handle()) {
		fmt.Printf("%d (%d.%d) Refund %x: remaining: %d, price: %d val: %s\n", st.state.BlockNumber(), st.state.TxIndex(), st.state.Incarnation(), st.msg.From(), st.gasRemaining, st.gasPrice, remaining.String())
	}
	st.state.AddBalance(st.msg.From(), remaining, tracing.BalanceIncreaseGasReturn)
}

func (st *TxnExecutor) calcIntrinsicGas(contractCreation bool, auths []types.Authorization, accessTuples types.AccessList) (mdgas.IntrinsicGasCalcResult, bool) {
	rules := st.evm.ChainRules()
	vmConfig := st.evm.Config()
	return mdgas.IntrinsicGas(mdgas.IntrinsicGasCalcArgs{
		Data:               st.data,
		AuthorizationsLen:  uint64(len(auths)),
		AccessListLen:      uint64(len(accessTuples)),
		StorageKeysLen:     uint64(accessTuples.StorageKeys()),
		IsContractCreation: contractCreation,
		IsSelfTransfer:     !contractCreation && st.msg.To() == st.msg.From(),
		HasValue:           !st.msg.Value().IsZero(),
		IsEIP2:             rules.IsHomestead,
		IsEIP2028:          rules.IsIstanbul,
		IsEIP3860:          vmConfig.HasEip3860(rules),
		IsEIP7623:          rules.IsPrague,
		IsEIP7976:          rules.IsAmsterdam,
		IsEIP7981:          rules.IsAmsterdam,
		IsEIP2780:          rules.IsAmsterdam,
	})
}
