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
	"bytes"
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

 1. Validate transaction (nonce, gas cap, intrinsic gas, balance)
 2. Buy gas (debit sender, reserve from block gas pool)
 3. Increment sender nonce
 4. Execute: if contract creation, run initcode and store result as code;
    otherwise, call the recipient
 5. Compute refunds and return unused gas to pool
 6. Pay tips to coinbase, burn base fee
*/

var ErrTxnExecutionFailed = errors.New("txn execution failed")

type ErrExecAbortError struct {
	DependencyTxIndex int
	OriginError       error
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

type TxnExecutor struct {
	gp                  *GasPool
	msg                 Message
	gasRemaining        mdgas.MdGas
	blockRegularGasUsed uint64 // Per-tx regular gas for block-level accounting (pre-Amsterdam: same as block gas)
	blockStateGasUsed   uint64 // Per-tx state gas for block-level Bottleneck (EIP-8037)
	txnGasUsed          uint64
	txnGasUsedB4Refunds uint64 // txnGasUsed before refunds
	gasPrice            *uint256.Int
	feeCap              *uint256.Int
	tipCap              *uint256.Int
	initialGas          mdgas.MdGas
	value               uint256.Int
	data                []byte
	state               *state.IntraBlockState
	evm                 *vm.EVM

	// If true, fee burning and tipping won't happen during transition. Instead, their values will be included in the
	// ExecutionResult, which caller can use the values to update the balance of burner and coinbase account.
	// This is useful during parallel txn execution, where the common account read/write should be minimized.
	noFeeBurnAndTip bool
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

func (st *TxnExecutor) buyGas(gasBailout bool) error {
	gasVal, overflow := u256.MulOverflow(u256.U64(st.msg.Gas()), *st.gasPrice)
	if overflow {
		return fmt.Errorf("%w: address %v", ErrInsufficientFunds, st.msg.From())
	}

	// compute blob fee for eip-4844 data blobs if any
	blobGasVal := uint256.Int{}
	if st.evm.ChainRules().IsCancun {
		blobGasVal, overflow = u256.MulOverflow(st.evm.Context.BlobBaseFee, u256.U64(st.msg.BlobGas()))
		if overflow {
			return fmt.Errorf("%w: overflow converting blob gas: %v", ErrInsufficientFunds, &blobGasVal)
		}
		if err := st.gp.SubBlobGas(st.msg.BlobGas()); err != nil {
			return err
		}
	}

	if !gasBailout {
		balanceCheck := gasVal

		if st.feeCap != nil {
			balanceCheck, overflow = u256.MulOverflow(u256.U64(st.msg.Gas()), *st.feeCap)
			if overflow {
				return fmt.Errorf("%w: address %v", ErrInsufficientFunds, st.msg.From())
			}
			balanceCheck, overflow = u256.AddOverflow(balanceCheck, st.value)
			if overflow {
				return fmt.Errorf("%w: address %v", ErrInsufficientFunds, st.msg.From())
			}
			if st.evm.ChainRules().IsCancun {
				maxBlobFee, overflow := u256.MulOverflow(*st.msg.MaxFeePerBlobGas(), u256.U64(st.msg.BlobGas()))
				if overflow {
					return fmt.Errorf("%w: address %v", ErrInsufficientFunds, st.msg.From())
				}
				balanceCheck, overflow = u256.AddOverflow(balanceCheck, maxBlobFee)
				if overflow {
					return fmt.Errorf("%w: address %v", ErrInsufficientFunds, st.msg.From())
				}
			}
		}
		balance, err := st.state.GetBalance(st.msg.From())
		if err != nil {
			return err
		}
		if have, want := balance, balanceCheck; have.Cmp(&want) < 0 {
			return fmt.Errorf("%w: address %v have %v want %v", ErrInsufficientFunds, st.msg.From(), &have, &want)
		}
		st.state.SubBalance(st.msg.From(), gasVal, tracing.BalanceDecreaseGasBuy)
		st.state.SubBalance(st.msg.From(), blobGasVal, tracing.BalanceDecreaseGasBuy)
	}

	if err := st.gp.SubGas(st.msg.Gas()); err != nil {
		return err
	}
	// Correct blockRegularGasUsed will be set later in Execute; set it for the moment to the txn's gas limit
	// so that st.gp.AddGas(st.blockRegularGasUsed) in the recover block works correctly even if a panic occurs beforehand.
	st.blockRegularGasUsed = st.msg.Gas()

	if st.evm.Config().Tracer != nil && st.evm.Config().Tracer.OnGasChange != nil {
		st.evm.Config().Tracer.OnGasChange(0, st.msg.Gas(), tracing.GasChangeTxInitialBalance)
	}

	st.evm.BlobFee = blobGasVal
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

// preCheck validates consensus rules (nonce, fees, EIP-7825 gas cap) and buys gas.
// DESCRIBED: docs/programmers_guide/guide.md#nonce
func (st *TxnExecutor) preCheck(gasBailout bool, intrinsicGasResult mdgas.IntrinsicGasCalcResult) error {
	rules := st.evm.ChainRules()
	from := st.msg.From()
	if rules.IsOsaka && len(st.msg.BlobHashes()) > params.MaxBlobsPerTxn {
		return fmt.Errorf("%w: address %v, blobs: %d", ErrTooManyBlobs, from, len(st.msg.BlobHashes()))
	}

	// Make sure this transaction's nonce is correct.
	if st.msg.CheckNonce() {
		stNonce, err := st.state.GetNonce(from)
		if err != nil {
			return fmt.Errorf("%w: %w", ErrTxnExecutionFailed, err)
		}
		if msgNonce := st.msg.Nonce(); stNonce < msgNonce {
			return fmt.Errorf("%w: address %v, tx: %d state: %d", ErrNonceTooHigh,
				from, msgNonce, stNonce)
		} else if stNonce > msgNonce {
			return fmt.Errorf("%w: address %v, tx: %d state: %d", ErrNonceTooLow,
				from, msgNonce, stNonce)
		} else if stNonce+1 < stNonce {
			return fmt.Errorf("%w: address %v, nonce: %d", ErrNonceMax,
				from, stNonce)
		}
	}

	if st.msg.CheckTransaction() {
		// Make sure the sender is an EOA (EIP-3607)
		codeHash, err := st.state.GetCodeHash(from)
		if err != nil {
			return fmt.Errorf("%w: %w", ErrTxnExecutionFailed, err)
		}
		if !codeHash.IsEmpty() {
			// common.Hash{} means that the sender is not in the state.
			// Historically there were transactions with 0 gas price and non-existing sender,
			// so we have to allow that.

			// eip-7702 allows tx origination from accounts having delegated designation code.
			_, ok, err := st.state.GetDelegatedDesignation(from)
			if err != nil {
				return fmt.Errorf("%w: %w", ErrTxnExecutionFailed, err)
			}
			if !ok {
				return fmt.Errorf("%w: address %v, codehash: %s", ErrSenderNoEOA,
					from, codeHash)
			}
		}
	}

	// Make sure the transaction feeCap is greater than the block's baseFee.
	if rules.IsLondon {
		// Skip the checks if gas fields are zero and baseFee was explicitly disabled (eth_call)
		skipCheck := st.evm.Config().NoBaseFee && st.feeCap.IsZero() && st.tipCap.IsZero()
		if !skipCheck {
			if err := CheckEip1559TxGasFeeCap(from, st.feeCap, st.tipCap, &st.evm.Context.BaseFee, st.msg.IsFree()); err != nil {
				return err
			}
		}
	}
	if st.msg.BlobGas() > 0 && rules.IsCancun {
		blobGasPrice := st.evm.Context.BlobBaseFee
		maxFeePerBlobGas := st.msg.MaxFeePerBlobGas()
		if !st.evm.Config().NoBaseFee && blobGasPrice.Cmp(maxFeePerBlobGas) > 0 {
			return fmt.Errorf("%w: address %v, maxFeePerBlobGas: %v < blobGasPrice: %v",
				ErrMaxFeePerBlobGas, from, st.msg.MaxFeePerBlobGas(), blobGasPrice)
		}
	}

	// EIP-7825: Transaction Gas Limit Cap.
	// Intrinsic gas is computed before preCheck() in Execute so that the
	// fork-dependent cap can be validated here, before buyGas(), so pool gas
	// is never consumed for rejected txs.
	if st.msg.CheckGas() && rules.IsOsaka {
		if rules.IsAmsterdam {
			// EIP-8037: TX_MAX_GAS_LIMIT applies to the regular gas dimension only.
			gasToCap := max(intrinsicGasResult.RegularGas, intrinsicGasResult.FloorGasCost)
			if gasToCap > params.MaxTxnGasLimit {
				return fmt.Errorf("%w: regular gas cap %d exceeds TX_MAX_GAS_LIMIT %d",
					ErrIntrinsicGas, gasToCap, params.MaxTxnGasLimit)
			}
		} else if st.msg.Gas() > params.MaxTxnGasLimit {
			return fmt.Errorf("%w: address %v, gas limit %d", ErrGasLimitTooHigh, from, st.msg.Gas())
		}
	}

	return st.buyGas(gasBailout)
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

	// set code tx
	auths := msg.Authorizations()
	verifiedAuthorities, stateIgasRefund, err := st.verifyAuthorities(auths, contractCreation, rules.ChainID.String())
	if err != nil {
		return nil, err
	}

	// Check whether the init code size has been exceeded.
	if contractCreation {
		if err := vm.CheckMaxInitCodeSize(uint64(len(st.data)), isEIP3860, rules.IsAmsterdam); err != nil {
			return nil, err
		}
	}

	intrinsicGasResult, overflow := st.calcIntrinsicGas(contractCreation, auths, accessTuples)
	if overflow {
		return nil, ErrGasUintOverflow
	}
	intrinsicGas, overflow := math.SafeAdd(intrinsicGasResult.RegularGas, intrinsicGasResult.StateGas)
	if overflow {
		return nil, ErrGasUintOverflow
	}
	if st.msg.Gas() < intrinsicGas {
		return nil, fmt.Errorf("%w: have %d, want regular %d + state %d = %d",
			ErrIntrinsicGas, st.msg.Gas(), intrinsicGasResult.RegularGas, intrinsicGasResult.StateGas, intrinsicGas)
	}
	imdGas := mdgas.MdGas{
		Regular: intrinsicGasResult.RegularGas,
		State:   intrinsicGasResult.StateGas,
	}
	st.gasRemaining = mdgas.SplitTxnGasLimit(st.msg.Gas(), imdGas, rules)
	// EIP-8037 × EIP-7702: authority-exists refund moves from intrinsic state
	// gas into the reservoir so execution-time state ops can draw from it.
	if stateIgasRefund > 0 && rules.IsAmsterdam {
		imdGas.State -= stateIgasRefund
		st.gasRemaining.State += stateIgasRefund
	}
	st.initialGas = st.gasRemaining.Plus(imdGas)

	// Execute the preparatory steps for txn execution which includes:
	// - prepare accessList(post-berlin; eip-7702)
	// - reset transient storage(eip 1153)
	st.state.Prepare(rules, msg.From(), coinbase, msg.To(), vm.ActivePrecompiles(rules), accessTuples, verifiedAuthorities)

	var (
		ret   []byte
		vmerr error // vm errors do not affect consensus and are therefore not assigned to err
	)

	ret, st.gasRemaining, vmerr = st.evm.Call(sender, st.to(), st.data, st.gasRemaining, st.value, false)

	result := &evmtypes.ExecutionResult{
		ReceiptGasUsed:      st.txnGasUsed,
		BlockRegularGasUsed: st.blockRegularGasUsed,
		BlockStateGasUsed:   st.blockStateGasUsed,
		Err:                 vmerr,
		Reverted:            errors.Is(vmerr, vm.ErrExecutionReverted),
		ReturnData:          ret,
		SenderInitBalance:   senderInitBalance,
		CoinbaseInitBalance: coinbaseInitBalance,
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
				// Recover from dependency panic and retry the execution.
				if r != state.ErrDependency {
					log.Debug("Recovered from transition exec failure.", "Error:", r, "stack", dbg.Stack())
				}
				st.gp.AddGas(st.blockRegularGasUsed)
				depTxIndex := st.evm.IntraBlockState().DepTxIndex()
				if depTxIndex < 0 {
					err = fmt.Errorf("transition exec failure: %s at: %s", r, dbg.Stack())
				}
				err = ErrExecAbortError{
					DependencyTxIndex: depTxIndex,
					OriginError:       err}
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
	// First check this message satisfies all consensus rules before
	// applying the message. The rules include these clauses
	//
	// 1. there is no overflow when calculating intrinsic gas
	// 2. the transaction gas limit does not exceed the EIP-7825 cap (Osaka+)
	// 3. the nonce of the message caller is correct
	// 4. caller has enough balance to cover transaction fee(gaslimit * gasprice)
	// 5. the amount of gas required is available in the block
	// 6. caller has enough balance to cover asset transfer for **topmost** call
	// 7. the purchased gas is enough to cover intrinsic usage

	msg := st.msg
	sender := msg.From()
	contractCreation := msg.To().IsNil()
	accessTuples := slices.Clone[types.AccessList](msg.AccessList())
	auths := msg.Authorizations()

	// Check clause 1: compute intrinsic gas before preCheck so the EIP-7825
	// cap can be checked there (before buyGas) for all fork variants.
	intrinsicGasResult, overflow := st.calcIntrinsicGas(contractCreation, auths, accessTuples)
	if overflow {
		return nil, ErrGasUintOverflow
	}

	// Check clauses 2-6, buy gas if everything is correct
	if err := st.preCheck(gasBailout, intrinsicGasResult); err != nil {
		return nil, err
	}

	rules := st.evm.ChainRules()
	vmConfig := st.evm.Config()
	isEIP3860 := vmConfig.HasEip3860(rules)

	if !contractCreation {
		// Increment the nonce for the next transaction
		nonce, err := st.state.GetNonce(sender)
		if err != nil {
			return nil, fmt.Errorf("%w: %w", ErrTxnExecutionFailed, err)
		}
		st.state.SetNonce(msg.From(), nonce+1)
	}

	// Check clause 7, subtract intrinsic gas if everything is correct
	// EIP-8037: intrinsic_gas = intrinsic_regular_gas + intrinsic_state_gas.
	// The tx must cover the sum, not just each component individually.
	intrinsicGas, overflow := math.SafeAdd(intrinsicGasResult.RegularGas, intrinsicGasResult.StateGas)
	if overflow {
		return nil, ErrGasUintOverflow
	}
	if st.msg.Gas() < intrinsicGas || st.msg.Gas() < intrinsicGasResult.FloorGasCost {
		return nil, fmt.Errorf("%w: have %d, want %d", ErrIntrinsicGas, st.msg.Gas(), intrinsicGas)
	}

	verifiedAuthorities, stateIgasRefund, err := st.verifyAuthorities(auths, contractCreation, rules.ChainID.String())
	if err != nil {
		return nil, err
	}

	imdGas := mdgas.MdGas{
		Regular: intrinsicGasResult.RegularGas,
		State:   intrinsicGasResult.StateGas,
	}
	st.gasRemaining = mdgas.SplitTxnGasLimit(st.msg.Gas(), imdGas, rules)
	if rules.IsAmsterdam && stateIgasRefund > 0 {
		imdGas.State -= stateIgasRefund
		st.gasRemaining.State += stateIgasRefund
	}
	st.initialGas = st.gasRemaining.Plus(imdGas)

	if t := st.evm.Config().Tracer; t != nil && t.OnGasChange != nil {
		t.OnGasChange(st.initialGas.Total(), st.gasRemaining.Total(), tracing.GasChangeTxIntrinsicGas)
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

	// Check whether the init code size has been exceeded.
	if contractCreation {
		if err := vm.CheckMaxInitCodeSize(uint64(len(st.data)), isEIP3860, rules.IsAmsterdam); err != nil {
			return nil, err
		}
	}

	// Execute the preparatory steps for txn execution which includes:
	// - prepare accessList(post-berlin; eip-7702)
	// - reset transient storage(eip 1153)
	if err = st.state.Prepare(rules, msg.From(), coinbase, msg.To(), vm.ActivePrecompiles(rules), accessTuples, verifiedAuthorities); err != nil {
		return nil, fmt.Errorf("%w: %w", ErrTxnExecutionFailed, err)
	}
	var (
		ret   []byte
		vmerr error // vm errors do not affect consensus and are therefore not assigned to err
	)

	st.evm.ResetGasConsumed()

	if contractCreation {
		// The reason why we don't increment nonce here is that we need the original
		// nonce to calculate the address of the contract that is being created
		// It does get incremented inside the `Create` call, after the computation
		// of the contract's address, but before the execution of the code.
		ret, _, st.gasRemaining, vmerr = st.evm.Create(sender, st.data, st.gasRemaining, st.value, nil, bailout)
	} else {
		ret, st.gasRemaining, vmerr = st.evm.Call(sender, st.to(), st.data, st.gasRemaining, st.value, bailout)
	}

	if refunds && !gasBailout {
		refundQuotient := params.RefundQuotient
		if rules.IsLondon {
			refundQuotient = params.RefundQuotientEIP3529
		}
		if rules.IsAmsterdam {
			// EIP-8037 + EIP-7778: Block gas accounting uses two dimensions.
			// stateGasConsumed tracks ALL state gas charges (including spill to regular gas).
			// regularGasConsumed tracks only regular-dimension opcode gas.
			blockState := imdGas.State + st.evm.StateGasConsumed()
			blockRegular := imdGas.Regular + st.evm.RegularGasConsumed()
			st.blockRegularGasUsed = max(blockRegular, intrinsicGasResult.FloorGasCost)
			st.blockStateGasUsed = blockState
			// Receipt gasUsed: EIP-8037 formula tx.gas - gas_left - reservoir.
			// Use Total()-level subtraction to avoid per-component uint64 underflow
			// when gasRemaining.State > initialGas.State (reservoir grew via child reverts).
			st.txnGasUsedB4Refunds = st.initialGas.Total() - st.gasRemaining.Total() + st.evm.RevertedSpillGas()
			refund := min(st.txnGasUsedB4Refunds/refundQuotient, st.state.GetRefund().Total())
			st.txnGasUsed = max(intrinsicGasResult.FloorGasCost, st.txnGasUsedB4Refunds-refund)
		} else if rules.IsPrague {
			st.txnGasUsedB4Refunds = st.initialGas.Regular - st.gasRemaining.Regular
			refund := min(st.txnGasUsedB4Refunds/refundQuotient, st.state.GetRefund().Regular)
			st.txnGasUsed = max(intrinsicGasResult.FloorGasCost, st.txnGasUsedB4Refunds-refund)
			st.blockRegularGasUsed = st.txnGasUsed
		} else {
			st.txnGasUsedB4Refunds = st.initialGas.Regular - st.gasRemaining.Regular
			refund := min(st.txnGasUsedB4Refunds/refundQuotient, st.state.GetRefund().Regular)
			st.txnGasUsed = st.txnGasUsedB4Refunds - refund
			st.blockRegularGasUsed = st.txnGasUsed
		}
		st.refundGas()
	} else if rules.IsAmsterdam {
		blockState := imdGas.State + st.evm.StateGasConsumed()
		blockRegular := imdGas.Regular + st.evm.RegularGasConsumed()
		st.blockRegularGasUsed = max(blockRegular, intrinsicGasResult.FloorGasCost)
		st.blockStateGasUsed = blockState
		st.txnGasUsedB4Refunds = st.initialGas.Total() - st.gasRemaining.Total() + st.evm.RevertedSpillGas()
		st.txnGasUsed = max(st.txnGasUsedB4Refunds, intrinsicGasResult.FloorGasCost)
	} else {
		// No-refund path: gasBailout (trace_call) or !refunds.
		// Don't apply Prague floor or refunds — just record raw gas used.
		st.txnGasUsedB4Refunds = st.initialGas.Regular - st.gasRemaining.Regular
		st.txnGasUsed = st.txnGasUsedB4Refunds
		st.blockRegularGasUsed = st.msg.Gas() // match pre-refactor: consume full gas limit from pool
	}
	// Also return remaining gas to the block gas counter so it is
	// available for the next transaction.
	// EIP-8037: The net per-tx pool deduction must be blockRegularGasUsed,
	// not max(regular, state). Using max per-tx would give Σ max(r_i, s_i) ≥
	// max(Σ r_i, Σ s_i), rejecting valid blocks. State gas (including any
	// spill from reservoir into gas_left) is tracked in stateGasConsumed and
	// validated at block end via GasUsed.BlockGasUsed() = max(Σ regular, Σ state).
	st.gp.AddGas(st.initialGas.Total() - st.blockRegularGasUsed)

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
				burnAmount = u256.Add(burnAmount, st.evm.BlobFee)
			}

			if !st.noFeeBurnAndTip {
				st.state.AddBalance(burntContractAddress, burnAmount, tracing.BalanceChangeUnspecified)
			}
		}
	}

	if dbg.TraceGas || st.state.Trace() || dbg.TraceAccount(st.msg.From().Handle()) {
		fmt.Printf("%d (%d.%d) Fees %x: tipped: %d, burnt: %d, price: %d, gas: %d\n", st.state.BlockNumber(), st.state.TxIndex(), st.state.Incarnation(), st.msg.From(), &tipAmount, &burnAmount, st.gasPrice, st.txnGasUsed)
	}

	result = &evmtypes.ExecutionResult{
		ReceiptGasUsed:      st.txnGasUsed,
		BlockRegularGasUsed: st.blockRegularGasUsed,
		BlockStateGasUsed:   st.blockStateGasUsed,
		MaxGasUsed:          max(st.txnGasUsedB4Refunds, intrinsicGasResult.FloorGasCost),
		Err:                 vmerr,
		Reverted:            errors.Is(vmerr, vm.ErrExecutionReverted),
		ReturnData:          ret,
		SenderInitBalance:   senderInitBalance,
		CoinbaseInitBalance: coinbaseInitBalance,
		FeeTipped:           tipAmount,
		FeeBurnt:            burnAmount,
	}

	result.BurntContractAddress = burntContractAddress

	if st.evm.Context.PostApplyMessage != nil {
		st.evm.Context.PostApplyMessage(st.state, msg.From(), coinbase, result, rules)
	}

	return result, nil
}

func (st *TxnExecutor) verifyAuthorities(auths []types.Authorization, contractCreation bool, chainID string) ([]accounts.Address, uint64, error) {
	var stateIgasRefund uint64
	var stateIgasRefundInc uint64
	if st.evm.ChainRules().IsAmsterdam {
		stateIgasRefundInc = params.StateBytesNewAccount * st.evm.Context.CostPerStateByte
	}
	verifiedAuthorities := make([]accounts.Address, 0)
	if len(auths) > 0 {
		if contractCreation {
			return nil, stateIgasRefund, errors.New("contract creation not allowed with type4 txs")
		}
		var b [32]byte
		data := bytes.NewBuffer(nil)
		for i, auth := range auths {
			data.Reset()

			// 1. chainId check
			if !auth.ChainID.IsZero() && chainID != auth.ChainID.String() {
				log.Debug("invalid chainID, skipping", "chainId", auth.ChainID, "auth index", i)
				continue
			}

			// 2. authority recover
			authorityPtr, err := auth.RecoverSigner(data, b[:])
			if err != nil {
				log.Trace("authority recover failed, skipping", "err", err, "auth index", i)
				continue
			}
			authority := accounts.InternAddress(*authorityPtr)

			// 3. add authority account to accesses_addresses
			verifiedAuthorities = append(verifiedAuthorities, authority)
			// authority is added to accessed_address in prepare step

			// BAL: captures authorities even if validation fails
			st.state.MarkAddressAccess(authority, false)

			// 4. authority code should be empty or already delegated
			codeHash, err := st.state.GetCodeHash(authority)
			if err != nil {
				return nil, stateIgasRefund, fmt.Errorf("%w: %w", ErrTxnExecutionFailed, err)
			}
			if !codeHash.IsEmpty() {
				// check for delegation
				_, ok, err := st.state.GetDelegatedDesignation(authority)
				if err != nil {
					return nil, stateIgasRefund, fmt.Errorf("%w: %w", ErrTxnExecutionFailed, err)
				}
				if !ok {
					log.Debug("authority code is not empty or not delegated, skipping", "auth index", i)
					continue
				}
				// noop: has delegated designation
			}

			// 5. nonce check
			authorityNonce, err := st.state.GetNonce(authority)
			if err != nil {
				return nil, stateIgasRefund, fmt.Errorf("%w: %w", ErrTxnExecutionFailed, err)
			}
			if authorityNonce != auth.Nonce {
				log.Trace("invalid nonce, skipping", "auth index", i)
				continue
			}

			// 6. Add PER_EMPTY_ACCOUNT_COST - PER_AUTH_BASE_COST gas to the global refund counter if authority exists in the trie.
			exists, err := st.state.Exist(authority)
			if err != nil {
				return nil, stateIgasRefund, fmt.Errorf("%w: %w", ErrTxnExecutionFailed, err)
			}
			if exists {
				if st.evm.ChainRules().IsAmsterdam {
					stateIgasRefund += stateIgasRefundInc
				} else {
					st.state.AddRefund(params.PerEmptyAccountCost - params.PerAuthBaseCost)
				}
			}

			// 7. set authority code
			if auth.Address == (common.Address{}) {
				if err := st.state.SetCode(authority, nil); err != nil {
					return nil, stateIgasRefund, fmt.Errorf("%w: %w", ErrTxnExecutionFailed, err)
				}
			} else {
				if err := st.state.SetCode(authority, types.AddressToDelegation(accounts.InternAddress(auth.Address))); err != nil {
					return nil, stateIgasRefund, fmt.Errorf("%w: %w", ErrTxnExecutionFailed, err)
				}
			}

			// 8. increase the nonce of authority
			if err := st.state.SetNonce(authority, authorityNonce+1); err != nil {
				return nil, stateIgasRefund, fmt.Errorf("%w: %w", ErrTxnExecutionFailed, err)
			}
		}
	}

	return verifiedAuthorities, stateIgasRefund, nil
}

func (st *TxnExecutor) refundGas() {
	// Return ETH for remaining gas, exchanged at the original rate.
	remaining := u256.Mul(u256.U64(st.initialGas.Total()-st.txnGasUsed), *st.gasPrice)
	if dbg.TraceGas || st.state.Trace() || dbg.TraceAccount(st.msg.From().Handle()) {
		fmt.Printf("%d (%d.%d) Refund %x: remaining: %d, price: %d val: %d\n", st.state.BlockNumber(), st.state.TxIndex(), st.state.Incarnation(), st.msg.From(), st.gasRemaining, st.gasPrice, &remaining)
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
		CostPerStateByte:   st.evm.Context.CostPerStateByte,
		IsContractCreation: contractCreation,
		IsEIP2:             rules.IsHomestead,
		IsEIP2028:          rules.IsIstanbul,
		IsEIP3860:          vmConfig.HasEip3860(rules),
		IsEIP7623:          rules.IsPrague,
		IsEIP8037:          rules.IsAmsterdam,
	})
}
