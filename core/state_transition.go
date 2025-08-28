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

package core

import (
	"bytes"
	"errors"
	"fmt"
	"slices"

	"github.com/holiman/uint256"

	"github.com/erigontech/erigon-lib/common"
	"github.com/erigontech/erigon-lib/common/dbg"
	"github.com/erigontech/erigon-lib/common/empty"
	"github.com/erigontech/erigon-lib/common/math"
	"github.com/erigontech/erigon-lib/common/u256"
	"github.com/erigontech/erigon-lib/log/v3"
	"github.com/erigontech/erigon/core/state"
	"github.com/erigontech/erigon/core/tracing"
	"github.com/erigontech/erigon/core/vm"
	"github.com/erigontech/erigon/core/vm/evmtypes"
	"github.com/erigontech/erigon/execution/chain/params"
	"github.com/erigontech/erigon/execution/consensus"
	"github.com/erigontech/erigon/execution/fixedgas"
	"github.com/erigontech/erigon/execution/types"
)

/*
The State Transitioning Model

A state transition is a change made when a transaction is applied to the current world state
The state transitioning model does all the necessary work to work out a valid new state root.

1) Nonce handling
2) Pre pay gas
3) Create a new state object if the recipient is \0*32
4) Value transfer
== If contract creation ==

	4a) Attempt to run transaction data
	4b) If valid, use result as code for the new state object

== end ==
5) Run Script section
6) Derive new state root
*/

var ErrStateTransitionFailed = errors.New("state transition failed")

type ErrExecAbortError struct {
	DependencyTxIndex int
	OriginError       error
}

func (e ErrExecAbortError) Error() string {
	if e.DependencyTxIndex >= 0 {
		return fmt.Sprintf("execution aborted due to dependency %d", e.DependencyTxIndex)
	} else {
		if e.OriginError != nil {
			return fmt.Sprintf("execution aborted: %s", e.OriginError)
		}
		return "execution aborted"
	}
}

type StateTransition struct {
	gp           *GasPool
	msg          Message
	gasRemaining uint64
	gasPrice     *uint256.Int
	feeCap       *uint256.Int
	tipCap       *uint256.Int
	initialGas   uint64
	value        *uint256.Int
	data         []byte
	state        *state.IntraBlockState
	evm          *vm.EVM

	// If true, fee burning and tipping won't happen during transition. Instead, their values will be included in the
	// ExecutionResult, which caller can use the values to update the balance of burner and coinbase account.
	// This is useful during parallel state transition, where the common account read/write should be minimized.
	noFeeBurnAndTip bool
}

// Message represents a message sent to a contract.
type Message interface {
	From() common.Address
	To() *common.Address

	GasPrice() *uint256.Int
	FeeCap() *uint256.Int
	TipCap() *uint256.Int
	Gas() uint64
	BlobGas() uint64
	MaxFeePerBlobGas() *uint256.Int
	Value() *uint256.Int

	Nonce() uint64
	CheckNonce() bool
	Data() []byte
	AccessList() types.AccessList
	BlobHashes() []common.Hash
	Authorizations() []types.Authorization

	IsFree() bool // service transactions on Gnosis are exempt from EIP-1559 mandatory fees
	SetIsFree(bool)
}

// NewStateTransition initialises and returns a new state transition object.
func NewStateTransition(evm *vm.EVM, msg Message, gp *GasPool) *StateTransition {
	return &StateTransition{
		gp:       gp,
		evm:      evm,
		msg:      msg,
		gasPrice: msg.GasPrice(),
		feeCap:   msg.FeeCap(),
		tipCap:   msg.TipCap(),
		value:    msg.Value(),
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
func ApplyMessage(evm *vm.EVM, msg Message, gp *GasPool, refunds bool, gasBailout bool, engine consensus.EngineReader) (*evmtypes.ExecutionResult, error) {
	return applyMessage(evm, msg, gp, refunds, gasBailout, false, engine)
}

func applyMessage(evm *vm.EVM, msg Message, gp *GasPool, refunds bool, gasBailout bool, noFeeBurnAndTip bool, engine consensus.EngineReader) (
	*evmtypes.ExecutionResult, error) {
	// Only zero-gas transactions may be service ones
	if msg.FeeCap().IsZero() && !msg.IsFree() && engine != nil {
		blockContext := evm.Context
		blockContext.Coinbase = state.SystemAddress
		syscall := func(contract common.Address, data []byte) ([]byte, error) {
			ret, err := SysCallContractWithBlockContext(contract, data, evm.ChainConfig(), evm.IntraBlockState(), blockContext, true, evm.Config())
			return ret, err
		}
		msg.SetIsFree(engine.IsServiceTransaction(msg.From(), syscall))
	}
	st := NewStateTransition(evm, msg, gp)
	st.noFeeBurnAndTip = noFeeBurnAndTip
	return st.TransitionDb(refunds, gasBailout)
}

func ApplyMessageNoFeeBurnOrTip(evm *vm.EVM, msg Message, gp *GasPool, refunds bool, gasBailout bool, engine consensus.EngineReader) (*evmtypes.ExecutionResult, error) {
	return applyMessage(evm, msg, gp, refunds, gasBailout, true, engine)
}

func ApplyFrame(evm *vm.EVM, msg Message, gp *GasPool) (*evmtypes.ExecutionResult, error) {
	return NewStateTransition(evm, msg, gp).ApplyFrame()
}

// to returns the recipient of the message.
func (st *StateTransition) to() common.Address {
	if st.msg == nil || st.msg.To() == nil /* contract creation */ {
		return common.Address{}
	}
	return *st.msg.To()
}

func (st *StateTransition) buyGas(gasBailout bool) error {
	gasVal := &uint256.Int{}
	gasVal.SetUint64(st.msg.Gas())
	gasVal, overflow := gasVal.MulOverflow(gasVal, st.gasPrice)
	if overflow {
		return fmt.Errorf("%w: address %v", ErrInsufficientFunds, st.msg.From().Hex())
	}

	// compute blob fee for eip-4844 data blobs if any
	blobGasVal := &uint256.Int{}
	if st.evm.ChainRules().IsCancun {
		blobGasPrice := st.evm.Context.BlobBaseFee
		if blobGasPrice == nil {
			return fmt.Errorf("%w: Cancun is active but ExcessBlobGas is nil", ErrInternalFailure)
		}
		blobGasVal, overflow = blobGasVal.MulOverflow(blobGasPrice, new(uint256.Int).SetUint64(st.msg.BlobGas()))
		if overflow {
			return fmt.Errorf("%w: overflow converting blob gas: %v", ErrInsufficientFunds, blobGasVal)
		}
		if err := st.gp.SubBlobGas(st.msg.BlobGas()); err != nil {
			return err
		}
	}

	if !gasBailout {
		balanceCheck := gasVal

		if st.feeCap != nil {
			balanceCheck = (&uint256.Int{}).SetUint64(st.msg.Gas())
			balanceCheck, overflow = balanceCheck.MulOverflow(balanceCheck, st.feeCap)
			if overflow {
				return fmt.Errorf("%w: address %v", ErrInsufficientFunds, st.msg.From().Hex())
			}
			balanceCheck, overflow = balanceCheck.AddOverflow(balanceCheck, st.value)
			if overflow {
				return fmt.Errorf("%w: address %v", ErrInsufficientFunds, st.msg.From().Hex())
			}
			if st.evm.ChainRules().IsCancun {
				maxBlobFee, overflow := new(uint256.Int).MulOverflow(st.msg.MaxFeePerBlobGas(), new(uint256.Int).SetUint64(st.msg.BlobGas()))
				if overflow {
					return fmt.Errorf("%w: address %v", ErrInsufficientFunds, st.msg.From().Hex())
				}
				balanceCheck, overflow = balanceCheck.AddOverflow(balanceCheck, maxBlobFee)
				if overflow {
					return fmt.Errorf("%w: address %v", ErrInsufficientFunds, st.msg.From().Hex())
				}
			}
		}
		balance, err := st.state.GetBalance(st.msg.From())
		if err != nil {
			return err
		}
		if have, want := balance, balanceCheck; have.Cmp(want) < 0 {
			return fmt.Errorf("%w: address %v have %v want %v", ErrInsufficientFunds, st.msg.From().Hex(), &have, want)
		}
		st.state.SubBalance(st.msg.From(), *gasVal, tracing.BalanceDecreaseGasBuy)
		st.state.SubBalance(st.msg.From(), *blobGasVal, tracing.BalanceDecreaseGasBuy)
	}

	if err := st.gp.SubGas(st.msg.Gas()); err != nil {
		return err
	}

	if st.evm.Config().Tracer != nil && st.evm.Config().Tracer.OnGasChange != nil {
		st.evm.Config().Tracer.OnGasChange(0, st.msg.Gas(), tracing.GasChangeTxInitialBalance)
	}

	st.gasRemaining += st.msg.Gas()
	st.initialGas = st.msg.Gas()
	st.evm.BlobFee = blobGasVal
	return nil
}

func CheckEip1559TxGasFeeCap(from common.Address, feeCap, tipCap, baseFee *uint256.Int, isFree bool) error {
	if feeCap.Lt(tipCap) {
		return fmt.Errorf("%w: address %v, tipCap: %s, feeCap: %s", ErrTipAboveFeeCap,
			from.Hex(), tipCap, feeCap)
	}
	if baseFee != nil && feeCap.Lt(baseFee) && !isFree {
		return fmt.Errorf("%w: address %v, feeCap: %s baseFee: %s", ErrFeeCapTooLow,
			from.Hex(), feeCap, baseFee)
	}
	return nil
}

// DESCRIBED: docs/programmers_guide/guide.md#nonce
func (st *StateTransition) preCheck(gasBailout bool) error {
	// Make sure this transaction's nonce is correct.
	if st.msg.CheckNonce() {
		stNonce, err := st.state.GetNonce(st.msg.From())
		if err != nil {
			return fmt.Errorf("%w: %w", ErrStateTransitionFailed, err)
		}
		if msgNonce := st.msg.Nonce(); stNonce < msgNonce {
			return fmt.Errorf("%w: address %v, tx: %d state: %d", ErrNonceTooHigh,
				st.msg.From().Hex(), msgNonce, stNonce)
		} else if stNonce > msgNonce {
			return fmt.Errorf("%w: address %v, tx: %d state: %d", ErrNonceTooLow,
				st.msg.From().Hex(), msgNonce, stNonce)
		} else if stNonce+1 < stNonce {
			return fmt.Errorf("%w: address %v, nonce: %d", ErrNonceMax,
				st.msg.From().Hex(), stNonce)
		}

		// Make sure the sender is an EOA (EIP-3607)
		codeHash, err := st.state.GetCodeHash(st.msg.From())
		if err != nil {
			return fmt.Errorf("%w: %w", ErrStateTransitionFailed, err)
		}
		if codeHash != empty.CodeHash && codeHash != (common.Hash{}) {
			// common.Hash{} means that the sender is not in the state.
			// Historically there were transactions with 0 gas price and non-existing sender,
			// so we have to allow that.

			// eip-7702 allows tx origination from accounts having delegated designation code.
			_, ok, err := st.state.GetDelegatedDesignation(st.msg.From())
			if err != nil {
				return fmt.Errorf("%w: %w", ErrStateTransitionFailed, err)
			}
			if !ok {
				return fmt.Errorf("%w: address %v, codehash: %s", ErrSenderNoEOA,
					st.msg.From().Hex(), codeHash)
			}
		}
	}

	// Make sure the transaction feeCap is greater than the block's baseFee.
	if st.evm.ChainRules().IsLondon {
		// Skip the checks if gas fields are zero and baseFee was explicitly disabled (eth_call)
		skipCheck := st.evm.Config().NoBaseFee && st.feeCap.IsZero() && st.tipCap.IsZero()
		if !skipCheck {
			if err := CheckEip1559TxGasFeeCap(st.msg.From(), st.feeCap, st.tipCap, st.evm.Context.BaseFee, st.msg.IsFree()); err != nil {
				return err
			}
		}
	}
	if st.msg.BlobGas() > 0 && st.evm.ChainRules().IsCancun {
		blobGasPrice := st.evm.Context.BlobBaseFee
		if blobGasPrice == nil {
			return fmt.Errorf("%w: Cancun is active but ExcessBlobGas is nil", ErrInternalFailure)
		}
		maxFeePerBlobGas := st.msg.MaxFeePerBlobGas()
		if !st.evm.Config().NoBaseFee && blobGasPrice.Cmp(maxFeePerBlobGas) > 0 {
			return fmt.Errorf("%w: address %v, maxFeePerBlobGas: %v < blobGasPrice: %v",
				ErrMaxFeePerBlobGas, st.msg.From().Hex(), st.msg.MaxFeePerBlobGas(), blobGasPrice)
		}
	}

	// EIP-7825: Transaction Gas Limit Cap
	if st.evm.ChainRules().IsOsaka && st.msg.Gas() > params.MaxTxnGasLimit {
		return fmt.Errorf("%w: address %v, gas limit %d", ErrGasLimitTooHigh, st.msg.From().Hex(), st.msg.Gas())
	}

	return st.buyGas(gasBailout)
}

// ApplyFrame is similar to TransitionDb but without gas accounting, for use in RIP-7560 transactions
func (st *StateTransition) ApplyFrame() (*evmtypes.ExecutionResult, error) {
	coinbase := st.evm.Context.Coinbase
	senderInitBalance, err := st.state.GetBalance(st.msg.From())
	if err != nil {
		return nil, fmt.Errorf("%w: %w", ErrStateTransitionFailed, err)
	}
	coinbaseInitBalance, err := st.state.GetBalance(coinbase)
	if err != nil {
		return nil, fmt.Errorf("%w: %w", ErrStateTransitionFailed, err)
	}

	msg := st.msg
	st.gasRemaining += st.msg.Gas()
	st.initialGas = st.msg.Gas()
	sender := vm.AccountRef(msg.From())
	contractCreation := msg.To() == nil
	rules := st.evm.ChainRules()
	vmConfig := st.evm.Config()
	isEIP3860 := vmConfig.HasEip3860(rules)
	accessTuples := slices.Clone[types.AccessList](msg.AccessList())

	// set code tx
	auths := msg.Authorizations()
	verifiedAuthorities, err := st.verifyAuthorities(auths, contractCreation, rules.ChainID.String())
	if err != nil {
		return nil, err
	}

	// Check whether the init code size has been exceeded.
	if isEIP3860 && contractCreation && len(st.data) > params.MaxInitCodeSize {
		return nil, fmt.Errorf("%w: code size %v limit %v", ErrMaxInitCodeSizeExceeded, len(st.data), params.MaxInitCodeSize)
	}

	// Execute the preparatory steps for state transition which includes:
	// - prepare accessList(post-berlin; eip-7702)
	// - reset transient storage(eip 1153)
	st.state.Prepare(rules, msg.From(), coinbase, msg.To(), vm.ActivePrecompiles(rules), accessTuples, verifiedAuthorities)

	var (
		ret   []byte
		vmerr error // vm errors do not effect consensus and are therefore not assigned to err
	)

	ret, st.gasRemaining, vmerr = st.evm.Call(sender, st.to(), st.data, st.gasRemaining, st.value, false)

	result := &evmtypes.ExecutionResult{
		GasUsed:             st.gasUsed(),
		Err:                 vmerr,
		Reverted:            errors.Is(vmerr, vm.ErrExecutionReverted),
		ReturnData:          ret,
		SenderInitBalance:   senderInitBalance,
		CoinbaseInitBalance: coinbaseInitBalance,
	}

	if st.evm.Context.PostApplyMessage != nil {
		st.evm.Context.PostApplyMessage(st.state, msg.From(), coinbase, result)
	}

	return result, nil
}

// TransitionDb will transition the state by applying the current message and
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
func (st *StateTransition) TransitionDb(refunds bool, gasBailout bool) (result *evmtypes.ExecutionResult, err error) {
	if st.evm.IntraBlockState().IsVersioned() {
		defer func() {
			if r := recover(); r != nil {
				// Recover from dependency panic and retry the execution.
				if r != state.ErrDependency {
					log.Debug("Recovered from transition exec failure.", "Error:", r, "stack", dbg.Stack())
				}
				st.gp.AddGas(st.gasUsed())
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
		return nil, fmt.Errorf("%w: %w", ErrStateTransitionFailed, err)
	}

	var coinbaseInitBalance uint256.Int
	if !st.noFeeBurnAndTip {
		coinbaseInitBalance, err = st.state.GetBalance(coinbase)
		if err != nil {
			return nil, fmt.Errorf("%w: %w", ErrStateTransitionFailed, err)
		}
	}
	// First check this message satisfies all consensus rules before
	// applying the message. The rules include these clauses
	//
	// 1. the nonce of the message caller is correct
	// 2. caller has enough balance to cover transaction fee(gaslimit * gasprice)
	// 3. the amount of gas required is available in the block
	// 4. the purchased gas is enough to cover intrinsic usage
	// 5. there is no overflow when calculating intrinsic gas
	// 6. caller has enough balance to cover asset transfer for **topmost** call

	// Check clauses 1-3 and 6, buy gas if everything is correct
	if err := st.preCheck(gasBailout); err != nil {
		return nil, err
	}

	msg := st.msg
	sender := vm.AccountRef(msg.From())
	contractCreation := msg.To() == nil
	rules := st.evm.ChainRules()
	vmConfig := st.evm.Config()
	isEIP3860 := vmConfig.HasEip3860(rules)
	accessTuples := slices.Clone[types.AccessList](msg.AccessList())

	if !contractCreation {
		// Increment the nonce for the next transaction
		nonce, err := st.state.GetNonce(sender.Address())
		if err != nil {
			return nil, fmt.Errorf("%w: %w", ErrStateTransitionFailed, err)
		}
		st.state.SetNonce(msg.From(), nonce+1)
	}

	// set code tx
	auths := msg.Authorizations()

	// Check clauses 4-5, subtract intrinsic gas if everything is correct
	gas, floorGas7623, overflow := fixedgas.IntrinsicGas(st.data, uint64(len(accessTuples)), uint64(accessTuples.StorageKeys()), contractCreation, rules.IsHomestead, rules.IsIstanbul, isEIP3860, rules.IsPrague, false, uint64(len(auths)))
	if overflow {
		return nil, ErrGasUintOverflow
	}
	if st.gasRemaining < gas || st.gasRemaining < floorGas7623 {
		return nil, fmt.Errorf("%w: have %d, want %d", ErrIntrinsicGas, st.gasRemaining, max(gas, floorGas7623))
	}

	verifiedAuthorities, err := st.verifyAuthorities(auths, contractCreation, rules.ChainID.String())
	if err != nil {
		return nil, err
	}

	if t := st.evm.Config().Tracer; t != nil && t.OnGasChange != nil {
		t.OnGasChange(st.gasRemaining, st.gasRemaining-gas, tracing.GasChangeTxIntrinsicGas)
	}
	st.gasRemaining -= gas

	var bailout bool
	// Gas bailout (for trace_call) should only be applied if there is not sufficient balance to perform value transfer
	if gasBailout {
		canTransfer, err := st.evm.Context.CanTransfer(st.state, msg.From(), msg.Value())
		if err != nil {
			return nil, fmt.Errorf("%w: %w", ErrStateTransitionFailed, err)
		}
		if !msg.Value().IsZero() && !canTransfer {
			bailout = true
		}
	}

	// Check whether the init code size has been exceeded.
	if isEIP3860 && contractCreation && len(st.data) > params.MaxInitCodeSize {
		return nil, fmt.Errorf("%w: code size %v limit %v", ErrMaxInitCodeSizeExceeded, len(st.data), params.MaxInitCodeSize)
	}

	// Execute the preparatory steps for state transition which includes:
	// - prepare accessList(post-berlin; eip-7702)
	// - reset transient storage(eip 1153)
	if err = st.state.Prepare(rules, msg.From(), coinbase, msg.To(), vm.ActivePrecompiles(rules), accessTuples, verifiedAuthorities); err != nil {
		return nil, fmt.Errorf("%w: %w", ErrStateTransitionFailed, err)
	}
	var (
		ret   []byte
		vmerr error // vm errors do not effect consensus and are therefore not assigned to err
	)

	if contractCreation {
		// The reason why we don't increment nonce here is that we need the original
		// nonce to calculate the address of the contract that is being created
		// It does get incremented inside the `Create` call, after the computation
		// of the contract's address, but before the execution of the code.
		ret, _, st.gasRemaining, vmerr = st.evm.Create(sender, st.data, st.gasRemaining, st.value, bailout)
	} else {
		ret, st.gasRemaining, vmerr = st.evm.Call(sender, st.to(), st.data, st.gasRemaining, st.value, bailout)
	}

	if refunds && !gasBailout {
		refundQuotient := params.RefundQuotient
		if rules.IsLondon {
			refundQuotient = params.RefundQuotientEIP3529
		}
		gasUsed := st.gasUsed()
		refund := min(gasUsed/refundQuotient, st.state.GetRefund())
		gasUsed = gasUsed - refund
		if rules.IsPrague {
			gasUsed = max(floorGas7623, gasUsed)
		}
		st.gasRemaining = st.initialGas - gasUsed
		st.refundGas()
	} else if rules.IsPrague {
		st.gasRemaining = st.initialGas - max(floorGas7623, st.gasUsed())
	}

	effectiveTip := st.gasPrice
	if rules.IsLondon {
		if st.feeCap.Gt(st.evm.Context.BaseFee) {
			effectiveTip = math.U256Min(st.tipCap, (&uint256.Int{}).Sub(st.feeCap, st.evm.Context.BaseFee))
		} else {
			effectiveTip = u256.Num0
		}
	}

	tipAmount := (&uint256.Int{}).SetUint64(st.gasUsed())
	tipAmount.Mul(tipAmount, effectiveTip) // gasUsed * effectiveTip = how much goes to the block producer (miner, validator)

	if !st.noFeeBurnAndTip {
		if err := st.state.AddBalance(coinbase, *tipAmount, tracing.BalanceIncreaseRewardTransactionFee); err != nil {
			return nil, fmt.Errorf("%w: %w", ErrStateTransitionFailed, err)
		}
	}

	var burnAmount uint256.Int
	var burntContractAddress *common.Address

	if !msg.IsFree() && rules.IsLondon {
		burntContractAddress = st.evm.ChainConfig().GetBurntContract(st.evm.Context.BlockNumber)
		if burntContractAddress != nil {
			burnAmount = *(&uint256.Int{}).Mul((&uint256.Int{}).SetUint64(st.gasUsed()), st.evm.Context.BaseFee)

			if rules.IsAura && rules.IsPrague {
				// https://github.com/gnosischain/specs/blob/master/network-upgrades/pectra.md#eip-4844-pectra
				burnAmount = *(&uint256.Int{}).Add(&burnAmount, st.evm.BlobFee)
			}

			if !st.noFeeBurnAndTip {
				st.state.AddBalance(*burntContractAddress, burnAmount, tracing.BalanceChangeUnspecified)
			}
		}
	}

	if st.state.Trace() || st.state.TraceAccount(st.msg.From()) {
		fmt.Printf("(%d.%d) Fees %x: tipped: %d, burnt: %d, price: %d, gas: %d\n", st.state.TxIndex(), st.state.Incarnation(), st.msg.From(), tipAmount, &burnAmount, st.gasPrice, st.gasUsed())
	}

	result = &evmtypes.ExecutionResult{
		GasUsed:             st.gasUsed(),
		Err:                 vmerr,
		Reverted:            vmerr == vm.ErrExecutionReverted,
		ReturnData:          ret,
		SenderInitBalance:   senderInitBalance,
		CoinbaseInitBalance: coinbaseInitBalance,
		FeeTipped:           *tipAmount,
		FeeBurnt:            burnAmount,
		EvmRefund:           st.state.GetRefund(),
	}

	if burntContractAddress != nil {
		result.BurntContractAddress = *burntContractAddress
	}

	if st.evm.Context.PostApplyMessage != nil {
		st.evm.Context.PostApplyMessage(st.state, msg.From(), coinbase, result)
	}

	return result, nil
}

func (st *StateTransition) verifyAuthorities(auths []types.Authorization, contractCreation bool, chainID string) ([]common.Address, error) {
	verifiedAuthorities := make([]common.Address, 0)
	if len(auths) > 0 {
		if contractCreation {
			return nil, errors.New("contract creation not allowed with type4 txs")
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
				log.Debug("authority recover failed, skipping", "err", err, "auth index", i)
				continue
			}
			authority := *authorityPtr

			// 3. add authority account to accesses_addresses
			verifiedAuthorities = append(verifiedAuthorities, authority)
			// authority is added to accessed_address in prepare step

			// 4. authority code should be empty or already delegated
			codeHash, err := st.state.GetCodeHash(authority)
			if err != nil {
				return nil, fmt.Errorf("%w: %w", ErrStateTransitionFailed, err)
			}
			if codeHash != empty.CodeHash && codeHash != (common.Hash{}) {
				// check for delegation
				_, ok, err := st.state.GetDelegatedDesignation(authority)
				if err != nil {
					return nil, fmt.Errorf("%w: %w", ErrStateTransitionFailed, err)
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
				return nil, fmt.Errorf("%w: %w", ErrStateTransitionFailed, err)
			}
			if authorityNonce != auth.Nonce {
				log.Debug("invalid nonce, skipping", "auth index", i)
				continue
			}

			// 6. Add PER_EMPTY_ACCOUNT_COST - PER_AUTH_BASE_COST gas to the global refund counter if authority exists in the trie.
			exists, err := st.state.Exist(authority)
			if err != nil {
				return nil, fmt.Errorf("%w: %w", ErrStateTransitionFailed, err)
			}
			if exists {
				st.state.AddRefund(params.PerEmptyAccountCost - params.PerAuthBaseCost)
			}

			// 7. set authority code
			if auth.Address == (common.Address{}) {
				if err := st.state.SetCode(authority, nil); err != nil {
					return nil, fmt.Errorf("%w: %w", ErrStateTransitionFailed, err)
				}
			} else {
				if err := st.state.SetCode(authority, types.AddressToDelegation(auth.Address)); err != nil {
					return nil, fmt.Errorf("%w: %w", ErrStateTransitionFailed, err)
				}
			}

			// 8. increase the nonce of authority
			if err := st.state.SetNonce(authority, authorityNonce+1); err != nil {
				return nil, fmt.Errorf("%w: %w", ErrStateTransitionFailed, err)
			}
		}
	}

	return verifiedAuthorities, nil
}

func (st *StateTransition) refundGas() {
	// Return ETH for remaining gas, exchanged at the original rate.
	remaining := new(uint256.Int).Mul(new(uint256.Int).SetUint64(st.gasRemaining), st.gasPrice)
	if st.state.Trace() || st.state.TraceAccount(st.msg.From()) {
		fmt.Printf("(%d.%d) Refund %x: remaining: %d, price: %d val: %d\n", st.state.TxIndex(), st.state.Incarnation(), st.msg.From(), st.gasRemaining, st.gasPrice, remaining)
	}

	st.state.AddBalance(st.msg.From(), *remaining, tracing.BalanceIncreaseGasReturn)

	// Also return remaining gas to the block gas counter so it is
	// available for the next transaction.
	st.gp.AddGas(st.gasRemaining)
}

// gasUsed returns the amount of gas used up by the state transition.
func (st *StateTransition) gasUsed() uint64 {
	return st.initialGas - st.gasRemaining
}
