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
	"github.com/erigontech/erigon/common/u256"
	"github.com/erigontech/erigon/execution/protocol/fixedgas"
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
			return e.OriginError.Error()
		}
		return "execution aborted"
	}
}

type StateTransition struct {
	gp           *GasPool
	msg          Message
	gasRemaining uint64
	blockGasUsed uint64 // Gas used by the transaction relevant for block limit accounting - see EIP-7778
	gasPrice     *uint256.Int
	feeCap       *uint256.Int
	tipCap       *uint256.Int
	initialGas   uint64
	value        uint256.Int
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

// NewStateTransition initialises and returns a new state transition object.
func NewStateTransition(evm *vm.EVM, msg Message, gp *GasPool) *StateTransition {
	return &StateTransition{
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
	st := NewStateTransition(evm, msg, gp)
	st.noFeeBurnAndTip = noFeeBurnAndTip
	return st.TransitionDb(refunds, gasBailout)
}

func ApplyMessageNoFeeBurnOrTip(evm *vm.EVM, msg Message, gp *GasPool, refunds bool, gasBailout bool, engine rules.EngineReader) (*evmtypes.ExecutionResult, error) {
	return applyMessage(evm, msg, gp, refunds, gasBailout, true, engine)
}

func ApplyFrame(evm *vm.EVM, msg Message, gp *GasPool) (*evmtypes.ExecutionResult, error) {
	return NewStateTransition(evm, msg, gp).ApplyFrame()
}

func (st *StateTransition) SetTrace(trace bool) {
	st.evm.IntraBlockState().SetTrace(trace)
}

// to returns the recipient of the message.
func (st *StateTransition) to() accounts.Address {
	if st.msg == nil || st.msg.To().IsNil() /* contract creation */ {
		return accounts.ZeroAddress
	}
	return st.msg.To()
}

func (st *StateTransition) buyGas(gasBailout bool) error {
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
	// Correct blockGasUsed will be set later in TransitionDb; set it for the moment to the txn's gas limit
	// so that st.gp.AddGas(st.blockGasUsed) in the recover block works correctly even if a panic occurs beforehand.
	st.blockGasUsed = st.msg.Gas()

	if st.evm.Config().Tracer != nil && st.evm.Config().Tracer.OnGasChange != nil {
		st.evm.Config().Tracer.OnGasChange(0, st.msg.Gas(), tracing.GasChangeTxInitialBalance)
	}

	st.gasRemaining += st.msg.Gas()
	st.initialGas = st.msg.Gas()
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

// DESCRIBED: docs/programmers_guide/guide.md#nonce
func (st *StateTransition) preCheck(gasBailout bool) error {
	rules := st.evm.ChainRules()
	from := st.msg.From()
	if rules.IsOsaka && len(st.msg.BlobHashes()) > params.MaxBlobsPerTxn {
		return fmt.Errorf("%w: address %v, blobs: %d", ErrTooManyBlobs, from, len(st.msg.BlobHashes()))
	}

	// Make sure this transaction's nonce is correct.
	if st.msg.CheckNonce() {
		stNonce, err := st.state.GetNonce(from)
		if err != nil {
			return fmt.Errorf("%w: %w", ErrStateTransitionFailed, err)
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
			return fmt.Errorf("%w: %w", ErrStateTransitionFailed, err)
		}
		if !codeHash.IsEmpty() {
			// common.Hash{} means that the sender is not in the state.
			// Historically there were transactions with 0 gas price and non-existing sender,
			// so we have to allow that.

			// eip-7702 allows tx origination from accounts having delegated designation code.
			_, ok, err := st.state.GetDelegatedDesignation(from)
			if err != nil {
				return fmt.Errorf("%w: %w", ErrStateTransitionFailed, err)
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

	// EIP-7825: Transaction Gas Limit Cap
	if st.msg.CheckGas() && rules.IsOsaka && st.msg.Gas() > params.MaxTxnGasLimit {
		return fmt.Errorf("%w: address %v, gas limit %d", ErrGasLimitTooHigh, from, st.msg.Gas())
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
	sender := msg.From()
	contractCreation := msg.To().IsNil()
	rules := st.evm.ChainRules()
	accessTuples := slices.Clone[types.AccessList](msg.AccessList())

	// set code tx
	auths := msg.Authorizations()
	verifiedAuthorities, err := st.verifyAuthorities(auths, contractCreation, rules.ChainID.String())
	if err != nil {
		return nil, err
	}

	// Check whether the init code size has been exceeded.
	if contractCreation {
		if err := vm.CheckMaxInitCodeSize(rules.IsAmsterdam, rules.IsShanghai, uint64(len(st.data))); err != nil {
			return nil, err
		}
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
		ReceiptGasUsed:      st.gasUsed(),
		BlockGasUsed:        st.blockGasUsed,
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
				st.gp.AddGas(st.blockGasUsed)
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
	sender := msg.From()
	contractCreation := msg.To().IsNil()
	rules := st.evm.ChainRules()
	vmConfig := st.evm.Config()
	isEIP3860 := vmConfig.HasEip3860(rules)
	accessTuples := slices.Clone[types.AccessList](msg.AccessList())

	if !contractCreation {
		// Increment the nonce for the next transaction
		nonce, err := st.state.GetNonce(sender)
		if err != nil {
			return nil, fmt.Errorf("%w: %w", ErrStateTransitionFailed, err)
		}
		st.state.SetNonce(msg.From(), nonce+1)
	}

	// set code tx
	auths := msg.Authorizations()

	// Check clauses 4-5, subtract intrinsic gas if everything is correct
	intrinsicGasResult, overflow := fixedgas.IntrinsicGas(fixedgas.IntrinsicGasCalcArgs{
		Data:               st.data,
		AuthorizationsLen:  uint64(len(auths)),
		AccessListLen:      uint64(len(accessTuples)),
		StorageKeysLen:     uint64(accessTuples.StorageKeys()),
		IsContractCreation: contractCreation,
		IsEIP2:             rules.IsHomestead,
		IsEIP2028:          rules.IsIstanbul,
		IsEIP3860:          isEIP3860,
		IsEIP7623:          rules.IsPrague,
	})
	if overflow {
		return nil, ErrGasUintOverflow
	}
	if st.gasRemaining < intrinsicGasResult.RegularGas || st.gasRemaining < intrinsicGasResult.FloorGasCost {
		return nil, fmt.Errorf("%w: have %d, want %d", ErrIntrinsicGas, st.gasRemaining, max(intrinsicGasResult.RegularGas, intrinsicGasResult.FloorGasCost))
	}

	verifiedAuthorities, err := st.verifyAuthorities(auths, contractCreation, rules.ChainID.String())
	if err != nil {
		return nil, err
	}

	if t := st.evm.Config().Tracer; t != nil && t.OnGasChange != nil {
		t.OnGasChange(st.gasRemaining, st.gasRemaining-intrinsicGasResult.RegularGas, tracing.GasChangeTxIntrinsicGas)
	}
	st.gasRemaining -= intrinsicGasResult.RegularGas

	var bailout bool
	// Gas bailout (for trace_call) should only be applied if there is not sufficient balance to perform value transfer
	if gasBailout {
		canTransfer, err := st.evm.Context.CanTransfer(st.state, msg.From(), *msg.Value())
		if err != nil {
			return nil, fmt.Errorf("%w: %w", ErrStateTransitionFailed, err)
		}
		if !msg.Value().IsZero() && !canTransfer {
			bailout = true
		}
	}

	// Check whether the init code size has been exceeded.
	if contractCreation {
		if err := vm.CheckMaxInitCodeSize(rules.IsAmsterdam, rules.IsShanghai, uint64(len(st.data))); err != nil {
			return nil, err
		}
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
		st.blockGasUsed = gasUsed
		refund := min(gasUsed/refundQuotient, st.state.GetRefund())
		gasUsed = gasUsed - refund
		if rules.IsPrague {
			gasUsed = max(intrinsicGasResult.FloorGasCost, gasUsed)
		}
		if rules.IsAmsterdam {
			// EIP-7778: Block Gas Accounting without Refunds
			st.blockGasUsed = max(intrinsicGasResult.FloorGasCost, st.blockGasUsed)
		} else {
			st.blockGasUsed = gasUsed
		}
		st.gasRemaining = st.initialGas - gasUsed
		st.refundGas()
	} else if rules.IsPrague {
		st.blockGasUsed = max(intrinsicGasResult.FloorGasCost, st.gasUsed())
		st.gasRemaining = st.initialGas - st.blockGasUsed
	} else {
		st.blockGasUsed = st.gasUsed()
	}
	// Also return remaining gas to the block gas counter so it is
	// available for the next transaction.
	st.gp.AddGas(st.initialGas - st.blockGasUsed)

	effectiveTip := *st.gasPrice
	if rules.IsLondon {
		if st.feeCap.Gt(&st.evm.Context.BaseFee) {
			effectiveTip = u256.Min(*st.tipCap, u256.Sub(*st.feeCap, st.evm.Context.BaseFee))
		} else {
			effectiveTip = u256.Num0
		}
	}

	tipAmount := u256.Mul(u256.U64(st.gasUsed()), effectiveTip) // gasUsed * effectiveTip = how much goes to the block producer (miner, validator)

	if !st.noFeeBurnAndTip {
		if err := st.state.AddBalance(coinbase, tipAmount, tracing.BalanceIncreaseRewardTransactionFee); err != nil {
			return nil, fmt.Errorf("%w: %w", ErrStateTransitionFailed, err)
		}
	}

	var burnAmount uint256.Int
	var burntContractAddress accounts.Address

	if !msg.IsFree() && rules.IsLondon {
		burntContractAddress = st.evm.ChainConfig().GetBurntContract(st.evm.Context.BlockNumber)
		if !burntContractAddress.IsNil() {
			burnAmount = u256.Mul(u256.U64(st.gasUsed()), st.evm.Context.BaseFee)

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
		fmt.Printf("%d (%d.%d) Fees %x: tipped: %d, burnt: %d, price: %d, gas: %d\n", st.state.BlockNumber(), st.state.TxIndex(), st.state.Incarnation(), st.msg.From(), &tipAmount, &burnAmount, st.gasPrice, st.gasUsed())
	}

	result = &evmtypes.ExecutionResult{
		ReceiptGasUsed:      st.gasUsed(),
		BlockGasUsed:        st.blockGasUsed,
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

func (st *StateTransition) verifyAuthorities(auths []types.Authorization, contractCreation bool, chainID string) ([]accounts.Address, error) {
	verifiedAuthorities := make([]accounts.Address, 0)
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
				return nil, fmt.Errorf("%w: %w", ErrStateTransitionFailed, err)
			}
			if !codeHash.IsEmpty() {
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
				log.Trace("invalid nonce, skipping", "auth index", i)
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
				if err := st.state.SetCode(authority, types.AddressToDelegation(accounts.InternAddress(auth.Address))); err != nil {
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
	remaining := u256.Mul(u256.U64(st.gasRemaining), *st.gasPrice)
	if dbg.TraceGas || st.state.Trace() || dbg.TraceAccount(st.msg.From().Handle()) {
		fmt.Printf("%d (%d.%d) Refund %x: remaining: %d, price: %d val: %d\n", st.state.BlockNumber(), st.state.TxIndex(), st.state.Incarnation(), st.msg.From(), st.gasRemaining, st.gasPrice, &remaining)
	}
	st.state.AddBalance(st.msg.From(), remaining, tracing.BalanceIncreaseGasReturn)
}

// Gas used by the transaction with refunds (what the user pays) - see EIP-7778
func (st *StateTransition) gasUsed() uint64 {
	return st.initialGas - st.gasRemaining
}
