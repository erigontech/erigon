// Copyright 2014 The go-ethereum Authors
// This file is part of the go-ethereum library.
//
// The go-ethereum library is free software: you can redistribute it and/or modify
// it under the terms of the GNU Lesser General Public License as published by
// the Free Software Foundation, either version 3 of the License, or
// (at your option) any later version.
//
// The go-ethereum library is distributed in the hope that it will be useful,
// but WITHOUT ANY WARRANTY; without even the implied warranty of
// MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE. See the
// GNU Lesser General Public License for more details.
//
// You should have received a copy of the GNU Lesser General Public License
// along with the go-ethereum library. If not, see <http://www.gnu.org/licenses/>.

package core

import (
	"fmt"

	"github.com/holiman/uint256"

	libcommon "github.com/ledgerwatch/erigon-lib/common"
	"github.com/ledgerwatch/erigon-lib/txpool"
	types2 "github.com/ledgerwatch/erigon-lib/types"

	"github.com/ledgerwatch/erigon/common"
	cmath "github.com/ledgerwatch/erigon/common/math"
	"github.com/ledgerwatch/erigon/common/u256"
	"github.com/ledgerwatch/erigon/consensus/misc"
	"github.com/ledgerwatch/erigon/core/vm"
	"github.com/ledgerwatch/erigon/core/vm/evmtypes"
	"github.com/ledgerwatch/erigon/crypto"
	"github.com/ledgerwatch/erigon/params"
)

var emptyCodeHash = crypto.Keccak256Hash(nil)

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
type StateTransition struct {
	gp         *GasPool
	msg        Message
	gas        uint64
	gasPrice   *uint256.Int
	gasFeeCap  *uint256.Int
	tip        *uint256.Int
	initialGas uint64
	value      *uint256.Int
	data       []byte
	state      evmtypes.IntraBlockState
	evm        vm.VMInterface

	//some pre-allocated intermediate variables
	sharedBuyGas        *uint256.Int
	sharedBuyGasBalance *uint256.Int

	isBor bool
}

// Message represents a message sent to a contract.
type Message interface {
	From() libcommon.Address
	To() *libcommon.Address

	GasPrice() *uint256.Int
	FeeCap() *uint256.Int
	Tip() *uint256.Int
	Gas() uint64
	DataGas() uint64
	MaxFeePerDataGas() *uint256.Int
	Value() *uint256.Int

	Nonce() uint64
	CheckNonce() bool
	Data() []byte
	AccessList() types2.AccessList
	DataHashes() []libcommon.Hash

	IsFree() bool
}

// ExecutionResult includes all output after executing given evm
// message no matter the execution itself is successful or not.
type ExecutionResult struct {
	UsedGas    uint64 // Total used gas but include the refunded gas
	Err        error  // Any error encountered during the execution(listed in core/vm/errors.go)
	ReturnData []byte // Returned data from evm(function result or data supplied with revert opcode)
}

// Unwrap returns the internal evm error which allows us for further
// analysis outside.
func (result *ExecutionResult) Unwrap() error {
	return result.Err
}

// Failed returns the indicator whether the execution is successful or not
func (result *ExecutionResult) Failed() bool { return result.Err != nil }

// Return is a helper function to help caller distinguish between revert reason
// and function return. Return returns the data after execution if no error occurs.
func (result *ExecutionResult) Return() []byte {
	if result.Err != nil {
		return nil
	}
	return common.CopyBytes(result.ReturnData)
}

// Revert returns the concrete revert reason if the execution is aborted by `REVERT`
// opcode. Note the reason can be nil if no data supplied with revert opcode.
func (result *ExecutionResult) Revert() []byte {
	if result.Err != vm.ErrExecutionReverted {
		return nil
	}
	return common.CopyBytes(result.ReturnData)
}

// IntrinsicGas computes the 'intrinsic gas' for a message with the given data.
func IntrinsicGas(data []byte, accessList types2.AccessList, isContractCreation bool, isHomestead, isEIP2028, isEIP3860 bool) (uint64, error) {
	// Zero and non-zero bytes are priced differently
	dataLen := uint64(len(data))
	dataNonZeroLen := uint64(0)
	for _, byt := range data {
		if byt != 0 {
			dataNonZeroLen++
		}
	}

	gas, status := txpool.CalcIntrinsicGas(dataLen, dataNonZeroLen, accessList, isContractCreation, isHomestead, isEIP2028, isEIP3860)
	if status != txpool.Success {
		return 0, ErrGasUintOverflow
	}
	return gas, nil
}

// NewStateTransition initialises and returns a new state transition object.
func NewStateTransition(evm vm.VMInterface, msg Message, gp *GasPool) *StateTransition {
	isBor := evm.ChainConfig().Bor != nil
	return &StateTransition{
		gp:        gp,
		evm:       evm,
		msg:       msg,
		gasPrice:  msg.GasPrice(),
		gasFeeCap: msg.FeeCap(),
		tip:       msg.Tip(),
		value:     msg.Value(),
		data:      msg.Data(),
		state:     evm.IntraBlockState(),

		sharedBuyGas:        uint256.NewInt(0),
		sharedBuyGasBalance: uint256.NewInt(0),

		isBor: isBor,
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
// for trace_call to replicate OE/Pariry behaviour
func ApplyMessage(evm vm.VMInterface, msg Message, gp *GasPool, refunds bool, gasBailout bool) (*ExecutionResult, error) {
	return NewStateTransition(evm, msg, gp).TransitionDb(refunds, gasBailout)
}

// to returns the recipient of the message.
func (st *StateTransition) to() libcommon.Address {
	if st.msg == nil || st.msg.To() == nil /* contract creation */ {
		return libcommon.Address{}
	}
	return *st.msg.To()
}

func (st *StateTransition) buyGas(gasBailout bool) error {
	mgval := st.sharedBuyGas
	mgval.SetUint64(st.msg.Gas())
	mgval, overflow := mgval.MulOverflow(mgval, st.gasPrice)
	if overflow {
		return fmt.Errorf("%w: address %v", ErrInsufficientFunds, st.msg.From().Hex())
	}
	balanceCheck := mgval
	if st.gasFeeCap != nil {
		balanceCheck = st.sharedBuyGasBalance.SetUint64(st.msg.Gas())
		balanceCheck, overflow = balanceCheck.MulOverflow(balanceCheck, st.gasFeeCap)
		if overflow {
			return fmt.Errorf("%w: address %v", ErrInsufficientFunds, st.msg.From().Hex())
		}
		balanceCheck, overflow = balanceCheck.AddOverflow(balanceCheck, st.value)
		if overflow {
			return fmt.Errorf("%w: address %v", ErrInsufficientFunds, st.msg.From().Hex())
		}
	}
	var subBalance = false
	if have, want := st.state.GetBalance(st.msg.From()), balanceCheck; have.Cmp(want) < 0 {
		if !gasBailout {
			return fmt.Errorf("%w: address %v have %v want %v", ErrInsufficientFunds, st.msg.From().Hex(), have, want)
		}
	} else {
		subBalance = true
	}
	if err := st.gp.SubGas(st.msg.Gas()); err != nil {
		if !gasBailout {
			return err
		}
	}
	st.gas += st.msg.Gas()

	st.initialGas = st.msg.Gas()
	if subBalance {
		st.state.SubBalance(st.msg.From(), mgval)
	}
	return nil
}

func CheckEip1559TxGasFeeCap(from libcommon.Address, gasFeeCap, tip, baseFee *uint256.Int, isFree bool) error {
	if gasFeeCap.Lt(tip) {
		return fmt.Errorf("%w: address %v, tip: %s, gasFeeCap: %s", ErrTipAboveFeeCap,
			from.Hex(), tip, gasFeeCap)
	}
	if baseFee != nil && gasFeeCap.Lt(baseFee) && !isFree {
		return fmt.Errorf("%w: address %v, gasFeeCap: %s baseFee: %s", ErrFeeCapTooLow,
			from.Hex(), gasFeeCap, baseFee)
	}
	return nil
}

// DESCRIBED: docs/programmers_guide/guide.md#nonce
func (st *StateTransition) preCheck(gasBailout bool) error {
	// Make sure this transaction's nonce is correct.
	if st.msg.CheckNonce() {
		stNonce := st.state.GetNonce(st.msg.From())
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
		if codeHash := st.state.GetCodeHash(st.msg.From()); codeHash != emptyCodeHash && codeHash != (libcommon.Hash{}) {
			// libcommon.Hash{} means that the sender is not in the state.
			// Historically there were transactions with 0 gas price and non-existing sender,
			// so we have to allow that.
			return fmt.Errorf("%w: address %v, codehash: %s", ErrSenderNoEOA,
				st.msg.From().Hex(), codeHash)
		}
	}

	// Make sure the transaction gasFeeCap is greater than the block's baseFee.
	if st.evm.ChainRules().IsLondon {
		// Skip the checks if gas fields are zero and baseFee was explicitly disabled (eth_call)
		if !st.evm.Config().NoBaseFee || !st.gasFeeCap.IsZero() || !st.tip.IsZero() {
			if err := CheckEip1559TxGasFeeCap(st.msg.From(), st.gasFeeCap, st.tip, st.evm.Context().BaseFee, st.msg.IsFree()); err != nil {
				return err
			}
		}
	}
	return st.buyGas(gasBailout)
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
func (st *StateTransition) TransitionDb(refunds bool, gasBailout bool) (*ExecutionResult, error) {
	var input1 *uint256.Int
	var input2 *uint256.Int
	if st.isBor {
		input1 = st.state.GetBalance(st.msg.From()).Clone()
		input2 = st.state.GetBalance(st.evm.Context().Coinbase).Clone()
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
	if st.evm.Config().Debug {
		st.evm.Config().Tracer.CaptureTxStart(st.initialGas)
		defer func() {
			st.evm.Config().Tracer.CaptureTxEnd(st.gas)
		}()
	}

	msg := st.msg
	sender := vm.AccountRef(msg.From())
	contractCreation := msg.To() == nil
	rules := st.evm.ChainRules()
	vmConfig := st.evm.Config()
	isEIP3860 := vmConfig.HasEip3860(rules)

	// Check clauses 4-5, subtract intrinsic gas if everything is correct
	gas, err := IntrinsicGas(st.data, st.msg.AccessList(), contractCreation, rules.IsHomestead, rules.IsIstanbul, isEIP3860)
	if err != nil {
		return nil, err
	}
	if st.gas < gas {
		return nil, fmt.Errorf("%w: have %d, want %d", ErrIntrinsicGas, st.gas, gas)
	}
	st.gas -= gas

	var bailout bool
	// Gas bailout (for trace_call) should only be applied if there is not sufficient balance to perform value transfer
	if gasBailout {
		if !msg.Value().IsZero() && !st.evm.Context().CanTransfer(st.state, msg.From(), msg.Value()) {
			bailout = true
		}
	}

	// Check whether the init code size has been exceeded.
	if isEIP3860 && contractCreation && len(st.data) > params.MaxInitCodeSize {
		return nil, fmt.Errorf("%w: code size %v limit %v", ErrMaxInitCodeSizeExceeded, len(st.data), params.MaxInitCodeSize)
	}

	// Set up the initial access list.
	if rules.IsBerlin {
		st.state.PrepareAccessList(msg.From(), msg.To(), vm.ActivePrecompiles(rules), msg.AccessList())
		// EIP-3651 warm COINBASE
		if rules.IsShanghai {
			st.state.AddAddressToAccessList(st.evm.Context().Coinbase)
		}
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
		ret, _, st.gas, vmerr = st.evm.Create(sender, st.data, st.gas, st.value)
	} else {
		// Increment the nonce for the next transaction
		st.state.SetNonce(msg.From(), st.state.GetNonce(sender.Address())+1)
		ret, st.gas, vmerr = st.evm.Call(sender, st.to(), st.data, st.gas, st.value, bailout)
	}
	if refunds {
		if rules.IsLondon {
			// After EIP-3529: refunds are capped to gasUsed / 5
			st.refundGas(params.RefundQuotientEIP3529)
		} else {
			// Before EIP-3529: refunds were capped to gasUsed / 2
			st.refundGas(params.RefundQuotient)
		}
	}
	effectiveTip := st.gasPrice
	if rules.IsLondon {
		if st.gasFeeCap.Gt(st.evm.Context().BaseFee) {
			effectiveTip = cmath.Min256(st.tip, new(uint256.Int).Sub(st.gasFeeCap, st.evm.Context().BaseFee))
		} else {
			effectiveTip = u256.Num0
		}
	}
	amount := new(uint256.Int).SetUint64(st.gasUsed())
	amount.Mul(amount, effectiveTip) // gasUsed * effectiveTip = how much goes to the block producer (miner, validator)
	st.state.AddBalance(st.evm.Context().Coinbase, amount)
	if !msg.IsFree() && rules.IsLondon && rules.IsEip1559FeeCollector {
		burntContractAddress := *st.evm.ChainConfig().Eip1559FeeCollector
		burnAmount := new(uint256.Int).Mul(new(uint256.Int).SetUint64(st.gasUsed()), st.evm.Context().BaseFee)
		st.state.AddBalance(burntContractAddress, burnAmount)
	}
	if st.isBor {
		// Deprecating transfer log and will be removed in future fork. PLEASE DO NOT USE this transfer log going forward. Parameters won't get updated as expected going forward with EIP1559
		// add transfer log
		output1 := input1.Clone()
		output2 := input2.Clone()
		AddFeeTransferLog(
			st.state,

			msg.From(),
			st.evm.Context().Coinbase,

			amount,
			input1,
			input2,
			output1.Sub(output1, amount),
			output2.Add(output2, amount),
		)
	}

	return &ExecutionResult{
		UsedGas:    st.gasUsed(),
		Err:        vmerr,
		ReturnData: ret,
	}, nil
}

func (st *StateTransition) refundGas(refundQuotient uint64) {
	// Apply refund counter, capped to half of the used gas.
	refund := st.gasUsed() / refundQuotient
	if refund > st.state.GetRefund() {
		refund = st.state.GetRefund()
	}
	st.gas += refund

	// Return ETH for remaining gas, exchanged at the original rate.
	remaining := new(uint256.Int).Mul(new(uint256.Int).SetUint64(st.gas), st.gasPrice)
	st.state.AddBalance(st.msg.From(), remaining)

	// Also return remaining gas to the block gas counter so it is
	// available for the next transaction.
	st.gp.AddGas(st.gas)
}

// gasUsed returns the amount of gas used up by the state transition.
func (st *StateTransition) gasUsed() uint64 {
	return st.initialGas - st.gas
}

func (st *StateTransition) dataGasUsed() uint64 {
	return misc.GetDataGasUsed(len(st.msg.DataHashes()))
}
