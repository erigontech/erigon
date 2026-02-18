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
	"math"
	"slices"

	"github.com/erigontech/erigon/arb/multigas"
	"github.com/erigontech/erigon/execution/chain"
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

var arbTrace bool

func init() {
	arbTrace = dbg.EnvBool("ARB_TRACE", false)
}

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
		blockContext.Coinbase = state.SystemAddress
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
	if st.evm.ChainRules().IsCancun && !st.evm.ChainRules().IsArbitrum {
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

	if st.evm.Config().Tracer != nil && st.evm.Config().Tracer.OnGasChange != nil {
		st.evm.Config().Tracer.OnGasChange(0, st.msg.Gas(), tracing.GasChangeTxInitialBalance)
	}

	if tracer := st.evm.Config().Tracer; tracer != nil && tracer.CaptureArbitrumTransfer != nil {
		tracer.CaptureArbitrumTransfer(st.msg.From(), accounts.ZeroAddress, &gasVal, true, "feePayment")
	}

	// Check for overflow before adding gas
	if st.gasRemaining > math.MaxUint64-st.msg.Gas() {
		panic(fmt.Sprintf("gasRemaining overflow in buyGas: gasRemaining=%d, msg.Gas()=%d", st.gasRemaining, st.msg.Gas()))
	}

	//fmt.Printf("buyGas: adding gas %d from %x\n", st.msg.Gas(), st.msg.From())
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
	isCancun := rules.IsCancun
	if st.evm.ChainConfig().IsArbitrum() {
		isCancun = false
	}
	if st.msg.BlobGas() > 0 && isCancun {
		blobGasPrice := st.evm.Context.BlobBaseFee
		maxFeePerBlobGas := st.msg.MaxFeePerBlobGas()
		if !st.evm.Config().NoBaseFee && blobGasPrice.Cmp(maxFeePerBlobGas) > 0 {
			return fmt.Errorf("%w: address %v, maxFeePerBlobGas: %v < blobGasPrice: %v",
				ErrMaxFeePerBlobGas, from, st.msg.MaxFeePerBlobGas(), blobGasPrice)
		}
	}

	// TODO arbitrum
	// Check that the user is paying at least the current blob fee
	// if st.evm.ChainConfig().IsCancun(st.evm.Context.BlockNumber, st.evm.Context.Time, st.evm.Context.ArbOSVersion) {
	// 	if st.blobGasUsed() > 0 {
	// 		// Skip the checks if gas fields are zero and blobBaseFee was explicitly disabled (eth_call)
	// 		skipCheck := st.evm.Config.NoBaseFee && msg.BlobGasFeeCap.BitLen() == 0
	// 		if !skipCheck {
	// 			// This will panic if blobBaseFee is nil, but blobBaseFee presence
	// 			// is verified as part of header validation.
	// 			if msg.BlobGasFeeCap.Cmp(st.evm.Context.BlobBaseFee) < 0 {
	// 				return fmt.Errorf("%w: address %v blobGasFeeCap: %v, blobBaseFee: %v", ErrBlobFeeCapTooLow,
	// 					msg.From.Hex(), msg.BlobGasFeeCap, st.evm.Context.BlobBaseFee)
	// 			}
	// 		}
	// 	}
	// }
	// EIP-7825: Transaction Gas Limit Cap
	// TODO should skip for arbitrum?
	if !rules.IsArbitrum && st.msg.CheckGas() && rules.IsOsaka && st.msg.Gas() > params.MaxTxnGasLimit {
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
	// Check for overflow before adding gas
	if st.gasRemaining > math.MaxUint64-st.msg.Gas() {
		panic(fmt.Sprintf("gasRemaining overflow in ApplyFrame: gasRemaining=%d, msg.Gas()=%d", st.gasRemaining, st.msg.Gas()))
	}
	st.gasRemaining += st.msg.Gas()
	st.initialGas = st.msg.Gas()
	sender := msg.From()
	contractCreation := msg.To().IsNil()
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

	ret, st.gasRemaining, _, vmerr = st.evm.Call(sender, st.to(), st.data, st.gasRemaining, &st.value, false)

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
	endTxNow, startHookUsedMultiGas, err, returnData := st.evm.ProcessingHook.StartTxHook()
	startHookUsedSingleGas := startHookUsedMultiGas.SingleGas()
	if endTxNow {
		return &evmtypes.ExecutionResult{
			GasUsed:       startHookUsedSingleGas,
			Err:           err,
			ReturnData:    returnData,
			ScheduledTxes: st.evm.ProcessingHook.ScheduledTxes(),
			UsedMultiGas:  startHookUsedMultiGas,
		}, nil
	}

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

	// Arbitrum: drop tip for delayed (and old) messages
	if st.evm.ProcessingHook.DropTip() && st.msg.GasPrice().Cmp(&st.evm.Context.BaseFee) > 0 {
		mmsg := st.msg.(*types.Message)
		mmsg.SetGasPrice(&st.evm.Context.BaseFee)
		mmsg.SetTip(common.Num0)
		mmsg.TxRunContext = types.NewMessageCommitContext(nil)

		st.gasPrice = &st.evm.Context.BaseFee
		st.tipCap = common.Num0
		st.msg = mmsg
	}
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

	var gas uint64
	var floorGas7623 uint64
	var overflow bool
	var usedMultiGas = multigas.ZeroGas()
	var multiGas multigas.MultiGas

	// TODO only for arbos50?
	//if st.evm.ProcessingHook.IsArbitrum() {
	multiGas, floorGas7623, overflow = multigas.IntrinsicMultiGas(st.data, uint64(len(accessTuples)), uint64(accessTuples.StorageKeys()), contractCreation, rules.IsHomestead, rules.IsIstanbul, isEIP3860, rules.IsPrague, false, uint64(len(auths)))
	//usedMultiGas = usedMultiGas.SaturatingAdd(multiGas)
	gas = multiGas.SingleGas()
	//} else {
	// Check clauses 4-5, subtract intrinsic gas if everything is correct
	gas2, floorGas76232, overflow2 := fixedgas.IntrinsicGas(st.data, uint64(len(accessTuples)), uint64(accessTuples.StorageKeys()), contractCreation, rules.IsHomestead, rules.IsIstanbul, isEIP3860, rules.IsPrague, false, uint64(len(auths)))
	if multiGas.SingleGas() != gas2 || floorGas7623 != floorGas76232 || overflow != overflow2 {
		fmt.Printf("Mg %d, fg7623 %d, ovf %v\n", multiGas.SingleGas(), floorGas7623, overflow)
		fmt.Printf("g %d, fg7623 %d, ovf %v\n", gas2, floorGas76232, overflow2)
		panic("intrinsic gas mismatch between multigas and fixedgas")
	}
	//}

	if overflow {
		return nil, ErrGasUintOverflow
	}
	if !rules.IsArbitrum && (st.gasRemaining < gas || st.gasRemaining < floorGas7623) {
		fmt.Printf("st.gasRemaining %d, gas %d, floorGas7623 %d\n", st.gasRemaining, gas, floorGas7623)
		return nil, fmt.Errorf("%w: have %d, want %d", ErrIntrinsicGas, st.gasRemaining, max(gas, floorGas7623))
	}

	verifiedAuthorities, err := st.verifyAuthorities(auths, contractCreation, rules.ChainID.String())
	if err != nil {
		return nil, err
	}

	// Gas limit suffices for the floor data cost (EIP-7623)
	// TODO enable only at arbos50? skip at all??
	if rules.IsPrague && st.evm.ProcessingHook.IsCalldataPricingIncreaseEnabled() {
		floorDataGas, err := FloorDataGas(msg.Data())
		if err != nil {
			return nil, err
		}
		fmt.Printf("Checking floor data gas at tx with msg gas limit %d and floorDataGas %d\n", msg.Gas(), floorDataGas)
		if msg.Gas() < floorDataGas {
			return nil, fmt.Errorf("%w: have %d, want %d", errors.New("floor data gas bigger than gasLimit"), msg.Gas(), floorDataGas)
		}
		if floorDataGas != floorGas7623 {
			fmt.Errorf("fdg %d - intrinsic gas %d", floorDataGas, floorGas7623)
		}
	}
	if t := st.evm.Config().Tracer; t != nil && t.OnGasChange != nil {
		t.OnGasChange(st.gasRemaining, st.gasRemaining-gas, tracing.GasChangeTxIntrinsicGas)
	}
	// Check for underflow before subtracting intrinsic gas (should be caught by earlier check, but be safe)
	if st.gasRemaining < gas {
		panic(fmt.Sprintf("gasRemaining underflow in TransitionDb (intrinsic gas): gasRemaining=%d, gas=%d", st.gasRemaining, gas))
	}
	st.gasRemaining -= gas
	usedMultiGas = usedMultiGas.SaturatingAdd(multiGas)

	tipReceipient, multiGas, err := st.evm.ProcessingHook.GasChargingHook(&st.gasRemaining, gas)
	if err != nil {
		return nil, err
	}

	usedMultiGas = usedMultiGas.SaturatingAdd(multiGas)

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

		deployedContract = new(accounts.Address)
	)
	if contractCreation {
		// The reason why we don't increment nonce here is that we need the original
		// nonce to calculate the address of the contract that is being created
		// It does get incremented inside the `Create` call, after the computation
		// of the contract's address, but before the execution of the code.
		ret, *deployedContract, st.gasRemaining, multiGas, vmerr = st.evm.Create(sender, st.data, st.gasRemaining, &st.value, bailout)
		usedMultiGas = usedMultiGas.SaturatingAdd(multiGas)
	} else {
		ret, st.gasRemaining, multiGas, vmerr = st.evm.Call(sender, st.to(), st.data, st.gasRemaining, &st.value, bailout)
		// TODO multiGas was not updated since last addition, why add again?
		usedMultiGas = usedMultiGas.SaturatingAdd(multiGas)
	}

	if refunds && !gasBailout {
		//refund := st.calcGasRefund(rules)
		//usedMultiGas = st.reimburseGas(rules, refund, floorGas7623, usedMultiGas)

		refundQuotient := params.RefundQuotient
		if rules.IsLondon {
			refundQuotient = params.RefundQuotientEIP3529
		}

		if st.evm.ProcessingHook.IsArbitrum() {
			// Refund the gas that was held to limit the amount of computation done.
			//st.gasRemaining += st.calcHeldGasRefund() // affects .gasUsed()
			frg := st.evm.ProcessingHook.ForceRefundGas()
			//fmt.Printf("[%d] gas used %d force refund gas: %d, remains %d\n",
			//	st.evm.Context.BlockNumber, st.gasUsed(), frg, st.gasRemaining)
			st.gasRemaining += frg

			nonrefundable := st.evm.ProcessingHook.NonrefundableGas()
			if nonrefundable < st.gasUsed() {
				// Apply refund counter, capped to a refund quotient
				refund := (st.gasUsed() - nonrefundable) / refundQuotient // Before EIP-3529
				if refund > st.state.GetRefund() {
					refund = st.state.GetRefund()
				}
				st.gasRemaining += refund
				// Arbitrum: set the multigas refunds
				usedMultiGas = usedMultiGas.WithRefund(refund)
			}

			if rules.IsPrague && st.evm.ProcessingHook.IsCalldataPricingIncreaseEnabled() {
				// After EIP-7623: Data-heavy transactions pay the floor gas.
				if st.gasUsed() < floorGas7623 {
					usedMultiGas = usedMultiGas.SaturatingIncrement(multigas.ResourceKindL2Calldata, floorGas7623-usedMultiGas.SingleGas())
					prev := st.gasRemaining
					st.gasRemaining = st.initialGas - floorGas7623
					if t := st.evm.Config().Tracer; t != nil && t.OnGasChange != nil {
						t.OnGasChange(prev, st.gasRemaining, tracing.GasChangeTxDataFloor)
					}
				}
				//if peakGasUsed < floorGas7623 {
				//	peakGasUsed = floorGas7623
				//}// todo
			}

		} else { // Other networks
			gasUsed := st.gasUsed()
			refund := min(gasUsed/refundQuotient, st.state.GetRefund())
			gasUsed = gasUsed - refund

			if rules.IsPrague {
				gasUsed = max(floorGas7623, gasUsed)
			}
			st.gasRemaining = st.initialGas - gasUsed
		}

		st.refundGas()
	} else if rules.IsPrague {
		fmt.Println("i was not supposed to be in non-arbitrum prague")
		st.gasRemaining = st.initialGas - max(floorGas7623, st.gasUsed())
	}

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
		// MERGE_ARBITRUM, the following was in arbitrum branch:
		/*
			if rules.IsArbitrum {
				if err := st.state.AddBalance(coinbase, *tipAmount, tracing.BalanceIncreaseRewardTransactionFee); err != nil {
					return nil, fmt.Errorf("%w: %w", ErrStateTransitionFailed, err)
			}
		*/
		if err := st.state.AddBalance(coinbase, tipAmount, tracing.BalanceIncreaseRewardTransactionFee); err != nil {
			return nil, fmt.Errorf("%w: %w", ErrStateTransitionFailed, err)
		}
	}
	if st.evm.Config().NoBaseFee && msg.FeeCap().Sign() == 0 && msg.TipCap().Sign() == 0 {
		// Skip fee payment when NoBaseFee is set and the fee fields
		// are 0. This avoids a negative effectiveTip being applied to
		// the coinbase when simulating calls.
	} else {
		if err := st.state.AddBalance(tipReceipient, tipAmount, tracing.BalanceIncreaseRewardTransactionFee); err != nil {
			return nil, fmt.Errorf("%w: %w", ErrStateTransitionFailed, err)
		}
	}

	var burnAmount uint256.Int
	var burntContractAddress accounts.Address
	var tracingTipAmount *uint256.Int

	if !msg.IsFree() && rules.IsLondon {
		burntContractAddress = st.evm.ChainConfig().GetBurntContract(st.evm.Context.BlockNumber)
		if !burntContractAddress.IsNil() {
			burnAmount = u256.Mul(u256.U64(st.gasUsed()), st.evm.Context.BaseFee)

			if arbTrace {
				fmt.Printf("burnAddr %x tipAddr %x\n", burntContractAddress, tipReceipient)
			}
			tracingTipAmount = burnAmount.Clone()

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
	// Arbitrum: record the tip
	if tracer := st.evm.Config().Tracer; tracer != nil && tracer.CaptureArbitrumTransfer != nil && !st.evm.ProcessingHook.DropTip() {
		if !tracingTipAmount.IsZero() {
			tracer.CaptureArbitrumTransfer(accounts.NilAddress, tipReceipient, tracingTipAmount, false, "tip")
		}
	}
	//fmt.Printf("tx from %x used gas: %d, initGas %d remain %d %s\n", st.msg.From(), st.gasUsed(), st.initialGas, st.gasRemaining, usedMultiGas)

	st.evm.ProcessingHook.EndTxHook(st.gasRemaining, vmerr == nil)
	var topLvlDeployed common.Address
	if deployedContract != nil && !deployedContract.IsNil() {
		topLvlDeployed = deployedContract.Value()
	}
	result = &evmtypes.ExecutionResult{
		GasUsed:             st.gasUsed(),
		Err:                 vmerr,
		Reverted:            errors.Is(vmerr, vm.ErrExecutionReverted),
		ReturnData:          ret,
		SenderInitBalance:   senderInitBalance,
		CoinbaseInitBalance: coinbaseInitBalance,
		FeeTipped:           tipAmount,
		FeeBurnt:            burnAmount,
		EvmRefund:           st.state.GetRefund(),

		// Arbitrum
		ScheduledTxes:    st.evm.ProcessingHook.ScheduledTxes(),
		TopLevelDeployed: &topLvlDeployed,
		UsedMultiGas:     usedMultiGas,
	}

	result.BurntContractAddress = burntContractAddress

	if st.evm.Context.PostApplyMessage != nil {
		st.evm.Context.PostApplyMessage(st.state, msg.From(), coinbase, result)
	}

	return result, nil
}

// FloorDataGas computes the minimum gas required for a transaction based on its data tokens (EIP-7623).
func FloorDataGas(data []byte) (uint64, error) {

	var (
		z                            = uint64(bytes.Count(data, []byte{0}))
		nz                           = uint64(len(data)) - z
		TxTokenPerNonZeroByte uint64 = 4  // Token cost per non-zero byte as specified by EIP-7623.
		TxCostFloorPerToken   uint64 = 10 // Cost floor per byte of data as specified by EIP-7623.
		tokens                       = nz*TxTokenPerNonZeroByte + z
	)
	// Check for overflow
	if (math.MaxUint64-params.TxGas)/TxCostFloorPerToken < tokens {
		return 0, ErrGasUintOverflow
	}
	// Minimum gas required for a transaction based on its data tokens (EIP-7623).
	return params.TxGas + tokens*TxCostFloorPerToken, nil
}

func (st *StateTransition) calcHeldGasRefund() uint64 {
	return st.evm.ProcessingHook.ForceRefundGas()
}

// Arbitrum  // TODO move
// RevertedTxGasUsed maps specific transaction hashes that have been previously reverted to the amount
// of GAS used by that specific transaction alone.
var RevertedTxGasUsed = map[common.Hash]uint64{
	// Arbitrum Sepolia (chain_id=421614). Tx timestamp: Oct-13-2025 03:30:36 AM +UTC
	common.HexToHash("0x58df300a7f04fe31d41d24672786cbe1c58b4f3d8329d0d74392d814dd9f7e40"): 45174,
}

// handleRevertedTx attempts to process a reverted transaction. It returns
// ErrExecutionReverted with the updated multiGas if a matching reverted
// tx is found; otherwise, it returns nil error with unchangedmultiGas
func (st *StateTransition) handleRevertedTx(msg *types.Message, usedMultiGas multigas.MultiGas) (multigas.MultiGas, error) {
	if msg.Tx == nil {
		return usedMultiGas, nil
	}

	txHash := msg.Tx.Hash()
	if l2GasUsed, ok := RevertedTxGasUsed[txHash]; ok {
		pn, err := st.state.GetNonce(msg.From())
		if err != nil {
			return usedMultiGas, fmt.Errorf("handle revert: %w", err)
		}
		err = st.state.SetNonce(msg.From(), uint64(pn)+1)
		if err != nil {
			return usedMultiGas, fmt.Errorf("handle revert: %w", err)
		}

		// Calculate adjusted gas since l2GasUsed contains params.TxGas
		if l2GasUsed < params.TxGas {
			panic(fmt.Sprintf("adjustedGas underflow in handleRevertedTx: l2GasUsed=%d, params.TxGas=%d", l2GasUsed, params.TxGas))
		}
		adjustedGas := l2GasUsed - params.TxGas
		if st.gasRemaining < adjustedGas {
			panic(fmt.Sprintf("gasRemaining underflow in handleRevertedTx: gasRemaining=%d, adjustedGas=%d", st.gasRemaining, adjustedGas))
		}
		st.gasRemaining -= adjustedGas

		// Update multigas and return ErrExecutionReverted error
		usedMultiGas = usedMultiGas.SaturatingAdd(multigas.ComputationGas(adjustedGas))
		return usedMultiGas, vm.ErrExecutionReverted
	}

	return usedMultiGas, nil
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
			st.state.MarkAddressAccess(authority)

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

func (st *StateTransition) calcGasRefund(rules *chain.Rules) uint64 {
	//
	//refundQuotient := params.RefundQuotient
	//if rules.IsLondon {
	//	refundQuotient = params.RefundQuotientEIP3529
	//}
	//
	//// Refund the gas that was held to limit the amount of computation done.
	//st.gasRemaining += st.calcHeldGasRefund()
	//
	//if st.evm.ProcessingHook.IsArbitrum() {
	//	st.gasRemaining += st.evm.ProcessingHook.ForceRefundGas()
	//	nonrefundable := st.evm.ProcessingHook.NonrefundableGas()
	//	var refund uint64
	//	if nonrefundable < st.gasUsed() {
	//		// Apply refund counter, capped to a refund quotient
	//		refund = (st.gasUsed() - nonrefundable) / refundQuotient // Before EIP-3529
	//		if refund > st.state.GetRefund() {
	//			refund = st.state.GetRefund()
	//		}
	//		st.gasRemaining += refund
	//	}
	//
	//	// Arbitrum: set the multigas refunds
	//	usedMultiGas = usedMultiGas.WithRefund(refund)
	//	if rules.IsPrague && st.evm.ProcessingHook.IsCalldataPricingIncreaseEnabled() {
	//		// After EIP-7623: Data-heavy transactions pay the floor gas.
	//		if st.gasUsed() < floorGas7623 {
	//			usedMultiGas = usedMultiGas.SaturatingIncrement(multigas.ResourceKindL2Calldata, floorGas7623-usedMultiGas.SingleGas())
	//			prev := st.gasRemaining
	//			st.gasRemaining = st.initialGas - floorGas7623
	//			if t := st.evm.Config().Tracer; t != nil && t.OnGasChange != nil {
	//				t.OnGasChange(prev, st.gasRemaining, tracing.GasChangeTxDataFloor)
	//			}
	//		}
	//	}
	//
	//} else { // Other networks
	//	gasUsed := st.gasUsed()
	//	refund := min(gasUsed/refundQuotient, st.state.GetRefund())
	//	gasUsed = gasUsed - refund
	//
	//	if rules.IsPrague {
	//		gasUsed = max(floorGas7623, gasUsed)
	//	}
	//	st.gasRemaining = st.initialGas - gasUsed
	//}
	//
	//st.refundGas()
	refundQuotient := params.RefundQuotient
	if rules.IsLondon {
		refundQuotient = params.RefundQuotientEIP3529
	}

	var refund uint64
	if !st.evm.ProcessingHook.IsArbitrum() {
		refund = min(st.gasUsed()/refundQuotient, st.state.GetRefund())
	} else { // Arbitrum
		nonrefundable := st.evm.ProcessingHook.NonrefundableGas()
		if nonrefundable < st.gasUsed() {
			// Apply refund counter, capped to a refund quotient
			refund = (st.gasUsed() - nonrefundable) / refundQuotient // Before EIP-3529
			if refund > st.state.GetRefund() {
				refund = st.state.GetRefund()
			}
		}
	}

	// Refund the gas that was held to limit the amount of computation done.
	heldRefund := st.calcHeldGasRefund()
	totalRefund := refund + heldRefund
	if totalRefund < refund || totalRefund < heldRefund {
		panic(fmt.Sprintf("calcGasRefund overflow: refund=%d, heldRefund=%d", refund, heldRefund))
	}
	return totalRefund
}

func (st *StateTransition) reimburseGas(rules *chain.Rules, refund, floorGas7623 uint64, usedMultiGas multigas.MultiGas) multigas.MultiGas {
	if !st.evm.ProcessingHook.IsArbitrum() {
		if st.gasUsed() < refund {
			panic(fmt.Sprintf("gasUsed underflow in reimburseGas: gasUsed=%d, refund=%d", st.gasUsed(), refund))
		}
		gasUsed := st.gasUsed() - refund
		if rules.IsPrague {
			gasUsed = max(floorGas7623, gasUsed)
		}
		if st.initialGas < gasUsed {
			panic(fmt.Sprintf("gasRemaining underflow in reimburseGas (non-Arbitrum): initialGas=%d, gasUsed=%d", st.initialGas, gasUsed))
		}
		st.gasRemaining = st.initialGas - gasUsed
	} else { // Arbitrum: set the multigas refunds
		forceRefund := st.evm.ProcessingHook.ForceRefundGas()
		totalRefund := forceRefund + refund
		// Check for overflow in refund addition
		if totalRefund < forceRefund || totalRefund < refund {
			panic(fmt.Sprintf("refund overflow in reimburseGas: forceRefund=%d, refund=%d", forceRefund, refund))
		}
		// Check for overflow when adding to gasRemaining
		if st.gasRemaining > math.MaxUint64-totalRefund {
			panic(fmt.Sprintf("gasRemaining overflow in reimburseGas (Arbitrum): gasRemaining=%d, totalRefund=%d", st.gasRemaining, totalRefund))
		}
		st.gasRemaining += totalRefund

		usedMultiGas = usedMultiGas.WithRefund(refund)
		if rules.IsPrague && st.evm.ProcessingHook.IsCalldataPricingIncreaseEnabled() {
			// After EIP-7623: Data-heavy transactions pay the floor gas.
			if st.gasUsed() < floorGas7623 {
				usedMultiGas = usedMultiGas.SaturatingIncrement(multigas.ResourceKindL2Calldata, floorGas7623-usedMultiGas.SingleGas())
				prev := st.gasRemaining
				if st.initialGas < floorGas7623 {
					panic(fmt.Sprintf("gasRemaining underflow in reimburseGas (Arbitrum Prague floor): initialGas=%d, floorGas7623=%d", st.initialGas, floorGas7623))
				}
				st.gasRemaining = st.initialGas - floorGas7623

				if t := st.evm.Config().Tracer; t != nil && t.OnGasChange != nil {
					t.OnGasChange(prev, st.gasRemaining, tracing.GasChangeTxDataFloor)
				}
			}
		}
	}
	st.refundGas()
	return usedMultiGas
}

func (st *StateTransition) refundGas() {
	// Return ETH for remaining gas, exchanged at the original rate.
	remaining := u256.Mul(u256.U64(st.gasRemaining), *st.gasPrice)
	if dbg.TraceGas || st.state.Trace() || dbg.TraceAccount(st.msg.From().Handle()) {
		fmt.Printf("%d (%d.%d) Refund %x: remaining: %d, price: %d val: %d\n", st.state.BlockNumber(), st.state.TxIndex(), st.state.Incarnation(), st.msg.From(), st.gasRemaining, st.gasPrice, &remaining)
	}
	if arbTrace {
		fmt.Printf("[ST] refund remaining gas %d to %x\n", remaining, st.msg.From())
	}

	st.state.AddBalance(st.msg.From(), remaining, tracing.BalanceIncreaseGasReturn)

	// Arbitrum: record the gas refund
	if tracer := st.evm.Config().Tracer; tracer != nil && tracer.CaptureArbitrumTransfer != nil {
		from := st.msg.From()
		// TODO revisit CaptureArbitrumTransfer - set NilADdress somewhere where i set Zero adress and also make non ptr gas passing
		tracer.CaptureArbitrumTransfer(accounts.NilAddress, from, &remaining, false, "gasRefund")
	}

	// Also return remaining gas to the block gas counter so it is
	// available for the next transaction.
	st.gp.AddGas(st.gasRemaining)
}

// gasUsed returns the amount of gas used up by the state transition.
func (st *StateTransition) gasUsed() uint64 {
	if st.initialGas < st.gasRemaining {
		panic(fmt.Sprintf("gasUsed underflow: initialGas=%d, gasRemaining=%d", st.initialGas, st.gasRemaining))
	}
	return st.initialGas - st.gasRemaining
}

// IntrinsicGas computes the 'intrinsic gas' for a message with the given data.
// TODO: convert the input to a struct
func IntrinsicGas(data []byte, accessList types.AccessList, isContractCreation bool, isHomestead, isEIP2028, isEIP3860, isPrague bool, authorizationsLen uint64) (uint64, uint64, error) {
	// Zero and non-zero bytes are priced differently
	dataLen := uint64(len(data))
	dataNonZeroLen := uint64(0)
	for _, byt := range data {
		if byt != 0 {
			dataNonZeroLen++
		}
	}

	// TODO arbitrum - do we need a separate one intrinsic estimator
	gas, floorGas7623, overflow := fixedgas.CalcIntrinsicGas(dataLen, dataNonZeroLen, authorizationsLen, uint64(len(accessList)), uint64(accessList.StorageKeys()), isContractCreation, isHomestead, isEIP2028, isEIP3860, isPrague, false /*isAAtxn*/)
	if overflow != false {
		return 0, 0, ErrGasUintOverflow
	}
	return gas, floorGas7623, nil
}
