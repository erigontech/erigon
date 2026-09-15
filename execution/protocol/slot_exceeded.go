package protocol

import (
	"fmt"

	"github.com/holiman/uint256"

	"github.com/erigontech/erigon/common/dbg"
	"github.com/erigontech/erigon/common/log/v3"
	"github.com/erigontech/erigon/common/math"
	"github.com/erigontech/erigon/common/u256"
	"github.com/erigontech/erigon/execution/chain"
	"github.com/erigontech/erigon/execution/protocol/params"
	"github.com/erigontech/erigon/execution/protocol/rules"
	"github.com/erigontech/erigon/execution/state"
	"github.com/erigontech/erigon/execution/tracing"
	"github.com/erigontech/erigon/execution/vm"
	"github.com/erigontech/erigon/execution/vm/evmtypes"
)

// A SLOT-EXCEEDED transaction is one its block's producer could not execute within the slot. It is included
// the way an out-of-gas transaction is — nonce consumed, the whole gas limit charged, no other state change,
// a failed receipt — and nothing ever executes it. How long execution takes differs between machines, so the
// producer's verdict travels in the header's extra-data and every node applies it instead of timing the
// transaction itself.

const slotExceededTag = 0x5e

// MaxSlotExceeded is how many verdicts one header carries: a tag byte then two bytes per body index, inside
// the 32-byte extra-data limit.
var MaxSlotExceeded = int(params.MaximumExtraDataSize-1) / 2

// EncodeSlotExceeded returns the extra-data naming these body indices, which must be ascending. Nil for none.
func EncodeSlotExceeded(indices []int) ([]byte, error) {
	if len(indices) == 0 {
		return nil, nil
	}
	if len(indices) > MaxSlotExceeded {
		return nil, fmt.Errorf("slot-exceeded: %d transactions, a header carries at most %d", len(indices), MaxSlotExceeded)
	}
	out := make([]byte, 1, 1+2*len(indices))
	out[0] = slotExceededTag
	prev := -1
	for _, i := range indices {
		if i <= prev || i > 0xffff {
			return nil, fmt.Errorf("slot-exceeded: index %d is out of order or range", i)
		}
		out = append(out, byte(i>>8), byte(i))
		prev = i
	}
	return out, nil
}

// SlotExceededIndices reads the verdicts out of a header's extra-data. Nil unless the chain enables them and
// the extra-data is exactly the tag followed by ascending two-byte indices.
func SlotExceededIndices(config *chain.Config, extra []byte) []uint16 {
	if config == nil || !config.SlotExceededTxs || len(extra) < 3 || extra[0] != slotExceededTag || (len(extra)-1)%2 != 0 {
		return nil
	}
	out := make([]uint16, 0, (len(extra)-1)/2)
	for k := 1; k < len(extra); k += 2 {
		i := uint16(extra[k])<<8 | uint16(extra[k+1])
		if len(out) > 0 && i <= out[len(out)-1] {
			return nil
		}
		out = append(out, i)
	}
	return out
}

// ApplyBlockMessage applies a message that is one of its block's own transactions: a slot-exceeded one gets
// the producer's verdict, any other goes through ApplyMessage. The body index is the one set on the state
// (SetTxContext). Only paths replaying a block's transactions call this — a call or simulation does not.
func ApplyBlockMessage(evm *vm.EVM, msg Message, gp *GasPool, refunds bool, gasBailout bool, engine rules.EngineReader) (*evmtypes.ExecutionResult, error) {
	if evm.Context.IsSlotExceeded(evm.IntraBlockState().TxIndex()) {
		return NewTxnExecutor(evm, msg, gp).executeSlotExceeded()
	}
	return ApplyMessage(evm, msg, gp, refunds, gasBailout, engine)
}

// ApplyBlockMessageNoFeeBurnOrTip is ApplyBlockMessage for callers that settle tips and burns themselves.
func ApplyBlockMessageNoFeeBurnOrTip(evm *vm.EVM, msg Message, gp *GasPool, refunds bool, gasBailout bool, engine rules.EngineReader) (*evmtypes.ExecutionResult, error) {
	if evm.Context.IsSlotExceeded(evm.IntraBlockState().TxIndex()) {
		st := NewTxnExecutor(evm, msg, gp)
		st.noFeeBurnAndTip = true
		return st.executeSlotExceeded()
	}
	return ApplyMessageNoFeeBurnOrTip(evm, msg, gp, refunds, gasBailout, engine)
}

// executeSlotExceeded is Execute for a transaction that is not executed: the same consensus checks and gas
// purchase, the nonce consumed, the whole gas limit used with nothing refunded, the tip and burn settled on
// that gas, and an out-of-gas result. Nothing else is touched — no value moves, no code runs, and no
// EIP-7702 authorization is applied.
func (st *TxnExecutor) executeSlotExceeded() (result *evmtypes.ExecutionResult, err error) {
	if st.evm.IntraBlockState().IsVersioned() {
		defer func() {
			if r := recover(); r != nil {
				if r != state.ErrDependency {
					log.Debug("Recovered from slot-exceeded transition failure.", "Error:", r, "stack", dbg.Stack())
				}
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

	msg := st.msg
	coinbase := st.evm.Context.Coinbase
	senderInitBalance, err := st.state.GetBalance(msg.From())
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

	intrinsicGasResult, overflow := st.calcIntrinsicGas(msg.To().IsNil(), msg.Authorizations(), msg.AccessList())
	if overflow {
		return nil, ErrGasUintOverflow
	}
	if err := st.preCheck(false, intrinsicGasResult); err != nil {
		return nil, err
	}
	// The nonce is consumed whatever the transaction is: an out-of-gas creation consumes its nonce too.
	nonce, err := st.state.GetNonce(msg.From())
	if err != nil {
		return nil, fmt.Errorf("%w: %w", ErrTxnExecutionFailed, err)
	}
	st.state.SetNonce(msg.From(), nonce+1, tracing.NonceChangeEoACall)

	intrinsicGas, overflow := math.SafeAdd(intrinsicGasResult.RegularGas, intrinsicGasResult.StateGas)
	if overflow {
		return nil, ErrGasUintOverflow
	}
	if msg.Gas() < intrinsicGas || msg.Gas() < intrinsicGasResult.FloorGasCost {
		return nil, fmt.Errorf("%w: have %d, want %d", ErrIntrinsicGas, msg.Gas(), intrinsicGas)
	}

	// The whole gas limit is used and none of it is refunded, all of it in the regular dimension.
	rules := st.evm.ChainRules()
	st.txnGasUsedB4Refunds = msg.Gas()
	st.txnGasUsed = msg.Gas()
	st.blockRegularGasUsed = msg.Gas()
	if err := st.gp.ConsumeRegular(st.blockRegularGasUsed); err != nil {
		return nil, fmt.Errorf("%w: %w", ErrTxnExecutionFailed, err)
	}

	effectiveTip := *st.gasPrice
	if rules.IsLondon {
		if st.feeCap.Gt(&st.evm.Context.BaseFee) {
			effectiveTip = u256.Min(*st.tipCap, u256.Sub(*st.feeCap, st.evm.Context.BaseFee))
		} else {
			effectiveTip = u256.Num0
		}
	}
	tipAmount := u256.Mul(u256.U64(st.txnGasUsed), effectiveTip)
	if !st.noFeeBurnAndTip {
		if err := st.state.AddBalance(coinbase, tipAmount, tracing.BalanceIncreaseRewardTransactionFee); err != nil {
			return nil, fmt.Errorf("%w: %w", ErrTxnExecutionFailed, err)
		}
	}

	var burnAmount uint256.Int
	burntContractAddress := st.evm.ChainConfig().GetBurntContract(st.evm.Context.BlockNumber)
	if !msg.IsFree() && rules.IsLondon && !burntContractAddress.IsNil() {
		burnAmount = u256.Mul(u256.U64(st.txnGasUsed), st.evm.Context.BaseFee)
		if rules.IsAura && rules.IsPrague {
			burnAmount = u256.Add(burnAmount, st.evm.BlobFee)
		}
		if !st.noFeeBurnAndTip {
			st.state.AddBalance(burntContractAddress, burnAmount, tracing.BalanceChangeUnspecified)
		}
	}

	result = &evmtypes.ExecutionResult{
		ReceiptGasUsed:      st.txnGasUsed,
		BlockRegularGasUsed: st.blockRegularGasUsed,
		BlockStateGasUsed:   st.blockStateGasUsed,
		MaxGasUsed:          st.txnGasUsedB4Refunds,
		IntrinsicGas:        intrinsicGasResult,
		Err:                 vm.ErrOutOfGas,
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
