package protocol

import (
	"fmt"

	"github.com/erigontech/erigon/common/dbg"
	"github.com/erigontech/erigon/execution/chain"
	"github.com/erigontech/erigon/execution/protocol/mdgas"
	"github.com/erigontech/erigon/execution/protocol/params"
	"github.com/erigontech/erigon/execution/protocol/rules"
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
// that gas, and an out-of-gas result. No value moves, no code runs, and no EIP-7702 authorization is applied.
func (st *TxnExecutor) executeSlotExceeded() (result *evmtypes.ExecutionResult, err error) {
	if st.evm.IntraBlockState().IsVersioned() {
		defer func() {
			if r := recover(); r != nil {
				err = fmt.Errorf("%w: %v at: %s", ErrExecPanic, r, dbg.Stack())
			}
		}()
	}

	msg := st.msg
	rules := st.evm.ChainRules()
	vmConfig := st.evm.Config()
	intrinsicGasResult, overflow := intrinsicGas(msg, rules, vmConfig.HasEip3860(rules))
	if overflow {
		return nil, ErrGasUintOverflow
	}
	fees, err := st.preCheck(false, intrinsicGasResult)
	if err != nil {
		return nil, err
	}
	if err := st.buyGas(fees, false); err != nil {
		return nil, err
	}
	nonce, err := st.state.GetNonce(msg.From())
	if err != nil {
		return nil, fmt.Errorf("%w: %w", ErrTxnExecutionFailed, err)
	}
	if err := st.state.SetNonce(msg.From(), nonce+1, tracing.NonceChangeEoACall); err != nil {
		return nil, err
	}

	st.txnGasUsedB4Refunds = msg.Gas()
	st.txnGasUsed = msg.Gas()
	st.blockExecutionGasUsed = msg.Gas()
	if err := st.gp.ConsumeExecution(st.blockExecutionGasUsed); err != nil {
		return nil, fmt.Errorf("%w: %w", ErrTxnExecutionFailed, err)
	}

	tipAmount, err := st.payCoinbase(st.txnGasUsed)
	if err != nil {
		return nil, fmt.Errorf("%w: %w", ErrTxnExecutionFailed, err)
	}
	burnAmount, burntContractAddress, err := st.burnFees(st.txnGasUsed, fees.blobGasVal)
	if err != nil {
		return nil, err
	}

	result = &evmtypes.ExecutionResult{
		TxnGasUsage:    mdgas.TxnGasUsage{BlockExecutionGasUsed: st.blockExecutionGasUsed},
		ReceiptGasUsed: st.txnGasUsed,
		MaxGasUsed:     st.txnGasUsedB4Refunds,
		Err:            vm.ErrOutOfGas,
		FeeTipped:      tipAmount,
		FeeBurnt:       burnAmount,
	}
	result.BurntContractAddress = burntContractAddress
	if st.evm.Context.PostApplyMessage != nil {
		st.evm.Context.PostApplyMessage(st.state, msg.From(), st.evm.Context.Coinbase, result, rules)
	}
	return result, nil
}
