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
	"errors"
	"fmt"
	"math"

	"github.com/holiman/uint256"

	"github.com/erigontech/erigon/common/u256"
	"github.com/erigontech/erigon/execution/protocol/mdgas"
	"github.com/erigontech/erigon/execution/protocol/params"
	"github.com/erigontech/erigon/execution/tracing"
	"github.com/erigontech/erigon/execution/types"
	"github.com/erigontech/erigon/execution/types/accounts"
	"github.com/erigontech/erigon/execution/vm"
	"github.com/erigontech/erigon/execution/vm/evmtypes"
)

// executeFrameTransaction owns transaction validity, frame sequencing, rollback
// boundaries and fee settlement; the EVM executes each individual frame.
// A non-nil validation tracer limits execution to the pool's validation prefix
// and causes all simulated changes to be reverted.
func (st *TxnExecutor) executeFrameTransaction(tx *types.FrameTransaction, refunds, gasBailout bool, validation *frameValidationTracer) (result *evmtypes.ExecutionResult, err error) {
	rules := st.evm.ChainRules()
	if !rules.IsBogota {
		return nil, types.ErrTxTypeNotSupported
	}
	intrinsic, floor, limits, err := tx.GasLimits()
	if err != nil {
		return nil, err
	}
	if err := CheckBlockGasInclusion(st.gp, limits.Execution, limits.State, tx.GetBlobGas()); err != nil {
		return nil, err
	}
	sender := accounts.InternAddress(tx.SenderAddress)
	nonce, err := st.state.GetNonce(sender)
	if err != nil {
		return nil, err
	}
	if tx.Nonce == math.MaxUint64 {
		return nil, ErrNonceMax
	}
	if st.msg.CheckNonce() && tx.UsesLegacyNonce() {
		if nonce < tx.Nonce {
			return nil, &nonceError{ErrNonceTooHigh, sender, tx.Nonce, nonce}
		}
		if nonce > tx.Nonce {
			return nil, &nonceError{ErrNonceTooLow, sender, tx.Nonce, nonce}
		}
	} else if st.msg.CheckNonce() {
		for _, key := range tx.NonceKeys {
			sequence, err := st.state.GetState(accounts.InternAddress(types.FrameNonceManager), accounts.InternKey(types.FrameNonceSlot(tx.SenderAddress, key)))
			if err != nil {
				return nil, err
			}
			if sequence.LtUint64(tx.Nonce) {
				return nil, fmt.Errorf("%w: key %s, tx: %d state: %s", ErrNonceTooHigh, key.String(), tx.Nonce, sequence.String())
			}
			if sequence.GtUint64(tx.Nonce) {
				return nil, fmt.Errorf("%w: key %s, tx: %d state: %s", ErrNonceTooLow, key.String(), tx.Nonce, sequence.String())
			}
		}
	}
	skipFeeCheck := st.evm.Config().NoBaseFee && st.feeCap.IsZero() && st.tipCap.IsZero()
	if !skipFeeCheck {
		if err := CheckEip1559TxGasFeeCap(sender, st.feeCap, st.tipCap, &st.evm.Context.BaseFee, st.msg.IsFree()); err != nil {
			return nil, err
		}
	}
	skipBlobCheck := st.evm.Config().NoBaseFee && tx.Fees.BlobFeeCap.IsZero()
	if tx.GetBlobGas() > 0 && !skipBlobCheck && tx.Fees.BlobFeeCap.Lt(&st.evm.Context.BlobBaseFee) {
		return nil, ErrMaxFeePerBlobGas
	}
	// Execution and state gas are reserved at the fee cap; blob gas uses the
	// current blob base fee. Approval debits this budget, and settlement returns
	// unused gas and the difference from the effective execution gas price.
	maxCost, overflow := u256.MulOverflow(u256.U64(limits.Total()), tx.Fees.FeeCap)
	if overflow {
		return nil, errors.New("frame transaction gas cost overflow")
	}
	blobCost, overflow := u256.MulOverflow(u256.U64(tx.GetBlobGas()), st.evm.Context.BlobBaseFee)
	if overflow {
		return nil, ErrInsufficientFunds
	}
	maxCost, overflow = u256.AddOverflow(maxCost, blobCost)
	if overflow {
		return nil, ErrInsufficientFunds
	}
	st.state.Prepare(rules, sender, st.evm.Context.Coinbase, accounts.NilAddress, vm.ActivePrecompiles(rules), nil)
	if st.evm.Config().NoBAL {
		st.state.StopAccessRecording()
	}
	snapshot := st.state.PushSnapshot()
	defer st.state.PopSnapshot(snapshot)
	ctx := st.state.NewFrameContext(tx, maxCost)
	ctx.LegacyNonce = nonce
	ctx.GasBailout = gasBailout
	st.evm.FrameContext = ctx
	defer func() {
		st.evm.FrameContext = nil
		// Transaction rejection and mempool simulation undo every frame's changes.
		// Ordinary execution failures use the frame or batch snapshot instead,
		// preserving approvals and nonce consumption from earlier successful frames.
		if err != nil || validation != nil {
			st.state.RevertToSnapshot(snapshot, err)
		}
	}()
	st.evm.BlobHashes = tx.BlobVersionedHashes
	st.evm.GasPrice = *st.msg.GasPrice()
	// Tracers need one transaction root with frames as ordered children. This
	// synthetic call emits hooks only; it does not execute code or add EVM depth.
	if tracer := st.evm.Config().Tracer; tracer.HasEnterHook() || tracer.HasExitHook() {
		tracer.EmitEnter(0, byte(vm.CALL), sender, vm.FrameEntryPoint, false, nil, limits, uint256.Int{}, nil)
		defer func() {
			traceErr := err
			var gasUsed mdgas.MdGasUsage
			if result != nil {
				traceErr = result.Err
				gasUsed = mdgas.MdGasUsage{Execution: result.ReceiptGasUsed - result.BlockStateGasUsed, State: int64(result.BlockStateGasUsed)}
			}
			tracer.EmitExit(0, nil, gasUsed, vm.VMErrorFromErr(traceErr), err != nil)
		}()
	}
	var lastErr error
	batchStart, batchSnapshot := -1, -1
	for i := 0; i < len(tx.Frames); i++ {
		frame := &tx.Frames[i]
		ctx.Index = i
		if validation != nil {
			if err := validation.beforeFrame(); err != nil {
				return nil, err
			}
		}
		if frame.Mode == types.FrameSender && !ctx.SenderApproved {
			return nil, errors.New("sender frame without execution approval")
		}
		if batchStart < 0 && frame.Flags&types.FrameAtomicBatch != 0 {
			batchStart, batchSnapshot = i, st.state.PushSnapshot()
		}
		st.state.ResetFrameTransientStorage()
		frameSnapshot := st.state.PushSnapshot()
		logStart := st.state.LogCount()
		_, left, frameErr := st.evm.ExecuteTransactionFrame()
		if frameErr != nil {
			st.state.RevertToSnapshot(frameSnapshot, frameErr)
		}
		st.state.PopSnapshot(frameSnapshot)
		if readErr := st.state.StateReadError(); readErr != nil {
			return nil, readErr
		}
		if errors.Is(frameErr, vm.ErrIntraBlockStateFailed) {
			return nil, frameErr
		}
		if validation != nil && validation.err != nil {
			return nil, validation.err
		}
		if st.evm.Cancelled() {
			return nil, errors.New("frame execution cancelled")
		}
		ctx.Receipts[i].GasUsed.Execution = frame.Limits.Execution - left
		if frameErr == nil {
			ctx.Receipts[i].Status = types.ReceiptStatusSuccessful
			ctx.Receipts[i].Logs = st.state.LogsSince(logStart)
		} else {
			lastErr = frameErr
			// A failed VERIFY rejects the transaction. Other frame failures remain
			// receipt outcomes and allow later frames to run, subject to atomic batches.
			if frame.Mode == types.FrameVerify {
				return nil, fmt.Errorf("verification frame %d failed: %w", i, frameErr)
			}
		}
		if validation != nil {
			if frameErr != nil {
				return nil, frameErr
			}
			if err := validation.afterFrame(); err != nil {
				return nil, err
			}
			if i == validation.end {
				return nil, nil
			}
		}
		if batchStart >= 0 {
			if frameErr != nil {
				// The journal restores state-gas charges, including refills to earlier
				// frames. Executed frames keep their status and execution-gas usage.
				st.state.RevertToSnapshot(batchSnapshot, frameErr)
				for j := batchStart; j <= i; j++ {
					ctx.Receipts[j].Logs = nil
				}
				// The first frame without the flag is still part of this batch.
				for tx.Frames[i].Flags&types.FrameAtomicBatch != 0 {
					i++
					ctx.Receipts[i].Status = types.ReceiptStatusSkipped
					st.evm.TraceSkippedFrame(i)
				}
			}
			if frameErr != nil || frame.Flags&types.FrameAtomicBatch == 0 {
				st.state.PopSnapshot(batchSnapshot)
				batchStart, batchSnapshot = -1, -1
			}
		}
	}
	if ctx.Payer.IsNil() {
		return nil, errors.New("frame transaction without payment approval")
	}
	var used mdgas.MdGas
	used.Execution = intrinsic
	for _, receipt := range ctx.Receipts {
		used.Execution += receipt.GasUsed.Execution
		used.State += receipt.GasUsed.State
	}
	var refund uint64
	if refunds && !gasBailout {
		refund = min(st.state.GetRefund(), used.Total()/params.RefundQuotientEIP3529)
	}
	// Refunds reduce the payer's fee, but do not free block execution capacity.
	// The calldata floor applies to execution gas; state gas is paid in addition.
	blockExecution := max(used.Execution, floor)
	paidExecution := floor
	if used.Execution > refund {
		paidExecution = max(used.Execution-refund, floor)
	}
	paid := paidExecution + used.State
	fee := u256.Mul(u256.U64(paid), *st.gasPrice)
	change := u256.Sub(maxCost, u256.Add(fee, blobCost))
	if !gasBailout {
		if err := st.state.AddBalance(ctx.Payer, change, tracing.BalanceIncreaseGasReturn); err != nil {
			return nil, err
		}
	}
	tip := tx.GetEffectiveGasTip(&st.evm.Context.BaseFee)
	tipped := u256.Mul(u256.U64(paid), tip)
	if !st.noFeeBurnAndTip {
		if err := st.state.AddBalance(st.evm.Context.Coinbase, tipped, tracing.BalanceIncreaseRewardTransactionFee); err != nil {
			return nil, err
		}
	}
	var burned uint256.Int
	var burnAddress accounts.Address
	if !st.msg.IsFree() {
		burnAddress = st.evm.ChainConfig().GetBurntContract(st.evm.Context.BlockNumber)
	}
	if !burnAddress.IsNil() {
		burned = u256.Mul(u256.U64(paid), st.evm.Context.BaseFee)
		if rules.IsAura && rules.IsPrague {
			burned = u256.Add(burned, blobCost)
		}
		if !st.noFeeBurnAndTip {
			if err := st.state.AddBalance(burnAddress, burned, tracing.BalanceChangeUnspecified); err != nil {
				return nil, err
			}
		}
	}
	if err := st.gp.ConsumeExecution(blockExecution); err != nil {
		return nil, err
	}
	if err := st.gp.ConsumeState(used.State); err != nil {
		return nil, err
	}
	if err := st.gp.SubBlobGas(tx.GetBlobGas()); err != nil {
		return nil, err
	}
	payer := ctx.Payer.Value()
	result = &evmtypes.ExecutionResult{
		TxnGasUsage:    mdgas.TxnGasUsage{BlockExecutionGasUsed: blockExecution, BlockStateGasUsed: used.State, GasRefund: refund},
		ReceiptGasUsed: paid, MaxGasUsed: blockExecution + used.State,
		Payer: &payer, FrameReceipts: ctx.Receipts, Err: lastErr,
		FeeTipped: tipped, FeeBurnt: burned, BurntContractAddress: burnAddress,
	}
	if st.evm.Context.PostApplyMessage != nil {
		st.evm.Context.PostApplyMessage(st.state, sender, st.evm.Context.Coinbase, result, rules)
	}
	return result, nil
}
