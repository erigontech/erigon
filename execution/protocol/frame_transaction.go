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
	if tx.UsesLegacyNonce() && tx.Nonce == math.MaxUint64 {
		return nil, ErrNonceMax
	}
	if !tx.UsesLegacyNonce() {
		// EIP-8250 requires the manager to be predeployed. Otherwise nonce writes
		// create an empty account that finalization deletes, allowing replay.
		exists, err := st.state.Exist(accounts.InternAddress(types.FrameNonceManager))
		if err != nil {
			return nil, err
		}
		if !exists {
			return nil, errors.New("nonce manager does not exist")
		}
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
			if tx.Nonce == math.MaxUint64 {
				if !sequence.IsZero() {
					return nil, fmt.Errorf("%w: single-use key %s is already used", ErrNonceTooLow, key.String())
				}
				continue
			}
			if sequence.LtUint64(tx.Nonce) {
				return nil, fmt.Errorf("%w: key %s, tx: %d state: %s", ErrNonceTooHigh, key.String(), tx.Nonce, sequence.String())
			}
			if sequence.GtUint64(tx.Nonce) {
				return nil, fmt.Errorf("%w: key %s, tx: %d state: %s", ErrNonceTooLow, key.String(), tx.Nonce, sequence.String())
			}
		}
	}
	baseFee, blobBaseFee := st.evm.Context.BaseFee, st.evm.Context.BlobBaseFee
	if st.isFree || st.evm.Config().NoBaseFee && tx.Fees.MaxFee.IsZero() && tx.Fees.TipCap.IsZero() {
		baseFee.Clear()
	}
	if st.evm.Config().NoBaseFee && tx.Fees.MaxFee.IsZero() {
		blobBaseFee.Clear()
	}
	budget, err := tx.Fees.Budget(limits.Total(), tx.GetBlobGas(), &baseFee, &blobBaseFee)
	if err != nil {
		return nil, err
	}
	maxCost, blobCost := budget.MaxCost, budget.BlobCost
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
		// A failure within an accepted transaction uses a frame, atomic batch or
		// ASSERT body snapshot, preserving changes before that boundary.
		if err != nil || validation != nil {
			st.state.RevertToSnapshot(snapshot, err)
		}
	}()
	st.evm.BlobHashes = tx.BlobVersionedHashes
	st.evm.GasPrice = budget.GasPrice
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
	bodyStart, bodySnapshot := -1, -1
	defer func() {
		if bodySnapshot >= 0 {
			st.state.PopSnapshot(bodySnapshot)
		}
	}()
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
			// A failed VERIFY rejects the transaction. Other failures affect the
			// receipts and the rollback boundary without invalidating the transaction.
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
			if i == validation.paymentFrame {
				return nil, nil
			}
		}
		if frame.Mode == types.FrameAssert && frameErr != nil {
			if bodySnapshot < 0 {
				return nil, errors.New("frame transaction without payment approval")
			}
			// Assertions preserve the complete payment-approval frame, including
			// its state changes after APPROVE returns from a nested call.
			st.state.RevertToSnapshot(bodySnapshot, frameErr)
			bodySnapshot = -1
			for j := bodyStart; j <= i; j++ {
				ctx.Receipts[j].Logs = nil
			}
			for j := i + 1; j < len(tx.Frames); j++ {
				ctx.Receipts[j].Status = types.ReceiptStatusSkipped
				st.evm.TraceSkippedFrame(j)
			}
			break
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
		if bodySnapshot < 0 && !ctx.Payer.IsNil() && tx.Frames[len(tx.Frames)-1].Mode == types.FrameAssert {
			bodyStart, bodySnapshot = i+1, st.state.PushSnapshot()
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
	basePaid := u256.Add(u256.Mul(u256.U64(paid), baseFee), blobCost)
	available := u256.Sub(maxCost, basePaid)
	tipPaid := tx.Fees.PriorityCost(paid, &available)
	change := u256.Sub(available, tipPaid)
	if !gasBailout {
		if err := st.state.AddBalance(ctx.Payer, change, tracing.BalanceIncreaseGasReturn); err != nil {
			return nil, err
		}
	}
	tipped, err := st.payCoinbaseAmount(tipPaid)
	if err != nil {
		return nil, err
	}
	burned, burnAddress, err := st.burnFees(paid, blobCost)
	if err != nil {
		return nil, err
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
