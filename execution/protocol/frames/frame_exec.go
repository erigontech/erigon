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

package frames

import (
	"errors"
	"fmt"

	"github.com/holiman/uint256"

	"github.com/erigontech/erigon/execution/protocol"
	"github.com/erigontech/erigon/execution/state"
	"github.com/erigontech/erigon/execution/tracing"
	"github.com/erigontech/erigon/execution/types"
	"github.com/erigontech/erigon/execution/types/accounts"
	"github.com/erigontech/erigon/execution/vm"
)

// ErrVerifyFrameNotApproved is returned when a VERIFY frame completes without
// executing the APPROVE opcode, as required by EIP-8141 §4.2 (draft).
var ErrVerifyFrameNotApproved = errors.New("EIP-8141: VERIFY frame did not execute APPROVE")

// ExecuteFrameTransaction executes an EIP-8141 frame transaction.
//
// Frames are executed in the order they appear in tx.Frames:
//
//   - VERIFY frames: run with IBS snapshot; must execute the APPROVE opcode.
//     State changes are rolled back on revert or if APPROVE is not seen.
//   - SENDER frames: run; the 20-byte return data becomes the effective sender.
//   - DEFAULT frames: run with msg.From set to the address returned by the
//     SENDER frame (ZeroAddress if none).
//
// NOTE: EIP-8141 is in draft status.  Gas accounting and frame semantics will
// be updated as the spec evolves.
func ExecuteFrameTransaction(
	tx *types.FrameTransaction,
	gasPool *protocol.GasPool,
	evm *vm.EVM,
	ibs *state.IntraBlockState,
) (gasUsed uint64, err error) {
	var senderAddr accounts.Address // set by the first SENDER frame

	for i, frame := range tx.Frames {
		switch frame.Kind {
		case types.FrameKindVerify:
			used, execErr := executeVerifyFrame(i, frame, tx, gasPool, evm, ibs)
			gasUsed += used
			if execErr != nil {
				return gasUsed, execErr
			}

		case types.FrameKindSender:
			addr, used, execErr := executeSenderFrame(i, frame, tx, gasPool, evm)
			gasUsed += used
			if execErr != nil {
				return gasUsed, execErr
			}
			senderAddr = addr

		case types.FrameKindDefault:
			used, execErr := executeDefaultFrame(i, frame, tx, senderAddr, gasPool, evm)
			gasUsed += used
			if execErr != nil {
				return gasUsed, execErr
			}

		default:
			return gasUsed, fmt.Errorf("EIP-8141: unknown frame kind %d at index %d", frame.Kind, i)
		}
	}

	return gasUsed, nil
}

// executeVerifyFrame runs a single VERIFY frame under an IBS snapshot.
//
// The frame must execute APPROVE; if it does not (or if it reverts), state
// changes are rolled back and an error is returned.
func executeVerifyFrame(
	idx int,
	frame types.TxFrame,
	tx *types.FrameTransaction,
	gasPool *protocol.GasPool,
	evm *vm.EVM,
	ibs *state.IntraBlockState,
) (gasUsed uint64, err error) {
	// Snapshot state so we can revert on failure.
	revid := ibs.PushSnapshot()

	// Compose existing tracer with VerifyFrameTracer.
	verifyTracer := &VerifyFrameTracer{}
	vmCfg := evm.Config()
	vmCfg.Tracer = tracing.NewComposite(vmCfg.Tracer, verifyTracer.Hooks())

	innerEVM := vm.NewEVM(evm.Context, evm.TxContext, ibs, evm.ChainConfig(), vmCfg)
	msg := frameMessage(frame, tx, accounts.ZeroAddress)

	result, execErr := protocol.ApplyFrame(innerEVM, msg, gasPool)
	if result != nil {
		gasUsed = result.ReceiptGasUsed
	}

	// Determine success: no hard error, no revert, and APPROVE was seen.
	approved := verifyTracer.Approved && (execErr == nil) && !result.Failed()

	if !approved {
		ibs.RevertToSnapshot(revid, nil)
		if execErr != nil {
			return gasUsed, fmt.Errorf("EIP-8141: VERIFY frame %d reverted: %w", idx, execErr)
		}
		if result.Failed() {
			return gasUsed, fmt.Errorf("EIP-8141: VERIFY frame %d reverted: %w", idx, result.Err)
		}
		return gasUsed, fmt.Errorf("%w (frame index %d)", ErrVerifyFrameNotApproved, idx)
	}

	// VERIFY succeeded; pop the snapshot (commit the state changes).
	ibs.PopSnapshot(revid)
	return gasUsed, nil
}

// executeSenderFrame runs a SENDER frame and extracts the 20-byte effective
// sender address from its return data.
func executeSenderFrame(
	idx int,
	frame types.TxFrame,
	tx *types.FrameTransaction,
	gasPool *protocol.GasPool,
	evm *vm.EVM,
) (sender accounts.Address, gasUsed uint64, err error) {
	msg := frameMessage(frame, tx, accounts.ZeroAddress)
	result, execErr := protocol.ApplyFrame(evm, msg, gasPool)
	if result != nil {
		gasUsed = result.ReceiptGasUsed
	}
	if execErr != nil {
		return accounts.ZeroAddress, gasUsed, fmt.Errorf("EIP-8141: SENDER frame %d failed: %w", idx, execErr)
	}
	if result.Failed() {
		return accounts.ZeroAddress, gasUsed, fmt.Errorf("EIP-8141: SENDER frame %d reverted: %w", idx, result.Err)
	}

	// The return data must be exactly 20 bytes (an Ethereum address).
	if len(result.ReturnData) != 20 {
		return accounts.ZeroAddress, gasUsed, fmt.Errorf(
			"EIP-8141: SENDER frame %d returned %d bytes, expected 20", idx, len(result.ReturnData),
		)
	}

	var rawAddr [20]byte
	copy(rawAddr[:], result.ReturnData)
	return accounts.InternAddress(rawAddr), gasUsed, nil
}

// executeDefaultFrame runs the DEFAULT frame with the given effective sender.
func executeDefaultFrame(
	idx int,
	frame types.TxFrame,
	tx *types.FrameTransaction,
	sender accounts.Address,
	gasPool *protocol.GasPool,
	evm *vm.EVM,
) (gasUsed uint64, err error) {
	msg := frameMessage(frame, tx, sender)
	result, execErr := protocol.ApplyFrame(evm, msg, gasPool)
	if result != nil {
		gasUsed = result.ReceiptGasUsed
	}
	if execErr != nil {
		return gasUsed, fmt.Errorf("EIP-8141: DEFAULT frame %d failed: %w", idx, execErr)
	}
	// DEFAULT frame failures (reverts) are non-fatal per EIP-8141 draft.
	return gasUsed, nil
}

// frameMessage builds a protocol.Message for a single frame within tx.
// All validation flags are disabled; fees are charged at the transaction level.
func frameMessage(frame types.TxFrame, tx *types.FrameTransaction, from accounts.Address) *types.Message {
	value := uint256.NewInt(0)
	if frame.Value != nil {
		value = frame.Value
	}
	to := accounts.InternAddress(frame.To)
	return types.NewMessage(
		from,
		to,
		0,     // nonce (not checked)
		value,
		frame.GasLimit,
		tx.FeeCap,  // gasPrice = feeCap (EIP-1559 style)
		tx.FeeCap,
		tx.Tip,
		frame.Data,
		nil,   // accessList (tx-level; not per-frame in draft)
		false, // checkNonce
		false, // checkTransaction
		false, // checkGas
		true,  // isFree (gas purchased at tx level)
		nil,   // maxFeePerBlobGas
	)
}
