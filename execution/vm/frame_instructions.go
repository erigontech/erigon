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

package vm

import (
	"errors"
	"math"

	"github.com/erigontech/erigon/execution/types"
)

var ErrFrameParameter = errors.New("invalid frame transaction parameter")

func opTxParam(pc uint64, evm *EVM, call *CallContext) (uint64, []byte, error) {
	ctx := evm.FrameContext
	if ctx == nil {
		return pc, nil, ErrFrameContext
	}
	param := call.Stack.peek()
	if !param.IsUint64() {
		return pc, nil, ErrFrameParameter
	}
	tx := ctx.Transaction
	switch param.Uint64() {
	case 0:
		param.SetUint64(types.FrameTxType)
	case 1:
		param.SetUint64(tx.Nonce)
	case 2:
		param.SetBytes(tx.SenderAddress[:])
	case 3:
		param.Set(&tx.Fees.TipCap)
	case 4:
		param.Set(&tx.Fees.FeeCap)
	case 5:
		param.Set(&tx.Fees.BlobFeeCap)
	case 6:
		param.Set(&ctx.MaxCost)
	case 7:
		param.SetUint64(uint64(len(tx.BlobVersionedHashes)))
	case 8:
		hash := ctx.SigningHash()
		param.SetBytes(hash[:])
	case 9:
		param.SetUint64(uint64(len(tx.Frames)))
	case 10:
		param.SetUint64(uint64(ctx.Index))
	case 11:
		param.SetUint64(uint64(len(tx.Signatures)))
	case 12:
		param.SetUint64(ctx.StateGasLeft())
	case 13:
		param.SetUint64(ctx.LegacyNonce)
	case 14:
		param.SetUint64(uint64(len(tx.NonceKeys)))
	case 15:
		hash := ctx.NonceKeysHash()
		param.SetBytes(hash[:])
	case 16:
		param.Set(&tx.NonceKeys[0])
	default:
		return pc, nil, ErrFrameParameter
	}
	return pc, nil, nil
}

func opFrameParam(pc uint64, evm *EVM, call *CallContext) (uint64, []byte, error) {
	ctx := evm.FrameContext
	if ctx == nil {
		return pc, nil, ErrFrameContext
	}
	index, param := call.Stack.pop1Peek1()
	if !index.IsUint64() || index.Uint64() >= uint64(len(ctx.Transaction.Frames)) || !param.IsUint64() {
		return pc, nil, ErrFrameParameter
	}
	i, p := index.Uint64(), param.Uint64()
	frame := &ctx.Transaction.Frames[i]
	// Outcome selectors are only available for earlier frames. The current and
	// future frames do not yet have a completed execution result.
	if (p == 5 || p == 10 || p == 11) && i >= uint64(ctx.Index) {
		return pc, nil, ErrFrameParameter
	}
	switch p {
	case 0:
		target := frame.ResolvedTarget(ctx.Transaction.SenderAddress)
		param.SetBytes(target[:])
	case 1:
		param.SetUint64(frame.Limits.Execution)
	case 2:
		param.SetUint64(uint64(frame.Mode))
	case 3:
		param.SetUint64(uint64(frame.Flags))
	case 4:
		param.SetUint64(uint64(len(frame.Data)))
	case 5:
		param.SetUint64(ctx.Receipts[i].Status)
	case 6:
		param.SetUint64(uint64(frame.Flags & types.FrameApproveBoth))
	case 7:
		param.SetUint64(uint64(frame.Flags&types.FrameAtomicBatch) >> 2)
	case 8:
		param.Set(&frame.Value)
	case 9:
		param.SetUint64(frame.Limits.State)
	case 10:
		param.SetUint64(ctx.Receipts[i].GasUsed.Execution)
	case 11:
		param.SetUint64(ctx.Receipts[i].GasUsed.State)
	default:
		return pc, nil, ErrFrameParameter
	}
	return pc, nil, nil
}

func opSigParam(pc uint64, evm *EVM, call *CallContext) (uint64, []byte, error) {
	ctx := evm.FrameContext
	if ctx == nil {
		return pc, nil, ErrFrameContext
	}
	index, param := call.Stack.pop1Peek1()
	if !index.IsUint64() || index.Uint64() >= uint64(len(ctx.Transaction.Signatures)) || !param.IsUint64() {
		return pc, nil, ErrFrameParameter
	}
	sig := &ctx.Transaction.Signatures[index.Uint64()]
	switch param.Uint64() {
	case 0:
		if sig.Scheme == types.FrameSignatureArbitrary {
			return pc, nil, ErrFrameParameter
		}
		signer := sig.ResolvedSigner(ctx.Transaction.SenderAddress)
		param.SetBytes(signer[:])
	case 1:
		param.SetUint64(uint64(sig.Scheme))
	case 2:
		param.SetBytes(sig.Message)
	case 3:
		if sig.Scheme != types.FrameSignatureArbitrary {
			return pc, nil, ErrFrameParameter
		}
		param.SetUint64(uint64(len(sig.Signature)))
	default:
		return pc, nil, ErrFrameParameter
	}
	return pc, nil, nil
}

func opFrameDataLoad(pc uint64, evm *EVM, call *CallContext) (uint64, []byte, error) {
	ctx := evm.FrameContext
	if ctx == nil {
		return pc, nil, ErrFrameContext
	}
	offset, index := call.Stack.pop1Peek1()
	if !index.IsUint64() || index.Uint64() >= uint64(len(ctx.Transaction.Frames)) {
		return pc, nil, ErrFrameParameter
	}
	if off, overflow := offset.Uint64WithOverflow(); overflow {
		index.Clear()
	} else {
		index.SetBytes(getData(ctx.Transaction.Frames[index.Uint64()].Data, off, 32))
	}
	return pc, nil, nil
}

func opFrameDataCopy(pc uint64, evm *EVM, call *CallContext) (uint64, []byte, error) {
	ctx := evm.FrameContext
	if ctx == nil {
		return pc, nil, ErrFrameContext
	}
	index := call.Stack.back(3)
	if !index.IsUint64() || index.Uint64() >= uint64(len(ctx.Transaction.Frames)) {
		return pc, nil, ErrFrameParameter
	}
	copyFrameData(call, ctx.Transaction.Frames[index.Uint64()].Data)
	return pc, nil, nil
}

func opSigDataCopy(pc uint64, evm *EVM, call *CallContext) (uint64, []byte, error) {
	ctx := evm.FrameContext
	if ctx == nil {
		return pc, nil, ErrFrameContext
	}
	index := call.Stack.back(3)
	if !index.IsUint64() || index.Uint64() >= uint64(len(ctx.Transaction.Signatures)) {
		return pc, nil, ErrFrameParameter
	}
	sig := &ctx.Transaction.Signatures[index.Uint64()]
	if sig.Scheme != types.FrameSignatureArbitrary {
		return pc, nil, ErrFrameParameter
	}
	copyFrameData(call, sig.Signature)
	return pc, nil, nil
}

func copyFrameData(call *CallContext, data []byte) {
	memOffset, dataOffset, length := call.Stack.pop3()
	call.Stack.drop()
	offset, overflow := dataOffset.Uint64WithOverflow()
	if overflow {
		offset = math.MaxUint64
	}
	call.Memory.SetFromData(memOffset.Uint64(), length.Uint64(), offset, data)
}
