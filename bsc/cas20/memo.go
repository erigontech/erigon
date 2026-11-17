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

package cas20

import (
	"github.com/erigontech/erigon/common"
	"github.com/erigontech/erigon/execution/vm"
)

// A memo is a 32-byte hash, which says nothing about how the payload behind it
// is encoded. These two entry points let a transfer declare the format its
// payload follows — one bytes32 naming a format definition maintained off chain.
// The chain records the declaration and nothing more: it never sees the payload,
// so it never verifies the claim, and it keeps no registry of formats. A transfer
// through the plain memo methods has declared nothing.

var (
	selTransferWithMemoFormat     = selector("transferWithMemoFormat(address,uint256,bytes32,bytes32)")
	selTransferFromWithMemoFormat = selector("transferFromWithMemoFormat(address,address,uint256,bytes32,bytes32)")

	// Emitted right after Memo, so an indexer ties the declaration to its transfer
	// by log position rather than by a memo hash two transfers may share.
	cas20TopicMemoFormatDeclared = eventTopic("MemoFormatDeclared(address,bytes32,bytes32)")
)

func (t cas20Token) dispatchMemoFormat(sel [4]byte, args []byte) (ret []byte, err error, ok bool) {
	switch sel {
	case selTransferWithMemoFormat:
		to, amount, memo, err := readToAmountMemo(args)
		if err != nil {
			return nil, err, true
		}
		format, err := readWord(args, 3)
		if err != nil {
			return nil, err, true
		}
		ret, err := t.transferWithMemoFormat(format, memo, func() ([]byte, error) { return t.transfer(t.ctx.Caller, to, amount) })
		return ret, err, true
	case selTransferFromWithMemoFormat:
		from, err := readAddress(args, 0)
		if err != nil {
			return nil, err, true
		}
		to, err := readAddress(args, 1)
		if err != nil {
			return nil, err, true
		}
		amount, err := readU256(args, 2)
		if err != nil {
			return nil, err, true
		}
		memo, err := readWord(args, 3)
		if err != nil {
			return nil, err, true
		}
		format, err := readWord(args, 4)
		if err != nil {
			return nil, err, true
		}
		// Always transferFrom, so a holder moving its own balance spends its
		// self-approval exactly as through transferFromWithMemo.
		ret, err := t.transferWithMemoFormat(format, memo, func() ([]byte, error) { return t.transferFrom(t.ctx.Caller, from, to, amount) })
		return ret, err, true
	}
	return nil, nil, false
}

// transferWithMemoFormat runs the entry point's own transfer, then the memo and
// the declaration. A zero format is refused: the plain memo methods are how a
// transfer declares nothing.
func (t cas20Token) transferWithMemoFormat(format, memo common.Hash, move func() ([]byte, error)) ([]byte, error) {
	if t.ctx.ReadOnly {
		return nil, vm.ErrWriteProtection
	}
	if format == (common.Hash{}) {
		return nil, revCAS20("InvalidFormatId()", errSelInvalidFormatId)
	}
	ret, err := move()
	if err != nil {
		return nil, err
	}
	if !t.emitMemo(memo) {
		return nil, vm.ErrOutOfGas
	}
	if !t.ctx.AddLog([]common.Hash{cas20TopicMemoFormatDeclared, addrKey(t.ctx.Caller), memo, format}, nil) {
		return nil, vm.ErrOutOfGas
	}
	return ret, nil
}
