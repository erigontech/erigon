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
	"bytes"
	"errors"
	"fmt"
	"slices"

	"github.com/erigontech/erigon/execution/abi"
)

// ErrSelector reports calldata of another function.
var ErrSelector = errors.New("unexpected selector")

// The decoders apply Solidity's acceptance rules: offsets may point anywhere
// in bounds and bytes after the arguments are ignored. The registry accepts
// any such encoding and anyone may resubmit a batch that does not bind its
// sender, so rejecting a non-canonical one would let a third party stall the
// follower. Aliased offsets can expand the result well beyond len(input);
// decode only calldata of executed transactions, which the gas spent hashing
// every entry bounds.

// DecodePostAndVerifyBatch decodes postAndVerifyBatch calldata.
func DecodePostAndVerifyBatch(input []byte) (*ProofSystemBatch, error) {
	var b ProofSystemBatch
	if err := decodeCall(postAndVerifyBatchMethod, input, &b); err != nil {
		return nil, err
	}
	if err := b.validate(); err != nil {
		return nil, fmt.Errorf("postAndVerifyBatch: %w", err)
	}
	return &b, nil
}

// EncodePostAndVerifyBatch returns the canonical postAndVerifyBatch calldata.
func EncodePostAndVerifyBatch(b *ProofSystemBatch) ([]byte, error) {
	if err := b.validate(); err != nil {
		return nil, fmt.Errorf("postAndVerifyBatch: %w", err)
	}
	return encodeCall(postAndVerifyBatchMethod, *b)
}

// DecodeLoadExecutionTable decodes loadExecutionTable calldata.
func DecodeLoadExecutionTable(input []byte) (*L2ExecutionTable, error) {
	return decodeTable(loadExecutionTableMethod, input)
}

// EncodeLoadExecutionTable returns the canonical loadExecutionTable calldata.
func EncodeLoadExecutionTable(t *L2ExecutionTable) ([]byte, error) {
	return encodeTable(loadExecutionTableMethod, t)
}

// DecodeExecuteIncomingCrossChainCall decodes executeIncomingCrossChainCall
// calldata.
func DecodeExecuteIncomingCrossChainCall(input []byte) (*L2ExecutionTable, error) {
	return decodeTable(executeIncomingCrossChainCallMethod, input)
}

// EncodeExecuteIncomingCrossChainCall returns the canonical
// executeIncomingCrossChainCall calldata.
func EncodeExecuteIncomingCrossChainCall(t *L2ExecutionTable) ([]byte, error) {
	return encodeTable(executeIncomingCrossChainCallMethod, t)
}

func decodeTable(m abi.Method, input []byte) (*L2ExecutionTable, error) {
	var t L2ExecutionTable
	if err := decodeCall(m, input, &t); err != nil {
		return nil, err
	}
	if err := t.validate(); err != nil {
		return nil, fmt.Errorf("%s: %w", m.Name, err)
	}
	return &t, nil
}

func encodeTable(m abi.Method, t *L2ExecutionTable) ([]byte, error) {
	if err := t.validate(); err != nil {
		return nil, fmt.Errorf("%s: %w", m.Name, err)
	}
	return encodeCall(m, t.Entries, t.StaticEntries)
}

func decodeCall(m abi.Method, input []byte, out any) error {
	if len(input) < 4 || !bytes.Equal(input[:4], m.ID) {
		return fmt.Errorf("%s: %w", m.Name, ErrSelector)
	}
	// Decoded byte strings alias the buffer they are decoded from.
	values, err := m.Inputs.Unpack(bytes.Clone(input[4:]))
	if err != nil {
		return fmt.Errorf("%s: %w", m.Name, err)
	}
	if err := m.Inputs.Copy(out, values); err != nil {
		return fmt.Errorf("%s: %w", m.Name, err)
	}
	return nil
}

func encodeCall(m abi.Method, args ...any) ([]byte, error) {
	enc, err := m.Inputs.Pack(args...)
	if err != nil {
		return nil, fmt.Errorf("%s: %w", m.Name, err)
	}
	return append(slices.Clone(m.ID), enc...), nil
}
