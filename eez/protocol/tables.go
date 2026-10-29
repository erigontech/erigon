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
	"fmt"

	"github.com/erigontech/erigon/common"
)

// Shape checks of eez-rollupX's system_tx.rs: the execution-table shapes
// both the verifier and the reconstruction admit.

// lowerEntry converts an L1-shaped entry to the EEZL2 table entry with the
// same calls and rows.
func lowerEntry(e *ExecutionEntry) L2ExecutionEntry {
	return L2ExecutionEntry{
		ProxyEntryHash:        e.ProxyEntryHash,
		IncomingCalls:         nonNil(e.L2ToL1Calls),
		ExpectedOutgoingCalls: nonNil(e.ExpectedL1ToL2Calls),
		RollingHash:           e.RollingHash,
		Success:               e.Success,
		ReturnData:            e.ReturnData,
	}
}

func nonNil[T any](s []T) []T {
	if s == nil {
		return []T{}
	}
	return s
}

// checkEntryShape rejects a failed entry with more than its root call or
// with rows, a successful row with a reverted rolling hash, a failed row
// without one or with calls, and call arrays checkCallArrayShape rejects.
func checkEntryShape(e *ExecutionEntry) error {
	if !e.Success && (len(e.L2ToL1Calls) > 1 || len(e.ExpectedL1ToL2Calls) > 0) {
		return failMaterialize("failed entry with descendants")
	}
	if err := checkCallArrayShape(e.L2ToL1Calls); err != nil {
		return err
	}
	for i := range e.ExpectedL1ToL2Calls {
		row := &e.ExpectedL1ToL2Calls[i]
		if err := checkCallArrayShape(row.Calls); err != nil {
			return fmt.Errorf("row %d: %w", i, err)
		}
		switch {
		case row.Success && row.RevertedOrStaticRollingHash != (common.Hash{}):
			return failMaterialize("successful row %d with a reverted rolling hash", i)
		case !row.Success && row.RevertedOrStaticRollingHash == (common.Hash{}):
			return failMaterialize("failed row %d without a reverted rolling hash", i)
		case !row.Success && len(row.Calls) > 0:
			return failMaterialize("failed row %d with calls", i)
		}
	}
	return nil
}

// checkCallArrayShape rejects call gas, static calls with value or a revert
// span, revert spans past the array and spans inside spans.
func checkCallArrayShape(calls []CrossChainCall) error {
	for i := 0; i < len(calls); {
		c := &calls[i]
		if c.Gas != 0 {
			return failMaterialize("call %d with call gas", i)
		}
		if c.IsStatic {
			if c.Value.Sign() != 0 || c.RevertNextNCalls != 0 {
				return failMaterialize("static call %d with value or a revert span", i)
			}
			i++
			continue
		}
		span := int(c.RevertNextNCalls)
		if span == 0 {
			i++
			continue
		}
		if i+span > len(calls) {
			return failMaterialize("call %d revert span %d exceeds %d calls", i, span, len(calls))
		}
		for j := i + 1; j < i+span; j++ {
			if calls[j].RevertNextNCalls != 0 {
				return failMaterialize("call %d starts a nested revert span", j)
			}
		}
		i += span
	}
	return nil
}

// checkL2StaticEntryShape checks a static read: a proxy hash, and callbacks
// that are static, carry no value, revert span or call gas, and fold to its
// rolling hash's zero when there are none.
func checkL2StaticEntryShape(e *L2StaticExecutionEntry) error {
	if e.ProxyEntryHash == (common.Hash{}) {
		return failMaterialize("static entry without a proxy hash")
	}
	if len(e.IncomingCalls) == 0 && e.RollingHash != (common.Hash{}) {
		return failMaterialize("static entry without callbacks has a rolling hash")
	}
	for i := range e.IncomingCalls {
		c := &e.IncomingCalls[i]
		if !c.IsStatic || c.Value.Sign() != 0 || c.RevertNextNCalls != 0 || c.Gas != 0 {
			return failMaterialize("static callback %d is not a plain static call", i)
		}
	}
	return nil
}

// OutboundRootCounts returns how many source cursor slots each outbound
// settlement host carries: one per top-level call, a revert window's calls
// retried in the slots after the window's first.
func OutboundRootCounts(hosts []ExecutionEntry) ([]int, error) {
	counts := make([]int, len(hosts))
	for i := range hosts {
		h := &hosts[i]
		if h.ProxyEntryHash != (common.Hash{}) {
			return nil, fmt.Errorf("%w: outbound host %d with a proxy hash", ErrBinding, i)
		}
		if err := checkCallArrayShape(h.L2ToL1Calls); err != nil {
			return nil, fmt.Errorf("%w: outbound host %d: %w", ErrBinding, i, err)
		}
		cursor, slots := 0, 0
		for j := 0; j < len(h.L2ToL1Calls); {
			span := int(h.L2ToL1Calls[j].RevertNextNCalls)
			n := max(span, 1)
			saved := cursor
			cursor += n
			slots = max(slots, cursor)
			j += n
			if span != 0 {
				cursor = saved
			}
		}
		if slots == 0 {
			return nil, fmt.Errorf("%w: outbound host %d without a root", ErrBinding, i)
		}
		counts[i] = slots
	}
	return counts, nil
}
