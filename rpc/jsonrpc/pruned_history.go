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

package jsonrpc

import (
	"errors"

	"github.com/erigontech/erigon/execution/state"
)

// prunedHistoryError is a state.ErrPruned rejection reported with the
// execution-apis "Pruned history unavailable" code 4444. The original error
// stays in the message and in the chain, so errors.Is(err, state.ErrPruned)
// still holds.
type prunedHistoryError struct{ err error }

func (e *prunedHistoryError) Error() string  { return "Pruned history unavailable: " + e.err.Error() }
func (e *prunedHistoryError) ErrorCode() int { return 4444 }
func (e *prunedHistoryError) Unwrap() error  { return e.err }

// wrapPrunedHistory gives a state.ErrPruned error the 4444 code and returns
// any other error unchanged.
func wrapPrunedHistory(err error) error {
	if errors.Is(err, state.ErrPruned) {
		return &prunedHistoryError{err: err}
	}
	return err
}
