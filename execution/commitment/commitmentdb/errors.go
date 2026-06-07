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

package commitmentdb

import (
	"errors"
	"fmt"
)

// ErrHistoryGap signals that mode-B commitment recompute cannot proceed
// because per-tx history records are missing in (FromTxNum, ToTxNum].
// SetHead's mode-B handler catches this, runs a historic re-execution
// of blocks (FromBlock, ToBlock] to populate the missing history, then
// retries the recompute.
//
// The gap surfaces when a snapshot step boundary cuts mid-block: the
// file's KeyCommitmentState record sits at FromTxNum (= step boundary
// lastTxNum), but the target block's lastTxNum is ToTxNum > FromTxNum.
// Forward exec was never run for those blocks locally (they came from
// preverified snapshots), and frozen history files end exactly at the
// step boundary — so no per-tx history exists for the in-block tail.
//
// Pure data type — caller decides retry policy.
type ErrHistoryGap struct {
	FromTxNum uint64
	ToTxNum   uint64
	FromBlock uint64
	ToBlock   uint64
}

func (e *ErrHistoryGap) Error() string {
	return fmt.Sprintf(
		"history gap for mode-B recompute: per-tx history records missing in (%d, %d] (blocks %d..%d); re-execution required to populate",
		e.FromTxNum, e.ToTxNum, e.FromBlock, e.ToBlock,
	)
}

// IsHistoryGap reports whether err wraps an ErrHistoryGap. Use this
// from mode-B retry sites; never compare error strings.
func IsHistoryGap(err error) (*ErrHistoryGap, bool) {
	var g *ErrHistoryGap
	if errors.As(err, &g) {
		return g, true
	}
	return nil, false
}
