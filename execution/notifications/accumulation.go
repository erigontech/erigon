// Copyright 2024 The Erigon Authors
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

package notifications

// Accumulation groups an Accumulator and RecentReceipts so they can be
// associated together — e.g. per-SD for SD chaining, or per-fork for
// fork validation.
type Accumulation struct {
	Accumulator    *Accumulator
	RecentReceipts *RecentReceipts
}

func NewAccumulation() *Accumulation {
	return &Accumulation{
		Accumulator:    NewAccumulator(),
		RecentReceipts: NewRecentReceipts(512),
	}
}

// Reset clears both the accumulator and recent receipts.
func (a *Accumulation) Reset() {
	a.Accumulator.Reset()
	a.RecentReceipts.Clear()
}
