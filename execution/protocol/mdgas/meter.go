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

package mdgas

// Meter owns a single call frame's two-dimensional gas: the regular pool, the
// state-gas reservoir, and the regular gas consumed when state charges spill out
// of an exhausted reservoir. It is the single home for charging, refilling, and
// reporting a frame's 2D gas usage, replacing the loose reservoir fields plus the
// exit-time re-derivation of usage.
type Meter struct {
	initial   MdGas  // budget the frame received; fixed for the frame's lifetime
	regular   uint64 // live regular pool
	reservoir uint64 // live state-gas reservoir
	spill     uint64 // regular gas consumed by state-gas spill (source-based refill bookkeeping)
}

// NewMeter starts a frame meter from its initial 2D gas budget.
func NewMeter(initial MdGas) Meter {
	return Meter{initial: initial, regular: initial.Regular, reservoir: initial.State}
}

// ChargeRegular draws amount from the regular pool, charging nothing and
// returning false when it can't be covered.
func (m *Meter) ChargeRegular(amount uint64) bool {
	if m.regular < amount {
		return false
	}
	m.regular -= amount
	return true
}

// ChargeState draws amount from the state reservoir, spilling the remainder into
// the regular pool once the reservoir is exhausted. Charges nothing and returns
// false when the two pools together can't cover it.
func (m *Meter) ChargeState(amount uint64) bool {
	if m.reservoir >= amount {
		m.reservoir -= amount
		return true
	}
	spill := amount - m.reservoir
	if m.regular < spill {
		return false
	}
	m.reservoir = 0
	m.regular -= spill
	m.spill += spill
	return true
}

// Refill credits amount back source-based (LIFO): the regular pool first, up to
// the amount previously spilled, then the reservoir.
func (m *Meter) Refill(amount uint64) {
	fromRegular := min(amount, m.spill)
	m.regular += fromRegular
	m.spill -= fromRegular
	m.reservoir += amount - fromRegular
}

// Burn zeroes the regular pool on an exceptional halt.
func (m *Meter) Burn() {
	m.regular = 0
}

// RefundRegular credits amount back to the regular pool (e.g. leftover call gas
// returned to the caller). The regular-dimension counterpart of Refill.
func (m *Meter) RefundRegular(amount uint64) {
	m.regular += amount
}

// Regular is the live regular pool.
func (m *Meter) Regular() uint64 {
	return m.regular
}

// Reservoir is the live state-gas reservoir.
func (m *Meter) Reservoir() uint64 {
	return m.reservoir
}

// Leftover is the unspent gas in both dimensions.
func (m *Meter) Leftover() MdGas {
	return MdGas{Regular: m.regular, State: m.reservoir}
}

// Spill is the regular gas currently consumed by state-gas spill.
func (m *Meter) Spill() uint64 {
	return m.spill
}

// Usage reports the frame's net gas consumption. State is signed: negative when
// refills exceed charges (the matching charge sits in an ancestor or in tx-level
// intrinsic gas). StateSpill is the regular gas consumed by spill. Computed in
// uint64 modular arithmetic on the regular dimension so a negative net state
// correctly grows the regular component.
func (m *Meter) Usage() MdGasUsage {
	state := (int64(m.initial.State) - int64(m.reservoir)) + int64(m.spill)
	return MdGasUsage{
		Regular:    (m.initial.Regular + m.initial.State) - (m.regular + m.reservoir) - uint64(state),
		State:      state,
		StateSpill: m.spill,
	}
}

// ForChild builds the gas to hand to a child frame: the given regular budget (the
// 63/64 forwarding amount) plus this frame's entire state reservoir.
func (m *Meter) ForChild(regular uint64) MdGas {
	return MdGas{Regular: regular, State: m.reservoir}
}

// AdoptChildReservoir reclaims a returned child frame's state gas: its leftover
// reservoir becomes this frame's reservoir (the child drew from the whole
// reservoir handed down by ForChild and returns the unused part), and its spill
// propagates so an ancestor revert refills from the right pool. The child's
// unspent regular gas is refunded separately by the caller so the refund keeps
// its tracer event.
func (m *Meter) AdoptChildReservoir(reservoir uint64, childSpill uint64) {
	m.reservoir = reservoir
	m.spill += childSpill
}
