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

import "testing"

func TestMeterChargeRegular(t *testing.T) {
	m := NewMeter(MdGas{Regular: 100, State: 50})
	if !m.ChargeRegular(30) || m.regular != 70 {
		t.Fatalf("charge: regular=%d", m.regular)
	}
	if m.ChargeRegular(1000) {
		t.Fatal("over-charge must fail")
	}
	if m.regular != 70 {
		t.Fatalf("failed charge must not mutate: regular=%d", m.regular)
	}
}

func TestMeterChargeStateWithinReservoir(t *testing.T) {
	m := NewMeter(MdGas{Regular: 100, State: 50})
	if !m.ChargeState(40) || m.reservoir != 10 || m.regular != 100 || m.spill != 0 {
		t.Fatalf("reservoir=%d regular=%d spill=%d", m.reservoir, m.regular, m.spill)
	}
}

func TestMeterChargeStateSpill(t *testing.T) {
	m := NewMeter(MdGas{Regular: 100, State: 50})
	// 50 from reservoir, 20 spills into regular.
	if !m.ChargeState(70) || m.reservoir != 0 || m.regular != 80 || m.spill != 20 {
		t.Fatalf("reservoir=%d regular=%d spill=%d", m.reservoir, m.regular, m.spill)
	}
}

func TestMeterChargeStateOOG(t *testing.T) {
	m := NewMeter(MdGas{Regular: 10, State: 50})
	if m.ChargeState(70) {
		t.Fatal("50 reservoir + 10 regular < 70 must fail")
	}
	if m.reservoir != 50 || m.regular != 10 || m.spill != 0 {
		t.Fatalf("failed charge must not mutate: reservoir=%d regular=%d spill=%d", m.reservoir, m.regular, m.spill)
	}
}

func TestMeterRefillLIFO(t *testing.T) {
	m := NewMeter(MdGas{Regular: 100, State: 50})
	m.ChargeState(70) // reservoir 0, regular 80, spill 20
	m.Refill(30)      // 20 back to regular (up to spill), 10 to reservoir
	if m.regular != 100 || m.reservoir != 10 || m.spill != 0 {
		t.Fatalf("regular=%d reservoir=%d spill=%d", m.regular, m.reservoir, m.spill)
	}
}

func TestMeterNegativeStateUsage(t *testing.T) {
	m := NewMeter(MdGas{Regular: 100, State: 50})
	m.ChargeState(20) // reservoir 30, no spill
	m.Refill(50)      // no spill to credit -> all 50 to reservoir (80)
	u := m.Usage()
	if u.State != -30 { // (50 - 80) + 0
		t.Fatalf("State=%d want -30", u.State)
	}
	if u.Regular != 0 { // 150 - (100+80) - (-30)
		t.Fatalf("Regular=%d want 0", u.Regular)
	}
	if u.StateSpill != 0 {
		t.Fatalf("StateSpill=%d want 0", u.StateSpill)
	}
}

// TestMeterUsageMatchesDerivation asserts Usage() equals the pre-Meter derivation
// (Run defer: State = (initial.State - reservoir) + spill; call/create defer:
// Regular = initial.Total() - leftover.Total() - State) across op sequences.
func TestMeterUsageMatchesDerivation(t *testing.T) {
	initial := MdGas{Regular: 100, State: 50}
	seqs := []func(m *Meter){
		func(m *Meter) { m.ChargeRegular(30); m.ChargeState(40) },
		func(m *Meter) { m.ChargeState(70) },
		func(m *Meter) { m.ChargeState(70); m.Refill(70) },
		func(m *Meter) { m.ChargeState(20); m.Refill(50) },
		func(m *Meter) { m.ChargeRegular(10); m.ChargeState(60); m.Refill(5) },
		func(m *Meter) { m.AdoptChildReservoir(30, 5) },
	}
	for i, seq := range seqs {
		m := NewMeter(initial)
		seq(&m)
		got := m.Usage()
		wantState := (int64(initial.State) - int64(m.reservoir)) + int64(m.spill)
		wantRegular := (initial.Regular + initial.State) - (m.regular + m.reservoir) - uint64(wantState)
		if got.State != wantState || got.Regular != wantRegular || got.StateSpill != m.spill {
			t.Fatalf("seq %d: Usage=%+v want{Regular:%d State:%d StateSpill:%d}", i, got, wantRegular, wantState, m.spill)
		}
	}
}

func TestMeterForChildAdoptReservoir(t *testing.T) {
	m := NewMeter(MdGas{Regular: 100, State: 50})
	m.ChargeRegular(10) // regular 90
	if c := m.ForChild(60); c.Regular != 60 || c.State != 50 {
		t.Fatalf("ForChild=%+v", c)
	}
	m.AdoptChildReservoir(30, 5) // child leftover reservoir adopted + spill propagated; regular untouched
	if m.regular != 90 || m.reservoir != 30 || m.spill != 5 {
		t.Fatalf("after adopt: regular=%d reservoir=%d spill=%d", m.regular, m.reservoir, m.spill)
	}
}
