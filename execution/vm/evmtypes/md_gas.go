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

package evmtypes

// MdGas represents multi-dimensional gas
type MdGas struct {
	Regular uint64
	State   uint64
}

func (g MdGas) Minus(other MdGas) MdGas {
	return MdGas{
		Regular: g.Regular - other.Regular,
		State:   g.State - other.State,
	}
}

func (g MdGas) Plus(other MdGas) MdGas {
	return MdGas{
		Regular: g.Regular + other.Regular,
		State:   g.State + other.State,
	}
}

func (g MdGas) Total() uint64 {
	return g.Regular + g.State
}

func (g MdGas) Bottleneck() uint64 {
	return max(g.Regular, g.State)
}
