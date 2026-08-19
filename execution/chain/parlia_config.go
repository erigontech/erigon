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

package chain

// ParliaConfig is the consensus-engine config for BSC's Parlia (PoSA) L1 engine.
// It is currently empty: epoch length, block interval and turn length are
// Parlia snapshot state rather than static chain config.
type ParliaConfig struct {
}

func (c *ParliaConfig) String() string { return "parlia" }
