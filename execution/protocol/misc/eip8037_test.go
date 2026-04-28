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

package misc

//
//import (
//	"testing"
//)
//
//func TestCostPerStateByte(t *testing.T) {
//	// Reference values computed by the EIP-8037 Python reference at devnets/bal/4
//	// (see src/ethereum/forks/amsterdam/vm/gas.py::state_gas_per_byte).
//	tests := []struct {
//		gasLimit uint64
//		want     uint64
//	}{
//		{gasLimit: 1_000_000, want: 1},
//		{gasLimit: 30_000_000, want: 150},
//		{gasLimit: 36_000_000, want: 150},
//		{gasLimit: 60_000_000, want: 662},
//		{gasLimit: 100_000_000, want: 1174},
//		{gasLimit: 120_000_000, want: 1174},
//		{gasLimit: 200_000_000, want: 2198},
//		{gasLimit: 300_000_000, want: 3222},
//		{gasLimit: 500_000_000, want: 5782},
//		{gasLimit: 1_000_000_000, want: 11926},
//	}
//	for _, tt := range tests {
//		got := CostPerStateByte(tt.gasLimit)
//		if got != tt.want {
//			t.Errorf("CostPerStateByte(%d) = %d, want %d", tt.gasLimit, got, tt.want)
//		}
//	}
//}
