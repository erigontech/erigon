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

package mem

import (
	"testing"

	"github.com/stretchr/testify/require"
)

func TestProcessMemGauges(t *testing.T) {
	updateGaugesFromProcStatus("Name:\terigon\nVmRSS:\t28 kB\nVmSize:\t168 kB\nVmSwap:\t8 kB\n")
	require.Equal(t, uint64(28*1024), memRssGauge.GetValueUint64())
	require.Equal(t, uint64(168*1024), memSizeGauge.GetValueUint64())
	require.Equal(t, uint64(8*1024), memSwapGauge.GetValueUint64())

	updateGaugesFromProcStatus("VmRSS:\t32 kB\nVmSwap:\t0 kB\n")
	require.Equal(t, uint64(32*1024), memRssGauge.GetValueUint64())
	require.Equal(t, uint64(168*1024), memSizeGauge.GetValueUint64())
	require.Zero(t, memSwapGauge.GetValueUint64())
}
