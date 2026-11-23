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
	"os"
	"path/filepath"
	"strconv"
	"testing"

	"github.com/shirou/gopsutil/v4/process"
	"github.com/stretchr/testify/require"
)

func TestProcessMemGauges(t *testing.T) {
	procDir := t.TempDir()
	t.Setenv("HOST_PROC", procDir)
	pidDir := filepath.Join(procDir, strconv.Itoa(os.Getpid()))
	require.NoError(t, os.Mkdir(pidDir, 0o700))
	statm := filepath.Join(pidDir, "statm")
	require.NoError(t, os.WriteFile(statm, []byte("42 7 0 0 0 0 0\n"), 0o600))

	pageSize := uint64(os.Getpagesize())
	updateProcessMemGauges()
	require.Equal(t, 7*pageSize, memRssGauge.GetValueUint64())
	require.Equal(t, 42*pageSize, memSizeGauge.GetValueUint64())

	UpdatePrometheusVirtualMemStats(process.MemoryMapsStat{Rss: 8 * pageSize, Pss: 5 * pageSize})
	require.Equal(t, 5*pageSize, memPssGauge.GetValueUint64())
	require.Equal(t, 7*pageSize, memRssGauge.GetValueUint64())
	require.Equal(t, 42*pageSize, memSizeGauge.GetValueUint64())
}

func TestVirtualMemStatFieldsOmitSize(t *testing.T) {
	stats := VirtualMemStat{process.MemoryMapsStat{Rss: 4096}}
	fields := stats.Fields()
	require.NotContains(t, fields, "Size")
	require.Contains(t, fields, "Rss")
}
