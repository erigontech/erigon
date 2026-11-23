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

//go:build linux

package mem

import (
	"os"
	"strconv"
	"strings"

	"github.com/erigontech/erigon/diagnostics/metrics"
)

var (
	memRssGauge  = metrics.NewGauge("mem_rss")
	memSizeGauge = metrics.NewGauge("mem_size")
	memSwapGauge = metrics.NewGauge("mem_swap")
)

func updateProcessMemGauges() {
	status, err := os.ReadFile("/proc/self/status")
	if err == nil {
		updateGaugesFromProcStatus(string(status))
	}
}

func updateGaugesFromProcStatus(status string) {
	gauges := map[string]metrics.Gauge{
		"VmRSS":  memRssGauge,
		"VmSize": memSizeGauge,
		"VmSwap": memSwapGauge,
	}
	for line := range strings.SplitSeq(status, "\n") {
		name, value, _ := strings.Cut(line, ":")
		gauge := gauges[name]
		if gauge == nil {
			continue
		}
		fields := strings.Fields(value)
		if len(fields) != 2 || fields[1] != "kB" {
			continue
		}
		kib, err := strconv.ParseUint(fields[0], 10, 54)
		if err == nil {
			gauge.SetUint64(kib * 1024)
		}
	}
}
