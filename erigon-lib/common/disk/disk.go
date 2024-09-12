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

//go:build !linux && !darwin

package disk

import (
	"os"
	"runtime"

	"github.com/shirou/gopsutil/v4/process"

	"github.com/erigontech/erigon-lib/metrics"
)

var (
	writeBytes = metrics.NewGauge(`process_io_storage_written_bytes_total`)
	readBytes  = metrics.NewGauge(`process_io_storage_read_bytes_total`)

	writeCount = metrics.NewGauge(`process_io_write_syscalls_total`)
	readCount  = metrics.NewGauge(`process_io_read_syscalls_total`)

	cgoCount = metrics.NewGauge(`go_cgo_calls_count`)
)

func UpdatePrometheusDiskStats() error {
	p, err := process.NewProcess(int32(os.Getpid()))
	if err != nil {
		return err
	}

	counters, err := p.IOCounters()
	if err != nil {
		return err
	}

	writeBytes.SetUint64(counters.WriteBytes)
	readBytes.SetUint64(counters.ReadBytes)

	writeCount.SetUint64(counters.WriteCount)
	readCount.SetUint64(counters.ReadCount)

	cgoCount.SetUint64(uint64(runtime.NumCgoCall()))

	return nil
}
