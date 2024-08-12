//go:build linux

package disk

import (
	"os"
	"runtime"

	"github.com/ledgerwatch/erigon-lib/metrics"
	"github.com/shirou/gopsutil/v4/process"
)

var (
	diskMajorFaults = metrics.NewGauge(`process_major_pagefaults_total`)
	diskMinorFaults = metrics.NewGauge(`process_minor_pagefaults_total`)

	writeCount = metrics.NewGauge(`process_io_write_syscalls_total`)
	readCount  = metrics.NewGauge(`process_io_read_syscalls_total`)

	writeBytes = metrics.NewGauge(`process_io_storage_written_bytes_total`)
	readBytes  = metrics.NewGauge(`process_io_storage_read_bytes_total`)

	cgoCount = metrics.NewGauge(`go_cgo_calls_count`)
)

func UpdatePrometheusDiskStats() error {
	p, err := process.NewProcess(int32(os.Getpid()))
	if err != nil {
		return err
	}

	faults, err := p.PageFaults()
	if err != nil {
		return err
	}

	counters, err := p.IOCounters()
	if err != nil {
		return err
	}

	diskMajorFaults.SetUint64(faults.MajorFaults)
	diskMinorFaults.SetUint64(faults.MinorFaults)

	writeCount.SetUint64(counters.WriteCount)
	readCount.SetUint64(counters.ReadCount)

	writeBytes.SetUint64(counters.WriteBytes)
	readBytes.SetUint64(counters.ReadBytes)

	cgoCount.SetUint64(uint64(runtime.NumCgoCall()))

	return nil
}
