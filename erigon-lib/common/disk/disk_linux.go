//go:build linux

package disk

import (
	"os"

	"github.com/shirou/gopsutil/v3/process"

	"github.com/ledgerwatch/erigon-lib/metrics"
)

var (
	diskMajorFaults = metrics.NewGauge(`process_major_pagefaults_total`)
	diskMinorFaults = metrics.NewGauge(`process_minor_pagefaults_total`)

	diskMajorFaults2 = metrics.NewGauge(`ru_majflt`)
	diskMinorFaults2 = metrics.NewGauge(`ru_minflt`)

	writeCount = metrics.NewGauge(`ru_outblock`)
	readCount  = metrics.NewGauge(`ru_inblock`)

	writeBytes = metrics.NewGauge(`system_disk_writebytes`)
	readBytes  = metrics.NewGauge(`system_disk_readbytes`)
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
	diskMajorFaults2.SetUint64(faults.MajorFaults)

	diskMinorFaults.SetUint64(faults.MinorFaults)
	diskMinorFaults2.SetUint64(faults.MinorFaults)

	writeCount.SetUint64(counters.WriteCount)
	readCount.SetUint64(counters.ReadCount)

	writeBytes.SetUint64(counters.WriteBytes)
	readBytes.SetUint64(counters.ReadBytes)

	return nil
}
