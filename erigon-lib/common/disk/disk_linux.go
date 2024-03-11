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

	diskMajorFaults.SetUint64(faults.MajorFaults)
	diskMinorFaults.SetUint64(faults.MinorFaults)

	return nil
}
