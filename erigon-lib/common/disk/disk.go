//go:build !linux && !darwin

package disk

import (
	"os"

	"github.com/shirou/gopsutil/v3/process"

	"github.com/ledgerwatch/erigon-lib/metrics"
)

var (
	writeBytes = metrics.NewGauge(`system_disk_writebytes`)
	readBytes  = metrics.NewGauge(`system_disk_readbytes`)

	writeCount = metrics.NewGauge(`ru_outblock`)
	readCount  = metrics.NewGauge(`ru_inblock`)
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
	writeCount.SetUint64(counters.WriteCount)
	readBytes.SetUint64(counters.ReadBytes)
	readCount.SetUint64(counters.ReadCount)

	return nil
}
