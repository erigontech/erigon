//go:build !linux && !darwin

package disk

import (
	"os"
	"runtime"

	"github.com/shirou/gopsutil/v4/process"

	"github.com/ledgerwatch/erigon-lib/metrics"
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
