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

	writeBytes2 = metrics.NewGauge(`process_io_storage_written_bytes_total`)
	readBytes2  = metrics.NewGauge(`process_io_storage_read_bytes_total`)

	writeCount = metrics.NewGauge(`ru_outblock`)
	readCount  = metrics.NewGauge(`ru_inblock`)

	writeCount2 = metrics.NewGauge(`process_io_write_syscalls_total`)
	readCount2  = metrics.NewGauge(`process_io_read_syscalls_total`)

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

	writeBytes2.SetUint64(counters.WriteBytes)
	readBytes2.SetUint64(counters.ReadBytes)

	writeCount.SetUint64(counters.WriteCount)
	readCount.SetUint64(counters.ReadCount)

	writeCount2.SetUint64(counters.WriteCount)
	readCount2.SetUint64(counters.ReadCount)

	cgoCount.SetUint64(uint64(runtime.NumCgoCall()))

	return nil
}
