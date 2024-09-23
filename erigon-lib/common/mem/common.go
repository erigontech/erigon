package mem

import (
	"context"
	"errors"

	"reflect"
	"runtime"
	"time"

	"github.com/ledgerwatch/erigon-lib/common"
	"github.com/ledgerwatch/erigon-lib/common/dbg"
	"github.com/ledgerwatch/erigon-lib/diagnostics"
	"github.com/ledgerwatch/log/v3"
	"github.com/shirou/gopsutil/v4/process"
)

var ErrorUnsupportedPlatform = errors.New("unsupported platform")

type VirtualMemStat struct {
	process.MemoryMapsStat
}

// Fields converts VirtualMemStat to slice
func (m VirtualMemStat) Fields() []interface{} {
	typ := reflect.TypeOf(m.MemoryMapsStat)
	val := reflect.ValueOf(m.MemoryMapsStat)

	var s []interface{}
	for i := 0; i < typ.NumField(); i++ {
		t := typ.Field(i).Name
		if t == "Path" { // always empty for aggregated smap statistics
			continue
		}

		value := val.Field(i).Interface()
		if uint64Val, ok := value.(uint64); ok {
			value = common.ByteCount(uint64Val)
		}

		s = append(s, t, value)
	}

	return s
}

func LogMemStats(ctx context.Context, logger log.Logger) {
	logEvery := time.NewTicker(180 * time.Second)
	defer logEvery.Stop()

	for {
		select {
		case <-ctx.Done():
			return
		case <-logEvery.C:
			vm, err := ReadVirtualMemStats()
			if err != nil {
				// suppress error if platform is unsupported, we just print out heap stats
				if errors.Is(err, ErrorUnsupportedPlatform) {
					logger.Warn("[mem] error reading virtual memory stats", "err", err)
					continue
				}
			}

			var m runtime.MemStats
			dbg.ReadMemStats(&m)

			v := VirtualMemStat{vm}
			l := v.Fields()
			l = append(l, "alloc", common.ByteCount(m.Alloc), "sys", common.ByteCount(m.Sys))

			diagnostics.Send(diagnostics.MemoryStats{
				Alloc:       m.Alloc,
				Sys:         m.Sys,
				OtherFields: v.Fields(),
				Timestamp:   time.Now(),
			})

			logger.Info("[mem] memory stats", l...)
			UpdatePrometheusVirtualMemStats(vm)
		}
	}
}
