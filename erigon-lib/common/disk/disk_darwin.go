//go:build darwin

package disk

import (
	"runtime"

	"github.com/erigontech/erigon-lib/metrics"
)

var cgoCount = metrics.NewGauge(`go_cgo_calls_count`)

func UpdatePrometheusDiskStats() error {
	cgoCount.SetUint64(uint64(runtime.NumCgoCall()))

	return nil
}
