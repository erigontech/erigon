// Go port of Coda Hale's Metrics library
//
// <https://github.com/rcrowley/go-metrics>
//
// Coda Hale's original work: <https://github.com/codahale/metrics>
package metrics

import (
	"runtime/metrics"
	"sync/atomic"
)

// callbacks - storing list of callbacks as type []func()
// use metrics.AddCallback to add your function to metrics collection loop (to avoid multiple goroutines collecting metrics)
var callbacks atomic.Value

func init() {
	metrics.All()
	callbacks.Store([]func(){})
}

// Calling Load method
