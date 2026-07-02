package kvmetrics

import (
	"time"

	"github.com/erigontech/erigon/diagnostics/metrics"
)

// publishInterval is how often the collector pushes the grouped totals to
// Prometheus. Coarse: these are cumulative process counters, scraped on
// Prometheus's own cadence, so a 10s refresh is ample and keeps the collector
// goroutine almost entirely idle between sends.
const publishInterval = 10 * time.Second

// Process-level, source-labelled KV-read gauges. Cumulative counts/durations
// (monotonic), so Prometheus rate()/increase() work as on a counter. Labels:
// source = exec|commitment|warmup|rpc|engine, domain = account|storage|…,
// op = cache|db|file|statecache_hit|statecache_miss. Additive to the existing
// exec-scoped mxExec* gauges — those stay as the per-batch view.
var (
	mxKVReadCount      = metrics.GetOrCreateGaugeVec("kv_read_count", []string{"source", "domain", "op"})
	mxKVReadDurationNs = metrics.GetOrCreateGaugeVec("kv_read_duration_ns", []string{"source", "domain", "op"})
)

// publish pushes the current grouped totals to Prometheus. Runs on the collector
// goroutine (single owner of grouped), so it reads without a lock.
func (c *Collector) publish() {
	for source, g := range c.grouped {
		s := source.String()
		for domain, dm := range g.Domains {
			d := domain.String()
			mxKVReadCount.WithLabelValues(s, d, "cache").SetUint64(uint64(dm.CacheReadCount))
			mxKVReadCount.WithLabelValues(s, d, "db").SetUint64(uint64(dm.DbReadCount))
			mxKVReadCount.WithLabelValues(s, d, "file").SetUint64(uint64(dm.FileReadCount))
			mxKVReadCount.WithLabelValues(s, d, "statecache_hit").SetUint64(uint64(dm.StateCacheHitCount))
			mxKVReadCount.WithLabelValues(s, d, "statecache_miss").SetUint64(uint64(dm.StateCacheMissCount))
			mxKVReadDurationNs.WithLabelValues(s, d, "cache").SetUint64(uint64(dm.CacheReadDuration))
			mxKVReadDurationNs.WithLabelValues(s, d, "db").SetUint64(uint64(dm.DbReadDuration))
			mxKVReadDurationNs.WithLabelValues(s, d, "file").SetUint64(uint64(dm.FileReadDuration))
		}
	}
}
