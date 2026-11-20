package kvmetrics

import (
	"fmt"
	"time"

	"github.com/erigontech/erigon/common"
	"github.com/erigontech/erigon/common/log/v3"
	"github.com/erigontech/erigon/db/kv"
)

// DetailLevel controls how much breakdown a metrics log line carries.
type DetailLevel uint8

const (
	// DetailSummary is the all-domains aggregate line (the former LogMetrics).
	DetailSummary DetailLevel = iota
	// DetailPerDomain additionally emits one line per domain (the former
	// DomainLogMetrics).
	DetailPerDomain
)

// FormatSummary builds the structured key/value log args for the all-domains
// totals of one snapshot. Does not lock — callers holding a shared aggregate
// must RLock around the call; a Collector snapshot is already a private copy.
func FormatSummary(m *DomainMetrics) []any {
	if m == nil {
		return nil
	}
	var out []any
	if readCount := m.CacheReadCount; readCount > 0 {
		out = append(out, "cache", common.PrettyCounter(readCount),
			"puts", common.PrettyCounter(m.CachePutCount),
			"size", fmt.Sprintf("%s(%s/%s)",
				common.PrettyCounter(m.CachePutSize), common.PrettyCounter(m.CachePutKeySize), common.PrettyCounter(m.CachePutValueSize)),
			"gets", common.PrettyCounter(m.CacheGetCount), "size", common.PrettyCounter(m.CacheGetSize),
			"cdur", common.Round(m.CacheReadDuration/time.Duration(readCount), 0))
	}
	if hits, misses := m.StateCacheHitCount, m.StateCacheMissCount; hits+misses > 0 {
		out = append(out, "stateCache", formatHitRate(hits, misses))
	}
	if readCount := m.DbReadCount; readCount > 0 {
		out = append(out, "db", common.PrettyCounter(readCount), "dbdur", common.Round(m.DbReadDuration/time.Duration(readCount), 0))
	}
	if readCount := m.FileReadCount; readCount > 0 {
		out = append(out, "files", common.PrettyCounter(readCount), "fdur", common.Round(m.FileReadDuration/time.Duration(readCount), 0))
	}
	return out
}

// FormatPerDomain builds the per-domain key/value log args of one snapshot. Same
// locking contract as FormatSummary.
func FormatPerDomain(m *DomainMetrics) map[kv.Domain][]any {
	if m == nil {
		return nil
	}
	out := map[kv.Domain][]any{}
	for domain, dm := range m.Domains {
		var dmetrics []any
		if readCount := dm.CacheReadCount; readCount > 0 {
			dmetrics = append(dmetrics, "cache", common.PrettyCounter(readCount), "cdur", common.Round(dm.CacheReadDuration/time.Duration(readCount), 0))
		}
		if hits, misses := dm.StateCacheHitCount, dm.StateCacheMissCount; hits+misses > 0 {
			dmetrics = append(dmetrics, "stateCache", formatHitRate(hits, misses))
		}
		if readCount := dm.DbReadCount; readCount > 0 {
			dmetrics = append(dmetrics, "db", common.PrettyCounter(readCount), "dbdur", common.Round(dm.DbReadDuration/time.Duration(readCount), 0))
		}
		if readCount := dm.FileReadCount; readCount > 0 {
			dmetrics = append(dmetrics, "files", common.PrettyCounter(readCount), "fdur", common.Round(dm.FileReadDuration/time.Duration(readCount), 0))
		}
		if len(dmetrics) > 0 {
			out[domain] = dmetrics
		}
	}
	return out
}

func formatHitRate(hits, misses int64) string {
	return fmt.Sprintf("hit=%s miss=%s rate=%.0f%%",
		common.PrettyCounter(hits), common.PrettyCounter(misses),
		100*float64(hits)/float64(hits+misses))
}

// LogMetrics emits the metrics for one source at the requested level and detail,
// using shared formatting. The snapshot m is a private copy (e.g. from
// Collector.Snapshot()[source]); no locking is done here. Nothing is logged when
// there is nothing to report.
func LogMetrics(logger log.Logger, level log.Lvl, source Source, m *DomainMetrics, detail DetailLevel) {
	if logger == nil || m == nil {
		return
	}
	if summary := FormatSummary(m); len(summary) > 0 {
		logger.Log(level, "[kv-metrics]", append([]any{"source", source.String()}, summary...)...)
	}
	if detail >= DetailPerDomain {
		for domain, dmetrics := range FormatPerDomain(m) {
			logger.Log(level, "[kv-metrics]", append([]any{"source", source.String(), "domain", domain.String()}, dmetrics...)...)
		}
	}
}
