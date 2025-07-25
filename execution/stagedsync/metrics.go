package stagedsync

import (
	"github.com/erigontech/erigon-lib/metrics"
	"github.com/erigontech/erigon/execution/stagedsync/stages"
)

var initialCycleDurationSecs = metrics.GetOrCreateGauge("initial_cycle_duration_secs")

type metricsCache struct {
	stageRunDurationSummaries    map[stages.SyncStage]metrics.Summary
	stagePruneDurationSummaries  map[stages.SyncStage]metrics.Summary
	stageUnwindDurationSummaries map[stages.SyncStage]metrics.Summary
}

func newMetricsCache() metricsCache {
	return metricsCache{
		stageRunDurationSummaries:    map[stages.SyncStage]metrics.Summary{},
		stagePruneDurationSummaries:  map[stages.SyncStage]metrics.Summary{},
		stageUnwindDurationSummaries: map[stages.SyncStage]metrics.Summary{},
	}
}

func (mc metricsCache) stageRunDurationSummary(stage stages.SyncStage) metrics.Summary {
	if summary, ok := mc.stageRunDurationSummaries[stage]; ok {
		return summary
	}
	summary := metrics.GetOrCreateSummaryWithLabels("run_stage_duration_secs", []string{"stage"}, []string{string(stage)})
	mc.stageRunDurationSummaries[stage] = summary
	return summary
}

func (mc metricsCache) stagePruneDurationSummary(stage stages.SyncStage) metrics.Summary {
	if summary, ok := mc.stagePruneDurationSummaries[stage]; ok {
		return summary
	}
	summary := metrics.GetOrCreateSummaryWithLabels("prune_stage_duration_secs", []string{"stage"}, []string{string(stage)})
	mc.stagePruneDurationSummaries[stage] = summary
	return summary
}

func (mc metricsCache) stageUnwindDurationSummary(stage stages.SyncStage) metrics.Summary {
	if summary, ok := mc.stageUnwindDurationSummaries[stage]; ok {
		return summary
	}
	summary := metrics.GetOrCreateSummaryWithLabels("unwind_stage_duration_secs", []string{"stage"}, []string{string(stage)})
	mc.stageUnwindDurationSummaries[stage] = summary
	return summary
}
