package sync_stages

import (
	"fmt"
	"github.com/VictoriaMetrics/metrics"
	"github.com/huandu/xstrings"
	"github.com/ledgerwatch/erigon-lib/kv"
)

var Metrics = map[SyncStage]*metrics.Counter{}

// TODO: this needs improving to support passing in different sets of stages
func init() {
	for _, v := range AllStages {
		Metrics[v] = metrics.GetOrCreateCounter(
			fmt.Sprintf(
				`sync{stage="%s"}`,
				xstrings.ToSnakeCase(string(v)),
			),
		)
	}
}

// UpdateMetrics - need update metrics manually because current "metrics" package doesn't support labels
// need to fix it in future
func UpdateMetrics(tx kv.Tx) error {
	for id, m := range Metrics {
		progress, err := GetStageProgress(tx, id)
		if err != nil {
			return err
		}
		m.Set(progress)
	}
	return nil
}
