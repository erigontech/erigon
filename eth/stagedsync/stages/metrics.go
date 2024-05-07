package stages

import (
	"fmt"

	"github.com/huandu/xstrings"

	"github.com/ledgerwatch/erigon-lib/kv"
	"github.com/ledgerwatch/erigon-lib/metrics"
)

var SyncMetrics = map[SyncStage]metrics.Gauge{}

// TODO: this needs improving to support passing in different sets of stages
func init() {
	for _, v := range AllStages {
		SyncMetrics[v] = metrics.GetOrCreateGauge(
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
	for id, m := range SyncMetrics {
		progress, err := GetStageProgress(tx, id)
		if err != nil {
			return err
		}
		m.SetUint64(progress)
	}
	return nil
}
