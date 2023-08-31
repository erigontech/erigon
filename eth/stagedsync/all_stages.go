package stagedsync

import (
	"fmt"

	metrics2 "github.com/VictoriaMetrics/metrics"
	"github.com/huandu/xstrings"
	"github.com/ledgerwatch/erigon-lib/kv"
	"github.com/ledgerwatch/erigon/eth/stagedsync/stages"
	metrics "github.com/ledgerwatch/erigon/metrics/methelp"
)

var syncMetrics = map[stages.SyncStage]*metrics2.Counter{}

func init() {
	for _, v := range stages.AllStages {
		syncMetrics[v] = metrics.GetOrCreateCounter(
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
	for id, m := range syncMetrics {
		progress, err := stages.GetStageProgress(tx, id)
		if err != nil {
			return err
		}
		m.Set(progress)
	}
	return nil
}
