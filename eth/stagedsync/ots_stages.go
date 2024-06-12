package stagedsync

import (
	"context"

	"github.com/ledgerwatch/erigon/eth/stagedsync/stages"
)

// Standard Otterscan V2 stages; if opted-in, they must be inserted before finish stage.
func OtsStages(ctx context.Context, caCfg ContractAnalyzerCfg) []*Stage {
	return []*Stage{
		{
			ID:          stages.OtsTestStage,
			Description: "Test stage workflow",
			Forward:     GenericStageForwardFunc(ctx, caCfg, stages.Execution, CustomTestExecutor),
			Unwind: GenericStageUnwindFunc(ctx, caCfg,
				nil,
			),
			Prune: NoopStagePrune(ctx, caCfg),
		},
	}
}
