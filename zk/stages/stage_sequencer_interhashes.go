package stages

import (
	"context"

	"github.com/gateway-fm/cdk-erigon-lib/kv"
	"github.com/ledgerwatch/erigon/eth/stagedsync"
)

// This stages does NOTHING while going forward, because its done during execution
// Even this stage progress is updated in execution stage
func SpawnSequencerInterhashesStage(
	s *stagedsync.StageState,
	u stagedsync.Unwinder,
	tx kv.RwTx,
	ctx context.Context,
	cfg ZkInterHashesCfg,
	quiet bool,
) error {
	return nil
}

// The unwind of interhashes must happen separate from execution’s unwind although execution includes interhashes while going forward.
// This is because interhashes MUST be unwound before history/calltraces while execution MUST be after history/calltraces
func UnwindSequencerInterhashsStage(
	u *stagedsync.UnwindState,
	s *stagedsync.StageState,
	tx kv.RwTx,
	ctx context.Context,
	cfg ZkInterHashesCfg,
) error {
	return UnwindZkIntermediateHashesStage(u, s, tx, cfg, ctx, false)
}

func PruneSequencerInterhashesStage(
	s *stagedsync.PruneState,
	tx kv.RwTx,
	cfg ZkInterHashesCfg,
	ctx context.Context,
) error {
	return nil
}
