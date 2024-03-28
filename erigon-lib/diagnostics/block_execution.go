package diagnostics

import (
	"context"

	"github.com/ledgerwatch/log/v3"
)

func (d *DiagnosticClient) setupBlockExecutionDiagnostics(rootCtx context.Context) {
	d.runBlockExecutionListener(rootCtx)
}

func (d *DiagnosticClient) runBlockExecutionListener(rootCtx context.Context) {
	go func() {
		ctx, ch, cancel := Context[BlockExecutionStatistics](context.Background(), 1)
		defer cancel()

		StartProviders(ctx, TypeOf(BlockExecutionStatistics{}), log.Root())
		for {
			select {
			case <-rootCtx.Done():
				cancel()
				return
			case info := <-ch:
				d.mu.Lock()
				d.syncStats.BlockExecution = info
				d.mu.Unlock()

				if int(d.syncStats.SyncStages.CurrentStage) >= len(d.syncStats.SyncStages.StagesList) {
					return
				}
			}
		}
	}()
}
