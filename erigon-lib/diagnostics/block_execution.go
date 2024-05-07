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
		ctx, ch, closeChannel := Context[BlockExecutionStatistics](rootCtx, 1)
		defer closeChannel()

		StartProviders(ctx, TypeOf(BlockExecutionStatistics{}), log.Root())
		for {
			select {
			case <-rootCtx.Done():
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
