package diagnostics

import (
	"context"

	"github.com/ledgerwatch/log/v3"
)

func (d *DiagnosticClient) setupStagesDiagnostics(rootCtx context.Context) {
	d.runCurrentSyncStageListener(rootCtx)
	d.runSyncStagesListListener(rootCtx)
}

func (d *DiagnosticClient) runSyncStagesListListener(rootCtx context.Context) {
	go func() {
		ctx, ch, closeChannel := Context[SyncStagesList](rootCtx, 1)
		defer closeChannel()

		StartProviders(ctx, TypeOf(SyncStagesList{}), log.Root())
		for {
			select {
			case <-rootCtx.Done():
				return
			case info := <-ch:
				d.mu.Lock()
				d.syncStats.SyncStages.StagesList = info.Stages
				d.mu.Unlock()
				return
			}
		}
	}()
}

func (d *DiagnosticClient) runCurrentSyncStageListener(rootCtx context.Context) {
	go func() {
		ctx, ch, closeChannel := Context[CurrentSyncStage](rootCtx, 1)
		defer closeChannel()

		StartProviders(ctx, TypeOf(CurrentSyncStage{}), log.Root())
		for {
			select {
			case <-rootCtx.Done():
				return
			case info := <-ch:
				d.mu.Lock()
				d.syncStats.SyncStages.CurrentStage = info.Stage
				if int(d.syncStats.SyncStages.CurrentStage) >= len(d.syncStats.SyncStages.StagesList) {
					return
				}
				d.mu.Unlock()
			}
		}
	}()
}
