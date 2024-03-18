package diagnostics

import (
	"context"

	"github.com/ledgerwatch/erigon-lib/common"
	"github.com/ledgerwatch/log/v3"
)

func (d *DiagnosticClient) setupStagesDiagnostics() {
	d.runCurrentSyncStageListener()
	d.runSyncStagesListListener()
}

func (d *DiagnosticClient) runSyncStagesListListener() {
	go func() {
		ctx, ch, cancel := Context[SyncStagesList](context.Background(), 1)
		defer cancel()

		rootCtx, _ := common.RootContext()

		StartProviders(ctx, TypeOf(SyncStagesList{}), log.Root())
		for {
			select {
			case <-rootCtx.Done():
				cancel()
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

func (d *DiagnosticClient) runCurrentSyncStageListener() {
	go func() {
		ctx, ch, cancel := Context[CurrentSyncStage](context.Background(), 1)
		defer cancel()

		rootCtx, _ := common.RootContext()

		StartProviders(ctx, TypeOf(CurrentSyncStage{}), log.Root())
		for {
			select {
			case <-rootCtx.Done():
				cancel()
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
