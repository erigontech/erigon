package diagnostics

import (
	"context"

	"github.com/ledgerwatch/log/v3"
)

func (d *DiagnosticClient) setupResourcesUsageDiagnostics(rootCtx context.Context) {
	d.runMemoryStatsListener(rootCtx)
}

func (d *DiagnosticClient) GetResourcesUsage() ResourcesUsage {
	d.resourcesUsageMutex.Lock()
	defer d.resourcesUsageMutex.Unlock()

	returnObj := d.resourcesUsage
	d.resourcesUsage = ResourcesUsage{}
	return returnObj
}

func (d *DiagnosticClient) runMemoryStatsListener(rootCtx context.Context) {
	go func() {
		ctx, ch, closeChannel := Context[MemoryStats](rootCtx, 1)
		defer closeChannel()

		StartProviders(ctx, TypeOf(MemoryStats{}), log.Root())
		for {
			select {
			case <-rootCtx.Done():
				return
			case info := <-ch:
				d.resourcesUsageMutex.Lock()
				info.StageIndex = int(d.syncStats.SyncStages.CurrentStage)
				d.resourcesUsage.MemoryUsage = append(d.resourcesUsage.MemoryUsage, info)
				d.resourcesUsageMutex.Unlock()
			}
		}
	}()
}
