package diagnostics

import (
	"context"

	"github.com/ledgerwatch/erigon-lib/common"
	"github.com/ledgerwatch/log/v3"
)

func (d *DiagnosticClient) setupResourcesUsageDiagnostics() {
	d.runMemoryStatsListener()
}

func (d *DiagnosticClient) GetResourcesUsage() ResourcesUsage {
	d.resourcesUsageMutex.Lock()
	defer d.resourcesUsageMutex.Unlock()

	returnObj := d.resourcesUsage
	d.resourcesUsage = ResourcesUsage{}
	return returnObj
}

func (d *DiagnosticClient) runMemoryStatsListener() {
	go func() {
		ctx, ch, cancel := Context[MemoryStats](context.Background(), 1)
		defer cancel()

		rootCtx, _ := common.RootContext()

		StartProviders(ctx, TypeOf(MemoryStats{}), log.Root())
		for {
			select {
			case <-rootCtx.Done():
				cancel()
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
