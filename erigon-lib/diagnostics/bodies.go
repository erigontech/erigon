package diagnostics

import (
	"context"

	"github.com/ledgerwatch/erigon-lib/common"
	"github.com/ledgerwatch/log/v3"
)

func (d *DiagnosticClient) setupBodiesDiagnostics() {
	d.runBodiesBlockDownloadListener()
	d.runBodiesBlockWriteListener()
	d.runBodiesProcessingListener()
}

func (d *DiagnosticClient) runBodiesBlockDownloadListener() {
	go func() {
		ctx, ch, cancel := Context[BodiesDownloadBlockUpdate](context.Background(), 1)
		defer cancel()

		rootCtx, _ := common.RootContext()

		StartProviders(ctx, TypeOf(BodiesDownloadBlockUpdate{}), log.Root())
		for {
			select {
			case <-rootCtx.Done():
				cancel()
				return
			case info := <-ch:
				d.mu.Lock()
				d.bodies.BlockDownload = info
				d.mu.Unlock()
			}
		}

	}()
}

func (d *DiagnosticClient) runBodiesBlockWriteListener() {
	go func() {
		ctx, ch, cancel := Context[BodiesWriteBlockUpdate](context.Background(), 1)
		defer cancel()

		rootCtx, _ := common.RootContext()

		StartProviders(ctx, TypeOf(BodiesWriteBlockUpdate{}), log.Root())
		for {
			select {
			case <-rootCtx.Done():
				cancel()
				return
			case info := <-ch:
				d.mu.Lock()
				d.bodies.BlockWrite = info
				d.mu.Unlock()
			}
		}

	}()
}

func (d *DiagnosticClient) runBodiesProcessingListener() {
	go func() {
		ctx, ch, cancel := Context[BodiesProcessingUpdate](context.Background(), 1)
		defer cancel()

		rootCtx, _ := common.RootContext()

		StartProviders(ctx, TypeOf(BodiesProcessingUpdate{}), log.Root())
		for {
			select {
			case <-rootCtx.Done():
				cancel()
				return
			case info := <-ch:
				d.mu.Lock()
				d.bodies.Processing = info
				d.mu.Unlock()
			}
		}

	}()
}

func (d *DiagnosticClient) GetBodiesInfo() BodiesInfo {
	return d.bodies
}
