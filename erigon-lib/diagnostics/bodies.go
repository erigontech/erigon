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
	d.runBodiesProcessedListener()
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
				d.bodiesMutex.Lock()
				d.bodies.BlockDownload = info
				d.bodiesMutex.Unlock()
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
				d.bodiesMutex.Lock()
				d.bodies.BlockWrite = info
				d.bodiesMutex.Unlock()
			}
		}

	}()
}

func (d *DiagnosticClient) runBodiesProcessedListener() {
	go func() {
		ctx, ch, cancel := Context[BodiesProcessedUpdate](context.Background(), 1)
		defer cancel()

		rootCtx, _ := common.RootContext()

		StartProviders(ctx, TypeOf(BodiesProcessedUpdate{}), log.Root())
		for {
			select {
			case <-rootCtx.Done():
				cancel()
				return
			case info := <-ch:
				d.bodiesMutex.Lock()
				d.bodies.Processed = info
				d.bodiesMutex.Unlock()
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
				d.bodiesMutex.Lock()
				d.bodies.Processing = info
				d.bodiesMutex.Unlock()
			}
		}

	}()
}

func (d *DiagnosticClient) GetBodiesInfo() BodiesInfo {
	d.bodiesMutex.Lock()
	defer d.bodiesMutex.Unlock()
	return d.bodies
}
