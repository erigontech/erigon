package diagnostics

import (
	"context"

	"github.com/ledgerwatch/log/v3"
)

func (d *DiagnosticClient) setupBodiesDiagnostics(rootCtx context.Context) {
	d.runBodiesBlockDownloadListener(rootCtx)
	d.runBodiesBlockWriteListener(rootCtx)
	d.runBodiesProcessingListener(rootCtx)
	d.runBodiesProcessedListener(rootCtx)
}

func (d *DiagnosticClient) runBodiesBlockDownloadListener(rootCtx context.Context) {
	go func() {
		ctx, ch, cancel := Context[BodiesDownloadBlockUpdate](context.Background(), 1)
		defer cancel()

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

func (d *DiagnosticClient) runBodiesBlockWriteListener(rootCtx context.Context) {
	go func() {
		ctx, ch, cancel := Context[BodiesWriteBlockUpdate](context.Background(), 1)
		defer cancel()

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

func (d *DiagnosticClient) runBodiesProcessedListener(rootCtx context.Context) {
	go func() {
		ctx, ch, cancel := Context[BodiesProcessedUpdate](context.Background(), 1)
		defer cancel()

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

func (d *DiagnosticClient) runBodiesProcessingListener(rootCtx context.Context) {
	go func() {
		ctx, ch, cancel := Context[BodiesProcessingUpdate](context.Background(), 1)
		defer cancel()

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
