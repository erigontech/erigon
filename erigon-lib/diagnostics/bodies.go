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
		ctx, ch, closeChannel := Context[BodiesDownloadBlockUpdate](rootCtx, 1)
		defer closeChannel()

		StartProviders(ctx, TypeOf(BodiesDownloadBlockUpdate{}), log.Root())
		for {
			select {
			case <-rootCtx.Done():
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
		ctx, ch, closeChannel := Context[BodiesWriteBlockUpdate](rootCtx, 1)
		defer closeChannel()

		StartProviders(ctx, TypeOf(BodiesWriteBlockUpdate{}), log.Root())
		for {
			select {
			case <-rootCtx.Done():
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
		ctx, ch, closeChannel := Context[BodiesProcessedUpdate](rootCtx, 1)
		defer closeChannel()

		StartProviders(ctx, TypeOf(BodiesProcessedUpdate{}), log.Root())
		for {
			select {
			case <-rootCtx.Done():
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
		ctx, ch, closeChannel := Context[BodiesProcessingUpdate](rootCtx, 1)
		defer closeChannel()

		StartProviders(ctx, TypeOf(BodiesProcessingUpdate{}), log.Root())
		for {
			select {
			case <-rootCtx.Done():
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
