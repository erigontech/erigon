package diagnostics

import (
	"context"
	"fmt"

	"github.com/ledgerwatch/log/v3"
)

func (d *DiagnosticClient) setupHeadersDiagnostics(rootCtx context.Context) {
	d.runHeadersWaitingListener(rootCtx)
	d.runWriteHeadersListener(rootCtx)
	d.runCanonicalMarkerListener(rootCtx)
	d.runProcessedListener(rootCtx)
}

func (d *DiagnosticClient) GetHeaders() Headers {
	return d.headers
}

func (d *DiagnosticClient) runHeadersWaitingListener(rootCtx context.Context) {
	go func() {
		ctx, ch, cancel := Context[HeadersWaitingUpdate](context.Background(), 1)
		defer cancel()

		StartProviders(ctx, TypeOf(HeadersWaitingUpdate{}), log.Root())
		for {
			select {
			case <-rootCtx.Done():
				cancel()
				return
			case info := <-ch:
				d.headerMutex.Lock()
				d.headers.WaitingForHeaders = info.From
				d.headerMutex.Unlock()

				return
			}
		}
	}()
}

func (d *DiagnosticClient) runWriteHeadersListener(rootCtx context.Context) {
	go func() {
		ctx, ch, cancel := Context[BlockHeadersUpdate](context.Background(), 1)
		defer cancel()

		StartProviders(ctx, TypeOf(BlockHeadersUpdate{}), log.Root())
		for {
			select {
			case <-rootCtx.Done():
				cancel()
				return
			case info := <-ch:
				d.headerMutex.Lock()s
				d.headers.WriteHeaders = info
				d.headerMutex.Unlock()

				return
			}
		}
	}()
}

func (d *DiagnosticClient) runCanonicalMarkerListener(rootCtx context.Context) {
	go func() {
		ctx, ch, cancel := Context[HeaderCanonicalMarkerUpdate](context.Background(), 1)
		defer cancel()

		StartProviders(ctx, TypeOf(HeaderCanonicalMarkerUpdate{}), log.Root())
		for {
			select {
			case <-rootCtx.Done():
				cancel()
				return
			case info := <-ch:
				d.headerMutex.Lock()
				d.headers.CanonicalMarker = info
				d.headerMutex.Unlock()

				return
			}
		}
	}()
}

func (d *DiagnosticClient) runProcessedListener(rootCtx context.Context) {
	go func() {
		ctx, ch, cancel := Context[HeadersProcessedUpdate](context.Background(), 1)
		defer cancel()

		StartProviders(ctx, TypeOf(HeadersProcessedUpdate{}), log.Root())
		for {
			select {
			case <-rootCtx.Done():
				cancel()
				return
			case info := <-ch:
				d.headerMutex.Lock()
				d.headers.Processed = info
				d.headerMutex.Unlock()

				return
			}
		}
	}()
}
