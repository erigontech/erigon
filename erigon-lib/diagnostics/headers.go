package diagnostics

import (
	"context"

	"github.com/ledgerwatch/erigon-lib/common"
	"github.com/ledgerwatch/log/v3"
)

func (d *DiagnosticClient) setupHeadersDiagnostics() {
	d.runHeadersWaitingListener()
	d.runWriteHeadersListener()
	d.runCanonicalMarkerListener()
	d.runProcessedListener()
}

func (d *DiagnosticClient) GetHeaders() Headers {
	return d.headers
}

func (d *DiagnosticClient) runHeadersWaitingListener() {
	go func() {
		ctx, ch, cancel := Context[HeadersWaitingUpdate](context.Background(), 1)
		defer cancel()

		rootCtx, _ := common.RootContext()

		StartProviders(ctx, TypeOf(HeadersWaitingUpdate{}), log.Root())
		for {
			select {
			case <-rootCtx.Done():
				cancel()
				return
			case info := <-ch:
				d.mu.Lock()
				d.headers.WaitingForHeaders = info.From
				d.mu.Unlock()

				return
			}
		}
	}()
}

func (d *DiagnosticClient) runWriteHeadersListener() {
	go func() {
		ctx, ch, cancel := Context[BlockHeadersUpdate](context.Background(), 1)
		defer cancel()

		rootCtx, _ := common.RootContext()

		StartProviders(ctx, TypeOf(BlockHeadersUpdate{}), log.Root())
		for {
			select {
			case <-rootCtx.Done():
				cancel()
				return
			case info := <-ch:
				d.mu.Lock()
				d.headers.WriteHeaders = info
				d.mu.Unlock()

				return
			}
		}
	}()
}

func (d *DiagnosticClient) runCanonicalMarkerListener() {
	go func() {
		ctx, ch, cancel := Context[HeaderCanonicalMarkerUpdate](context.Background(), 1)
		defer cancel()

		rootCtx, _ := common.RootContext()

		StartProviders(ctx, TypeOf(HeaderCanonicalMarkerUpdate{}), log.Root())
		for {
			select {
			case <-rootCtx.Done():
				cancel()
				return
			case info := <-ch:
				d.mu.Lock()
				d.headers.CanonicalMarker = info
				d.mu.Unlock()

				return
			}
		}
	}()
}

func (d *DiagnosticClient) runProcessedListener() {
	go func() {
		ctx, ch, cancel := Context[HeadersProcessedUpdate](context.Background(), 1)
		defer cancel()

		rootCtx, _ := common.RootContext()

		StartProviders(ctx, TypeOf(HeadersProcessedUpdate{}), log.Root())
		for {
			select {
			case <-rootCtx.Done():
				cancel()
				return
			case info := <-ch:
				d.mu.Lock()
				d.headers.Processed = info
				d.mu.Unlock()

				return
			}
		}
	}()
}
