package diagnostics

import (
	"context"

	"github.com/ledgerwatch/erigon-lib/common"
	"github.com/ledgerwatch/log/v3"
)

func (d *DiagnosticClient) setupHeadersDiagnostics() {
	d.runHeadersWaitingListener()
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
