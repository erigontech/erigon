package diagnostics_test

import (
	"context"
	"testing"
	"time"

	"github.com/ledgerwatch/erigon-lib/diagnostics"
	"github.com/ledgerwatch/log/v3"
)

type testInfo struct {
	count int
}

func (ti testInfo) Type() diagnostics.Type {
	return diagnostics.TypeOf(ti)
}

func StartDiagnostics(ctx context.Context) error {
	timer := time.NewTicker(1 * time.Second)
	defer timer.Stop()

	var count int

	for {
		select {
		case <-ctx.Done():
			return nil
		case <-timer.C:
			diagnostics.Send(testInfo{count})
			count++
		}
	}
}

func TestProviderRegistration(t *testing.T) {

	// diagnostics receiver
	ctx, ch, cancel := diagnostics.Context[testInfo](context.Background(), 1)
	diagnostics.StartProviders(ctx, diagnostics.TypeOf(testInfo{}), log.Root())

	go StartDiagnostics(ctx)

	for info := range ch {
		if info.count == 3 {
			cancel()
		}
	}
}
