//go:build windows

package datasource

import (
	"context"

	log "github.com/erigontech/erigon/common/log/v3"
)

// InfoAllStages is intentionally a no-op on Windows builds. The rest of the
// monitor stays functional (node control, logs, downloader, system health),
// while stage DB polling remains disabled until a Windows-safe backend is added.
func InfoAllStages(ctx context.Context, _ log.Logger, _ string, _ chan<- *StagesInfo) error {
	<-ctx.Done()
	return nil
}
