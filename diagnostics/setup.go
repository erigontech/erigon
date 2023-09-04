package diagnostics

import (
	"net/http"

	"github.com/ledgerwatch/erigon/turbo/node"
	"github.com/urfave/cli/v2"
)

func Setup(ctx *cli.Context, metricsMux *http.ServeMux, node *node.ErigonNode) {
	SetupLogsAccess(ctx, metricsMux)
	SetupDbAccess(ctx, metricsMux)
	SetupCmdLineAccess(metricsMux)
	SetupFlagsAccess(ctx, metricsMux)
	SetupVersionAccess(metricsMux)
	SetupBlockBodyDownload(metricsMux)
	SetupHeaderDownloadStats(metricsMux)
	SetupNodeInfoAccess(metricsMux, node)
}
