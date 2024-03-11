package diagnostics

import (
	"net/http"
	"strings"

	"github.com/ledgerwatch/log/v3"
	"github.com/urfave/cli/v2"

	"github.com/ledgerwatch/erigon/turbo/node"
)

func Setup(ctx *cli.Context, metricsMux *http.ServeMux, node *node.ErigonNode, logger log.Logger) {
	debugMux := http.NewServeMux()

	diagnostic := NewDiagnosticClient(ctx, debugMux, node, logger)
	diagnostic.Setup()

	metricsMux.HandleFunc("/debug/", func(w http.ResponseWriter, r *http.Request) {
		r.URL.Path = strings.TrimPrefix(r.URL.Path, "/debug")
		r.URL.RawPath = strings.TrimPrefix(r.URL.RawPath, "/debug")
		debugMux.ServeHTTP(w, r)
	})

	SetupLogsAccess(ctx, debugMux)
	SetupDbAccess(ctx, debugMux)
	SetupCmdLineAccess(debugMux)
	SetupFlagsAccess(ctx, debugMux)
	SetupVersionAccess(debugMux)
	SetupBlockBodyDownload(debugMux)
	SetupHeaderDownloadStats(debugMux)
	SetupNodeInfoAccess(debugMux, node)
	SetupPeersAccess(ctx, debugMux, node, diagnostic)
	SetupBootnodesAccess(debugMux, node)
	SetupStagesAccess(debugMux, diagnostic)
	SetupBlockMetricsAccess(debugMux, diagnostic)
	SetupMemAccess(debugMux)

}
