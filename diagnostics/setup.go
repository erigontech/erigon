package diagnostics

import (
	"net/http"
	"strings"

	"github.com/ledgerwatch/erigon/turbo/node"
	"github.com/urfave/cli/v2"
)

func Setup(ctx *cli.Context, metricsMux *http.ServeMux, node *node.ErigonNode) {
	debugMux := http.NewServeMux()

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
	SetupPeersAccess(ctx, debugMux, node)

}
