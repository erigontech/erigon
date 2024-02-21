package diagnostics

import (
	"github.com/ledgerwatch/erigon-lib/common/dbg"
	"github.com/ledgerwatch/log/v3"
	"net/http"
	"strings"
	"time"

	"github.com/ledgerwatch/erigon/turbo/node"
	"github.com/urfave/cli/v2"
)

func Setup(ctx *cli.Context, metricsMux *http.ServeMux, node *node.ErigonNode, logger log.Logger) {
	debugMux := http.NewServeMux()

	diagnostic := NewDiagnosticClient(ctx, debugMux, node)
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
	SetupPeersAccess(ctx, debugMux, node)
	SetupBootnodesAccess(debugMux, node)
	SetupStagesAccess(debugMux, diagnostic)
	SetupMemAccess(debugMux)

	// setup periodic logging and prometheus updates
	go func() {
		logEvery := time.NewTicker(180 * time.Second)
		defer logEvery.Stop()

		for {
			select {
			case <-ctx.Done():
				return
			case <-logEvery.C:
				memStats, err := dbg.ReadVirtualMemStats()
				if err != nil {
					logger.Warn("[mem] error reading virtual memory stats", "err", err)
				}

				logger.Info("[mem] virtual memory stats", memStats)
				dbg.UpdatePrometheusVirtualMemStats(memStats)
			}
		}
	}()
}
