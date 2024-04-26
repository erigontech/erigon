package diagnostics

import (
	"fmt"
	"net/http"
	"strings"

	"github.com/urfave/cli/v2"

	diaglib "github.com/ledgerwatch/erigon-lib/diagnostics"
	"github.com/ledgerwatch/erigon/turbo/node"
	"github.com/ledgerwatch/log/v3"
)

var (
	diagnosticsDisabledFlag = "diagnostics.disabled"
	diagnosticsAddrFlag     = "diagnostics.addr"
	diagnosticsPortFlag     = "diagnostics.port"
)

func Setup(ctx *cli.Context, node *node.ErigonNode) {
	if ctx.Bool(diagnosticsDisabledFlag) {
		return
	}

	diagMux := SetupDiagnosticsEndpoint(ctx)
	diagnostic := diaglib.NewDiagnosticClient(diagMux, node.Backend().DataDir())
	diagnostic.Setup()

	SetupEndpoints(ctx, node, diagMux, diagnostic)
}

func SetupDiagnosticsEndpoint(ctx *cli.Context) *http.ServeMux {
	address := ctx.String(diagnosticsAddrFlag)
	port := ctx.Uint(diagnosticsPortFlag)

	diagnosticsAddress := fmt.Sprintf("%s:%d", address, port)

	diagMux := http.NewServeMux()

	diagServer := &http.Server{
		Addr:    diagnosticsAddress,
		Handler: diagMux,
	}

	go func() {
		if err := diagServer.ListenAndServe(); err != nil {
			log.Error("[Diagnostics] Failure in running diagnostics server", "err", err)
		}
	}()

	diagMux.HandleFunc("/diag/", func(w http.ResponseWriter, r *http.Request) {
		r.URL.Path = strings.TrimPrefix(r.URL.Path, "/diag")
		r.URL.RawPath = strings.TrimPrefix(r.URL.RawPath, "/diag")
		diagMux.ServeHTTP(w, r)
	})

	return diagMux
}

func SetupEndpoints(ctx *cli.Context, node *node.ErigonNode, diagMux *http.ServeMux, diagnostic *diaglib.DiagnosticClient) {
	SetupLogsAccess(ctx, diagMux)
	SetupDbAccess(ctx, diagMux)
	SetupCmdLineAccess(diagMux)
	SetupFlagsAccess(ctx, diagMux)
	SetupVersionAccess(diagMux)
	SetupBlockBodyDownload(diagMux)
	SetupHeaderDownloadStats(diagMux)
	SetupNodeInfoAccess(diagMux, node)
	SetupPeersAccess(ctx, diagMux, node, diagnostic)
	SetupBootnodesAccess(diagMux, node)
	SetupStagesAccess(diagMux, diagnostic)
	SetupMemAccess(diagMux)
	SetupHeadersAccess(diagMux, diagnostic)
	SetupBodiesAccess(diagMux, diagnostic)
}
