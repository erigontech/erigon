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
	diagnosticsDisabledFlag = cli.BoolFlag{
		Name: "diagnostics.disabled",
	}
	diagnosticsAddrFlag = cli.StringFlag{
		Name:  "diagnostics.addr",
		Value: "127.0.0.1",
	}
	diagnosticsPortFlag = cli.UintFlag{
		Name:  "diagnostics.port",
		Value: 6059,
	}
)

func Setup(ctx *cli.Context, node *node.ErigonNode) {
	if ctx.Bool(diagnosticsDisabledFlag.Name) {
		return
	}

	diagMux := SetupDiagnosticsEndpoint(ctx)
	diagnostic := diaglib.NewDiagnosticClient(diagMux, node.Backend().DataDir())
	diagnostic.Setup()

	SetupEndpoints(ctx, node, diagMux, diagnostic)
}

func SetupDiagnosticsEndpoint(ctx *cli.Context) *http.ServeMux {
	address := diagnosticsAddrFlag.Value
	if ctx.String(diagnosticsAddrFlag.Name) != "" {
		address = ctx.String(diagnosticsAddrFlag.Name)
	}

	port := diagnosticsPortFlag.Value
	if ctx.Uint(diagnosticsPortFlag.Name) != 0 {
		port = ctx.Uint(diagnosticsPortFlag.Name)
	}

	diagnosticsAddress := fmt.Sprintf("%s:%d", address, port)

	diagMux := http.NewServeMux()

	promServer := &http.Server{
		Addr:    diagnosticsAddress,
		Handler: diagMux,
	}

	go func() {
		if err := promServer.ListenAndServe(); err != nil {
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
