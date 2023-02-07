package app

import (
	"fmt"
	"net/http"

	"github.com/ledgerwatch/erigon/cmd/utils"
	"github.com/urfave/cli/v2"
)

var diagnosticsMockCommand = cli.Command{
	Action:    MigrateFlags(diagnosticsMock),
	Name:      "diagnostics",
	Usage:     "Run mock of the diagnostics system",
	ArgsUsage: "--diag.mock.addr <Diagnostics mock addr: Diagnostics mock port>",
	Flags: []cli.Flag{
		&utils.DiagnosticsMockAddrFlag,
	},
	Category: "SUPPORT COMMANDS",
	Description: `
The diagnostics starts a web server which provides simplified (mock) representation of the diagnostics system.`,
}

type Page struct {
	Title string
	Body  []byte
}

func giveRequestsHandler(w http.ResponseWriter, r *http.Request) {
	fmt.Fprintf(w, "Give requests %s", r.URL.Path[1:])
}

func takeResponsesHandler(w http.ResponseWriter, r *http.Request) {
	fmt.Fprintf(w, "Take responses %s", r.URL.Path[1:])
}

func showHandler(w http.ResponseWriter, r *http.Request) {
	fmt.Fprintf(w, "Show %s", r.URL.Path[1:])
}

func diagnosticsMock(ctx *cli.Context) error {
	http.HandleFunc("/giveRequests", giveRequestsHandler)
	http.HandleFunc("/takeResponses", takeResponsesHandler)
	http.HandleFunc("/show", showHandler)

	err := http.ListenAndServe(":8080", nil)
	if err != nil {
		return err
	}
	return nil
}
