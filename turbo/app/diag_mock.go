package app

import (
	"context"
	"fmt"
	"net/http"
	"os"
	"os/signal"
	"regexp"
	"syscall"
	"time"

	"github.com/ledgerwatch/erigon/cmd/utils"
	"github.com/ledgerwatch/log/v3"
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

var validPath = regexp.MustCompile("^/([a-zA-Z0-9]+)/(giveRequests|takeResponses|show)$")

func (dh *DiagHandler) giveRequestsHandler(w http.ResponseWriter, r *http.Request) {
loop:
	for {
		select {
		case request := <-dh.requestCh:
			fmt.Fprintf(w, "%s\n", request)
		default:
			break loop
		}
	}
}

func takeResponsesHandler(w http.ResponseWriter, r *http.Request) {
	fmt.Fprintf(w, "Take responses %s", r.URL.Path)
}

func showHandler(w http.ResponseWriter, r *http.Request) {
	fmt.Fprintf(w, "Show %s", r.URL.Path)
}

type DiagHandler struct {
	// Requests can be of arbitrary types, they get converted to a string and sent to the support subcommand
	requestCh chan interface{}
}

func (dh *DiagHandler) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	m := validPath.FindStringSubmatch(r.URL.Path)
	if m == nil {
		http.NotFound(w, r)
		return
	}
	// User token is the first submatch m[1]
	// TODO: validate the token
	switch m[2] {
	case "giveRequests":
		dh.giveRequestsHandler(w, r)
	case "takeResponses":
		takeResponsesHandler(w, r)
	case "show":
		showHandler(w, r)
	default:
		http.NotFound(w, r)
	}
}

func (dh *DiagHandler) RequestNodeInfo() {
	dh.requestCh <- "nodeInfo"
}

func diagnosticsMock(ctx *cli.Context) error {
	sigs := make(chan os.Signal, 1)
	interruptCh := make(chan bool, 1)
	signal.Notify(sigs, syscall.SIGINT, syscall.SIGTERM)

	go func() {
		<-sigs
		interruptCh <- true
	}()
	dh := &DiagHandler{requestCh: make(chan interface{}, 1024)}
	s := &http.Server{
		Addr:           ":8080",
		Handler:        dh,
		ReadTimeout:    10 * time.Second,
		WriteTimeout:   10 * time.Second,
		MaxHeaderBytes: 1 << 20,
	}
	// Initial request for node info
	dh.RequestNodeInfo()
	interrupt := false
	go func() {
		err := s.ListenAndServe()
		if err != nil {
			log.Error("Running server problem", "err", err)
		}
	}()
	pollInterval := 500 * time.Millisecond
	pollEvery := time.NewTicker(pollInterval)
	defer pollEvery.Stop()
	for !interrupt {
		select {
		case interrupt = <-interruptCh:
			log.Info(fmt.Sprintf("interrupted, please wait for cleanup"))
		case <-pollEvery.C:
		}
	}
	s.Shutdown(context.Background())
	return nil
}
