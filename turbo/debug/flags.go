// Copyright 2016 The go-ethereum Authors
// This file is part of the go-ethereum library.
//
// The go-ethereum library is free software: you can redistribute it and/or modify
// it under the terms of the GNU Lesser General Public License as published by
// the Free Software Foundation, either version 3 of the License, or
// (at your option) any later version.
//
// The go-ethereum library is distributed in the hope that it will be useful,
// but WITHOUT ANY WARRANTY; without even the implied warranty of
// MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE. See the
// GNU Lesser General Public License for more details.
//
// You should have received a copy of the GNU Lesser General Public License
// along with the go-ethereum library. If not, see <http://www.gnu.org/licenses/>.

package debug

import (
	"fmt"
	"net/http"
	_ "net/http/pprof" //nolint:gosec

	metrics2 "github.com/VictoriaMetrics/metrics"
	"github.com/ledgerwatch/erigon-lib/common/metrics"
	"github.com/ledgerwatch/erigon/common/fdlimit"
	"github.com/ledgerwatch/erigon/diagnostics"
	"github.com/ledgerwatch/erigon/metrics/exp"
	"github.com/ledgerwatch/erigon/turbo/logging"
	"github.com/ledgerwatch/log/v3"
	"github.com/spf13/cobra"
	"github.com/urfave/cli/v2"
)

var (
	//nolint
	vmoduleFlag = cli.StringFlag{
		Name:  "vmodule",
		Usage: "Per-module verbosity: comma-separated list of <pattern>=<level> (e.g. eth/*=5,p2p=4)",
		Value: "",
	}
	metricsAddrFlag = cli.StringFlag{
		Name: "metrics.addr",
	}
	metricsPortFlag = cli.UintFlag{
		Name:  "metrics.port",
		Value: 6060,
	}
	pprofFlag = cli.BoolFlag{
		Name:  "pprof",
		Usage: "Enable the pprof HTTP server",
	}
	pprofPortFlag = cli.IntFlag{
		Name:  "pprof.port",
		Usage: "pprof HTTP server listening port",
		Value: 6060,
	}
	pprofAddrFlag = cli.StringFlag{
		Name:  "pprof.addr",
		Usage: "pprof HTTP server listening interface",
		Value: "127.0.0.1",
	}
	cpuprofileFlag = cli.StringFlag{
		Name:  "pprof.cpuprofile",
		Usage: "Write CPU profile to the given file",
	}
	traceFlag = cli.StringFlag{
		Name:  "trace",
		Usage: "Write execution trace to the given file",
	}
)

// Flags holds all command-line flags required for debugging.
var Flags = []cli.Flag{
	&pprofFlag, &pprofAddrFlag, &pprofPortFlag,
	&cpuprofileFlag, &traceFlag,
}

func SetupCobra(cmd *cobra.Command) error {
	RaiseFdLimit()
	flags := cmd.Flags()

	_ = logging.GetLoggerCmd("debug", cmd)

	traceFile, err := flags.GetString(traceFlag.Name)
	if err != nil {
		return err
	}
	cpuFile, err := flags.GetString(cpuprofileFlag.Name)
	if err != nil {
		return err
	}

	// profiling, tracing
	if traceFile != "" {
		if err2 := Handler.StartGoTrace(traceFile); err2 != nil {
			return err2
		}
	}
	if cpuFile != "" {
		if err2 := Handler.StartCPUProfile(cpuFile); err2 != nil {
			return err2
		}
	}

	go ListenSignals(nil)
	pprof, err := flags.GetBool(pprofFlag.Name)
	if err != nil {
		return err
	}
	pprofAddr, err := flags.GetString(pprofAddrFlag.Name)
	if err != nil {
		return err
	}
	pprofPort, err := flags.GetInt(pprofPortFlag.Name)
	if err != nil {
		return err
	}

	metricsAddr, err := flags.GetString(metricsAddrFlag.Name)
	if err != nil {
		return err
	}
	metricsPort, err := flags.GetInt(metricsPortFlag.Name)
	if err != nil {
		return err
	}

	if metrics.Enabled && metricsAddr != "" {
		address := fmt.Sprintf("%s:%d", metricsAddr, metricsPort)
		exp.Setup(address)
	}

	withMetrics := metrics.Enabled && metricsAddr == ""
	if pprof {
		// metrics and pprof server
		StartPProf(fmt.Sprintf("%s:%d", pprofAddr, pprofPort), withMetrics)
	}
	return nil
}

// Setup initializes profiling and logging based on the CLI flags.
// It should be called as early as possible in the program.
func Setup(ctx *cli.Context) error {
	RaiseFdLimit()

	_ = logging.GetLoggerCtx("debug", ctx)

	if traceFile := ctx.String(traceFlag.Name); traceFile != "" {
		if err := Handler.StartGoTrace(traceFile); err != nil {
			return err
		}
	}

	if cpuFile := ctx.String(cpuprofileFlag.Name); cpuFile != "" {
		if err := Handler.StartCPUProfile(cpuFile); err != nil {
			return err
		}
	}
	pprofEnabled := ctx.Bool(pprofFlag.Name)
	metricsAddr := ctx.String(metricsAddrFlag.Name)

	if metrics.Enabled && (!pprofEnabled || metricsAddr != "") {
		metricsPort := ctx.Int(metricsPortFlag.Name)
		address := fmt.Sprintf("%s:%d", metricsAddr, metricsPort)
		exp.Setup(address)
		diagnostics.SetupLogsAccess(ctx)
		diagnostics.SetupDbAccess(ctx)
	}

	// pprof server
	if pprofEnabled {
		pprofHost := ctx.String(pprofAddrFlag.Name)
		pprofPort := ctx.Int(pprofPortFlag.Name)
		address := fmt.Sprintf("%s:%d", pprofHost, pprofPort)
		// This context value ("metrics.addr") represents the utils.MetricsHTTPFlag.Name.
		// It cannot be imported because it will cause a cyclical dependency.
		withMetrics := metrics.Enabled && metricsAddr == ""
		StartPProf(address, withMetrics)
	}
	return nil
}

func StartPProf(address string, withMetrics bool) {
	// Hook go-metrics into expvar on any /debug/metrics request, load all vars
	// from the registry into expvar, and execute regular expvar handler.
	if withMetrics {
		http.HandleFunc("/debug/metrics/prometheus", func(w http.ResponseWriter, req *http.Request) {
			w.Header().Set("Access-Control-Allow-Origin", "*")
			metrics2.WritePrometheus(w, true)
		})
	}
	cpuMsg := fmt.Sprintf("go tool pprof -lines -http=: http://%s/%s", address, "debug/pprof/profile?seconds=20")
	heapMsg := fmt.Sprintf("go tool pprof -lines -http=: http://%s/%s", address, "debug/pprof/heap")
	log.Info("Starting pprof server", "cpu", cpuMsg, "heap", heapMsg)
	go func() {
		if err := http.ListenAndServe(address, nil); err != nil { // nolint:gosec
			log.Error("Failure in running pprof server", "err", err)
		}
	}()
}

// Exit stops all running profiles, flushing their output to the
// respective file.
func Exit() {
	_ = Handler.StopCPUProfile()
	_ = Handler.StopGoTrace()
}

// RaiseFdLimit raises out the number of allowed file handles per process
func RaiseFdLimit() {
	limit, err := fdlimit.Maximum()
	if err != nil {
		log.Error("Failed to retrieve file descriptor allowance", "err", err)
		return
	}
	if _, err = fdlimit.Raise(uint64(limit)); err != nil {
		log.Error("Failed to raise file descriptor allowance", "err", err)
	}
}
