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
	_ "net/http/pprof"
	"os"
	"os/signal"
	"runtime"
	"syscall"

	metrics2 "github.com/VictoriaMetrics/metrics"
	"github.com/ledgerwatch/erigon/metrics"
	"github.com/ledgerwatch/erigon/metrics/exp"
	"github.com/ledgerwatch/log/v3"
	"github.com/spf13/cobra"
	"github.com/urfave/cli"
)

var (
	verbosityFlag = cli.IntFlag{
		Name:  "verbosity",
		Usage: "Logging verbosity: 0=silent, 1=error, 2=warn, 3=info, 4=debug, 5=detail",
		Value: 3,
	}
	logjsonFlag = cli.BoolFlag{
		Name:  "log.json",
		Usage: "Format logs with JSON",
	}
	vmoduleFlag = cli.StringFlag{
		Name:  "vmodule",
		Usage: "Per-module verbosity: comma-separated list of <pattern>=<level> (e.g. eth/*=5,p2p=4)",
		Value: "",
	}
	backtraceAtFlag = cli.StringFlag{
		Name:  "backtrace",
		Usage: "Request a stack trace at a specific logging statement (e.g. \"block.go:271\")",
		Value: "",
	}
	metricsAddrFlag = cli.StringFlag{
		Name: "metrics.addr",
	}
	metricsPortFlag = cli.UintFlag{
		Name:  "metrics.port",
		Value: 6060,
	}
	debugFlag = cli.BoolFlag{
		Name:  "debug",
		Usage: "Prepends log messages with call-site location (file and line number)",
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
	memprofilerateFlag = cli.IntFlag{
		Name:  "pprof.memprofilerate",
		Usage: "Turn on memory profiling with the given rate",
		Value: runtime.MemProfileRate,
	}
	blockprofilerateFlag = cli.IntFlag{
		Name:  "pprof.blockprofilerate",
		Usage: "Turn on block profiling with the given rate",
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
	verbosityFlag, logjsonFlag, vmoduleFlag, backtraceAtFlag, debugFlag,
	pprofFlag, pprofAddrFlag, pprofPortFlag, memprofilerateFlag,
	blockprofilerateFlag, cpuprofileFlag, traceFlag,
}

//var glogger *log.GlogHandler

func init() {
	log.Root().SetHandler(log.LvlFilterHandler(log.LvlInfo, log.StderrHandler))
	//glogger = log.NewGlogHandler(log.StreamHandler(os.Stderr, log.TerminalFormat(false)))
	//glogger.Verbosity(log.LvlInfo)
	//log.Root().SetHandler(glogger)
}

func SetupCobra(cmd *cobra.Command) error {
	flags := cmd.Flags()
	lvl, err := flags.GetInt(verbosityFlag.Name)
	if err != nil {
		return err
	}

	/*
		dbg, err := flags.GetBool(debugFlag.Name)
		if err != nil {
			return err
		}
		vmodule, err := flags.GetString(vmoduleFlag.Name)
		if err != nil {
			return err
		}
		backtrace, err := flags.GetString(backtraceAtFlag.Name)
		if err != nil {
			return err
		}

		_, glogger = log.SetupDefaultTerminalLogger(log.Lvl(lvl), vmodule, backtrace)
		log.PrintOrigins(dbg)
	*/
	log.Root().SetHandler(log.LvlFilterHandler(log.Lvl(lvl), log.StderrHandler))

	memprofilerate, err := flags.GetInt(memprofilerateFlag.Name)
	if err != nil {
		return err
	}
	blockprofilerate, err := flags.GetInt(blockprofilerateFlag.Name)
	if err != nil {
		return err
	}
	traceFile, err := flags.GetString(traceFlag.Name)
	if err != nil {
		return err
	}
	cpuFile, err := flags.GetString(cpuprofileFlag.Name)
	if err != nil {
		return err
	}

	// profiling, tracing
	runtime.MemProfileRate = memprofilerate
	Handler.SetBlockProfileRate(blockprofilerate)
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

	go func() {
		c := make(chan os.Signal, 1)
		signal.Notify(c, syscall.SIGINT, syscall.SIGTERM)
		<-c
		Exit()
	}()
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
	//var ostream log.Handler
	//output := io.Writer(os.Stderr)
	if ctx.GlobalBool(logjsonFlag.Name) {
		log.Root().SetHandler(log.LvlFilterHandler(log.Lvl(ctx.GlobalInt(verbosityFlag.Name)), log.StreamHandler(os.Stderr, log.JsonFormat())))
		//ostream = log.StreamHandler(output, log.JsonFormat())
	} else {
		log.Root().SetHandler(log.LvlFilterHandler(log.Lvl(ctx.GlobalInt(verbosityFlag.Name)), log.StderrHandler))
	}
	//log.Root().SetHandler(ostream)

	/*
		glogger.SetHandler(ostream)
		// logging
		log.PrintOrigins(ctx.GlobalBool(debugFlag.Name))
		_, glogger = log.SetupDefaultTerminalLogger(
			log.Lvl(ctx.GlobalInt(verbosityFlag.Name)),
			ctx.GlobalString(vmoduleFlag.Name),
			ctx.GlobalString(backtraceAtFlag.Name),
		)
	*/

	// profiling, tracing
	runtime.MemProfileRate = ctx.GlobalInt(memprofilerateFlag.Name)

	Handler.SetBlockProfileRate(ctx.GlobalInt(blockprofilerateFlag.Name))

	if traceFile := ctx.GlobalString(traceFlag.Name); traceFile != "" {
		if err := Handler.StartGoTrace(traceFile); err != nil {
			return err
		}
	}

	if cpuFile := ctx.GlobalString(cpuprofileFlag.Name); cpuFile != "" {
		if err := Handler.StartCPUProfile(cpuFile); err != nil {
			return err
		}
	}
	pprofEnabled := ctx.GlobalBool(pprofFlag.Name)
	metricsAddr := ctx.GlobalString(metricsAddrFlag.Name)

	if metrics.Enabled && (!pprofEnabled || metricsAddr != "") {
		metricsPort := ctx.GlobalInt(metricsPortFlag.Name)
		address := fmt.Sprintf("%s:%d", metricsAddr, metricsPort)
		exp.Setup(address)
	}

	// pprof server
	if pprofEnabled {
		pprofHost := ctx.GlobalString(pprofAddrFlag.Name)
		pprofPort := ctx.GlobalInt(pprofPortFlag.Name)
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
			metrics2.WritePrometheus(w, true)
		})
	}
	cpuMsg := fmt.Sprintf("go tool pprof -lines -http=: http://%s/%s", address, "debug/pprof/profile?seconds=20")
	heapMsg := fmt.Sprintf("go tool pprof -lines -http=: http://%s/%s", address, "debug/pprof/heap")
	log.Info("Starting pprof server", "cpu", cpuMsg, "heap", heapMsg)
	go func() {
		if err := http.ListenAndServe(address, nil); err != nil {
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
