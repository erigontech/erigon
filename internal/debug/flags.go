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
	"io"
	"net/http"
	"net/http/pprof"
	"os"
	"os/signal"
	"runtime"
	"syscall"
	"time"

	"github.com/fjl/memsize/memsizeui"
	"github.com/ledgerwatch/turbo-geth/log"
	"github.com/ledgerwatch/turbo-geth/metrics"
	"github.com/ledgerwatch/turbo-geth/metrics/exp"
	colorable "github.com/mattn/go-colorable"
	"github.com/mattn/go-isatty"
	"github.com/spf13/cobra"
	"github.com/urfave/cli"
)

var Memsize memsizeui.Handler

var (
	verbosityFlag = cli.IntFlag{
		Name:  "verbosity",
		Usage: "Logging verbosity: 0=silent, 1=error, 2=warn, 3=info, 4=debug, 5=detail",
		Value: 3,
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
	// (Deprecated April 2020)
	legacyPprofPortFlag = cli.IntFlag{
		Name:  "pprofport",
		Usage: "pprof HTTP server listening port (deprecated, use --pprof.port)",
		Value: 6060,
	}
	legacyPprofAddrFlag = cli.StringFlag{
		Name:  "pprofaddr",
		Usage: "pprof HTTP server listening interface (deprecated, use --pprof.addr)",
		Value: "127.0.0.1",
	}
	legacyMemprofilerateFlag = cli.IntFlag{
		Name:  "memprofilerate",
		Usage: "Turn on memory profiling with the given rate (deprecated, use --pprof.memprofilerate)",
		Value: runtime.MemProfileRate,
	}
	legacyBlockprofilerateFlag = cli.IntFlag{
		Name:  "blockprofilerate",
		Usage: "Turn on block profiling with the given rate (deprecated, use --pprof.blockprofilerate)",
	}
	legacyCpuprofileFlag = cli.StringFlag{
		Name:  "cpuprofile",
		Usage: "Write CPU profile to the given file (deprecated, use --pprof.cpuprofile)",
	}
)

// Flags holds all command-line flags required for debugging.
var Flags = []cli.Flag{
	verbosityFlag, vmoduleFlag, backtraceAtFlag, debugFlag,
	pprofFlag, pprofAddrFlag, pprofPortFlag, memprofilerateFlag,
	blockprofilerateFlag, cpuprofileFlag, traceFlag,
}

var DeprecatedFlags = []cli.Flag{
	legacyPprofPortFlag, legacyPprofAddrFlag, legacyMemprofilerateFlag,
	legacyBlockprofilerateFlag, legacyCpuprofileFlag,
}

var (
	ostream log.Handler
	glogger *log.GlogHandler
)

func init() {
	usecolor := (isatty.IsTerminal(os.Stderr.Fd()) || isatty.IsCygwinTerminal(os.Stderr.Fd())) && os.Getenv("TERM") != "dumb"
	output := io.Writer(os.Stderr)
	if usecolor {
		output = colorable.NewColorableStderr()
	}
	ostream = log.StreamHandler(output, log.TerminalFormat(usecolor))
	glogger = log.NewGlogHandler(ostream)
}

func SetupCobra(cmd *cobra.Command) error {
	flags := cmd.Flags()
	dbg, err := flags.GetBool(debugFlag.Name)
	if err != nil {
		return err
	}
	lvl, err := flags.GetInt(verbosityFlag.Name)
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

	// logging
	log.PrintOrigins(dbg)
	glogger.Verbosity(log.Lvl(lvl))
	err = glogger.Vmodule(vmodule)
	if err != nil {
		return err
	}
	if backtrace != "" {
		err = glogger.BacktraceAt(backtrace)
		if err != nil {
			return err
		}
	}
	log.Root().SetHandler(glogger)

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

	// metrics and pprof server
	StartPProf(pprof, metrics.Enabled, fmt.Sprintf("%s:%d", pprofAddr, pprofPort))

	return nil
}

// Setup initializes profiling and logging based on the CLI flags.
// It should be called as early as possible in the program.
func Setup(ctx *cli.Context) error {
	// logging
	log.PrintOrigins(ctx.GlobalBool(debugFlag.Name))
	glogger.Verbosity(log.Lvl(ctx.GlobalInt(verbosityFlag.Name)))
	glogger.Vmodule(ctx.GlobalString(vmoduleFlag.Name))
	glogger.BacktraceAt(ctx.GlobalString(backtraceAtFlag.Name))
	log.Root().SetHandler(glogger)

	// profiling, tracing
	if ctx.GlobalIsSet(legacyMemprofilerateFlag.Name) {
		runtime.MemProfileRate = ctx.GlobalInt(legacyMemprofilerateFlag.Name)
		log.Warn("The flag --memprofilerate is deprecated and will be removed in the future, please use --pprof.memprofilerate")
	}
	runtime.MemProfileRate = ctx.GlobalInt(memprofilerateFlag.Name)

	if ctx.GlobalIsSet(legacyBlockprofilerateFlag.Name) {
		Handler.SetBlockProfileRate(ctx.GlobalInt(legacyBlockprofilerateFlag.Name))
		log.Warn("The flag --blockprofilerate is deprecated and will be removed in the future, please use --pprof.blockprofilerate")
	}
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
	if cpuFile := ctx.GlobalString(legacyCpuprofileFlag.Name); cpuFile != "" {
		log.Warn("The flag --cpuprofile is deprecated and will be removed in the future, please use --pprof.cpuprofile")
		if err := Handler.StartCPUProfile(cpuFile); err != nil {
			return err
		}
	}

	// pprof server
	listenHost := ctx.GlobalString(pprofAddrFlag.Name)
	if ctx.GlobalIsSet(legacyPprofAddrFlag.Name) && !ctx.GlobalIsSet(pprofAddrFlag.Name) {
		listenHost = ctx.GlobalString(legacyPprofAddrFlag.Name)
		log.Warn("The flag --pprofaddr is deprecated and will be removed in the future, please use --pprof.addr")
	}

	port := ctx.GlobalInt(pprofPortFlag.Name)
	if ctx.GlobalIsSet(legacyPprofPortFlag.Name) && !ctx.GlobalIsSet(pprofPortFlag.Name) {
		port = ctx.GlobalInt(legacyPprofPortFlag.Name)
		log.Warn("The flag --pprofport is deprecated and will be removed in the future, please use --pprof.port")
		Exit()
	}

	address := fmt.Sprintf("%s:%d", listenHost, port)
	StartPProf(ctx.GlobalBool(pprofFlag.Name), metrics.Enabled, address)
	return nil
}

func StartPProf(enablePprof bool, enableMetrics bool, address string) {
	mux := http.NewServeMux()
	if enablePprof {
		mux.HandleFunc("/debug/pprof/", pprof.Index)
		mux.HandleFunc("/debug/pprof/cmdline", pprof.Cmdline)
		mux.HandleFunc("/debug/pprof/profile", pprof.Profile)
		mux.HandleFunc("/debug/pprof/symbol", pprof.Symbol)
		mux.HandleFunc("/debug/pprof/trace", pprof.Trace)
		cpuMsg := fmt.Sprintf("go tool pprof -lines -http=: http://%s/%s", address, "?seconds=20")
		heapMsg := fmt.Sprintf("go tool pprof -lines -http=: http://%s/%s", address, "debug/pprof/heap")
		log.Info("Starting pprof server", "cpu", cpuMsg, "heap", heapMsg)
	}

	if enableMetrics {
		// Hook go-metrics into expvar on any /debug/metrics request, load all vars
		// from the registry into expvar, and execute regular expvar handler.
		exp.Exp(metrics.DefaultRegistry, mux)
		mux.Handle("/memsize/", http.StripPrefix("/memsize", &Memsize))
		// Start system runtime metrics collection
		go metrics.CollectProcessMetrics(3 * time.Second)
		log.Info("Starting metrics server", "addr", fmt.Sprintf("%s", address))
	}

	if enableMetrics || enablePprof {
		go func() {
			if err := http.ListenAndServe(address, mux); err != nil {
				log.Error("Failure in running metrics server", "err", err)
			}
		}()
	}
}

// Exit stops all running profiles, flushing their output to the
// respective file.
func Exit() {
	Handler.StopCPUProfile()
	Handler.StopGoTrace()
}
