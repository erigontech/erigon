// Copyright 2016 The go-ethereum Authors
// (original work)
// Copyright 2024 The Erigon Authors
// (modifications)
// This file is part of Erigon.
//
// Erigon is free software: you can redistribute it and/or modify
// it under the terms of the GNU Lesser General Public License as published by
// the Free Software Foundation, either version 3 of the License, or
// (at your option) any later version.
//
// Erigon is distributed in the hope that it will be useful,
// but WITHOUT ANY WARRANTY; without even the implied warranty of
// MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE. See the
// GNU Lesser General Public License for more details.
//
// You should have received a copy of the GNU Lesser General Public License
// along with Erigon. If not, see <http://www.gnu.org/licenses/>.

package debug

import (
	"errors"
	"fmt"
	"net/http"
	"net/http/pprof" //nolint:gosec
	"os"
	"path/filepath"

	"github.com/erigontech/erigon/erigon-lib/common/disk"
	"github.com/erigontech/erigon/erigon-lib/common/mem"
	"github.com/erigontech/erigon/erigon-lib/metrics"

	"github.com/pelletier/go-toml"
	"github.com/spf13/cobra"
	"github.com/urfave/cli/v2"
	"gopkg.in/yaml.v2"

	"github.com/erigontech/erigon/erigon-lib/log/v3"

	"github.com/erigontech/erigon/common/fdlimit"
	"github.com/erigontech/erigon/turbo/logging"
)

var (
	//nolint
	vmoduleFlag = cli.StringFlag{
		Name:  "vmodule",
		Usage: "Per-module verbosity: comma-separated list of <pattern>=<level> (e.g. eth/*=5,p2p=4)",
		Value: "",
	}
	configFlag = cli.StringFlag{
		Name: "config",
	}
	metricsEnabledFlag = cli.BoolFlag{
		Name: "metrics",
	}
	metricsAddrFlag = cli.StringFlag{
		Name:  "metrics.addr",
		Usage: "Prometheus HTTP server listening interface",
		Value: "127.0.0.1",
	}
	metricsPortFlag = cli.UintFlag{
		Name:  "metrics.port",
		Value: 6061,
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

// SetupCobra sets up logging, profiling and tracing for cobra commands
func SetupCobra(cmd *cobra.Command, filePrefix string) log.Logger {
	// ensure we've read in config file details before setting up metrics etc.
	if err := SetCobraFlagsFromConfigFile(cmd); err != nil {
		log.Warn("failed setting config flags from yaml/toml file", "err", err)
		panic(err)
	}
	RaiseFdLimit()
	flags := cmd.Flags()

	logger := logging.SetupLoggerCmd(filePrefix, cmd)

	traceFile, err := flags.GetString(traceFlag.Name)
	if err != nil {
		log.Error("failed setting config flags from yaml/toml file", "err", err)
		panic(err)
	}
	cpuFile, err := flags.GetString(cpuprofileFlag.Name)
	if err != nil {
		log.Error("failed setting config flags from yaml/toml file", "err", err)
		panic(err)
	}

	// profiling, tracing
	if traceFile != "" {
		if err2 := Handler.StartGoTrace(traceFile); err2 != nil {
			return logger
		}
	}
	if cpuFile != "" {
		if err2 := Handler.StartCPUProfile(cpuFile); err2 != nil {
			return logger
		}
	}

	go ListenSignals(nil, logger)
	pprof, err := flags.GetBool(pprofFlag.Name)
	if err != nil {
		log.Error("failed setting config flags from yaml/toml file", "err", err)
		panic(err)
	}
	pprofAddr, err := flags.GetString(pprofAddrFlag.Name)
	if err != nil {
		log.Error("failed setting config flags from yaml/toml file", "err", err)
		panic(err)
	}
	pprofPort, err := flags.GetInt(pprofPortFlag.Name)
	if err != nil {
		log.Error("failed setting config flags from yaml/toml file", "err", err)
		panic(err)
	}

	metricsEnabled, err := flags.GetBool(metricsEnabledFlag.Name)
	if err != nil {
		log.Error("failed setting config flags from yaml/toml file", "err", err)
		panic(err)
	}
	metricsAddr, err := flags.GetString(metricsAddrFlag.Name)
	if err != nil {
		log.Error("failed setting config flags from yaml/toml file", "err", err)
		panic(err)
	}
	metricsPort, err := flags.GetInt(metricsPortFlag.Name)
	if err != nil {
		log.Error("failed setting config flags from yaml/toml file", "err", err)
		panic(err)
	}

	// setup periodic logging and prometheus updates
	go mem.LogMemStats(cmd.Context(), log.Root())
	go disk.UpdateDiskStats(cmd.Context(), log.Root())

	var metricsMux *http.ServeMux
	var metricsAddress string

	if metricsEnabled && metricsAddr != "" {
		metricsAddress = fmt.Sprintf("%s:%d", metricsAddr, metricsPort)
		metricsMux = metrics.Setup(metricsAddress, logger)
	}

	if pprof {
		address := fmt.Sprintf("%s:%d", pprofAddr, pprofPort)
		if address == metricsAddress {
			StartPProf(address, metricsMux)
		} else {
			StartPProf(address, nil)
		}
	}

	return logger
}

// Setup initializes profiling and logging based on the CLI flags.
// It should be called as early as possible in the program.
func Setup(ctx *cli.Context, rootLogger bool) (log.Logger, *http.ServeMux, *http.ServeMux, error) {
	// ensure we've read in config file details before setting up metrics etc.
	if err := SetFlagsFromConfigFile(ctx); err != nil {
		log.Warn("failed setting config flags from yaml/toml file", "err", err)
	}

	RaiseFdLimit()

	logger := logging.SetupLoggerCtx("erigon", ctx, log.LvlInfo, log.LvlInfo, rootLogger)

	if traceFile := ctx.String(traceFlag.Name); traceFile != "" {
		if err := Handler.StartGoTrace(traceFile); err != nil {
			return logger, nil, nil, err
		}
	}

	if cpuFile := ctx.String(cpuprofileFlag.Name); cpuFile != "" {
		if err := Handler.StartCPUProfile(cpuFile); err != nil {
			return logger, nil, nil, err
		}
	}
	pprofEnabled := ctx.Bool(pprofFlag.Name)
	metricsEnabled := ctx.Bool(metricsEnabledFlag.Name)
	metricsAddr := ctx.String(metricsAddrFlag.Name)

	var metricsMux *http.ServeMux
	var metricsAddress string

	if metricsEnabled {
		metricsPort := ctx.Int(metricsPortFlag.Name)
		metricsAddress = fmt.Sprintf("%s:%d", metricsAddr, metricsPort)
		metricsMux = metrics.Setup(metricsAddress, logger)
	}

	if pprofEnabled {
		pprofHost := ctx.String(pprofAddrFlag.Name)
		pprofPort := ctx.Int(pprofPortFlag.Name)
		address := fmt.Sprintf("%s:%d", pprofHost, pprofPort)
		if (address == metricsAddress) && metricsEnabled {
			metricsMux = StartPProf(address, metricsMux)
		} else {
			pprofMux := StartPProf(address, nil)
			return logger, metricsMux, pprofMux, nil
		}
	}

	return logger, metricsMux, nil, nil
}

func StartPProf(address string, metricsMux *http.ServeMux) *http.ServeMux {
	cpuMsg := fmt.Sprintf("go tool pprof -lines -http=: http://%s/%s", address, "debug/pprof/profile?seconds=20")
	heapMsg := fmt.Sprintf("go tool pprof -lines -http=: http://%s/%s", address, "debug/pprof/heap")
	log.Info("Starting pprof server", "cpu", cpuMsg, "heap", heapMsg)

	if metricsMux == nil {
		pprofMux := http.NewServeMux()

		pprofMux.HandleFunc("/debug/pprof/", pprof.Index)
		pprofMux.HandleFunc("/debug/pprof/cmdline", pprof.Cmdline)
		pprofMux.HandleFunc("/debug/pprof/profile", pprof.Profile)
		pprofMux.HandleFunc("/debug/pprof/symbol", pprof.Symbol)
		pprofMux.HandleFunc("/debug/pprof/trace", pprof.Trace)

		pprofServer := &http.Server{
			Addr:    address,
			Handler: pprofMux,
		}

		go func() {
			if err := pprofServer.ListenAndServe(); err != nil {
				log.Error("Failure in running pprof server", "err", err)
			}
		}()

		return pprofMux
	} else {
		metricsMux.HandleFunc("/debug/pprof/", pprof.Index)
		metricsMux.HandleFunc("/debug/pprof/cmdline", pprof.Cmdline)
		metricsMux.HandleFunc("/debug/pprof/profile", pprof.Profile)
		metricsMux.HandleFunc("/debug/pprof/symbol", pprof.Symbol)
		metricsMux.HandleFunc("/debug/pprof/trace", pprof.Trace)

		return metricsMux
	}
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

var (
	metricsConfigs = []string{metricsEnabledFlag.Name, metricsAddrFlag.Name, metricsPortFlag.Name}
)

func SetFlagsFromConfigFile(ctx *cli.Context) error {
	filePath := ctx.String(configFlag.Name)
	if filePath == "" {
		return nil
	}

	fileConfig, err := readConfigAsMap(filePath)
	if err != nil {
		return err
	}

	for _, flag := range metricsConfigs {
		if v, ok := fileConfig[flag]; ok {
			err = ctx.Set(flag, fmt.Sprintf("%v", v))
			if err != nil {
				return err
			}
		}
	}

	return nil
}

func SetCobraFlagsFromConfigFile(cmd *cobra.Command) error {
	flags := cmd.Flags()

	// flag might not be set for any reason so just exit if we're not aware of it
	exists := flags.Lookup(configFlag.Name)
	if exists == nil {
		return nil
	}

	filePath, err := flags.GetString(configFlag.Name)
	if err != nil {
		return err
	}
	if filePath == "" {
		return nil
	}

	fileConfig, err := readConfigAsMap(filePath)
	if err != nil {
		return err
	}

	for _, flag := range metricsConfigs {
		if v, ok := fileConfig[flag]; ok {
			err = flags.Set(flag, fmt.Sprintf("%v", v))
			if err != nil {
				return err
			}
		}
	}

	return nil
}

func readConfigAsMap(filePath string) (map[string]interface{}, error) {
	fileExtension := filepath.Ext(filePath)

	fileConfig := make(map[string]interface{})

	if fileExtension == ".yaml" || fileExtension == ".yml" {
		yamlFile, err := os.ReadFile(filePath)
		if err != nil {
			return fileConfig, err
		}
		err = yaml.Unmarshal(yamlFile, fileConfig)
		if err != nil {
			return fileConfig, err
		}
	} else if fileExtension == ".toml" {
		tomlFile, err := os.ReadFile(filePath)
		if err != nil {
			return fileConfig, err
		}
		err = toml.Unmarshal(tomlFile, &fileConfig)
		if err != nil {
			return fileConfig, err
		}
	} else {
		return fileConfig, errors.New("config files only accepted are .yaml, .yml, and .toml")
	}

	return fileConfig, nil
}
