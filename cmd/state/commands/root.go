package commands

import (
	"fmt"
	"io"
	"os"
	"runtime"
	"runtime/pprof"

	"github.com/spf13/cobra"

	"github.com/ledgerwatch/turbo-geth/log"
)

var (
	cpuprofile     string
	cpuProfileFile io.WriteCloser

	memprofile string
)

func init() {
	rootCmd.PersistentFlags().StringVar(&cpuprofile, "cpuprofile", "", "write cpu profile `file`")
	rootCmd.PersistentFlags().StringVar(&cpuprofile, "memprofile", "", "write memory profile `file`")
}

var rootCmd = &cobra.Command{
	Use:   "state",
	Short: "state is a utility for Stateless ethereum clients",
	PersistentPreRun: func(cmd *cobra.Command, args []string) {
		startProfilingIfNeeded()

	},
	PersistentPostRun: func(cmd *cobra.Command, args []string) {
		stopProfilingIfNeeded()
	},
	RunE: func(cmd *cobra.Command, args []string) error {
		stateless.GasLimits(chaindata)
		return nil
	},
}

func Execute() {
	if err := rootCmd.Execute(); err != nil {
		fmt.Println(err)
		os.Exit(1)
	}
}

func startProfilingIfNeeded() {
	if cpuprofile != "" {
		fmt.Println("starting CPU profiling")
		cpuProfileFile, err := os.Create(cpuprofile)
		if err != nil {
			log.Error("could not create CPU profile", "error", err)
			return
		}
		if err := pprof.StartCPUProfile(cpuProfileFile); err != nil {
			log.Error("could not start CPU profile", "error", err)
			return
		}
	}
}

func stopProfilingIfNeeded() {
	if cpuprofile != "" {
		fmt.Println("stopping CPU profiling")
		pprof.StopCPUProfile()
	}

	if cpuProfileFile != nil {
		cpuProfileFile.Close()
	}
	if memprofile != "" {
		f, err := os.Create(memprofile)
		if err != nil {
			log.Error("could not create mem profile", "error", err)
			return
		}
		runtime.GC() // get up-to-date statistics
		if err := pprof.WriteHeapProfile(f); err != nil {
			log.Error("could not write memory profile", "error", err)
			return
		}
	}
}
