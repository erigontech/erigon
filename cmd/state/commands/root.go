package commands

import (
	"context"
	"fmt"
	"io"
	"os"
	"os/signal"
	"runtime"
	"runtime/pprof"
	"syscall"

	"github.com/ledgerwatch/turbo-geth/log"
	"github.com/spf13/cobra"
)

var (
	cpuprofile     string
	cpuProfileFile io.WriteCloser

	memprofile string
)

func init() {
	rootCmd.PersistentFlags().StringVar(&cpuprofile, "cpuprofile", "", "write cpu profile `file`")
	rootCmd.PersistentFlags().StringVar(&memprofile, "memprofile", "", "write memory profile `file`")
}

func getContext() context.Context {
	ctx, cancel := context.WithCancel(context.Background())
	go func() {
		ch := make(chan os.Signal, 1)
		signal.Notify(ch, os.Interrupt, syscall.SIGTERM)
		defer signal.Stop(ch)

		select {
		case <-ch:
			log.Info("Got interrupt, shutting down...")
		case <-ctx.Done():
		}

		cancel()
	}()
	return ctx
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
