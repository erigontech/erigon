package commands

import (
	"fmt"
	"io"
	"os"
	"runtime"
	"runtime/pprof"
	"strings"

	"github.com/spf13/cobra"

	"github.com/ledgerwatch/turbo-geth/log"
	"github.com/ledgerwatch/turbo-geth/node"
)

type Config struct {
	remoteDbAddress  string
	rpcListenAddress string
	rpcPort          int
	rpcCORSDomain    string
	rpcVirtualHost   string
	rpcAPI           string
}

var (
	cpuprofile     string
	cpuProfileFile io.WriteCloser

	memprofile string
	cfg        Config
)

func init() {
	rootCmd.PersistentFlags().StringVar(&cpuprofile, "cpuprofile", "", "write cpu profile `file`")
	rootCmd.PersistentFlags().StringVar(&memprofile, "memprofile", "", "write memory profile `file`")
	rootCmd.Flags().StringVar(&cfg.remoteDbAddress, "remote-db-addr", "localhost:9999", "address of remote DB listener of a turbo-geth node")
	rootCmd.Flags().StringVar(&cfg.rpcListenAddress, "rpcaddr", node.DefaultHTTPHost, "HTTP-RPC server listening interface")
	rootCmd.Flags().IntVar(&cfg.rpcPort, "rpcport", node.DefaultHTTPPort, "HTTP-RPC server listening port")
	rootCmd.Flags().StringVar(&cfg.rpcCORSDomain, "rpccorsdomain", "", "Comma separated list of domains from which to accept cross origin requests (browser enforced)")
	rootCmd.Flags().StringVar(&cfg.rpcVirtualHost, "rpcvhosts", strings.Join(node.DefaultConfig.HTTPVirtualHosts, ","), "Comma separated list of virtual hostnames from which to accept requests (server enforced). Accepts '*' wildcard.")
	rootCmd.Flags().StringVar(&cfg.rpcAPI, "rpcapi", "", "API's offered over the HTTP-RPC interface")
}

var rootCmd = &cobra.Command{
	Use:   "rpcdaemon",
	Short: "rpcdaemon is JSON RPC server that connects to turbo-geth node for remote DB access",
	PersistentPreRun: func(cmd *cobra.Command, args []string) {
		startProfilingIfNeeded()

	},
	PersistentPostRun: func(cmd *cobra.Command, args []string) {
		stopProfilingIfNeeded()
	},
	RunE: func(cmd *cobra.Command, args []string) error {
		daemon(cfg)
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
