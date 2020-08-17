package cli

import (
	"context"
	"github.com/ledgerwatch/turbo-geth/cmd/rpcdaemon/commands"
	"github.com/ledgerwatch/turbo-geth/log"
	"github.com/ledgerwatch/turbo-geth/node"
	"github.com/mattn/go-colorable"
	"github.com/mattn/go-isatty"
	"github.com/spf13/cobra"
	"io"
	"os"
	"os/signal"
	"strings"
	"syscall"
)

type Flags struct {
	PrivateApiAddr    string
	Chaindata         string
	HttpListenAddress string
	HttpPort          int
	HttpCORSDomain    string
	HttpVirtualHost   string
	API               string
	Gascap            uint64
}

var (
	DefaultConfig Flags
)

var rootCmd = &cobra.Command{
	Use:   "rpcdaemon",
	Short: "rpcdaemon is JSON RPC server that connects to turbo-geth node for remote DB access",
	RunE: func(cmd *cobra.Command, args []string) error {
		commands.Daemon(cmd, DefaultConfig)
		return nil
	},
}

func RootCommand() *cobra.Command {
	rootCmd.Flags().StringVar(&DefaultConfig.PrivateApiAddr, "private.api.addr", "", "private api network address, for example: 127.0.0.1:9090, empty string means not to start the listener. do not expose to public network. serves remote database interface")
	rootCmd.Flags().StringVar(&DefaultConfig.Chaindata, "chaindata", "", "path to the database")
	rootCmd.Flags().StringVar(&DefaultConfig.HttpListenAddress, "http.addr", node.DefaultHTTPHost, "HTTP-RPC server listening interface")
	rootCmd.Flags().IntVar(&DefaultConfig.HttpPort, "http.port", node.DefaultHTTPPort, "HTTP-RPC server listening port")
	rootCmd.Flags().StringVar(&DefaultConfig.HttpCORSDomain, "http.corsdomain", "", "Comma separated list of domains from which to accept cross origin requests (browser enforced)")
	rootCmd.Flags().StringVar(&DefaultConfig.HttpVirtualHost, "http.vhosts", strings.Join(node.DefaultConfig.HTTPVirtualHosts, ","), "Comma separated list of virtual hostnames from which to accept requests (server enforced). Accepts '*' wildcard.")
	rootCmd.Flags().StringVar(&DefaultConfig.API, "http.api", "", "API's offered over the HTTP-RPC interface")
	rootCmd.Flags().Uint64Var(&DefaultConfig.Gascap, "rpc.gascap", 0, "Sets a cap on gas that can be used in eth_call/estimateGas")

	return rootCmd
}

func RootContext() context.Context {
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

func SetupDefaultLogger(lvl log.Lvl) {
	var (
		ostream log.Handler
		glogger *log.GlogHandler
	)

	usecolor := (isatty.IsTerminal(os.Stderr.Fd()) || isatty.IsCygwinTerminal(os.Stderr.Fd())) && os.Getenv("TERM") != "dumb"
	output := io.Writer(os.Stderr)
	if usecolor {
		output = colorable.NewColorableStderr()
	}
	ostream = log.StreamHandler(output, log.TerminalFormat(usecolor))
	glogger = log.NewGlogHandler(ostream)
	log.Root().SetHandler(glogger)
	glogger.Verbosity(lvl)
}
