package observer

import (
	"context"
	"errors"
	"runtime"
	"time"

	"github.com/spf13/cobra"
	"github.com/urfave/cli/v2"

	"github.com/ledgerwatch/erigon/cmd/utils"
	"github.com/ledgerwatch/erigon/turbo/debug"
	"github.com/ledgerwatch/erigon/turbo/logging"
	"github.com/ledgerwatch/log/v3"
)

type CommandFlags struct {
	DataDir         string
	StatusLogPeriod time.Duration

	Chain     string
	Bootnodes string

	ListenPort  int
	NATDesc     string
	NetRestrict string

	NodeKeyFile string
	NodeKeyHex  string

	CrawlerConcurrency uint
	RefreshTimeout     time.Duration
	MaxPingTries       uint

	KeygenTimeout     time.Duration
	KeygenConcurrency uint

	HandshakeRefreshTimeout time.Duration
	HandshakeRetryDelay     time.Duration
	HandshakeMaxTries       uint

	ErigonLogPath string
}

type Command struct {
	command cobra.Command
	flags   CommandFlags
}

func NewCommand() *Command {
	command := cobra.Command{
		Short: "P2P network crawler",
	}

	// debug flags
	utils.CobraFlags(&command, debug.Flags, utils.MetricFlags, logging.Flags)

	instance := Command{
		command: command,
	}

	instance.withDatadir()
	instance.withStatusLogPeriod()

	instance.withChain()
	instance.withBootnodes()

	instance.withListenPort()
	instance.withNAT()
	instance.withNetRestrict()

	instance.withNodeKeyFile()
	instance.withNodeKeyHex()

	instance.withCrawlerConcurrency()
	instance.withRefreshTimeout()
	instance.withMaxPingTries()

	instance.withKeygenTimeout()
	instance.withKeygenConcurrency()

	instance.withHandshakeRefreshTimeout()
	instance.withHandshakeRetryDelay()
	instance.withHandshakeMaxTries()

	instance.withErigonLogPath()

	return &instance
}

func (command *Command) withDatadir() {
	flag := utils.DataDirFlag
	command.command.Flags().StringVar(&command.flags.DataDir, flag.Name, flag.Value.String(), flag.Usage)
	must(command.command.MarkFlagDirname(utils.DataDirFlag.Name))
}

func (command *Command) withStatusLogPeriod() {
	flag := cli.DurationFlag{
		Name:  "status-log-period",
		Usage: "How often to log status summaries",
		Value: 10 * time.Second,
	}
	command.command.Flags().DurationVar(&command.flags.StatusLogPeriod, flag.Name, flag.Value, flag.Usage)
}

func (command *Command) withChain() {
	flag := utils.ChainFlag
	command.command.Flags().StringVar(&command.flags.Chain, flag.Name, flag.Value, flag.Usage)
}

func (command *Command) withBootnodes() {
	flag := utils.BootnodesFlag
	command.command.Flags().StringVar(&command.flags.Bootnodes, flag.Name, flag.Value, flag.Usage)
}

func (command *Command) withListenPort() {
	flag := utils.ListenPortFlag
	command.command.Flags().IntVar(&command.flags.ListenPort, flag.Name, flag.Value, flag.Usage)
}

func (command *Command) withNAT() {
	flag := utils.NATFlag
	command.command.Flags().StringVar(&command.flags.NATDesc, flag.Name, flag.Value, flag.Usage)
}

func (command *Command) withNetRestrict() {
	flag := utils.NetrestrictFlag
	command.command.Flags().StringVar(&command.flags.NetRestrict, flag.Name, flag.Value, flag.Usage)
}

func (command *Command) withNodeKeyFile() {
	flag := utils.NodeKeyFileFlag
	command.command.Flags().StringVar(&command.flags.NodeKeyFile, flag.Name, flag.Value, flag.Usage)
}

func (command *Command) withNodeKeyHex() {
	flag := utils.NodeKeyHexFlag
	command.command.Flags().StringVar(&command.flags.NodeKeyHex, flag.Name, flag.Value, flag.Usage)
}

func (command *Command) withCrawlerConcurrency() {
	flag := cli.UintFlag{
		Name:  "crawler-concurrency",
		Usage: "A number of maximum parallel node interrogations",
		Value: uint(runtime.GOMAXPROCS(-1)) * 10,
	}
	command.command.Flags().UintVar(&command.flags.CrawlerConcurrency, flag.Name, flag.Value, flag.Usage)
}

func (command *Command) withRefreshTimeout() {
	flag := cli.DurationFlag{
		Name:  "refresh-timeout",
		Usage: "A timeout to wait before considering to re-crawl a node",
		Value: 2 * 24 * time.Hour,
	}
	command.command.Flags().DurationVar(&command.flags.RefreshTimeout, flag.Name, flag.Value, flag.Usage)
}

func (command *Command) withMaxPingTries() {
	flag := cli.UintFlag{
		Name:  "max-ping-tries",
		Usage: "How many times to try PING before applying exponential back-off logic",
		Value: 3,
	}
	command.command.Flags().UintVar(&command.flags.MaxPingTries, flag.Name, flag.Value, flag.Usage)
}

func (command *Command) withKeygenTimeout() {
	flag := cli.DurationFlag{
		Name:  "keygen-timeout",
		Usage: "How much time can be used to generate node bucket keys",
		Value: 2 * time.Second,
	}
	command.command.Flags().DurationVar(&command.flags.KeygenTimeout, flag.Name, flag.Value, flag.Usage)
}

func (command *Command) withKeygenConcurrency() {
	flag := cli.UintFlag{
		Name:  "keygen-concurrency",
		Usage: "How many parallel goroutines can be used by the node bucket keys generator",
		Value: 2,
	}
	command.command.Flags().UintVar(&command.flags.KeygenConcurrency, flag.Name, flag.Value, flag.Usage)
}

func (command *Command) withHandshakeRefreshTimeout() {
	flag := cli.DurationFlag{
		Name:  "handshake-refresh-timeout",
		Usage: "When a node's handshake data is considered expired and needs to be re-crawled",
		Value: 20 * 24 * time.Hour,
	}
	command.command.Flags().DurationVar(&command.flags.HandshakeRefreshTimeout, flag.Name, flag.Value, flag.Usage)
}

func (command *Command) withHandshakeRetryDelay() {
	flag := cli.DurationFlag{
		Name:  "handshake-retry-delay",
		Usage: "How long to wait before retrying a failed handshake",
		Value: 4 * time.Hour,
	}
	command.command.Flags().DurationVar(&command.flags.HandshakeRetryDelay, flag.Name, flag.Value, flag.Usage)
}

func (command *Command) withHandshakeMaxTries() {
	flag := cli.UintFlag{
		Name:  "handshake-max-tries",
		Usage: "How many times to retry handshake before abandoning a candidate",
		Value: 3,
	}
	command.command.Flags().UintVar(&command.flags.HandshakeMaxTries, flag.Name, flag.Value, flag.Usage)
}

func (command *Command) withErigonLogPath() {
	flag := cli.StringFlag{
		Name:  "erigon-log",
		Usage: "Erigon log file path. Enables sentry candidates intake.",
	}
	command.command.Flags().StringVar(&command.flags.ErigonLogPath, flag.Name, flag.Value, flag.Usage)
}

func (command *Command) ExecuteContext(ctx context.Context, runFunc func(ctx context.Context, flags CommandFlags, logger log.Logger) error) error {
	command.command.PersistentPostRun = func(cmd *cobra.Command, args []string) {
		debug.Exit()
	}
	command.command.RunE = func(cmd *cobra.Command, args []string) error {
		logger := debug.SetupCobra(cmd, "sentry")
		defer debug.Exit()
		err := runFunc(cmd.Context(), command.flags, logger)
		if errors.Is(err, context.Canceled) {
			return nil
		}
		return err
	}
	return command.command.ExecuteContext(ctx)
}

func (command *Command) AddSubCommand(subCommand *cobra.Command) {
	command.command.AddCommand(subCommand)
}

func must(err error) {
	if err != nil {
		panic(err)
	}
}
