// Package cli contains framework for building a command-line based Erigon node.
package cli

import (
	"encoding/json"
	"os"

	"github.com/ledgerwatch/erigon-lib/kv"
	"github.com/ledgerwatch/erigon/cmd/utils"
	"github.com/ledgerwatch/erigon/core"
	"github.com/ledgerwatch/erigon/internal/debug"
	"github.com/ledgerwatch/erigon/internal/flags"
	"github.com/ledgerwatch/erigon/node"
	"github.com/ledgerwatch/erigon/params"

	"github.com/ledgerwatch/log/v3"
	"github.com/urfave/cli"
)

// MakeApp creates a cli application (based on `github.com/urlfave/cli` package).
// The application exits when `action` returns.
// Parameters:
// * action: the main function for the application. receives `*cli.Context` with parsed command-line flags. Returns no error, if some error could not be recovered from write to the log or panic.
// * cliFlags: the list of flags `cli.Flag` that the app should set and parse. By default, use `DefaultFlags()`. If you want to specify your own flag, use `append(DefaultFlags(), myFlag)` for this parameter.
func MakeApp(action func(*cli.Context), cliFlags []cli.Flag) *cli.App {
	app := flags.NewApp("", "", "erigon experimental cli")
	app.Action = action
	app.Flags = append(cliFlags, debug.Flags...) // debug flags are required
	app.Before = func(ctx *cli.Context) error {
		return debug.Setup(ctx)
	}
	app.After = func(ctx *cli.Context) error {
		debug.Exit()
		return nil
	}
	app.Commands = []cli.Command{initCommand}
	return app
}

var initCommand = cli.Command{
	Action:    MigrateFlags(initGenesis),
	Name:      "init",
	Usage:     "Bootstrap and initialize a new genesis block",
	ArgsUsage: "<genesisPath>",
	Flags: []cli.Flag{
		utils.DataDirFlag,
	},
	Category: "BLOCKCHAIN COMMANDS",
	Description: `
The init command initializes a new genesis block and definition for the network.
This is a destructive action and changes the network in which you will be
participating.

It expects the genesis file as argument.`,
}

// initGenesis will initialise the given JSON format genesis file and writes it as
// the zero'd block (i.e. genesis) or will fail hard if it can't succeed.
func initGenesis(ctx *cli.Context) error {
	// Make sure we have a valid genesis JSON
	genesisPath := ctx.Args().First()
	if len(genesisPath) == 0 {
		utils.Fatalf("Must supply path to genesis JSON file")
	}

	file, err := os.Open(genesisPath)
	if err != nil {
		utils.Fatalf("Failed to read genesis file: %v", err)
	}
	defer file.Close()

	genesis := new(core.Genesis)
	if err := json.NewDecoder(file).Decode(genesis); err != nil {
		utils.Fatalf("invalid genesis file: %v", err)
	}

	// Open and initialise both full and light databases
	stack := MakeConfigNodeDefault(ctx)
	defer stack.Close()

	chaindb, err := node.OpenDatabase(stack.Config(), log.New(ctx), kv.ChainDB)
	if err != nil {
		utils.Fatalf("Failed to open database: %v", err)
	}
	_, hash, err := core.CommitGenesisBlock(chaindb, genesis)
	if err != nil {
		utils.Fatalf("Failed to write genesis block: %v", err)
	}
	chaindb.Close()
	log.Info("Successfully wrote genesis state", "hash", hash.Hash())
	return nil
}

func MigrateFlags(action func(ctx *cli.Context) error) func(*cli.Context) error {
	return func(ctx *cli.Context) error {
		for _, name := range ctx.FlagNames() {
			if ctx.IsSet(name) {
				ctx.GlobalSet(name, ctx.String(name))
			}
		}
		return action(ctx)
	}
}

func NewNodeConfig(ctx *cli.Context) *node.Config {
	nodeConfig := node.DefaultConfig
	// see simiar changes in `cmd/geth/config.go#defaultNodeConfig`
	if commit := params.GitCommit; commit != "" {
		nodeConfig.Version = params.VersionWithCommit(commit, "")
	} else {
		nodeConfig.Version = params.Version
	}
	nodeConfig.IPCPath = "" // force-disable IPC endpoint
	nodeConfig.Name = "erigon"
	if ctx.GlobalIsSet(utils.DataDirFlag.Name) {
		nodeConfig.DataDir = ctx.GlobalString(utils.DataDirFlag.Name)
	}
	return &nodeConfig
}

func MakeConfigNodeDefault(ctx *cli.Context) *node.Node {
	return makeConfigNode(NewNodeConfig(ctx))
}

func makeConfigNode(config *node.Config) *node.Node {
	stack, err := node.New(config)
	if err != nil {
		utils.Fatalf("Failed to create Erigon node: %v", err)
	}

	return stack
}
