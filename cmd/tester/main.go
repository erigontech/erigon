package main

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"io/ioutil"
	"math/rand"
	"net/http"
	"net/http/pprof"
	"os"
	"os/signal"
	"runtime"
	"sort"
	"syscall"
	"time"

	//nolint:gosec
	_ "net/http/pprof"

	"github.com/ledgerwatch/turbo-geth/cmd/utils"
	"github.com/ledgerwatch/turbo-geth/console"
	"github.com/ledgerwatch/turbo-geth/crypto"
	"github.com/ledgerwatch/turbo-geth/eth"
	"github.com/ledgerwatch/turbo-geth/internal/debug"
	"github.com/ledgerwatch/turbo-geth/log"
	"github.com/ledgerwatch/turbo-geth/p2p"
	"github.com/ledgerwatch/turbo-geth/p2p/enode"
	"github.com/ledgerwatch/turbo-geth/params"
	"github.com/mattn/go-colorable"
	"github.com/mattn/go-isatty"
	"github.com/urfave/cli"
)

func init() {
	// go tool pprof -http=:8081 http://localhost:6060/
	_ = pprof.Handler // just to avoid adding manually: import _ "net/http/pprof"
	go func() {
		r := rand.New(rand.NewSource(int64(time.Now().Nanosecond())))
		randomPort := func(min, max int) int {
			return r.Intn(max-min) + min
		}
		port := randomPort(6000, 7000)

		fmt.Fprintf(os.Stderr, "go tool pprof -lines -http=: :%d/%s\n", port, "\\?seconds\\=20")
		fmt.Fprintf(os.Stderr, "go tool pprof -lines -http=: :%d/%s\n", port, "debug/pprof/heap")
		fmt.Fprintf(os.Stderr, "%s\n", http.ListenAndServe(fmt.Sprintf("127.0.0.1:%d", port), nil))
	}()
}

var (
	// Git SHA1 commit hash of the release (set via linker flags)
	gitCommit = ""
	gitDate   = ""
	// The app that holds all commands and flags.
	app = utils.NewApp(gitCommit, "", "Ethereum Tester")
	// flags that configure the node
	flags = []cli.Flag{}
)

func init() {
	// Initialize the CLI app and start Geth
	app.Action = tester
	app.HideVersion = true // we have a command to print the version
	app.Copyright = "Copyright 2018 The go-ethereum Authors"
	app.Commands = []cli.Command{}
	sort.Sort(cli.CommandsByName(app.Commands))

	app.Flags = append(app.Flags, flags...)

	app.Before = func(ctx *cli.Context) error {
		runtime.GOMAXPROCS(runtime.NumCPU())
		if err := debug.Setup(ctx); err != nil {
			return err
		}
		return nil
	}

	app.After = func(ctx *cli.Context) error {
		debug.Exit()
		console.Stdin.Close() // Resets terminal mode.
		return nil
	}

	app.Commands = []cli.Command{
		{
			Action:    utils.MigrateFlags(genesisCmd),
			Name:      "genesis",
			Usage:     "Initialize the signer, generate secret storage",
			ArgsUsage: "",
			Description: `
The init command generates a master seed which Clef can use to store credentials and data needed for
the rule-engine to work.`,
		},
	}

}

func main() {
	if err := app.Run(os.Args); err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
}

func genesisCmd(c *cli.Context) error {
	res, err := json.Marshal(genesis())
	if err != nil {
		return err
	}
	_, err = fmt.Fprintf(os.Stdout, string(res))
	if err != nil {
		return err
	}
	return nil
}

func rootContext() context.Context {
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

func tester(cliCtx *cli.Context) error {
	ctx := rootContext()

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
	glogger.Verbosity(log.LvlTrace)

	go func() {
		log.Info("HTTP", "error", http.ListenAndServe("localhost:6060", nil))
	}()

	var enodeAddress string
	if len(cliCtx.Args()) < 1 {
		addr, err := ioutil.ReadFile(p2p.EnodeAddressFileName)
		if err != nil {
			return err
		}
		enodeAddress = string(addr)
	} else {
		enodeAddress = cliCtx.Args()[0]
	}
	if enodeAddress == "" {
		fmt.Printf("Usage: tester <enode>\n")
		return nil
	}
	nodeToConnect, err := enode.ParseV4(enodeAddress)
	if err != nil {
		panic(fmt.Sprintf("Could not parse the node info: %v", err))
	}
	fmt.Printf("Parsed node: %s, IP: %s\n", nodeToConnect, nodeToConnect.IP())
	//fmt.Printf("%s %s\n", ctx.Args()[0], ctx.Args()[1])
	tp := NewTesterProtocol()
	//tp.blockFeeder, err = NewBlockAccessor(ctx.Args()[0]/*, ctx.Args()[1]*/)
	blockGen, err := NewBlockGenerator(ctx, "blocks", 60000)
	if err != nil {
		panic(fmt.Sprintf("Failed to create block generator: %v", err))
	}
	defer blockGen.Close()
	tp.blockFeeder = blockGen
	tp.forkBase = 49998
	tp.forkHeight = 5
	tp.forkFeeder, err = NewForkGenerator(ctx, blockGen, "forkblocks", tp.forkBase, tp.forkHeight)
	if err != nil {
		panic(fmt.Sprintf("Failed to create fork generator: %v", err))
	}
	defer tp.forkFeeder.Close()
	tp.protocolVersion = uint32(eth.ProtocolVersions[2])
	tp.networkId = 1 // Mainnet
	tp.genesisBlockHash = params.MainnetGenesisHash
	serverKey, err := crypto.GenerateKey()
	if err != nil {
		panic(fmt.Sprintf("Failed to generate server key: %v", err))
	}
	p2pConfig := p2p.Config{}
	p2pConfig.PrivateKey = serverKey
	p2pConfig.Name = "geth tester"
	p2pConfig.Logger = log.New()
	p2pConfig.MaxPeers = 1
	p2pConfig.Protocols = []p2p.Protocol{
		{
			Name:    eth.ProtocolName,
			Version: eth.ProtocolVersions[2],
			Length:  eth.ProtocolLengths[eth.ProtocolVersions[2]],
			Run:     tp.protocolRun,
		},
		{
			Name:    eth.DebugName,
			Version: eth.DebugVersions[0],
			Length:  eth.DebugLengths[eth.DebugVersions[0]],
			Run:     tp.debugProtocolRun,
		},
	}
	server := &p2p.Server{Config: p2pConfig}
	// Add protocol
	if err := server.Start(); err != nil {
		panic(fmt.Sprintf("Could not start server: %v", err))
	}
	server.AddPeer(nodeToConnect)

	_ = <-ctx.Done()
	return nil
}
