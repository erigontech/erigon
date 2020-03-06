package main

import (
	"fmt"
	"io"
	"io/ioutil"
	"net/http"
	"os"
	"os/signal"
	"runtime"
	"sort"
	"syscall"

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
}

func main() {
	if err := app.Run(os.Args); err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
}

func tester(ctx *cli.Context) error {
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
	if len(ctx.Args()) < 1 {
		addr, err := ioutil.ReadFile(p2p.EnodeAddressFileName)
		if err != nil {
			return err
		}
		enodeAddress = string(addr)
	} else {
		enodeAddress = ctx.Args()[0]
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
	blockGen, err := NewBlockGenerator("blocks", 50000)
	defer blockGen.Close()
	if err != nil {
		panic(fmt.Sprintf("Failed to create block generator: %v", err))
	}
	tp.blockFeeder = blockGen
	tp.forkBase = 49998
	tp.forkHeight = 5
	tp.forkFeeder, err = NewForkGenerator(blockGen, "forkblocks", tp.forkBase, tp.forkHeight)
	defer tp.forkFeeder.Close()
	if err != nil {
		panic(fmt.Sprintf("Failed to create fork generator: %v", err))
	}
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
	}
	server := &p2p.Server{Config: p2pConfig}
	// Add protocol
	if err := server.Start(); err != nil {
		panic(fmt.Sprintf("Could not start server: %v", err))
	}
	server.AddPeer(nodeToConnect)

	sigs := make(chan os.Signal, 1)
	interruptCh := make(chan bool, 1)
	signal.Notify(sigs, syscall.SIGINT, syscall.SIGTERM)

	go func() {
		<-sigs
		interruptCh <- true
	}()

	_ = <-interruptCh
	return nil
}
