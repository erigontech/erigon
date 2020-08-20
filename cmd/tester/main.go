package main

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io/ioutil"
	"os"
	"os/signal"
	"runtime"
	"sort"
	"syscall"

	"github.com/ledgerwatch/turbo-geth/cmd/utils"
	"github.com/ledgerwatch/turbo-geth/console/prompt"
	"github.com/ledgerwatch/turbo-geth/crypto"
	"github.com/ledgerwatch/turbo-geth/eth"
	"github.com/ledgerwatch/turbo-geth/internal/debug"
	ff "github.com/ledgerwatch/turbo-geth/internal/flags"
	"github.com/ledgerwatch/turbo-geth/log"
	"github.com/ledgerwatch/turbo-geth/p2p"
	"github.com/ledgerwatch/turbo-geth/p2p/enode"
	"github.com/urfave/cli"
)

var (
	// Git SHA1 commit hash of the release (set via linker flags)
	gitCommit = ""
	gitDate   = ""
	// The app that holds all commands and flags.
	app = ff.NewApp(gitCommit, gitDate, "Ethereum Tester")
	// flags that configure the node
	VerbosityFlag = cli.IntFlag{
		Name:  "verbosity",
		Usage: "Logging verbosity: 0=silent, 1=error, 2=warn, 3=info, 4=debug, 5=detail",
		Value: 3,
	}

	flags = []cli.Flag{VerbosityFlag}
)

func init() {

	// Initialize the CLI app and start TurboGeth
	app.Action = tester
	app.HideVersion = true // we have a command to print the version
	app.Copyright = "Copyright 2020 The turbo-geth Authors"
	app.Commands = []cli.Command{}
	sort.Sort(cli.CommandsByName(app.Commands))

	app.Flags = append(app.Flags, flags...)

	app.Before = func(ctx *cli.Context) error {
		log.SetupDefaultTerminalLogger(log.Lvl(ctx.GlobalInt(VerbosityFlag.Name)), "", "")
		runtime.GOMAXPROCS(runtime.NumCPU())
		if err := debug.Setup(ctx); err != nil {
			return err
		}
		return nil
	}

	app.After = func(ctx *cli.Context) error {
		debug.Exit()
		prompt.Stdin.Close() // Resets terminal mode.
		return nil
	}

	app.Commands = []cli.Command{
		{
			Action: utils.MigrateFlags(genesisCmd),
			Name:   "genesis",
			Usage:  "Produce genesis.json file for geth",
		},
	}

}

func main() {
	if err := app.Run(os.Args); err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
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
	nodeToConnect, err := getTargetAddr(cliCtx)
	if err != nil {
		return err
	}

	blockGen, err := NewBlockGenerator(ctx, "blocks", 50000)
	if err != nil {
		panic(fmt.Sprintf("Failed to create block generator: %v", err))
	}
	defer blockGen.Close()
	forkBase := uint64(49998)
	forkHeight := uint64(5)
	forkGen, err := NewForkGenerator(ctx, blockGen, "forkblocks", forkBase, forkHeight)
	if err != nil {
		panic(fmt.Sprintf("Failed to create fork generator: %v", err))
	}
	defer forkGen.Close()
	tp1 := NewTesterProtocol("tp1", true /* fork */, true /* debug */)
	tp1.blockFeeder = blockGen
	tp1.forkBase = forkBase
	tp1.forkHeight = forkHeight
	tp1.forkFeeder = forkGen
	tp1.protocolVersion = uint32(eth.ProtocolVersions[0])
	tp1.networkId = 1 // Mainnet
	tp1.genesisBlockHash = forkGen.Genesis().Hash()
	server1 := makeP2PServer(ctx, tp1, []string{eth.ProtocolName, eth.DebugName})
	// Add protocol
	if err := server1.Start(); err != nil {
		panic(fmt.Errorf("could not start server 1: %w", err))
	}
	server1.AddPeer(nodeToConnect)

	tp2 := NewTesterProtocol("tp2", false, false)
	tp2.blockFeeder = blockGen
	tp2.protocolVersion = uint32(eth.ProtocolVersions[0])
	tp2.networkId = 1 // Mainnet
	tp2.genesisBlockHash = blockGen.Genesis().Hash()
	server2 := makeP2PServer(ctx, tp2, []string{eth.ProtocolName})
	tp1.WaitForFork(ctx)
	// Add protocol
	if err := server2.Start(); err != nil {
		panic(fmt.Errorf("could not start server 2: %w", err))
	}
	server2.AddPeer(nodeToConnect)

	<-ctx.Done()
	return nil
}

func genesisCmd(cliCtx *cli.Context) error {
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

func makeP2PServer(ctx context.Context, tp *TesterProtocol, protocols []string) *p2p.Server {
	serverKey, err := crypto.GenerateKey()
	if err != nil {
		panic(fmt.Sprintf("Failed to generate server key: %v", err))
	}

	p2pConfig := p2p.Config{}
	p2pConfig.PrivateKey = serverKey
	p2pConfig.Name = "geth tester"
	p2pConfig.Logger = log.New()
	p2pConfig.MaxPeers = 1
	p2pConfig.Protocols = []p2p.Protocol{}
	pMap := map[string]p2p.Protocol{
		eth.ProtocolName: {
			Name:    eth.ProtocolName,
			Version: eth.ProtocolVersions[0],
			Length:  eth.ProtocolLengths[eth.ProtocolVersions[0]],
			Run: func(peer *p2p.Peer, rw p2p.MsgReadWriter) error {
				return tp.protocolRun(ctx, peer, rw)
			},
		},
		eth.DebugName: {
			Name:    eth.DebugName,
			Version: eth.DebugVersions[0],
			Length:  eth.DebugLengths[eth.DebugVersions[0]],
			Run: func(peer *p2p.Peer, rw p2p.MsgReadWriter) error {
				return tp.debugProtocolRun(ctx, peer, rw)
			},
		},
	}

	for _, protocolName := range protocols {
		p2pConfig.Protocols = append(p2pConfig.Protocols, pMap[protocolName])
	}
	return &p2p.Server{Config: p2pConfig}
}

func getTargetAddr(cliCtx *cli.Context) (*enode.Node, error) {
	var enodeAddress string
	if len(cliCtx.Args()) < 1 {
		addr, err := ioutil.ReadFile(p2p.EnodeAddressFileName)
		if err != nil {
			return nil, err
		}
		enodeAddress = string(addr)
	} else {
		enodeAddress = cliCtx.Args()[0]
	}
	if enodeAddress == "" {
		return nil, errors.New("Usage: tester <enode>\n")
	}
	nodeToConnect, err := enode.ParseV4(enodeAddress)
	if err != nil {
		return nil, fmt.Errorf("could not parse the node info: %w", err)
	}
	fmt.Printf("Parsed node: %s, IP: %s\n", nodeToConnect, nodeToConnect.IP())

	return nodeToConnect, nil
}
