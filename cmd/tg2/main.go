package main

import (
	"github.com/ledgerwatch/turbo-geth/cmd/utils"
	"github.com/ledgerwatch/turbo-geth/eth"
	"github.com/ledgerwatch/turbo-geth/eth/stagedsync"
	"github.com/ledgerwatch/turbo-geth/node"

	"github.com/urfave/cli"
)

func main() {

	var ctx *cli.Context

	sync := stagedsync.New(
		stagedsync.DefaultStages(),
		stagedsync.DefaultUnwindOrder(),
	)

	tg := MakeTurboGethNode(ctx, sync)
	tg.Serve()
}

type TurboGeth struct {
	stack   *node.Node
	backend *eth.Ethereum
}

func (tg *TurboGeth) Serve() error {
	defer tg.stack.Close()

	tg.run()

	tg.stack.Wait()

	return nil
}

func (tg *TurboGeth) run() {
	utils.StartNode(tg.stack)
	// we don't have accounts locally and we don't do mining
	// so these parts are ignored
	// see cmd/geth/main.go#startNode for full implementation
}

func MakeTurboGethNode(ctx *cli.Context, sync *stagedsync.StagedSync) *TurboGeth {
	node, cfg := MakeConfigNode(ctx)
	cfg.StagedSync = sync

	ethereum := utils.RegisterEthService(node, &cfg)

	return &TurboGeth{stack: node, backend: ethereum}
}

func MakeConfigNode(ctx *cli.Context) (*node.Node, eth.Config) {
	panic("implement me")
}
