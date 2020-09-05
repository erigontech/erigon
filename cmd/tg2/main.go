package main

import (
	"fmt"
	"os"

	"github.com/ledgerwatch/turbo-geth/eth/stagedsync"
	"github.com/ledgerwatch/turbo-geth/eth/stagedsync/stages"
	"github.com/ledgerwatch/turbo-geth/turbo/node"

	turbocli "github.com/ledgerwatch/turbo-geth/turbo/cli"

	"github.com/urfave/cli"
)

func main() {
	app := turbocli.MakeApp(runTurboGeth, turbocli.DefaultFlags)
	if err := app.Run(os.Args); err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
}

func syncStages() stagedsync.StageBuilders {
	return stagedsync.StageBuilders(append(
		stagedsync.DefaultStages(),
		stagedsync.StageBuilder{
			ID: stages.SyncStage(0x0F),
			Build: func(world stagedsync.StageParameters) *stagedsync.Stage {
				return &stagedsync.Stage{
					ID:          stages.SyncStage(0x0F),
					Description: "Custom Stage",
					ExecFunc: func(s *stagedsync.StageState, _ stagedsync.Unwinder) error {
						fmt.Println("hello from the custom stage")
						return nil
					},
					UnwindFunc: func(u *stagedsync.UnwindState, s *stagedsync.StageState) error {
						fmt.Println("hello from the custom stage unwind")
						return nil
					},
				}
			},
		},
	))
}

func runTurboGeth(ctx *cli.Context) {
	sync := stagedsync.New(
		syncStages(),
		stagedsync.DefaultUnwindOrder(),
	)

	tg := node.New(ctx, sync)
	tg.Serve()
}
