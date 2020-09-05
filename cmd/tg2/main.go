package main

import (
	"fmt"
	"os"

	"github.com/ledgerwatch/turbo-geth/eth/stagedsync"
	"github.com/ledgerwatch/turbo-geth/eth/stagedsync/stages"
	"github.com/ledgerwatch/turbo-geth/log"
	"github.com/ledgerwatch/turbo-geth/turbo/node"

	turbocli "github.com/ledgerwatch/turbo-geth/turbo/cli"

	"github.com/urfave/cli"
)

var flag = cli.StringFlag{
	Name:  "custom-stage-greeting",
	Value: "default-value",
}

func main() {
	app := turbocli.MakeApp(runTurboGeth, append(turbocli.DefaultFlags, flag))
	if err := app.Run(os.Args); err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
}

func syncStages(ctx *cli.Context) stagedsync.StageBuilders {
	return append(
		stagedsync.DefaultStages(),
		stagedsync.StageBuilder{
			ID: stages.SyncStage("0x0F_CUSTOM"),
			Build: func(world stagedsync.StageParameters) *stagedsync.Stage {
				return &stagedsync.Stage{
					ID:          stages.SyncStage("0x0F_CUSTOM"),
					Description: "Custom Stage",
					ExecFunc: func(s *stagedsync.StageState, _ stagedsync.Unwinder) error {
						fmt.Println("hello from the custom stage", ctx.String(flag.Name))
						s.Done()
						return nil
					},
					UnwindFunc: func(u *stagedsync.UnwindState, s *stagedsync.StageState) error {
						fmt.Println("hello from the custom stage unwind", ctx.String(flag.Name))
						return nil
					},
				}
			},
		},
	)
}

func runTurboGeth(ctx *cli.Context) {
	sync := stagedsync.New(
		syncStages(ctx),
		stagedsync.DefaultUnwindOrder(),
	)

	tg := node.New(ctx, sync)
	err := tg.Serve()

	if err != nil {
		log.Error("error while serving a turbo-geth node", "err", err)
	}
}
