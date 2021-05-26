package main

import (
	"fmt"
	"os"

	"github.com/ledgerwatch/erigon/common/dbutils"
	"github.com/ledgerwatch/erigon/core/state"
	"github.com/ledgerwatch/erigon/eth/stagedsync"
	"github.com/ledgerwatch/erigon/eth/stagedsync/stages"
	"github.com/ledgerwatch/erigon/ethdb"
	"github.com/ledgerwatch/erigon/log"
	"github.com/ledgerwatch/erigon/turbo/node"

	erigoncli "github.com/ledgerwatch/erigon/turbo/cli"

	"github.com/urfave/cli"
)

// defining a custom command-line flag, a string
var flag = cli.StringFlag{
	Name:  "custom-stage-greeting",
	Value: "default-value",
}

// defining a custom bucket name
const (
	customBucketName = "ch.torquem.demo.tgcustom.CUSTOM_BUCKET"
)

// the regular main function
func main() {
	// initializing Erigon application here and providing our custom flag
	app := erigoncli.MakeApp(runErigon,
		append(erigoncli.DefaultFlags, flag), // always use DefaultFlags, but add a new one in the end.
	)
	if err := app.Run(os.Args); err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
}

func syncStages(ctx *cli.Context) stagedsync.StageBuilders {
	return append(
		stagedsync.DefaultStages(), // adding all default stages
		stagedsync.StageBuilder{ // adding our custom stage
			ID: stages.SyncStage("ch.torquem.demo.tgcustom.CUSTOM_STAGE"),
			Build: func(world stagedsync.StageParameters) *stagedsync.Stage {
				return &stagedsync.Stage{
					ID:          stages.SyncStage("ch.torquem.demo.tgcustom.CUSTOM_STAGE"),
					Description: "Custom Stage",
					ExecFunc: func(s *stagedsync.StageState, _ stagedsync.Unwinder, tx ethdb.RwTx) error {
						fmt.Println("hello from the custom stage", ctx.String(flag.Name))
						val, err := tx.GetOne(customBucketName, []byte("test"))
						fmt.Println("val", string(val), "err", err)
						if err := tx.Put(customBucketName, []byte("test"), []byte(ctx.String(flag.Name))); err != nil {
							return err
						}
						s.Done()
						return nil
					},
					UnwindFunc: func(u *stagedsync.UnwindState, s *stagedsync.StageState, tx ethdb.RwTx) error {
						fmt.Println("hello from the custom stage unwind", ctx.String(flag.Name))
						if err := tx.Delete(customBucketName, []byte("test"), nil); err != nil {
							return err
						}
						return u.Done(tx)
					},
				}
			},
		},
	)
}

// Erigon main function
func runErigon(ctx *cli.Context) {
	// creating a staged sync with our new stage
	sync := stagedsync.New(
		syncStages(ctx),
		stagedsync.DefaultUnwindOrder(),
		stagedsync.OptionalParameters{
			StateReaderBuilder: func(db ethdb.Database) state.StateReader {
				// put your custom caching code here
				return state.NewPlainStateReader(db)
			},
			StateWriterBuilder: func(db ethdb.Database, changeSetsDB ethdb.RwTx, blockNumber uint64) state.WriterWithChangeSets {
				// put your custom cache update code here
				return state.NewPlainStateWriter(db, changeSetsDB, blockNumber)
			},
		},
	)

	// running a node and initializing a custom bucket with all default settings
	eri := node.New(ctx, sync, node.Params{
		CustomBuckets: map[string]dbutils.BucketConfigItem{
			customBucketName: {},
		},
	})

	err := eri.Serve()

	if err != nil {
		log.Error("error while serving a Erigon node", "err", err)
	}
}
