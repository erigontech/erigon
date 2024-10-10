package main

import (
	"fmt"
	"os"

	"github.com/urfave/cli/v2"

	erigonapp "github.com/ledgerwatch/erigon/turbo/app"
	erigoncli "github.com/ledgerwatch/erigon/turbo/cli"
)

// defining a custom command-line flag, a string
var flag = cli.StringFlag{
	Name:  "custom-stage-greeting",
	Value: "default-value",
}

// defining a custom bucket name
const (
	customBucketName = "ch.torquem.demo.tgcustom.CUSTOM_BUCKET" //nolint
)

// the regular main function
func main() {
	// initializing Erigon application here and providing our custom flag
	app := erigonapp.MakeApp("erigoncustom", runErigon,
		append(erigoncli.DefaultFlags, &flag), // always use DefaultFlags, but add a new one in the end.
	)
	if err := app.Run(os.Args); err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
}

// Erigon main function
func runErigon(ctx *cli.Context) error {
	// running a node and initializing a custom bucket with all default settings
	//eri := node.New(ctx, node.Params{
	//	CustomBuckets: map[string]dbutils.BucketConfigItem{
	//		customBucketName: {},
	//	},
	//})

	//err := eri.Serve()

	//if err != nil {
	//	log.Error("error while serving a Erigon node", "err", err)
	//  return err
	//}
	return nil
}
