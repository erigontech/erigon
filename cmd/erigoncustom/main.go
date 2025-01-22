// Copyright 2024 The Erigon Authors
// This file is part of Erigon.
//
// Erigon is free software: you can redistribute it and/or modify
// it under the terms of the GNU Lesser General Public License as published by
// the Free Software Foundation, either version 3 of the License, or
// (at your option) any later version.
//
// Erigon is distributed in the hope that it will be useful,
// but WITHOUT ANY WARRANTY; without even the implied warranty of
// MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE. See the
// GNU Lesser General Public License for more details.
//
// You should have received a copy of the GNU Lesser General Public License
// along with Erigon. If not, see <http://www.gnu.org/licenses/>.

package main

import (
	"fmt"
	"os"

	"github.com/urfave/cli/v2"

	erigonapp "github.com/erigontech/erigon/turbo/app"
	erigoncli "github.com/erigontech/erigon/turbo/cli"
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
	//	log.Error("error while serving an Erigon node", "err", err)
	//  return err
	//}
	return nil
}
