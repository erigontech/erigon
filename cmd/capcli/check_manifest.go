package main

import (
	"github.com/urfave/cli/v3"
)

func caplinCheckManifestCommand() *cli.Command {
	return &cli.Command{
		Usage:       "check a Caplin snapshot manifest; reads the TOML only without the datadir lock",
		Description: "Reads and checks the supplied Caplin manifest for gaps, overlaps, state-table differences, and unregistered files. It does not open or change a datadir and does not take a lock.",
	}
}
