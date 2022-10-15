package utils

import "github.com/urfave/cli"

var (
	LightClientPort = cli.Int64Flag{
		Name:  "lc.port",
		Usage: "sets the lightclient port (default: 7777)",
		Value: 7777,
	}
	LightClientAddr = cli.StringFlag{
		Name:  "lc.addr",
		Usage: "sets the lightclient host addr (default: localhost)",
		Value: "localhost",
	}
)
