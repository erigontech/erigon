package app

import (
	"runtime"

	"github.com/urfave/cli/v2"
)

var (
	PreverifiedFlag = cli.StringFlag{
		Name:     "preverified",
		Category: "Snapshots",
		Usage:    "preverified to use (remote, local, embedded)",
		Value:    "remote",
	}
	ConcurrencyFlag = cli.IntFlag{
		Name:  "concurrency",
		Usage: "level of concurrency for some operation",
		Value: runtime.GOMAXPROCS(0),
	}
	VerifyChainFlag = cli.StringFlag{
		Name:  "verify.chain",
		Usage: "name of the chain to verify",
	}
)
