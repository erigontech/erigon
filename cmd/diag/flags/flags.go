package flags

import "github.com/urfave/cli/v2"

var (
	DebugURLFlag = cli.StringFlag{
		Name:     "debug.addr",
		Aliases:  []string{"da"},
		Usage:    "URL to the debug endpoint",
		Required: false,
		Value:    "localhost:6060",
	}

	OutputFlag = cli.StringFlag{
		Name:     "output",
		Aliases:  []string{"o"},
		Usage:    "Output format [text|json]",
		Required: false,
		Value:    "text",
	}
)
