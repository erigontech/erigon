package flags

import "github.com/urfave/cli/v2"

var (
	ApiPath = "/debug/diag"

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

	AutoUpdateFlag = cli.BoolFlag{
		Name:     "autoupdate",
		Aliases:  []string{"au"},
		Usage:    "Auto update the output",
		Required: false,
		Value:    false,
	}

	AutoUpdateIntervalFlag = cli.IntFlag{
		Name:     "autoupdate.interval",
		Aliases:  []string{"aui"},
		Usage:    "Auto update interval in milliseconds",
		Required: false,
		Value:    20000,
	}
)
