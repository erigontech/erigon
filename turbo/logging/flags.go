package logging

import (
	"github.com/ledgerwatch/log/v3"
	"github.com/urfave/cli/v2"
)

var (
	LogJsonFlag = cli.BoolFlag{
		Name:  "log.json",
		Usage: "Format console logs with JSON",
	}

	LogConsoleJsonFlag = cli.BoolFlag{
		Name:  "log.console.json",
		Usage: "Format console logs with JSON",
	}

	LogDirJsonFlag = cli.BoolFlag{
		Name:  "log.dir.json",
		Usage: "Format file logs with JSON",
	}

	LogVerbosityFlag = cli.StringFlag{
		Name:  "verbosity",
		Usage: "Set the log level for console logs",
		Value: log.LvlInfo.String(),
	}

	LogConsoleVerbosityFlag = cli.StringFlag{
		Name:  "log.console.verbosity",
		Usage: "Set the log level for console logs",
		Value: log.LvlInfo.String(),
	}
	LogDirDisableFlag = cli.BoolFlag{
		Name:  "log.dir.disable",
		Usage: "disable disk logging",
	}
	LogDirPathFlag = cli.StringFlag{
		Name:  "log.dir.path",
		Usage: "Path to store user and error logs to disk",
	}

	LogDirPrefixFlag = cli.StringFlag{
		Name:  "log.dir.prefix",
		Usage: "The file name prefix for logs stored to disk",
	}

	LogDirVerbosityFlag = cli.StringFlag{
		Name:  "log.dir.verbosity",
		Usage: "Set the log verbosity for logs stored to disk",
		Value: log.LvlInfo.String(),
	}

	LogBlockDelayFlag = cli.BoolFlag{
		Name:  "log.delays",
		Usage: "Enable block delay logging",
	}
)

var Flags = []cli.Flag{
	&LogJsonFlag,
	&LogConsoleJsonFlag,
	&LogDirJsonFlag,
	&LogVerbosityFlag,
	&LogConsoleVerbosityFlag,
	&LogDirDisableFlag,
	&LogDirPathFlag,
	&LogDirPrefixFlag,
	&LogDirVerbosityFlag,
	&LogBlockDelayFlag,
}
