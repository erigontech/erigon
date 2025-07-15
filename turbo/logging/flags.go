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

package logging

import (
	"github.com/urfave/cli/v2"

	"github.com/erigontech/erigon/erigon-lib/log/v3"
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
