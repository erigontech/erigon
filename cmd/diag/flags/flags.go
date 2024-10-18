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

package flags

import "github.com/urfave/cli/v2"

var (
	ApiPath = "/debug/diag"

	DebugURLFlag = cli.StringFlag{
		Name:     "debug.addr",
		Aliases:  []string{"da"},
		Usage:    "URL to the debug endpoint",
		Required: false,
		Value:    "localhost:6062",
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
