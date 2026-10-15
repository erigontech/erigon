// Copyright 2026 The Erigon Authors
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
// Package eez links the EEZ stack into an Erigon binary. Importing it
// registers nothing: until Register runs, the binary behaves as upstream
// Erigon.
package eez

import (
	"github.com/urfave/cli/v3"

	"github.com/erigontech/erigon/eez/config"
	"github.com/erigontech/erigon/eez/networks"
	"github.com/erigontech/erigon/eez/rules"
	"github.com/erigontech/erigon/eez/systemtx"
)

// EnabledFlag turns EEZ support on; --chain then selects the EEZ network.
var EnabledFlag = cli.BoolFlag{
	Name:  "eez.enabled",
	Usage: "Enable EEZ support: EEZ networks for --chain, the l2 config section, transaction type 0x76 and the EEZ rules engine",
}

// Flags lists the EEZ command-line flags.
var Flags = []cli.Flag{&EnabledFlag}

// Register adds the EEZ networks, the eez l2 config section, transaction
// type 0x76 and the EEZ rules engine to the process-wide registries. It is
// idempotent.
func Register() {
	config.Register()
	networks.Register()
	systemtx.Register()
	rules.Register()
}
