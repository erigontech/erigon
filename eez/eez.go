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
	"path/filepath"

	"github.com/urfave/cli/v3"

	"github.com/erigontech/erigon/common/dir"
	"github.com/erigontech/erigon/db/datadir"
	"github.com/erigontech/erigon/eez/config"
	"github.com/erigontech/erigon/eez/networks"
	eezrpc "github.com/erigontech/erigon/eez/rpc"
	"github.com/erigontech/erigon/eez/rules"
	"github.com/erigontech/erigon/eez/systemtx"
)

// EnabledFlag makes the node run an EEZ chain; --chain then selects the EEZ
// network.
var EnabledFlag = cli.BoolFlag{
	Name:  "eez.enabled",
	Usage: "Run an EEZ chain, kept under <datadir>/eez and <datadir>/snapshots/eez: EEZ networks for --chain, the l2 config section, transaction type 0x76 and the EEZ rules engine",
}

// Dir is where a node keeps its EEZ chain's database and stores.
func Dir(dirs datadir.Dirs) string { return filepath.Join(dirs.DataDir, "eez") }

// Dirs moves the chain database of dirs to <datadir>/eez/chaindata and its
// snapshots to <datadir>/snapshots/eez, as Caplin keeps its own; the other
// directories stay the node's.
func Dirs(dirs datadir.Dirs) datadir.Dirs {
	dirs.Chaindata = filepath.Join(Dir(dirs), "chaindata")
	dirs.Snap = filepath.Join(dirs.Snap, "eez")
	dirs.SnapIdx = filepath.Join(dirs.Snap, "idx")
	dirs.SnapHistory = filepath.Join(dirs.Snap, "history")
	dirs.SnapDomain = filepath.Join(dirs.Snap, "domain")
	dirs.SnapAccessors = filepath.Join(dirs.Snap, "accessor")
	dir.MustExist(dirs.Chaindata, dirs.SnapIdx, dirs.SnapHistory, dirs.SnapDomain, dirs.SnapAccessors)
	return dirs
}

// Register adds the EEZ networks, the eez l2 config section, transaction
// type 0x76 with its RPC rendering and the EEZ rules engine to the
// process-wide registries. It is idempotent.
func Register() {
	config.Register()
	networks.Register()
	systemtx.Register()
	eezrpc.Register()
	rules.Register()
}
