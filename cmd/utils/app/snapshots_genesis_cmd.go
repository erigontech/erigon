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

package app

import (
	"fmt"
	"os"

	"github.com/urfave/cli/v2"

	"github.com/erigontech/erigon/cmd/utils"
	"github.com/erigontech/erigon/db/datadir"
	"github.com/erigontech/erigon/db/snapcfg"
)

var genesisLocationFlag = cli.StringFlag{
	Name:  "location",
	Usage: "Path to the preverified.toml to pin as canonical genesis v0. When omitted, the binary's embedded preverified set for --chain is used.",
}

// doSnapshotGenesis is the action body for `erigon snapshots genesis`.
// It pins canonical genesis v0 — a deliberate, segregated act, like
// `erigon init` for chain state — recording <snapDir>/canonical.v0.toml
// so the node anchors its canonical view on a fixed v0 that no runtime
// flag can change (docs/plans/20260520-chaintoml-ucan-flow-spec.md).
func doSnapshotGenesis(cliCtx *cli.Context) error {
	dirs := datadir.New(cliCtx.String(utils.DataDirFlag.Name))

	var (
		items snapcfg.PreverifiedItems
		src   string
	)
	if loc := genesisLocationFlag.Get(cliCtx); loc != "" {
		data, err := os.ReadFile(loc)
		if err != nil {
			return fmt.Errorf("reading genesis source %q: %w", loc, err)
		}
		items, err = snapcfg.ParsePreverifiedItems(data)
		if err != nil {
			return fmt.Errorf("parsing genesis source %q: %w", loc, err)
		}
		src = loc
	} else {
		chain := cliCtx.String(utils.ChainFlag.Name)
		cfg, ok := snapcfg.KnownCfg(chain)
		if !ok || cfg == nil {
			return fmt.Errorf("no embedded preverified set for chain %q; pass --location", chain)
		}
		items = cfg.Preverified.Items
		src = "embedded preverified for " + chain
	}
	if len(items) == 0 {
		return fmt.Errorf("genesis source %s is empty", src)
	}

	if err := snapcfg.WritePinnedGenesis(dirs.Snap, items); err != nil {
		return err
	}
	fmt.Fprintf(cliCtx.App.Writer, "pinned canonical genesis v0 (%d entries from %s) to %s\n",
		len(items), src, dirs.Snap)
	return nil
}
