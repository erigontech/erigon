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
	"context"

	"github.com/urfave/cli/v3"

	"github.com/erigontech/erigon/cmd/utils"
	"github.com/erigontech/erigon/common/log/v3"
	"github.com/erigontech/erigon/db/datadir"
	"github.com/erigontech/erigon/db/kv/backup"
	"github.com/erigontech/erigon/node/debug"
)

var dbCommand = cli.Command{
	Name:  "db",
	Usage: "Managing the mdbx databases of a datadir",
	Commands: []*cli.Command{
		{
			Name:  "compact",
			Usage: "Rewrite every mdbx db of --datadir without its free pages. Erigon must be stopped. WARNING: may take hours and needs free disk space for the biggest db",
			Before: func(ctx context.Context, cliCtx *cli.Command) (context.Context, error) {
				_, err := debug.SetupSimple(ctx, cliCtx, true /* rootLogger */)
				return ctx, err
			},
			Action: func(ctx context.Context, cliCtx *cli.Command) error {
				dirs := datadir.Open(cliCtx.String(utils.DataDirFlag.Name))
				return backup.CompactDatadir(ctx, dirs, log.Root())
			},
			Flags: joinFlags([]cli.Flag{&utils.DataDirFlag}),
		},
	},
}
