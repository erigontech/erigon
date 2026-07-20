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
	"os"
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/require"
	"github.com/urfave/cli/v3"
)

func TestMakeApp_ConfigFileAppliesToSubcommands(t *testing.T) {
	cfgFile := filepath.Join(t.TempDir(), "config.toml")
	require.NoError(t, os.WriteFile(cfgFile, []byte("datadir = \"/tmp/from-config\"\n"), 0o600))

	// Fresh flag copy, not the real utils.DataDirFlag singleton — same reason
	// as node/cli/config_file_test.go's fresh-copies-per-run pattern.
	datadirFlag := &cli.StringFlag{Name: "datadir", Value: "/default"}

	app := MakeApp("erigon", func(ctx context.Context, cmd *cli.Command) error {
		return nil
	}, []cli.Flag{datadirFlag})

	var sawDatadir string
	app.Commands = []*cli.Command{
		{
			Name:  "import",
			Flags: []cli.Flag{datadirFlag},
			Action: func(ctx context.Context, cmd *cli.Command) error {
				sawDatadir = cmd.String("datadir")
				return nil
			},
		},
	}

	require.NoError(t, app.Run(context.Background(), []string{"erigon", "--config", cfgFile, "import"}))
	require.Equal(t, "/tmp/from-config", sawDatadir, "config.toml settings must apply even when a subcommand like import is invoked")
}
