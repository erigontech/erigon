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
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/require"
	"github.com/urfave/cli/v2"

	erigoncli "github.com/erigontech/erigon/node/cli"
)

// runApp runs the erigon CLI app in-process; the no-op ExitErrHandler makes a
// failing command return its error instead of calling os.Exit.
func runApp(args ...string) error {
	app := MakeApp("erigon", func(*cli.Context) error { return nil }, erigoncli.DefaultFlags)
	app.ExitErrHandler = func(*cli.Context, error) {}
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	return app.RunContext(ctx, append([]string{"erigon"}, args...))
}

// TestInitErrorsReturn pins that init failures surface as returned errors
// rather than killing the process — required for in-process test drivers of
// the CLI.
func TestInitErrorsReturn(t *testing.T) {
	dataDir := t.TempDir()

	err := runApp("init", "--datadir", dataDir, filepath.Join(dataDir, "missing-genesis.json"))
	require.ErrorContains(t, err, "genesis")

	err = runApp("init", "--datadir", dataDir)
	require.ErrorContains(t, err, "genesis")
}

func TestImportWithoutArgsErrorsReturn(t *testing.T) {
	err := runApp("import", "--datadir", t.TempDir())
	require.ErrorContains(t, err, "argument")
}
