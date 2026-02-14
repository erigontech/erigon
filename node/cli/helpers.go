// Copyright 2020 The go-ethereum Authors
// (original work)
// Copyright 2024 The Erigon Authors
// (modifications)
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

package cli

import (
	"fmt"
	"os"
	"path/filepath"

	"github.com/urfave/cli/v2"

	"github.com/erigontech/erigon/db/version"
)

// HelpData is a one shot struct to pass to the usage template
type HelpData struct {
	App        any
	FlagGroups []FlagGroup
}

// FlagGroup is a collection of flags belonging to a single topic.
type FlagGroup struct {
	Name  string
	Flags []cli.Flag
}

// ByCategory sorts an array of FlagGroup by Name in the order
// defined in AppHelpFlagGroups.
type ByCategory []FlagGroup

func (a ByCategory) Len() int      { return len(a) }
func (a ByCategory) Swap(i, j int) { a[i], a[j] = a[j], a[i] }
func (a ByCategory) Less(i, j int) bool {
	iCat, jCat := a[i].Name, a[j].Name
	iIdx, jIdx := len(a), len(a) // ensure non categorized flags come last

	for i, group := range a {
		if iCat == group.Name {
			iIdx = i
		}
		if jCat == group.Name {
			jIdx = i
		}
	}

	return iIdx < jIdx
}

// NewApp creates an app with sane defaults.
func NewApp(desc string) *cli.App {
	app := cli.NewApp()
	app.Name = filepath.Base(os.Args[0])
	app.Version = version.VersionWithCommit(version.GitCommit)
	app.Usage = desc
	app.EnableBashCompletion = true

	app.Suggest = true

	// Only show usage if explicitly requested via --help
	app.OnUsageError = func(ctx *cli.Context, err error, isSubcommand bool) error {
		// Print the error but not the usage
		if ctx != nil && ctx.App != nil {
			fmt.Fprintf(ctx.App.ErrWriter, "Error: %v\n", err)
			fmt.Fprintf(ctx.App.ErrWriter, "Run '%s --help' for usage.\n", ctx.App.Name)
		}
		// Return cli.Exit to signal we've handled the error
		return cli.Exit("", 1)
	}

	// Configure exit error handler to prevent additional output
	app.ExitErrHandler = func(ctx *cli.Context, err error) {
		if err == nil {
			return
		}
		// For cli.Exit errors, just exit with the code
		if exitErr, ok := err.(cli.ExitCoder); ok {
			cli.OsExiter(exitErr.ExitCode())
		} else {
			// For other errors, print them and exit
			if err.Error() != "" {
				fmt.Fprintf(ctx.App.ErrWriter, "Error: %v\n", err)
			}
			cli.OsExiter(1)
		}
	}

	return app
}
