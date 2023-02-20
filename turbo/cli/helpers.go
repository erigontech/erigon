// Copyright 2020 The go-ethereum Authors
// This file is part of go-ethereum.
//
// go-ethereum is free software: you can redistribute it and/or modify
// it under the terms of the GNU General Public License as published by
// the Free Software Foundation, either version 3 of the License, or
// (at your option) any later version.
//
// go-ethereum is distributed in the hope that it will be useful,
// but WITHOUT ANY WARRANTY; without even the implied warranty of
// MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE. See the
// GNU General Public License for more details.
//
// You should have received a copy of the GNU General Public License
// along with go-ethereum. If not, see <http://www.gnu.org/licenses/>.

package cli

import (
	"os"
	"path/filepath"

	"github.com/urfave/cli/v2"

	"github.com/ledgerwatch/erigon/params"
)

// HelpData is a one shot struct to pass to the usage template
type HelpData struct {
	App        interface{}
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
func NewApp(gitCommit, usage string) *cli.App {
	app := cli.NewApp()
	app.Name = filepath.Base(os.Args[0])
	app.Version = params.VersionWithCommit(gitCommit)
	app.Usage = usage
	return app
}
