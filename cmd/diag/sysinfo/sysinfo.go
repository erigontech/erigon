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

package sysinfo

import (
	"github.com/urfave/cli/v2"

	"github.com/erigontech/erigon-lib/diagnostics"
	"github.com/erigontech/erigon/cmd/diag/flags"
	"github.com/erigontech/erigon/cmd/diag/util"
)

var (
	ExportPathFlag = cli.StringFlag{
		Name:     "export.path",
		Aliases:  []string{"ep"},
		Usage:    "Path to folder for export result",
		Required: true,
		Value:    "",
	}

	ExportFileNameFlag = cli.StringFlag{
		Name:     "export.file",
		Aliases:  []string{"ef"},
		Usage:    "File name to export result default is sysinfo.txt",
		Required: false,
		Value:    "sysinfo.txt",
	}
)

var Command = cli.Command{
	Name:      "sysinfo",
	Aliases:   []string{"sinfo"},
	ArgsUsage: "",
	Action:    collectInfo,
	Flags: []cli.Flag{
		&flags.DebugURLFlag,
		&ExportPathFlag,
		&ExportFileNameFlag,
	},
	Description: "Collect information about system and save it to file in order to provide to support person",
}

func collectInfo(cliCtx *cli.Context) error {
	data, err := getData(cliCtx)
	if err != nil {
		util.RenderError(err)
	}

	// Save data to file
	err = util.SaveDataToFile(cliCtx.String(ExportPathFlag.Name), cliCtx.String(ExportFileNameFlag.Name), data.Disk.Details)
	if err != nil {
		util.RenderError(err)
	}

	return nil
}

func getData(cliCtx *cli.Context) (diagnostics.HardwareInfo, error) {
	var data diagnostics.HardwareInfo
	url := "http://" + cliCtx.String(flags.DebugURLFlag.Name) + flags.ApiPath + "/hardware-info"

	err := util.MakeHttpGetCall(cliCtx.Context, url, &data)

	if err != nil {
		return data, err
	}

	return data, nil
}
