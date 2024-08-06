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
	"strings"

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

	stringToSave := "Disk info:\n"
	stringToSave += data.Disk.Details + "\n"
	stringToSave += "CPU info:\n"
	stringToSave += cpuToString(data.CPU) + "\n"

	// Save data to file
	err = util.SaveDataToFile(cliCtx.String(ExportPathFlag.Name), cliCtx.String(ExportFileNameFlag.Name), stringToSave)
	if err != nil {
		util.RenderError(err)
	}

	return nil
}

func cpuToString(cpuInfo []diagnostics.CPUInfo) string {
	result := ""

	spacing := calculateSpacing([]string{"CPU", "VendorID", "Family", "Model", "Stepping", "PhysicalID", "CoreID", "Cores", "ModelName", "Mhz", "CacheSize", "Flags", "Microcode"})

	for _, cpu := range cpuInfo {
		addStringToResult(&result, "CPU", util.Int32ToString(cpu.CPU), spacing)
		addStringToResult(&result, "VendorID", cpu.VendorID, spacing)
		addStringToResult(&result, "Family", cpu.Family, spacing)
		addStringToResult(&result, "Model", cpu.Model, spacing)
		addStringToResult(&result, "Stepping", util.Int32ToString(cpu.Stepping), spacing)
		addStringToResult(&result, "PhysicalID", cpu.PhysicalID, spacing)
		addStringToResult(&result, "CoreID", cpu.CoreID, spacing)
		addStringToResult(&result, "Cores", util.Int32ToString(cpu.Cores), spacing)
		addStringToResult(&result, "ModelName", cpu.ModelName, spacing)
		addStringToResult(&result, "Mhz", util.Float64ToString(cpu.Mhz), spacing)
		addStringToResult(&result, "CacheSize", util.Int32ToString(cpu.CacheSize), spacing)
		addStringToResult(&result, "Flags", strings.Join(cpu.Flags, ", "), spacing)
		addStringToResult(&result, "Microcode", cpu.Microcode, spacing)
	}

	return result
}

func calculateSpacing(keysArray []string) int {
	max := 0
	for _, key := range keysArray {
		if len(key) > max {
			max = len(key)
		}
	}

	return max + 3
}

func addStringToResult(result *string, name string, value string, spacing int) {
	marging := 3
	if value == "" {
		value = "N/A"
	}

	*result += strings.Repeat(" ", marging) + name + ":" + strings.Repeat(" ", spacing-len(name)) + value + "\n"
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
