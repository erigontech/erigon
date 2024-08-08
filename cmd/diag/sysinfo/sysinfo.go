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
	"fmt"
	"sort"
	"strconv"
	"strings"

	"github.com/jedib0t/go-pretty/v6/table"
	"github.com/urfave/cli/v2"

	"github.com/erigontech/erigon-lib/diagnostics"
	"github.com/erigontech/erigon-lib/sysutils"
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

type SortType int

const (
	SortByCPU SortType = iota
	SortByMemory
	SortByPID
)

func collectInfo(cliCtx *cli.Context) error {
	data, err := getData(cliCtx)
	if err != nil {
		util.RenderError(err)
	}

	var builder strings.Builder
	builder.WriteString("Disk info:\n")
	builder.WriteString(data.Disk.Details)
	builder.WriteString("\n\n")
	builder.WriteString("CPU info:\n")
	writeCPUToStringBuilder(data.CPU, &builder)

	processes := sysutils.GetProcessesInfo()
	builder.WriteString("\n\nProcesses info:\n")
	writeProcessesToStringBuilder(processes, &builder)

	// Save data to file
	err = util.SaveDataToFile(cliCtx.String(ExportPathFlag.Name), cliCtx.String(ExportFileNameFlag.Name), builder.String())
	if err != nil {
		util.RenderError(err)
	}

	return nil
}

func writeCPUToStringBuilder(cpuInfo []diagnostics.CPUInfo, builder *strings.Builder) {
	spacing := calculateSpacing([]string{"CPU", "VendorID", "Family", "Model", "Stepping", "PhysicalID", "CoreID", "Cores", "ModelName", "Mhz", "CacheSize", "Flags", "Microcode"})

	for _, cpu := range cpuInfo {
		writeStringToBuilder(builder, "CPU", strconv.Itoa(int(cpu.CPU)), spacing)
		writeStringToBuilder(builder, "VendorID", cpu.VendorID, spacing)
		writeStringToBuilder(builder, "Family", cpu.Family, spacing)
		writeStringToBuilder(builder, "Model", cpu.Model, spacing)
		writeStringToBuilder(builder, "Stepping", strconv.Itoa(int(cpu.Stepping)), spacing)
		writeStringToBuilder(builder, "PhysicalID", cpu.PhysicalID, spacing)
		writeStringToBuilder(builder, "CoreID", cpu.CoreID, spacing)
		writeStringToBuilder(builder, "Cores", strconv.Itoa(int(cpu.Cores)), spacing)
		writeStringToBuilder(builder, "ModelName", cpu.ModelName, spacing)
		writeStringToBuilder(builder, "Mhz", fmt.Sprintf("%g", cpu.Mhz), spacing)
		writeStringToBuilder(builder, "CacheSize", strconv.Itoa(int(cpu.CacheSize)), spacing)
		writeStringToBuilder(builder, "Flags", strings.Join(cpu.Flags, ", "), spacing)
		writeStringToBuilder(builder, "Microcode", cpu.Microcode, spacing)
	}
}

func writeProcessesToStringBuilder(prcInfo []*sysutils.ProcessInfo, builder *strings.Builder) {
	prcInfo = sortProcessesByCPU(prcInfo)
	rows := make([]table.Row, 0)
	header := table.Row{"PID", "Name", "% CPU", "% Memory"}
	for _, process := range prcInfo {
		cpu := fmt.Sprintf("%.2f", process.CPUUsage)
		memory := fmt.Sprintf("%.2f", process.Memory)
		rows = append(rows, table.Row{process.Pid, process.Name, cpu, memory})
	}

	t := table.NewWriter()

	t.AppendHeader(header)
	if len(rows) > 0 {
		t.AppendRows(rows)
	}

	t.AppendSeparator()
	result := t.Render()
	builder.WriteString(result)
}

func sortProcesses(prcInfo []*sysutils.ProcessInfo, sorting SortType) []*sysutils.ProcessInfo {
	sort.Slice(prcInfo, func(i, j int) bool {
		switch sorting {
		case SortByCPU:
			return prcInfo[i].CPUUsage > prcInfo[j].CPUUsage
		case SortByMemory:
			return prcInfo[i].Memory > prcInfo[j].Memory
		default:
			return prcInfo[i].Pid < prcInfo[j].Pid
		}

	})

	return prcInfo
}

func sortProcessesByCPU(prcInfo []*sysutils.ProcessInfo) []*sysutils.ProcessInfo {
	return sortProcesses(prcInfo, SortByCPU)
}

func sortProcessesByMemory(prcInfo []*sysutils.ProcessInfo) []*sysutils.ProcessInfo {
	return sortProcesses(prcInfo, SortByMemory)
}

func sortProcessesByPID(prcInfo []*sysutils.ProcessInfo) []*sysutils.ProcessInfo {
	return sortProcesses(prcInfo, SortByPID)
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

func writeStringToBuilder(result *strings.Builder, name string, value string, spacing int) {
	marging := 3
	if value == "" {
		value = "N/A"
	}

	writeSpacesToBuilder(result, marging)
	result.WriteString(name)
	result.WriteString(":")
	writeSpacesToBuilder(result, spacing-len(name)-1)
	result.WriteString(value)
	result.WriteString("\n")
}

func writeSpacesToBuilder(result *strings.Builder, spaces int) {
	result.WriteString(strings.Repeat(" ", spaces))
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
