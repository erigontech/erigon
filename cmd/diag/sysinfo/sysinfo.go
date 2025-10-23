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
	"strings"

	"github.com/jedib0t/go-pretty/v6/table"
	"github.com/urfave/cli/v2"

	"github.com/erigontech/erigon/cmd/diag/flags"
	"github.com/erigontech/erigon/cmd/diag/util"
	"github.com/erigontech/erigon/diagnostics/diaglib"
	"github.com/erigontech/erigon/diagnostics/sysutils"
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

type Flag struct {
	Name    string      `json:"name"`
	Value   interface{} `json:"value"`
	Usage   string      `json:"usage"`
	Default bool        `json:"default"`
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

	flagsData, err := getFlagsData(cliCtx)
	if err != nil {
		util.RenderError(err)
	}

	cpuusage := sysutils.CPUUsage()
	processes := sysutils.GetProcessesInfo()
	totalMemory := sysutils.TotalMemoryUsage()

	var builder strings.Builder
	builder.WriteString("Flags applied by launch command:\n")
	writeFlagsInfoToStringBuilder(flagsData, &builder)
	builder.WriteString("\n\n")

	writeDiskInfoToStringBuilder(data.Disk, &builder)
	writeCPUInfoToStringBuilder(data.CPU, cpuusage, &builder)

	writeProcessesToStringBuilder(processes, cpuusage.Total, totalMemory, &builder)

	// Save data to file
	err = util.SaveDataToFile(cliCtx.String(ExportPathFlag.Name), cliCtx.String(ExportFileNameFlag.Name), builder.String())
	if err != nil {
		util.RenderError(err)
	}

	return nil
}

func writeFlagsInfoToStringBuilder(flags []Flag, builder *strings.Builder) {
	flagsRows := make([]table.Row, 0, len(flags))
	for _, flag := range flags {
		flagsRows = append(flagsRows, table.Row{flag.Name, flag.Value})
	}
	flagsTableData := util.ExportTable(table.Row{"Flag", "Value"}, flagsRows, nil)
	builder.WriteString(flagsTableData)
}

func writeDiskInfoToStringBuilder(diskInfo diaglib.DiskInfo, builder *strings.Builder) {
	builder.WriteString("Disk info:\n")
	builder.WriteString(diskInfo.Details)
	builder.WriteString("\n\n")
}

func writeCPUInfoToStringBuilder(cpuInfo []diaglib.CPUInfo, cpuusage sysutils.CPUUsageInfo, builder *strings.Builder) {
	writeOweralCPUInfoToStringBuilder(cpuInfo, builder)
	writeCPUUsageToStringBuilder(cpuusage.Cores, builder)
}

func writeOweralCPUInfoToStringBuilder(cpuInfo []diaglib.CPUInfo, builder *strings.Builder) {
	builder.WriteString("CPU info:\n")
	header := table.Row{"CPU", "VendorID", "Family", "Model", "Stepping", "PhysicalID", "CoreID", "Cores", "ModelName", "Mhz", "CacheSize", "Flags", "Microcode"}
	rows := make([]table.Row, 0, len(cpuInfo))
	for _, cpu := range cpuInfo {
		rows = append(rows, table.Row{cpu.CPU, cpu.VendorID, cpu.Family, cpu.Model, cpu.Stepping, cpu.PhysicalID, cpu.CoreID, cpu.Cores, cpu.ModelName, cpu.Mhz, cpu.CacheSize, strings.Join(cpu.Flags, ", "), cpu.Microcode})
	}

	cpuDataTable := util.ExportTable(header, rows, nil)
	builder.WriteString(cpuDataTable)
	builder.WriteString("\n\n")
}

func writeCPUUsageToStringBuilder(cpuUsage []float64, builder *strings.Builder) {
	builder.WriteString("CPU usage:\n")
	header := table.Row{"Core #", "% CPU"}
	rows := make([]table.Row, 0, len(cpuUsage))
	for idx, core := range cpuUsage {
		rows = append(rows, table.Row{idx + 1, fmt.Sprintf("%.2f", core)})
	}

	cpuUsageDataTable := util.ExportTable(header, rows, nil)
	builder.WriteString(cpuUsageDataTable)
}

func writeProcessesToStringBuilder(prcInfo []*sysutils.ProcessInfo, cpuUsage float64, totalMemory float64, builder *strings.Builder) {
	builder.WriteString("\n\nProcesses info:\n")

	prcInfo = sortProcessesByCPU(prcInfo)
	rows := make([]table.Row, 0)
	header := table.Row{"PID", "Name", "% CPU", "% Memory"}

	for _, process := range prcInfo {
		cpu := fmt.Sprintf("%.2f", process.CPUUsage)
		memory := fmt.Sprintf("%.2f", process.Memory)
		rows = append(rows, table.Row{process.Pid, process.Name, cpu, memory})
	}

	footer := table.Row{"Totals", "", fmt.Sprintf("%.2f", cpuUsage), fmt.Sprintf("%.2f", totalMemory)}

	processesTable := util.ExportTable(header, rows, footer)
	builder.WriteString(processesTable)
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

func getData(cliCtx *cli.Context) (diaglib.HardwareInfo, error) {
	var data diaglib.HardwareInfo
	url := "http://" + cliCtx.String(flags.DebugURLFlag.Name) + flags.ApiPath + "/hardware-info"

	err := util.MakeHttpGetCall(cliCtx.Context, url, &data)

	if err != nil {
		return data, err
	}

	return data, nil
}

func getFlagsData(cliCtx *cli.Context) ([]Flag, error) {
	var rawData map[string]map[string]interface{}
	url := "http://" + cliCtx.String(flags.DebugURLFlag.Name) + flags.ApiPath + "/flags"

	err := util.MakeHttpGetCall(cliCtx.Context, url, &rawData)

	if err != nil {
		return nil, err
	}

	flagItems := make([]Flag, 0, len(rawData))
	for name, item := range rawData {
		if item["default"].(bool) {
			continue
		}

		flagItem := Flag{
			Name:    name,
			Value:   item["value"],
			Usage:   item["usage"].(string),
			Default: item["default"].(bool),
		}
		flagItems = append(flagItems, flagItem)
	}

	return flagItems, nil
}
