package downloader

import (
	"fmt"
	"time"

	"github.com/jedib0t/go-pretty/v6/table"
	"github.com/jedib0t/go-pretty/v6/text"
	"github.com/ledgerwatch/erigon-lib/common"
	"github.com/ledgerwatch/erigon-lib/diagnostics"
	"github.com/ledgerwatch/erigon/cmd/diag/flags"
	"github.com/ledgerwatch/erigon/cmd/diag/util"
	"github.com/urfave/cli/v2"
)

var (
	FileFilterFlag = cli.StringFlag{
		Name:     "downloader.file.filter",
		Aliases:  []string{"dff"},
		Usage:    "Filter files list [all|active|inactive|downloaded|queued], dafault value is all",
		Required: false,
		Value:    "all",
	}

	FileNameFlag = cli.StringFlag{
		Name:     "downloader.file.name",
		Aliases:  []string{"dfn"},
		Usage:    "File name to print details about.",
		Required: false,
		Value:    "",
	}
)

var Command = cli.Command{
	Action:    printDownloadStatus,
	Name:      "downloader",
	Aliases:   []string{"dl"},
	Usage:     "Print snapshot download status",
	ArgsUsage: "",
	Flags: []cli.Flag{
		&flags.DebugURLFlag,
		&flags.OutputFlag,
	},
	Subcommands: []*cli.Command{
		{
			Name:      "files",
			Aliases:   []string{"fls"},
			Action:    printFiles,
			Usage:     "Print snapshot download files status",
			ArgsUsage: "",
			Flags: []cli.Flag{
				&flags.DebugURLFlag,
				&flags.OutputFlag,
				&FileFilterFlag,
				&FileNameFlag,
			},
		},
	},
	Description: ``,
}

func printDownloadStatus(cliCtx *cli.Context) error {
	data, err := getData(cliCtx)

	if err != nil {

		return err
	}

	snapshotDownloadStatus := getSnapshotStatusRow(data.SnapshotDownload)

	switch cliCtx.String(flags.OutputFlag.Name) {
	case "json":
		util.RenderJson(snapshotDownloadStatus)

	case "text":
		util.RenderTableWithHeader(
			"Snapshot download info:",
			table.Row{"Status", "Progress", "Downloaded", "Total", "Time Left", "Total Time", "Download Rate", "Upload Rate", "Peers", "Files", "Connections", "Alloc", "Sys"},
			[]table.Row{snapshotDownloadStatus},
		)
	}

	return nil
}

func printFiles(cliCtx *cli.Context) error {
	if cliCtx.String(FileNameFlag.Name) != "" {
		return printFile(cliCtx)
	}

	data, err := getData(cliCtx)

	if err != nil {
		txt := text.Colors{text.FgWhite, text.BgRed}
		fmt.Printf("%s %s", txt.Sprint("[ERROR]"), "Failed to connect to Erigon node.")
		return err
	}

	snapshotDownloadStatus := getSnapshotStatusRow(data.SnapshotDownload)

	snapDownload := data.SnapshotDownload

	files := snapDownload.SegmentsDownloading
	rows := []table.Row{}

	for _, file := range files {
		rows = append(rows, getFileRow(file))
	}

	filteredRows := filterRows(rows, cliCtx.String(FileFilterFlag.Name))

	switch cliCtx.String(flags.OutputFlag.Name) {
	case "json":
		util.RenderJson(snapshotDownloadStatus)
		util.RenderJson(filteredRows)
	case "text":
		//Print overall status
		util.RenderTableWithHeader(
			"Snapshot download info:",
			table.Row{"Status", "Progress", "Downloaded", "Total", "Time Left", "Total Time", "Download Rate", "Upload Rate", "Peers", "Files", "Connections", "Alloc", "Sys"},
			[]table.Row{snapshotDownloadStatus},
		)

		//Print files status
		util.RenderTableWithHeader(
			"Files download info:",
			table.Row{"File", "Progress", "Total", "Downloaded", "Peers", "Peers Download Rate", "Webseeds", "Webseeds Download Rate", "Time Left", "Active"},
			filteredRows,
		)
	}

	return nil
}

func printFile(cliCtx *cli.Context) error {
	data, err := getData(cliCtx)

	if err != nil {
		return err
	}

	snapDownload := data.SnapshotDownload

	if file, ok := snapDownload.SegmentsDownloading[cliCtx.String(FileNameFlag.Name)]; ok {

		if file.DownloadedBytes >= file.TotalBytes {
			fileRow := getDownloadedFileRow(file)
			switch cliCtx.String(flags.OutputFlag.Name) {
			case "json":
				util.RenderJson(fileRow)
			case "text":
				//Print file status
				util.RenderTableWithHeader(
					"File download info:",
					table.Row{"File", "Size", "Average Download Rate", "Time Took"},
					[]table.Row{fileRow},
				)
			}
		} else {
			fileRow := getFileRow(file)
			filePeers := getPeersRows(file.Peers)
			fileWebseeds := getPeersRows(file.Webseeds)

			switch cliCtx.String(flags.OutputFlag.Name) {
			case "json":
				util.RenderJson(fileRow)
				util.RenderJson(filePeers)
				util.RenderJson(fileWebseeds)
			case "text":
				//Print file status
				util.RenderTableWithHeader(
					"file download info:",
					table.Row{"File", "Progress", "Total", "Downloaded", "Peers", "Peers Download Rate", "Webseeds", "Webseeds Download Rate", "Time Left", "Active"},
					[]table.Row{fileRow},
				)

				//Print peers and webseeds status
				util.RenderTableWithHeader(
					"",
					table.Row{"Peer", "Download Rate"},
					filePeers,
				)

				util.RenderTableWithHeader(
					"",
					table.Row{"Webseed", "Download Rate"},
					fileWebseeds,
				)
			}
		}
	} else {
		txt := text.Colors{text.FgWhite, text.BgRed}
		fmt.Printf("%s %s", txt.Sprint("[ERROR]"), "File with name: "+cliCtx.String(FileNameFlag.Name)+" does not exist.")
	}

	return nil
}

func getDownloadedFileRow(file diagnostics.SegmentDownloadStatistics) table.Row {
	averageDownloadRate := common.ByteCount(file.DownloadedStats.AverageRate) + "/s"
	totalDownloadTimeString := time.Duration(file.DownloadedStats.TimeTook) * time.Second

	row := table.Row{
		file.Name,
		common.ByteCount(file.TotalBytes),
		averageDownloadRate,
		totalDownloadTimeString.String(),
	}

	return row
}

func getSnapshotStatusRow(snapDownload diagnostics.SnapshotDownloadStatistics) table.Row {
	status := "Downloading"
	if snapDownload.DownloadFinished {
		status = "Finished"
	}

	downloadedPercent := getPercentDownloaded(snapDownload.Downloaded, snapDownload.Total)

	remainingBytes := snapDownload.Total - snapDownload.Downloaded
	downloadTimeLeft := util.CalculateTime(remainingBytes, snapDownload.DownloadRate)

	totalDownloadTimeString := time.Duration(snapDownload.TotalTime) * time.Second

	rowObj := table.Row{
		status,            // Status
		downloadedPercent, // Progress
		common.ByteCount(snapDownload.Downloaded),          // Downloaded
		common.ByteCount(snapDownload.Total),               // Total
		downloadTimeLeft,                                   // Time Left
		totalDownloadTimeString.String(),                   // Total Time
		common.ByteCount(snapDownload.DownloadRate) + "/s", // Download Rate
		common.ByteCount(snapDownload.UploadRate) + "/s",   // Upload Rate
		snapDownload.Peers,                                 // Peers
		snapDownload.Files,                                 // Files
		snapDownload.Connections,                           // Connections
		common.ByteCount(snapDownload.Alloc),               // Alloc
		common.ByteCount(snapDownload.Sys),                 // Sys
	}

	return rowObj
}

func getFileRow(file diagnostics.SegmentDownloadStatistics) table.Row {
	peersDownloadRate := getFileDownloadRate(file.Peers)
	webseedsDownloadRate := getFileDownloadRate(file.Webseeds)
	totalDownloadRate := peersDownloadRate + webseedsDownloadRate
	downloadedPercent := getPercentDownloaded(file.DownloadedBytes, file.TotalBytes)
	remainingBytes := file.TotalBytes - file.DownloadedBytes
	downloadTimeLeft := util.CalculateTime(remainingBytes, totalDownloadRate)
	isActive := "false"
	if totalDownloadRate > 0 {
		isActive = "true"
	}

	row := table.Row{
		file.Name,
		downloadedPercent,
		common.ByteCount(file.TotalBytes),
		common.ByteCount(file.DownloadedBytes),
		len(file.Peers),
		common.ByteCount(peersDownloadRate) + "/s",
		len(file.Webseeds),
		common.ByteCount(webseedsDownloadRate) + "/s",
		downloadTimeLeft,
		isActive,
	}

	return row
}

func getPeersRows(peers []diagnostics.SegmentPeer) []table.Row {
	rows := make([]table.Row, 0)

	for _, peer := range peers {
		row := table.Row{
			peer.Url,
			common.ByteCount(peer.DownloadRate) + "/s",
		}

		rows = append(rows, row)
	}

	return rows
}

func getFileDownloadRate(peers []diagnostics.SegmentPeer) uint64 {
	var downloadRate uint64

	for _, peer := range peers {
		downloadRate += peer.DownloadRate
	}

	return downloadRate
}

func getData(cliCtx *cli.Context) (diagnostics.SyncStatistics, error) {
	var data diagnostics.SyncStatistics
	url := "http://" + cliCtx.String(flags.DebugURLFlag.Name) + flags.ApiPath + "/snapshot-sync"

	err := util.MakeHttpGetCall(cliCtx.Context, url, &data)

	if err != nil {
		return data, err
	}

	return data, nil
}

func filterRows(rows []table.Row, filter string) []table.Row {
	switch filter {
	case "all":
		return rows
	case "active":
		return filterActive(rows)
	case "inactive":
		return filterInactive(rows)
	case "downloaded":
		return filterDownloaded(rows)
	case "queued":
		return filterQueued(rows)
	}

	return rows
}

func filterActive(rows []table.Row) []table.Row {
	filtered := []table.Row{}

	for _, row := range rows {
		if row[len(row)-1] == "true" {
			filtered = append(filtered, row)
		}
	}

	return filtered
}

func filterInactive(rows []table.Row) []table.Row {
	filtered := []table.Row{}

	for _, row := range rows {
		if row[len(row)-1] == "false" {
			filtered = append(filtered, row)
		}
	}

	return filtered
}

func filterDownloaded(rows []table.Row) []table.Row {
	filtered := []table.Row{}

	for _, row := range rows {
		if row[1] == "100.00%" {
			filtered = append(filtered, row)
		}
	}

	return filtered
}

func filterQueued(rows []table.Row) []table.Row {
	filtered := []table.Row{}

	for _, row := range rows {
		if row[1] == "0.00%" {
			filtered = append(filtered, row)
		}
	}

	return filtered
}

func getPercentDownloaded(downloaded, total uint64) string {
	percent := float32(downloaded) / float32(total/100)

	if percent > 100 {
		percent = 100
	}

	return fmt.Sprintf("%.2f%%", percent)
}
