package downloader

import (
	"fmt"

	"github.com/jedib0t/go-pretty/v6/table"
	"github.com/ledgerwatch/erigon-lib/common"
	"github.com/ledgerwatch/erigon-lib/diagnostics"
	"github.com/ledgerwatch/erigon/cmd/diag/flags"
	"github.com/ledgerwatch/erigon/cmd/diag/util"
	"github.com/urfave/cli/v2"
)

var (
	SegmentsFilterFlag = cli.StringFlag{
		Name:     "downloader.segments.filter",
		Aliases:  []string{"dsf"},
		Usage:    "Filter segments list [all|active|inactive|downloaded|queued], dafault value is all",
		Required: false,
		Value:    "all",
	}

	SenmentNameFlag = cli.StringFlag{
		Name:     "downloader.segment.name",
		Aliases:  []string{"dsn"},
		Usage:    "Segment name to print details about.",
		Required: true,
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
			Name:      "segments",
			Aliases:   []string{"segs"},
			Action:    printSegmentsList,
			Usage:     "print current stage",
			ArgsUsage: "",
			Flags: []cli.Flag{
				&flags.DebugURLFlag,
				&flags.OutputFlag,
				&SegmentsFilterFlag,
			},
		},
		{
			Name:      "segment",
			Aliases:   []string{"seg"},
			Action:    printSegment,
			Usage:     "print current stage",
			ArgsUsage: "",
			Flags: []cli.Flag{
				&flags.DebugURLFlag,
				&flags.OutputFlag,
				&SenmentNameFlag,
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

	snapDownload := data.SnapshotDownload

	status := "Downloading"
	if snapDownload.DownloadFinished {
		status = "Finished"
	}

	downloadedPercent := float32(snapDownload.Downloaded) / float32(snapDownload.Total/100)

	remainingBytes := snapDownload.Total - snapDownload.Downloaded
	downloadTimeLeft := util.CalculateTime(remainingBytes, snapDownload.DownloadRate)

	rowObj := table.Row{
		status,                                   // Status
		fmt.Sprintf("%.2f%%", downloadedPercent), // Progress
		common.ByteCount(snapDownload.Downloaded),          // Downloaded
		common.ByteCount(snapDownload.Total),               // Total
		downloadTimeLeft,                                   // Time Left
		snapDownload.TotalTime,                             // Total Time
		common.ByteCount(snapDownload.DownloadRate) + "/s", // Download Rate
		common.ByteCount(snapDownload.UploadRate) + "/s",   // Upload Rate
		snapDownload.Peers,                                 // Peers
		snapDownload.Files,                                 // Files
		snapDownload.Connections,                           // Connections
		common.ByteCount(snapDownload.Alloc),               // Alloc
		common.ByteCount(snapDownload.Sys),                 // Sys
	}

	switch cliCtx.String(flags.OutputFlag.Name) {
	case "json":
		util.RenderJson(rowObj)

	case "text":
		util.RenderTableWithHeader(
			"Snapshot download info:",
			table.Row{"Status", "Progress", "Downloaded", "Total", "Time Left", "Total Time", "Download Rate", "Upload Rate", "Peers", "Files", "Connections", "Alloc", "Sys"},
			[]table.Row{rowObj},
		)
	}

	return nil
}

func printSegmentsList(cliCtx *cli.Context) error {
	data, err := getData(cliCtx)

	if err != nil {
		return err
	}

	snapDownload := data.SnapshotDownload

	segments := snapDownload.SegmentsDownloading
	rows := []table.Row{}

	for _, segment := range segments {
		rows = append(rows, getSegmentRow(segment))
	}

	filteredRows := filterRows(rows, cliCtx.String(SegmentsFilterFlag.Name))

	switch cliCtx.String(flags.OutputFlag.Name) {
	case "json":
		util.RenderJson(filteredRows)
	case "text":
		util.RenderTableWithHeader(
			"Snapshot download info:",
			table.Row{"Segment", "Progress", "Total", "Downloaded", "Peers", "Peers Download Rate", "Webseeds", "Webseeds Download Rate", "Time Left", "Active"},
			filteredRows,
		)
	}

	return nil
}

func printSegment(cliCtx *cli.Context) error {
	data, err := getData(cliCtx)

	if err != nil {
		return err
	}

	snapDownload := data.SnapshotDownload

	segment := snapDownload.SegmentsDownloading[cliCtx.String(SenmentNameFlag.Name)]
	segRow := getSegmentRow(segment)
	segPeers := getPeersRows(segment.Peers)
	segWebseeds := getPeersRows(segment.Webseeds)

	switch cliCtx.String(flags.OutputFlag.Name) {
	case "json":
		util.RenderJson(segRow)
		util.RenderJson(segPeers)
		util.RenderJson(segWebseeds)
	case "text":
		util.RenderTableWithHeader(
			"Segment download info:",
			table.Row{"Segment", "Progress", "Total", "Downloaded", "Peers", "Peers Download Rate", "Webseeds", "Webseeds Download Rate", "Time Left", "Active"},
			[]table.Row{segRow},
		)

		util.RenderTableWithHeader(
			"",
			table.Row{"Peer", "Download Rate"},
			segPeers,
		)

		util.RenderTableWithHeader(
			"",
			table.Row{"Webseed", "Download Rate"},
			segWebseeds,
		)
	}

	return nil
}

func getSegmentRow(segment diagnostics.SegmentDownloadStatistics) table.Row {
	peersDownloadRate := getSegmentDownloadRate(segment.Peers)
	webseedsDownloadRate := getSegmentDownloadRate(segment.Webseeds)
	totalDownloadRate := peersDownloadRate + webseedsDownloadRate
	downloadedPercent := float32(segment.DownloadedBytes) / float32(segment.TotalBytes/100)
	remainingBytes := segment.TotalBytes - segment.DownloadedBytes
	downloadTimeLeft := util.CalculateTime(remainingBytes, totalDownloadRate)
	isActive := "false"
	if totalDownloadRate > 0 {
		isActive = "true"
	}

	row := table.Row{
		segment.Name,
		fmt.Sprintf("%.2f%%", downloadedPercent),
		common.ByteCount(segment.TotalBytes),
		common.ByteCount(segment.DownloadedBytes),
		len(segment.Peers),
		common.ByteCount(peersDownloadRate) + "/s",
		len(segment.Webseeds),
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

func getSegmentDownloadRate(peers []diagnostics.SegmentPeer) uint64 {
	var downloadRate uint64

	for _, peer := range peers {
		downloadRate += peer.DownloadRate
	}

	return downloadRate
}

func getData(cliCtx *cli.Context) (diagnostics.SyncStatistics, error) {
	var data diagnostics.SyncStatistics
	url := "http://" + cliCtx.String(flags.DebugURLFlag.Name) + "/debug/diag/snapshot-sync"

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
		if row[9] == "true" {
			filtered = append(filtered, row)
		}
	}

	return filtered
}

func filterInactive(rows []table.Row) []table.Row {
	filtered := []table.Row{}

	for _, row := range rows {
		if row[9] == "false" {
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
