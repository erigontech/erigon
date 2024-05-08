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
			table.Row{"Segment", "Progress", "Total", "Downloaded", "Peers", "Peers Download Rate", "Webseeds", "Webseeds Download Rate", "Time Left"},
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
	/*var data diagnostics.SyncStatistics
	url := "http://" + cliCtx.String(flags.DebugURLFlag.Name) + "/debug/diag/snapshot-sync"

	err := util.MakeHttpGetCall(cliCtx.Context, url, &data)

	if err != nil {
		return data, err
	}

	return data, nil*/

	return response, nil
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

var response = diagnostics.SyncStatistics{
	SyncStages: diagnostics.SyncStages{
		StagesList: []string{
			"Snapshots",
			"BlockHashes",
			"Senders",
			"Execution",
			"HashState",
			"IntermediateHashes",
			"CallTraces",
			"AccountHistoryIndex",
			"StorageHistoryIndex",
			"LogIndex",
			"TxLookup",
			"Finish",
		},
		CurrentStage: 0,
	},
	SnapshotDownload: diagnostics.SnapshotDownloadStatistics{
		Downloaded:       1665042899,
		Total:            129371368265,
		TotalTime:        120,
		DownloadRate:     16452782,
		UploadRate:       0,
		Peers:            25,
		Files:            94,
		Connections:      32,
		Alloc:            194878864,
		Sys:              429359448,
		DownloadFinished: false,
		SegmentsDownloading: map[string]diagnostics.SegmentDownloadStatistics{
			"v1-000000-000100-beaconblocks.seg": {
				Name:            "v1-000000-000100-beaconblocks.seg",
				TotalBytes:      66893907,
				DownloadedBytes: 0,
				Webseeds: []diagnostics.SegmentPeer{
					{
						Url:          "caplin-snapshots-sepolia.erigon.network",
						DownloadRate: 0,
					},
					{
						Url:          "erigon2-v1-snapshots-sepolia.erigon.network",
						DownloadRate: 0,
					},
					{
						Url:          "erigon3-v1-snapshots-sepolia.erigon.network",
						DownloadRate: 0,
					},
					{
						Url:          "erigon3-v3-snapshots-sepolia.erigon.network",
						DownloadRate: 0,
					},
				},
				Peers: []diagnostics.SegmentPeer{},
			},
			"v1-000000-000500-bodies.seg": {
				Name:            "v1-000000-000500-bodies.seg",
				TotalBytes:      6973070,
				DownloadedBytes: 0,
				Webseeds: []diagnostics.SegmentPeer{
					{
						Url:          "erigon2-v1-snapshots-sepolia.erigon.network",
						DownloadRate: 0,
					},
					{
						Url:          "erigon3-v1-snapshots-sepolia.erigon.network",
						DownloadRate: 0,
					},
					{
						Url:          "erigon3-v3-snapshots-sepolia.erigon.network",
						DownloadRate: 0,
					},
				},
				Peers: []diagnostics.SegmentPeer{
					{
						Url:          "erigon: 2.57.3",
						DownloadRate: 0,
					},
					{
						Url:          "erigon: 2.57.1-9f1cd651",
						DownloadRate: 0,
					},
					{
						Url:          "erigon: 2.58.0-dev-c2153c75",
						DownloadRate: 0,
					},
					{
						Url:          "erigon: 2.57.3-705814b1",
						DownloadRate: 0,
					},
				},
			},
			"v1-000000-000500-headers.seg": {
				Name:            "v1-000000-000500-headers.seg",
				TotalBytes:      67342768,
				DownloadedBytes: 67342768,
				Webseeds:        []diagnostics.SegmentPeer{},
				Peers:           []diagnostics.SegmentPeer{},
			},
			"v1-000000-000500-transactions.seg": {
				Name:            "v1-000000-000500-transactions.seg",
				TotalBytes:      1086960,
				DownloadedBytes: 0,
				Webseeds: []diagnostics.SegmentPeer{
					{
						Url:          "erigon2-v1-snapshots-sepolia.erigon.network",
						DownloadRate: 0,
					},
					{
						Url:          "erigon3-v1-snapshots-sepolia.erigon.network",
						DownloadRate: 0,
					},
					{
						Url:          "erigon3-v3-snapshots-sepolia.erigon.network",
						DownloadRate: 0,
					},
				},
				Peers: []diagnostics.SegmentPeer{
					{
						Url:          "erigon: 2.55.1-12889fd0",
						DownloadRate: 0,
					},
					{
						Url:          "erigon: 2.57.3-705814b1",
						DownloadRate: 0,
					},
					{
						Url:          "erigon: 2.57.1-9f1cd651",
						DownloadRate: 0,
					},
					{
						Url:          "erigon: 2.58.0-dev-c2153c75",
						DownloadRate: 0,
					},
				},
			},
			"v1-000100-000200-beaconblocks.seg": {
				Name:            "v1-000100-000200-beaconblocks.seg",
				TotalBytes:      77868479,
				DownloadedBytes: 0,
				Webseeds: []diagnostics.SegmentPeer{
					{
						Url:          "caplin-snapshots-sepolia.erigon.network",
						DownloadRate: 0,
					},
					{
						Url:          "erigon2-v1-snapshots-sepolia.erigon.network",
						DownloadRate: 0,
					},
					{
						Url:          "erigon3-v1-snapshots-sepolia.erigon.network",
						DownloadRate: 0,
					},
					{
						Url:          "erigon3-v3-snapshots-sepolia.erigon.network",
						DownloadRate: 0,
					},
				},
				Peers: []diagnostics.SegmentPeer{},
			},
			"v1-000200-000300-beaconblocks.seg": {
				Name:            "v1-000200-000300-beaconblocks.seg",
				TotalBytes:      89325757,
				DownloadedBytes: 0,
				Webseeds: []diagnostics.SegmentPeer{
					{
						Url:          "caplin-snapshots-sepolia.erigon.network",
						DownloadRate: 0,
					},
					{
						Url:          "erigon2-v1-snapshots-sepolia.erigon.network",
						DownloadRate: 0,
					},
					{
						Url:          "erigon3-v1-snapshots-sepolia.erigon.network",
						DownloadRate: 0,
					},
					{
						Url:          "erigon3-v3-snapshots-sepolia.erigon.network",
						DownloadRate: 0,
					},
				},
				Peers: []diagnostics.SegmentPeer{},
			},
			"v1-000300-000400-beaconblocks.seg": {
				Name:            "v1-000300-000400-beaconblocks.seg",
				TotalBytes:      89512625,
				DownloadedBytes: 0,
				Webseeds: []diagnostics.SegmentPeer{
					{
						Url:          "erigon3-v1-snapshots-sepolia.erigon.network",
						DownloadRate: 0,
					},
					{
						Url:          "erigon3-v3-snapshots-sepolia.erigon.network",
						DownloadRate: 0,
					},
					{
						Url:          "caplin-snapshots-sepolia.erigon.network",
						DownloadRate: 0,
					},
					{
						Url:          "erigon2-v1-snapshots-sepolia.erigon.network",
						DownloadRate: 0,
					},
				},
				Peers: []diagnostics.SegmentPeer{},
			},
			"v1-000400-000500-beaconblocks.seg": {
				Name:            "v1-000400-000500-beaconblocks.seg",
				TotalBytes:      85858840,
				DownloadedBytes: 0,
				Webseeds: []diagnostics.SegmentPeer{
					{
						Url:          "caplin-snapshots-sepolia.erigon.network",
						DownloadRate: 0,
					},
					{
						Url:          "erigon2-v1-snapshots-sepolia.erigon.network",
						DownloadRate: 0,
					},
					{
						Url:          "erigon3-v1-snapshots-sepolia.erigon.network",
						DownloadRate: 0,
					},
					{
						Url:          "erigon3-v3-snapshots-sepolia.erigon.network",
						DownloadRate: 0,
					},
				},
				Peers: []diagnostics.SegmentPeer{},
			},
			"v1-000500-000600-beaconblocks.seg": {
				Name:            "v1-000500-000600-beaconblocks.seg",
				TotalBytes:      92750135,
				DownloadedBytes: 0,
				Webseeds: []diagnostics.SegmentPeer{
					{
						Url:          "erigon3-v1-snapshots-sepolia.erigon.network",
						DownloadRate: 0,
					},
					{
						Url:          "erigon3-v3-snapshots-sepolia.erigon.network",
						DownloadRate: 0,
					},
					{
						Url:          "caplin-snapshots-sepolia.erigon.network",
						DownloadRate: 0,
					},
					{
						Url:          "erigon2-v1-snapshots-sepolia.erigon.network",
						DownloadRate: 0,
					},
				},
				Peers: []diagnostics.SegmentPeer{},
			},
			"v1-000500-001000-bodies.seg": {
				Name:            "v1-000500-001000-bodies.seg",
				TotalBytes:      5847723,
				DownloadedBytes: 0,
				Webseeds: []diagnostics.SegmentPeer{
					{
						Url:          "erigon2-v1-snapshots-sepolia.erigon.network",
						DownloadRate: 0,
					},
					{
						Url:          "erigon3-v1-snapshots-sepolia.erigon.network",
						DownloadRate: 0,
					},
					{
						Url:          "erigon3-v3-snapshots-sepolia.erigon.network",
						DownloadRate: 0,
					},
				},
				Peers: []diagnostics.SegmentPeer{
					{
						Url:          "erigon: 2.57.3",
						DownloadRate: 0,
					},
					{
						Url:          "erigon: 2.57.3-6ce460d1",
						DownloadRate: 0,
					},
					{
						Url:          "erigon: 2.57.1",
						DownloadRate: 0,
					},
					{
						Url:          "erigon: 2.57.0-4f6eda76",
						DownloadRate: 0,
					},
				},
			},
			"v1-000500-001000-headers.seg": {
				Name:            "v1-000500-001000-headers.seg",
				TotalBytes:      67051048,
				DownloadedBytes: 0,
				Webseeds: []diagnostics.SegmentPeer{
					{
						Url:          "erigon3-v1-snapshots-sepolia.erigon.network",
						DownloadRate: 0,
					},
					{
						Url:          "erigon3-v3-snapshots-sepolia.erigon.network",
						DownloadRate: 0,
					},
					{
						Url:          "erigon2-v1-snapshots-sepolia.erigon.network",
						DownloadRate: 0,
					},
				},
				Peers: []diagnostics.SegmentPeer{
					{
						Url:          "erigon: 2.57.3",
						DownloadRate: 0,
					},
					{
						Url:          "erigon: 2.57.3",
						DownloadRate: 0,
					},
				},
			},
			"v1-000500-001000-transactions.seg": {
				Name:            "v1-000500-001000-transactions.seg",
				TotalBytes:      5564757,
				DownloadedBytes: 5564757,
				Webseeds:        []diagnostics.SegmentPeer{},
				Peers:           []diagnostics.SegmentPeer{},
			},
			"v1-000600-000700-beaconblocks.seg": {
				Name:            "v1-000600-000700-beaconblocks.seg",
				TotalBytes:      82484504,
				DownloadedBytes: 0,
				Webseeds: []diagnostics.SegmentPeer{
					{
						Url:          "caplin-snapshots-sepolia.erigon.network",
						DownloadRate: 0,
					},
					{
						Url:          "erigon2-v1-snapshots-sepolia.erigon.network",
						DownloadRate: 0,
					},
					{
						Url:          "erigon3-v1-snapshots-sepolia.erigon.network",
						DownloadRate: 0,
					},
					{
						Url:          "erigon3-v3-snapshots-sepolia.erigon.network",
						DownloadRate: 0,
					},
				},
				Peers: []diagnostics.SegmentPeer{},
			},
			"v1-000700-000800-beaconblocks.seg": {
				Name:            "v1-000700-000800-beaconblocks.seg",
				TotalBytes:      80954474,
				DownloadedBytes: 0,
				Webseeds: []diagnostics.SegmentPeer{
					{
						Url:          "erigon3-v1-snapshots-sepolia.erigon.network",
						DownloadRate: 0,
					},
					{
						Url:          "erigon3-v3-snapshots-sepolia.erigon.network",
						DownloadRate: 0,
					},
					{
						Url:          "caplin-snapshots-sepolia.erigon.network",
						DownloadRate: 0,
					},
					{
						Url:          "erigon2-v1-snapshots-sepolia.erigon.network",
						DownloadRate: 0,
					},
				},
				Peers: []diagnostics.SegmentPeer{},
			},
			"v1-000800-000900-beaconblocks.seg": {
				Name:            "v1-000800-000900-beaconblocks.seg",
				TotalBytes:      78768662,
				DownloadedBytes: 0,
				Webseeds: []diagnostics.SegmentPeer{
					{
						Url:          "erigon2-v1-snapshots-sepolia.erigon.network",
						DownloadRate: 0,
					},
					{
						Url:          "erigon3-v1-snapshots-sepolia.erigon.network",
						DownloadRate: 0,
					},
					{
						Url:          "erigon3-v3-snapshots-sepolia.erigon.network",
						DownloadRate: 0,
					},
					{
						Url:          "caplin-snapshots-sepolia.erigon.network",
						DownloadRate: 0,
					},
				},
				Peers: []diagnostics.SegmentPeer{},
			},
			"v1-000900-001000-beaconblocks.seg": {
				Name:            "v1-000900-001000-beaconblocks.seg",
				TotalBytes:      81316371,
				DownloadedBytes: 0,
				Webseeds: []diagnostics.SegmentPeer{
					{
						Url:          "caplin-snapshots-sepolia.erigon.network",
						DownloadRate: 0,
					},
					{
						Url:          "erigon2-v1-snapshots-sepolia.erigon.network",
						DownloadRate: 0,
					},
					{
						Url:          "erigon3-v1-snapshots-sepolia.erigon.network",
						DownloadRate: 0,
					},
					{
						Url:          "erigon3-v3-snapshots-sepolia.erigon.network",
						DownloadRate: 0,
					},
				},
				Peers: []diagnostics.SegmentPeer{},
			},
			"v1-001000-001100-beaconblocks.seg": {
				Name:            "v1-001000-001100-beaconblocks.seg",
				TotalBytes:      80106793,
				DownloadedBytes: 0,
				Webseeds: []diagnostics.SegmentPeer{
					{
						Url:          "caplin-snapshots-sepolia.erigon.network",
						DownloadRate: 0,
					},
					{
						Url:          "erigon2-v1-snapshots-sepolia.erigon.network",
						DownloadRate: 0,
					},
					{
						Url:          "erigon3-v1-snapshots-sepolia.erigon.network",
						DownloadRate: 0,
					},
					{
						Url:          "erigon3-v3-snapshots-sepolia.erigon.network",
						DownloadRate: 0,
					},
				},
				Peers: []diagnostics.SegmentPeer{},
			},
			"v1-001000-001500-bodies.seg": {
				Name:            "v1-001000-001500-bodies.seg",
				TotalBytes:      8171368,
				DownloadedBytes: 0,
				Webseeds: []diagnostics.SegmentPeer{
					{
						Url:          "erigon2-v1-snapshots-sepolia.erigon.network",
						DownloadRate: 0,
					},
					{
						Url:          "erigon3-v1-snapshots-sepolia.erigon.network",
						DownloadRate: 0,
					},
					{
						Url:          "erigon3-v3-snapshots-sepolia.erigon.network",
						DownloadRate: 0,
					},
				},
				Peers: []diagnostics.SegmentPeer{},
			},
			"v1-001000-001500-headers.seg": {
				Name:            "v1-001000-001500-headers.seg",
				TotalBytes:      82154550,
				DownloadedBytes: 0,
				Webseeds: []diagnostics.SegmentPeer{
					{
						Url:          "erigon2-v1-snapshots-sepolia.erigon.network",
						DownloadRate: 0,
					},
					{
						Url:          "erigon3-v1-snapshots-sepolia.erigon.network",
						DownloadRate: 0,
					},
					{
						Url:          "erigon3-v3-snapshots-sepolia.erigon.network",
						DownloadRate: 0,
					},
				},
				Peers: []diagnostics.SegmentPeer{
					{
						Url:          "erigon: 2.57.3",
						DownloadRate: 0,
					},
					{
						Url:          "erigon: 2.57.1",
						DownloadRate: 0,
					},
				},
			},
			"v1-001000-001500-transactions.seg": {
				Name:            "v1-001000-001500-transactions.seg",
				TotalBytes:      116625291,
				DownloadedBytes: 0,
				Webseeds: []diagnostics.SegmentPeer{
					{
						Url:          "erigon2-v1-snapshots-sepolia.erigon.network",
						DownloadRate: 0,
					},
					{
						Url:          "erigon3-v1-snapshots-sepolia.erigon.network",
						DownloadRate: 0,
					},
					{
						Url:          "erigon3-v3-snapshots-sepolia.erigon.network",
						DownloadRate: 0,
					},
				},
				Peers: []diagnostics.SegmentPeer{},
			},
			"v1-001100-001200-beaconblocks.seg": {
				Name:            "v1-001100-001200-beaconblocks.seg",
				TotalBytes:      81405651,
				DownloadedBytes: 0,
				Webseeds: []diagnostics.SegmentPeer{
					{
						Url:          "caplin-snapshots-sepolia.erigon.network",
						DownloadRate: 0,
					},
					{
						Url:          "erigon2-v1-snapshots-sepolia.erigon.network",
						DownloadRate: 0,
					},
					{
						Url:          "erigon3-v1-snapshots-sepolia.erigon.network",
						DownloadRate: 0,
					},
					{
						Url:          "erigon3-v3-snapshots-sepolia.erigon.network",
						DownloadRate: 0,
					},
				},
				Peers: []diagnostics.SegmentPeer{},
			},
			"v1-001200-001300-beaconblocks.seg": {
				Name:            "v1-001200-001300-beaconblocks.seg",
				TotalBytes:      84660453,
				DownloadedBytes: 84660453,
				Webseeds:        []diagnostics.SegmentPeer{},
				Peers:           []diagnostics.SegmentPeer{},
			},
			"v1-001300-001400-beaconblocks.seg": {
				Name:            "v1-001300-001400-beaconblocks.seg",
				TotalBytes:      82353516,
				DownloadedBytes: 0,
				Webseeds: []diagnostics.SegmentPeer{
					{
						Url:          "erigon3-v3-snapshots-sepolia.erigon.network",
						DownloadRate: 0,
					},
					{
						Url:          "caplin-snapshots-sepolia.erigon.network",
						DownloadRate: 0,
					},
					{
						Url:          "erigon2-v1-snapshots-sepolia.erigon.network",
						DownloadRate: 0,
					},
					{
						Url:          "erigon3-v1-snapshots-sepolia.erigon.network",
						DownloadRate: 0,
					},
				},
				Peers: []diagnostics.SegmentPeer{},
			},
			"v1-001400-001500-beaconblocks.seg": {
				Name:            "v1-001400-001500-beaconblocks.seg",
				TotalBytes:      85205467,
				DownloadedBytes: 85205467,
				Webseeds:        []diagnostics.SegmentPeer{},
				Peers:           []diagnostics.SegmentPeer{},
			},
			"v1-001500-001600-beaconblocks.seg": {
				Name:            "v1-001500-001600-beaconblocks.seg",
				TotalBytes:      85564180,
				DownloadedBytes: 0,
				Webseeds: []diagnostics.SegmentPeer{
					{
						Url:          "caplin-snapshots-sepolia.erigon.network",
						DownloadRate: 0,
					},
					{
						Url:          "erigon2-v1-snapshots-sepolia.erigon.network",
						DownloadRate: 0,
					},
					{
						Url:          "erigon3-v1-snapshots-sepolia.erigon.network",
						DownloadRate: 0,
					},
					{
						Url:          "erigon3-v3-snapshots-sepolia.erigon.network",
						DownloadRate: 0,
					},
				},
				Peers: []diagnostics.SegmentPeer{},
			},
			"v1-001500-002000-bodies.seg": {
				Name:            "v1-001500-002000-bodies.seg",
				TotalBytes:      4000085,
				DownloadedBytes: 0,
				Webseeds: []diagnostics.SegmentPeer{
					{
						Url:          "erigon2-v1-snapshots-sepolia.erigon.network",
						DownloadRate: 0,
					},
					{
						Url:          "erigon3-v1-snapshots-sepolia.erigon.network",
						DownloadRate: 0,
					},
					{
						Url:          "erigon3-v3-snapshots-sepolia.erigon.network",
						DownloadRate: 0,
					},
				},
				Peers: []diagnostics.SegmentPeer{},
			},
			"v1-001500-002000-headers.seg": {
				Name:            "v1-001500-002000-headers.seg",
				TotalBytes:      92889249,
				DownloadedBytes: 0,
				Webseeds: []diagnostics.SegmentPeer{
					{
						Url:          "erigon2-v1-snapshots-sepolia.erigon.network",
						DownloadRate: 0,
					},
					{
						Url:          "erigon3-v1-snapshots-sepolia.erigon.network",
						DownloadRate: 0,
					},
					{
						Url:          "erigon3-v3-snapshots-sepolia.erigon.network",
						DownloadRate: 0,
					},
				},
				Peers: []diagnostics.SegmentPeer{},
			},
			"v1-001500-002000-transactions.seg": {
				Name:            "v1-001500-002000-transactions.seg",
				TotalBytes:      440890246,
				DownloadedBytes: 0,
				Webseeds: []diagnostics.SegmentPeer{
					{
						Url:          "erigon2-v1-snapshots-sepolia.erigon.network",
						DownloadRate: 0,
					},
					{
						Url:          "erigon3-v1-snapshots-sepolia.erigon.network",
						DownloadRate: 0,
					},
					{
						Url:          "erigon3-v3-snapshots-sepolia.erigon.network",
						DownloadRate: 0,
					},
				},
				Peers: []diagnostics.SegmentPeer{},
			},
			"v1-001600-001700-beaconblocks.seg": {
				Name:            "v1-001600-001700-beaconblocks.seg",
				TotalBytes:      87316889,
				DownloadedBytes: 0,
				Webseeds: []diagnostics.SegmentPeer{
					{
						Url:          "erigon3-v1-snapshots-sepolia.erigon.network",
						DownloadRate: 0,
					},
					{
						Url:          "erigon3-v3-snapshots-sepolia.erigon.network",
						DownloadRate: 0,
					},
					{
						Url:          "caplin-snapshots-sepolia.erigon.network",
						DownloadRate: 0,
					},
					{
						Url:          "erigon2-v1-snapshots-sepolia.erigon.network",
						DownloadRate: 0,
					},
				},
				Peers: []diagnostics.SegmentPeer{},
			},
			"v1-001700-001800-beaconblocks.seg": {
				Name:            "v1-001700-001800-beaconblocks.seg",
				TotalBytes:      87100417,
				DownloadedBytes: 87100417,
				Webseeds:        []diagnostics.SegmentPeer{},
				Peers:           []diagnostics.SegmentPeer{},
			},
			"v1-001800-001900-beaconblocks.seg": {
				Name:            "v1-001800-001900-beaconblocks.seg",
				TotalBytes:      93955086,
				DownloadedBytes: 0,
				Webseeds: []diagnostics.SegmentPeer{
					{
						Url:          "erigon3-v1-snapshots-sepolia.erigon.network",
						DownloadRate: 0,
					},
					{
						Url:          "erigon3-v3-snapshots-sepolia.erigon.network",
						DownloadRate: 0,
					},
					{
						Url:          "caplin-snapshots-sepolia.erigon.network",
						DownloadRate: 0,
					},
					{
						Url:          "erigon2-v1-snapshots-sepolia.erigon.network",
						DownloadRate: 0,
					},
				},
				Peers: []diagnostics.SegmentPeer{},
			},
			"v1-001900-002000-beaconblocks.seg": {
				Name:            "v1-001900-002000-beaconblocks.seg",
				TotalBytes:      95372213,
				DownloadedBytes: 0,
				Webseeds: []diagnostics.SegmentPeer{
					{
						Url:          "erigon2-v1-snapshots-sepolia.erigon.network",
						DownloadRate: 0,
					},
					{
						Url:          "erigon3-v1-snapshots-sepolia.erigon.network",
						DownloadRate: 0,
					},
					{
						Url:          "erigon3-v3-snapshots-sepolia.erigon.network",
						DownloadRate: 0,
					},
					{
						Url:          "caplin-snapshots-sepolia.erigon.network",
						DownloadRate: 0,
					},
				},
				Peers: []diagnostics.SegmentPeer{},
			},
			"v1-002000-002100-beaconblocks.seg": {
				Name:            "v1-002000-002100-beaconblocks.seg",
				TotalBytes:      102466195,
				DownloadedBytes: 0,
				Webseeds: []diagnostics.SegmentPeer{
					{
						Url:          "erigon2-v1-snapshots-sepolia.erigon.network",
						DownloadRate: 0,
					},
					{
						Url:          "erigon3-v1-snapshots-sepolia.erigon.network",
						DownloadRate: 0,
					},
					{
						Url:          "erigon3-v3-snapshots-sepolia.erigon.network",
						DownloadRate: 0,
					},
					{
						Url:          "caplin-snapshots-sepolia.erigon.network",
						DownloadRate: 0,
					},
				},
				Peers: []diagnostics.SegmentPeer{},
			},
			"v1-002000-002500-bodies.seg": {
				Name:            "v1-002000-002500-bodies.seg",
				TotalBytes:      4000819,
				DownloadedBytes: 0,
				Webseeds: []diagnostics.SegmentPeer{
					{
						Url:          "erigon3-v1-snapshots-sepolia.erigon.network",
						DownloadRate: 0,
					},
					{
						Url:          "erigon3-v3-snapshots-sepolia.erigon.network",
						DownloadRate: 0,
					},
					{
						Url:          "erigon2-v1-snapshots-sepolia.erigon.network",
						DownloadRate: 0,
					},
				},
				Peers: []diagnostics.SegmentPeer{},
			},
			"v1-002000-002500-headers.seg": {
				Name:            "v1-002000-002500-headers.seg",
				TotalBytes:      94534387,
				DownloadedBytes: 0,
				Webseeds: []diagnostics.SegmentPeer{
					{
						Url:          "erigon2-v1-snapshots-sepolia.erigon.network",
						DownloadRate: 0,
					},
					{
						Url:          "erigon3-v1-snapshots-sepolia.erigon.network",
						DownloadRate: 0,
					},
					{
						Url:          "erigon3-v3-snapshots-sepolia.erigon.network",
						DownloadRate: 0,
					},
				},
				Peers: []diagnostics.SegmentPeer{},
			},
			"v1-002000-002500-transactions.seg": {
				Name:            "v1-002000-002500-transactions.seg",
				TotalBytes:      525304114,
				DownloadedBytes: 0,
				Webseeds: []diagnostics.SegmentPeer{
					{
						Url:          "erigon2-v1-snapshots-sepolia.erigon.network",
						DownloadRate: 0,
					},
					{
						Url:          "erigon3-v1-snapshots-sepolia.erigon.network",
						DownloadRate: 0,
					},
					{
						Url:          "erigon3-v3-snapshots-sepolia.erigon.network",
						DownloadRate: 0,
					},
				},
				Peers: []diagnostics.SegmentPeer{},
			},
			"v1-002100-002200-beaconblocks.seg": {
				Name:            "v1-002100-002200-beaconblocks.seg",
				TotalBytes:      103118662,
				DownloadedBytes: 0,
				Webseeds: []diagnostics.SegmentPeer{
					{
						Url:          "erigon3-v3-snapshots-sepolia.erigon.network",
						DownloadRate: 0,
					},
					{
						Url:          "caplin-snapshots-sepolia.erigon.network",
						DownloadRate: 0,
					},
					{
						Url:          "erigon2-v1-snapshots-sepolia.erigon.network",
						DownloadRate: 0,
					},
					{
						Url:          "erigon3-v1-snapshots-sepolia.erigon.network",
						DownloadRate: 0,
					},
				},
				Peers: []diagnostics.SegmentPeer{},
			},
			"v1-002200-002300-beaconblocks.seg": {
				Name:            "v1-002200-002300-beaconblocks.seg",
				TotalBytes:      99698462,
				DownloadedBytes: 0,
				Webseeds: []diagnostics.SegmentPeer{
					{
						Url:          "erigon2-v1-snapshots-sepolia.erigon.network",
						DownloadRate: 0,
					},
					{
						Url:          "erigon3-v1-snapshots-sepolia.erigon.network",
						DownloadRate: 0,
					},
					{
						Url:          "erigon3-v3-snapshots-sepolia.erigon.network",
						DownloadRate: 0,
					},
					{
						Url:          "caplin-snapshots-sepolia.erigon.network",
						DownloadRate: 0,
					},
				},
				Peers: []diagnostics.SegmentPeer{},
			},
			"v1-002300-002400-beaconblocks.seg": {
				Name:            "v1-002300-002400-beaconblocks.seg",
				TotalBytes:      104143870,
				DownloadedBytes: 0,
				Webseeds: []diagnostics.SegmentPeer{
					{
						Url:          "caplin-snapshots-sepolia.erigon.network",
						DownloadRate: 0,
					},
					{
						Url:          "erigon2-v1-snapshots-sepolia.erigon.network",
						DownloadRate: 0,
					},
					{
						Url:          "erigon3-v1-snapshots-sepolia.erigon.network",
						DownloadRate: 0,
					},
					{
						Url:          "erigon3-v3-snapshots-sepolia.erigon.network",
						DownloadRate: 0,
					},
				},
				Peers: []diagnostics.SegmentPeer{},
			},
			"v1-002400-002500-beaconblocks.seg": {
				Name:            "v1-002400-002500-beaconblocks.seg",
				TotalBytes:      102976931,
				DownloadedBytes: 0,
				Webseeds: []diagnostics.SegmentPeer{
					{
						Url:          "caplin-snapshots-sepolia.erigon.network",
						DownloadRate: 0,
					},
					{
						Url:          "erigon2-v1-snapshots-sepolia.erigon.network",
						DownloadRate: 0,
					},
					{
						Url:          "erigon3-v1-snapshots-sepolia.erigon.network",
						DownloadRate: 0,
					},
					{
						Url:          "erigon3-v3-snapshots-sepolia.erigon.network",
						DownloadRate: 0,
					},
				},
				Peers: []diagnostics.SegmentPeer{},
			},
			"v1-002500-002600-beaconblocks.seg": {
				Name:            "v1-002500-002600-beaconblocks.seg",
				TotalBytes:      106024800,
				DownloadedBytes: 0,
				Webseeds: []diagnostics.SegmentPeer{
					{
						Url:          "caplin-snapshots-sepolia.erigon.network",
						DownloadRate: 0,
					},
					{
						Url:          "erigon2-v1-snapshots-sepolia.erigon.network",
						DownloadRate: 0,
					},
					{
						Url:          "erigon3-v1-snapshots-sepolia.erigon.network",
						DownloadRate: 0,
					},
					{
						Url:          "erigon3-v3-snapshots-sepolia.erigon.network",
						DownloadRate: 0,
					},
				},
				Peers: []diagnostics.SegmentPeer{},
			},
			"v1-002500-003000-bodies.seg": {
				Name:            "v1-002500-003000-bodies.seg",
				TotalBytes:      4758876,
				DownloadedBytes: 0,
				Webseeds: []diagnostics.SegmentPeer{
					{
						Url:          "erigon3-v1-snapshots-sepolia.erigon.network",
						DownloadRate: 0,
					},
					{
						Url:          "erigon3-v3-snapshots-sepolia.erigon.network",
						DownloadRate: 0,
					},
					{
						Url:          "erigon2-v1-snapshots-sepolia.erigon.network",
						DownloadRate: 0,
					},
				},
				Peers: []diagnostics.SegmentPeer{},
			},
			"v1-002500-003000-headers.seg": {
				Name:            "v1-002500-003000-headers.seg",
				TotalBytes:      100959913,
				DownloadedBytes: 100959913,
				Webseeds:        []diagnostics.SegmentPeer{},
				Peers:           []diagnostics.SegmentPeer{},
			},
			"v1-002500-003000-transactions.seg": {
				Name:            "v1-002500-003000-transactions.seg",
				TotalBytes:      529139896,
				DownloadedBytes: 0,
				Webseeds: []diagnostics.SegmentPeer{
					{
						Url:          "erigon2-v1-snapshots-sepolia.erigon.network",
						DownloadRate: 0,
					},
					{
						Url:          "erigon3-v1-snapshots-sepolia.erigon.network",
						DownloadRate: 0,
					},
					{
						Url:          "erigon3-v3-snapshots-sepolia.erigon.network",
						DownloadRate: 0,
					},
				},
				Peers: []diagnostics.SegmentPeer{},
			},
			"v1-002600-002700-beaconblocks.seg": {
				Name:            "v1-002600-002700-beaconblocks.seg",
				TotalBytes:      104204696,
				DownloadedBytes: 104204696,
				Webseeds:        []diagnostics.SegmentPeer{},
				Peers:           []diagnostics.SegmentPeer{},
			},
			"v1-002700-002800-beaconblocks.seg": {
				Name:            "v1-002700-002800-beaconblocks.seg",
				TotalBytes:      103203354,
				DownloadedBytes: 0,
				Webseeds: []diagnostics.SegmentPeer{
					{
						Url:          "caplin-snapshots-sepolia.erigon.network",
						DownloadRate: 0,
					},
					{
						Url:          "erigon2-v1-snapshots-sepolia.erigon.network",
						DownloadRate: 0,
					},
					{
						Url:          "erigon3-v1-snapshots-sepolia.erigon.network",
						DownloadRate: 0,
					},
					{
						Url:          "erigon3-v3-snapshots-sepolia.erigon.network",
						DownloadRate: 0,
					},
				},
				Peers: []diagnostics.SegmentPeer{},
			},
			"v1-002800-002900-beaconblocks.seg": {
				Name:            "v1-002800-002900-beaconblocks.seg",
				TotalBytes:      102907908,
				DownloadedBytes: 0,
				Webseeds: []diagnostics.SegmentPeer{
					{
						Url:          "caplin-snapshots-sepolia.erigon.network",
						DownloadRate: 0,
					},
					{
						Url:          "erigon2-v1-snapshots-sepolia.erigon.network",
						DownloadRate: 0,
					},
					{
						Url:          "erigon3-v1-snapshots-sepolia.erigon.network",
						DownloadRate: 0,
					},
					{
						Url:          "erigon3-v3-snapshots-sepolia.erigon.network",
						DownloadRate: 0,
					},
				},
				Peers: []diagnostics.SegmentPeer{},
			},
			"v1-002900-003000-beaconblocks.seg": {
				Name:            "v1-002900-003000-beaconblocks.seg",
				TotalBytes:      99486950,
				DownloadedBytes: 99486950,
				Webseeds:        []diagnostics.SegmentPeer{},
				Peers:           []diagnostics.SegmentPeer{},
			},
			"v1-003000-003100-beaconblocks.seg": {
				Name:            "v1-003000-003100-beaconblocks.seg",
				TotalBytes:      103156852,
				DownloadedBytes: 0,
				Webseeds: []diagnostics.SegmentPeer{
					{
						Url:          "caplin-snapshots-sepolia.erigon.network",
						DownloadRate: 0,
					},
					{
						Url:          "erigon2-v1-snapshots-sepolia.erigon.network",
						DownloadRate: 0,
					},
					{
						Url:          "erigon3-v1-snapshots-sepolia.erigon.network",
						DownloadRate: 0,
					},
					{
						Url:          "erigon3-v3-snapshots-sepolia.erigon.network",
						DownloadRate: 0,
					},
				},
				Peers: []diagnostics.SegmentPeer{},
			},
			"v1-003000-003500-bodies.seg": {
				Name:            "v1-003000-003500-bodies.seg",
				TotalBytes:      58375905,
				DownloadedBytes: 0,
				Webseeds: []diagnostics.SegmentPeer{
					{
						Url:          "erigon2-v1-snapshots-sepolia.erigon.network",
						DownloadRate: 0,
					},
					{
						Url:          "erigon3-v1-snapshots-sepolia.erigon.network",
						DownloadRate: 0,
					},
					{
						Url:          "erigon3-v3-snapshots-sepolia.erigon.network",
						DownloadRate: 0,
					},
				},
				Peers: []diagnostics.SegmentPeer{
					{
						Url:          "erigon: 2.54.0",
						DownloadRate: 0,
					},
				},
			},
			"v1-003000-003500-headers.seg": {
				Name:            "v1-003000-003500-headers.seg",
				TotalBytes:      169897389,
				DownloadedBytes: 0,
				Webseeds: []diagnostics.SegmentPeer{
					{
						Url:          "erigon2-v1-snapshots-sepolia.erigon.network",
						DownloadRate: 0,
					},
					{
						Url:          "erigon3-v1-snapshots-sepolia.erigon.network",
						DownloadRate: 0,
					},
					{
						Url:          "erigon3-v3-snapshots-sepolia.erigon.network",
						DownloadRate: 0,
					},
				},
				Peers: []diagnostics.SegmentPeer{},
			},
			"v1-003000-003500-transactions.seg": {
				Name:            "v1-003000-003500-transactions.seg",
				TotalBytes:      7415206119,
				DownloadedBytes: 0,
				Webseeds: []diagnostics.SegmentPeer{
					{
						Url:          "erigon2-v1-snapshots-sepolia.erigon.network",
						DownloadRate: 0,
					},
					{
						Url:          "erigon3-v1-snapshots-sepolia.erigon.network",
						DownloadRate: 0,
					},
					{
						Url:          "erigon3-v3-snapshots-sepolia.erigon.network",
						DownloadRate: 0,
					},
				},
				Peers: []diagnostics.SegmentPeer{},
			},
			"v1-003100-003200-beaconblocks.seg": {
				Name:            "v1-003100-003200-beaconblocks.seg",
				TotalBytes:      104387989,
				DownloadedBytes: 0,
				Webseeds: []diagnostics.SegmentPeer{
					{
						Url:          "erigon2-v1-snapshots-sepolia.erigon.network",
						DownloadRate: 0,
					},
					{
						Url:          "erigon3-v1-snapshots-sepolia.erigon.network",
						DownloadRate: 0,
					},
					{
						Url:          "erigon3-v3-snapshots-sepolia.erigon.network",
						DownloadRate: 0,
					},
					{
						Url:          "caplin-snapshots-sepolia.erigon.network",
						DownloadRate: 0,
					},
				},
				Peers: []diagnostics.SegmentPeer{},
			},
			"v1-003200-003300-beaconblocks.seg": {
				Name:            "v1-003200-003300-beaconblocks.seg",
				TotalBytes:      106942540,
				DownloadedBytes: 0,
				Webseeds: []diagnostics.SegmentPeer{
					{
						Url:          "caplin-snapshots-sepolia.erigon.network",
						DownloadRate: 0,
					},
					{
						Url:          "erigon2-v1-snapshots-sepolia.erigon.network",
						DownloadRate: 0,
					},
					{
						Url:          "erigon3-v1-snapshots-sepolia.erigon.network",
						DownloadRate: 0,
					},
					{
						Url:          "erigon3-v3-snapshots-sepolia.erigon.network",
						DownloadRate: 0,
					},
				},
				Peers: []diagnostics.SegmentPeer{},
			},
			"v1-003300-003400-beaconblocks.seg": {
				Name:            "v1-003300-003400-beaconblocks.seg",
				TotalBytes:      111036925,
				DownloadedBytes: 0,
				Webseeds: []diagnostics.SegmentPeer{
					{
						Url:          "caplin-snapshots-sepolia.erigon.network",
						DownloadRate: 0,
					},
					{
						Url:          "erigon2-v1-snapshots-sepolia.erigon.network",
						DownloadRate: 0,
					},
					{
						Url:          "erigon3-v1-snapshots-sepolia.erigon.network",
						DownloadRate: 0,
					},
					{
						Url:          "erigon3-v3-snapshots-sepolia.erigon.network",
						DownloadRate: 0,
					},
				},
				Peers: []diagnostics.SegmentPeer{},
			},
			"v1-003400-003500-beaconblocks.seg": {
				Name:            "v1-003400-003500-beaconblocks.seg",
				TotalBytes:      105306366,
				DownloadedBytes: 0,
				Webseeds: []diagnostics.SegmentPeer{
					{
						Url:          "caplin-snapshots-sepolia.erigon.network",
						DownloadRate: 0,
					},
					{
						Url:          "erigon2-v1-snapshots-sepolia.erigon.network",
						DownloadRate: 0,
					},
					{
						Url:          "erigon3-v1-snapshots-sepolia.erigon.network",
						DownloadRate: 0,
					},
					{
						Url:          "erigon3-v3-snapshots-sepolia.erigon.network",
						DownloadRate: 0,
					},
				},
				Peers: []diagnostics.SegmentPeer{},
			},
			"v1-003500-003600-beaconblocks.seg": {
				Name:            "v1-003500-003600-beaconblocks.seg",
				TotalBytes:      112169756,
				DownloadedBytes: 0,
				Webseeds: []diagnostics.SegmentPeer{
					{
						Url:          "erigon3-v1-snapshots-sepolia.erigon.network",
						DownloadRate: 0,
					},
					{
						Url:          "erigon3-v3-snapshots-sepolia.erigon.network",
						DownloadRate: 0,
					},
					{
						Url:          "caplin-snapshots-sepolia.erigon.network",
						DownloadRate: 0,
					},
					{
						Url:          "erigon2-v1-snapshots-sepolia.erigon.network",
						DownloadRate: 0,
					},
				},
				Peers: []diagnostics.SegmentPeer{},
			},
			"v1-003500-004000-bodies.seg": {
				Name:            "v1-003500-004000-bodies.seg",
				TotalBytes:      64651872,
				DownloadedBytes: 0,
				Webseeds: []diagnostics.SegmentPeer{
					{
						Url:          "erigon2-v1-snapshots-sepolia.erigon.network",
						DownloadRate: 0,
					},
					{
						Url:          "erigon3-v1-snapshots-sepolia.erigon.network",
						DownloadRate: 0,
					},
					{
						Url:          "erigon3-v3-snapshots-sepolia.erigon.network",
						DownloadRate: 0,
					},
				},
				Peers: []diagnostics.SegmentPeer{},
			},
			"v1-003500-004000-headers.seg": {
				Name:            "v1-003500-004000-headers.seg",
				TotalBytes:      207983600,
				DownloadedBytes: 0,
				Webseeds: []diagnostics.SegmentPeer{
					{
						Url:          "erigon2-v1-snapshots-sepolia.erigon.network",
						DownloadRate: 0,
					},
					{
						Url:          "erigon3-v1-snapshots-sepolia.erigon.network",
						DownloadRate: 0,
					},
					{
						Url:          "erigon3-v3-snapshots-sepolia.erigon.network",
						DownloadRate: 0,
					},
				},
				Peers: []diagnostics.SegmentPeer{},
			},
			"v1-003500-004000-transactions.seg": {
				Name:            "v1-003500-004000-transactions.seg",
				TotalBytes:      30627044213,
				DownloadedBytes: 0,
				Webseeds: []diagnostics.SegmentPeer{
					{
						Url:          "erigon3-v3-snapshots-sepolia.erigon.network",
						DownloadRate: 0,
					},
					{
						Url:          "erigon2-v1-snapshots-sepolia.erigon.network",
						DownloadRate: 0,
					},
					{
						Url:          "erigon3-v1-snapshots-sepolia.erigon.network",
						DownloadRate: 0,
					},
				},
				Peers: []diagnostics.SegmentPeer{},
			},
			"v1-003600-003700-beaconblocks.seg": {
				Name:            "v1-003600-003700-beaconblocks.seg",
				TotalBytes:      106780671,
				DownloadedBytes: 0,
				Webseeds: []diagnostics.SegmentPeer{
					{
						Url:          "caplin-snapshots-sepolia.erigon.network",
						DownloadRate: 0,
					},
					{
						Url:          "erigon2-v1-snapshots-sepolia.erigon.network",
						DownloadRate: 0,
					},
					{
						Url:          "erigon3-v1-snapshots-sepolia.erigon.network",
						DownloadRate: 0,
					},
					{
						Url:          "erigon3-v3-snapshots-sepolia.erigon.network",
						DownloadRate: 0,
					},
				},
				Peers: []diagnostics.SegmentPeer{},
			},
			"v1-003700-003800-beaconblocks.seg": {
				Name:            "v1-003700-003800-beaconblocks.seg",
				TotalBytes:      105890302,
				DownloadedBytes: 0,
				Webseeds: []diagnostics.SegmentPeer{
					{
						Url:          "caplin-snapshots-sepolia.erigon.network",
						DownloadRate: 0,
					},
					{
						Url:          "erigon2-v1-snapshots-sepolia.erigon.network",
						DownloadRate: 0,
					},
					{
						Url:          "erigon3-v1-snapshots-sepolia.erigon.network",
						DownloadRate: 0,
					},
					{
						Url:          "erigon3-v3-snapshots-sepolia.erigon.network",
						DownloadRate: 0,
					},
				},
				Peers: []diagnostics.SegmentPeer{},
			},
			"v1-003800-003900-beaconblocks.seg": {
				Name:            "v1-003800-003900-beaconblocks.seg",
				TotalBytes:      104320593,
				DownloadedBytes: 0,
				Webseeds: []diagnostics.SegmentPeer{
					{
						Url:          "caplin-snapshots-sepolia.erigon.network",
						DownloadRate: 0,
					},
					{
						Url:          "erigon2-v1-snapshots-sepolia.erigon.network",
						DownloadRate: 0,
					},
					{
						Url:          "erigon3-v1-snapshots-sepolia.erigon.network",
						DownloadRate: 0,
					},
					{
						Url:          "erigon3-v3-snapshots-sepolia.erigon.network",
						DownloadRate: 0,
					},
				},
				Peers: []diagnostics.SegmentPeer{},
			},
			"v1-003900-004000-beaconblocks.seg": {
				Name:            "v1-003900-004000-beaconblocks.seg",
				TotalBytes:      103898296,
				DownloadedBytes: 0,
				Webseeds: []diagnostics.SegmentPeer{
					{
						Url:          "caplin-snapshots-sepolia.erigon.network",
						DownloadRate: 0,
					},
					{
						Url:          "erigon2-v1-snapshots-sepolia.erigon.network",
						DownloadRate: 0,
					},
					{
						Url:          "erigon3-v3-snapshots-sepolia.erigon.network",
						DownloadRate: 0,
					},
				},
				Peers: []diagnostics.SegmentPeer{},
			},
			"v1-004000-004100-bodies.seg": {
				Name:            "v1-004000-004100-bodies.seg",
				TotalBytes:      12647848,
				DownloadedBytes: 12647848,
				Webseeds:        []diagnostics.SegmentPeer{},
				Peers:           []diagnostics.SegmentPeer{},
			},
			"v1-004000-004100-headers.seg": {
				Name:            "v1-004000-004100-headers.seg",
				TotalBytes:      44765192,
				DownloadedBytes: 0,
				Webseeds: []diagnostics.SegmentPeer{
					{
						Url:          "erigon2-v1-snapshots-sepolia.erigon.network",
						DownloadRate: 0,
					},
					{
						Url:          "erigon3-v1-snapshots-sepolia.erigon.network",
						DownloadRate: 0,
					},
					{
						Url:          "erigon3-v3-snapshots-sepolia.erigon.network",
						DownloadRate: 0,
					},
				},
				Peers: []diagnostics.SegmentPeer{},
			},
			"v1-004000-004100-transactions.seg": {
				Name:            "v1-004000-004100-transactions.seg",
				TotalBytes:      11473456036,
				DownloadedBytes: 12845056,
				Webseeds: []diagnostics.SegmentPeer{
					{
						Url:          "erigon2-v1-snapshots-sepolia.erigon.network",
						DownloadRate: 149792,
					},
					{
						Url:          "erigon3-v1-snapshots-sepolia.erigon.network",
						DownloadRate: 132164,
					},
					{
						Url:          "erigon3-v3-snapshots-sepolia.erigon.network",
						DownloadRate: 149801,
					},
				},
				Peers: []diagnostics.SegmentPeer{},
			},
			"v1-004100-004200-bodies.seg": {
				Name:            "v1-004100-004200-bodies.seg",
				TotalBytes:      12569613,
				DownloadedBytes: 0,
				Webseeds: []diagnostics.SegmentPeer{
					{
						Url:          "erigon2-v1-snapshots-sepolia.erigon.network",
						DownloadRate: 0,
					},
					{
						Url:          "erigon3-v1-snapshots-sepolia.erigon.network",
						DownloadRate: 0,
					},
					{
						Url:          "erigon3-v3-snapshots-sepolia.erigon.network",
						DownloadRate: 0,
					},
				},
				Peers: []diagnostics.SegmentPeer{},
			},
			"v1-004100-004200-headers.seg": {
				Name:            "v1-004100-004200-headers.seg",
				TotalBytes:      45313015,
				DownloadedBytes: 0,
				Webseeds: []diagnostics.SegmentPeer{
					{
						Url:          "erigon3-v1-snapshots-sepolia.erigon.network",
						DownloadRate: 0,
					},
					{
						Url:          "erigon3-v3-snapshots-sepolia.erigon.network",
						DownloadRate: 0,
					},
					{
						Url:          "erigon2-v1-snapshots-sepolia.erigon.network",
						DownloadRate: 0,
					},
				},
				Peers: []diagnostics.SegmentPeer{},
			},
			"v1-004100-004200-transactions.seg": {
				Name:            "v1-004100-004200-transactions.seg",
				TotalBytes:      10202025917,
				DownloadedBytes: 0,
				Webseeds: []diagnostics.SegmentPeer{
					{
						Url:          "erigon2-v1-snapshots-sepolia.erigon.network",
						DownloadRate: 0,
					},
					{
						Url:          "erigon3-v1-snapshots-sepolia.erigon.network",
						DownloadRate: 0,
					},
					{
						Url:          "erigon3-v3-snapshots-sepolia.erigon.network",
						DownloadRate: 0,
					},
				},
				Peers: []diagnostics.SegmentPeer{},
			},
			"v1-004200-004300-bodies.seg": {
				Name:            "v1-004200-004300-bodies.seg",
				TotalBytes:      13692341,
				DownloadedBytes: 13692341,
				Webseeds:        []diagnostics.SegmentPeer{},
				Peers:           []diagnostics.SegmentPeer{},
			},
			"v1-004200-004300-headers.seg": {
				Name:            "v1-004200-004300-headers.seg",
				TotalBytes:      44207767,
				DownloadedBytes: 0,
				Webseeds: []diagnostics.SegmentPeer{
					{
						Url:          "erigon2-v1-snapshots-sepolia.erigon.network",
						DownloadRate: 0,
					},
					{
						Url:          "erigon3-v1-snapshots-sepolia.erigon.network",
						DownloadRate: 0,
					},
					{
						Url:          "erigon3-v3-snapshots-sepolia.erigon.network",
						DownloadRate: 0,
					},
				},
				Peers: []diagnostics.SegmentPeer{},
			},
			"v1-004200-004300-transactions.seg": {
				Name:            "v1-004200-004300-transactions.seg",
				TotalBytes:      7976828456,
				DownloadedBytes: 802947072,
				Webseeds: []diagnostics.SegmentPeer{
					{
						Url:          "erigon2-v1-snapshots-sepolia.erigon.network",
						DownloadRate: 145556,
					},
					{
						Url:          "erigon3-v1-snapshots-sepolia.erigon.network",
						DownloadRate: 142383,
					},
					{
						Url:          "erigon3-v3-snapshots-sepolia.erigon.network",
						DownloadRate: 106469,
					},
				},
				Peers: []diagnostics.SegmentPeer{
					{
						Url:          "erigon: 2.57.3",
						DownloadRate: 933625,
					},
					{
						Url:          "erigon: 2.58.0-dev",
						DownloadRate: 511054,
					},
					{
						Url:          "erigon: 2.57.3-705814b1",
						DownloadRate: 868844,
					},
					{
						Url:          "erigon: 2.57.1-9f1cd651",
						DownloadRate: 772824,
					},
					{
						Url:          "erigon: 2.57.3",
						DownloadRate: 930055,
					},
					{
						Url:          "erigon: 2.57.1-9f1cd651",
						DownloadRate: 971415,
					},
					{
						Url:          "erigon: 2.57.3-705814b1",
						DownloadRate: 927888,
					},
					{
						Url:          "erigon: 2.58.0-dev-c2153c75",
						DownloadRate: 896706,
					},
					{
						Url:          "erigon: 2.57.3",
						DownloadRate: 958391,
					},
					{
						Url:          "erigon: 2.57.1-9f1cd651",
						DownloadRate: 927119,
					},
				},
			},
			"v1-004300-004400-bodies.seg": {
				Name:            "v1-004300-004400-bodies.seg",
				TotalBytes:      12299440,
				DownloadedBytes: 0,
				Webseeds: []diagnostics.SegmentPeer{
					{
						Url:          "erigon3-v3-snapshots-sepolia.erigon.network",
						DownloadRate: 0,
					},
					{
						Url:          "erigon2-v1-snapshots-sepolia.erigon.network",
						DownloadRate: 0,
					},
					{
						Url:          "erigon3-v1-snapshots-sepolia.erigon.network",
						DownloadRate: 0,
					},
				},
				Peers: []diagnostics.SegmentPeer{},
			},
			"v1-004300-004400-headers.seg": {
				Name:            "v1-004300-004400-headers.seg",
				TotalBytes:      46040969,
				DownloadedBytes: 46040969,
				Webseeds:        []diagnostics.SegmentPeer{},
				Peers:           []diagnostics.SegmentPeer{},
			},
			"v1-004300-004400-transactions.seg": {
				Name:            "v1-004300-004400-transactions.seg",
				TotalBytes:      10616434080,
				DownloadedBytes: 0,
				Webseeds: []diagnostics.SegmentPeer{
					{
						Url:          "erigon2-v1-snapshots-sepolia.erigon.network",
						DownloadRate: 0,
					},
					{
						Url:          "erigon3-v1-snapshots-sepolia.erigon.network",
						DownloadRate: 0,
					},
					{
						Url:          "erigon3-v3-snapshots-sepolia.erigon.network",
						DownloadRate: 0,
					},
				},
				Peers: []diagnostics.SegmentPeer{},
			},
			"v1-004400-004500-bodies.seg": {
				Name:            "v1-004400-004500-bodies.seg",
				TotalBytes:      13123195,
				DownloadedBytes: 0,
				Webseeds: []diagnostics.SegmentPeer{
					{
						Url:          "erigon2-v1-snapshots-sepolia.erigon.network",
						DownloadRate: 0,
					},
					{
						Url:          "erigon3-v1-snapshots-sepolia.erigon.network",
						DownloadRate: 0,
					},
					{
						Url:          "erigon3-v3-snapshots-sepolia.erigon.network",
						DownloadRate: 0,
					},
				},
				Peers: []diagnostics.SegmentPeer{},
			},
			"v1-004400-004500-headers.seg": {
				Name:            "v1-004400-004500-headers.seg",
				TotalBytes:      46281163,
				DownloadedBytes: 0,
				Webseeds: []diagnostics.SegmentPeer{
					{
						Url:          "erigon2-v1-snapshots-sepolia.erigon.network",
						DownloadRate: 0,
					},
					{
						Url:          "erigon3-v1-snapshots-sepolia.erigon.network",
						DownloadRate: 0,
					},
					{
						Url:          "erigon3-v3-snapshots-sepolia.erigon.network",
						DownloadRate: 0,
					},
				},
				Peers: []diagnostics.SegmentPeer{},
			},
			"v1-004400-004500-transactions.seg": {
				Name:            "v1-004400-004500-transactions.seg",
				TotalBytes:      8214591543,
				DownloadedBytes: 0,
				Webseeds: []diagnostics.SegmentPeer{
					{
						Url:          "erigon2-v1-snapshots-sepolia.erigon.network",
						DownloadRate: 0,
					},
					{
						Url:          "erigon3-v1-snapshots-sepolia.erigon.network",
						DownloadRate: 0,
					},
					{
						Url:          "erigon3-v3-snapshots-sepolia.erigon.network",
						DownloadRate: 0,
					},
				},
				Peers: []diagnostics.SegmentPeer{},
			},
			"v1-004500-004600-bodies.seg": {
				Name:            "v1-004500-004600-bodies.seg",
				TotalBytes:      12614432,
				DownloadedBytes: 0,
				Webseeds: []diagnostics.SegmentPeer{
					{
						Url:          "erigon3-v1-snapshots-sepolia.erigon.network",
						DownloadRate: 0,
					},
					{
						Url:          "erigon3-v3-snapshots-sepolia.erigon.network",
						DownloadRate: 0,
					},
					{
						Url:          "erigon2-v1-snapshots-sepolia.erigon.network",
						DownloadRate: 0,
					},
				},
				Peers: []diagnostics.SegmentPeer{},
			},
			"v1-004500-004600-headers.seg": {
				Name:            "v1-004500-004600-headers.seg",
				TotalBytes:      46214061,
				DownloadedBytes: 0,
				Webseeds: []diagnostics.SegmentPeer{
					{
						Url:          "erigon2-v1-snapshots-sepolia.erigon.network",
						DownloadRate: 0,
					},
					{
						Url:          "erigon3-v1-snapshots-sepolia.erigon.network",
						DownloadRate: 0,
					},
					{
						Url:          "erigon3-v3-snapshots-sepolia.erigon.network",
						DownloadRate: 0,
					},
				},
				Peers: []diagnostics.SegmentPeer{},
			},
			"v1-004500-004600-transactions.seg": {
				Name:            "v1-004500-004600-transactions.seg",
				TotalBytes:      5833113405,
				DownloadedBytes: 0,
				Webseeds: []diagnostics.SegmentPeer{
					{
						Url:          "erigon3-v3-snapshots-sepolia.erigon.network",
						DownloadRate: 0,
					},
					{
						Url:          "erigon2-v1-snapshots-sepolia.erigon.network",
						DownloadRate: 0,
					},
					{
						Url:          "erigon3-v1-snapshots-sepolia.erigon.network",
						DownloadRate: 0,
					},
				},
				Peers: []diagnostics.SegmentPeer{},
			},
			"v1-004600-004700-bodies.seg": {
				Name:            "v1-004600-004700-bodies.seg",
				TotalBytes:      13295803,
				DownloadedBytes: 0,
				Webseeds: []diagnostics.SegmentPeer{
					{
						Url:          "erigon2-v1-snapshots-sepolia.erigon.network",
						DownloadRate: 0,
					},
					{
						Url:          "erigon3-v1-snapshots-sepolia.erigon.network",
						DownloadRate: 0,
					},
					{
						Url:          "erigon3-v3-snapshots-sepolia.erigon.network",
						DownloadRate: 0,
					},
				},
				Peers: []diagnostics.SegmentPeer{},
			},
			"v1-004600-004700-headers.seg": {
				Name:            "v1-004600-004700-headers.seg",
				TotalBytes:      46668740,
				DownloadedBytes: 0,
				Webseeds: []diagnostics.SegmentPeer{
					{
						Url:          "erigon3-v1-snapshots-sepolia.erigon.network",
						DownloadRate: 0,
					},
					{
						Url:          "erigon3-v3-snapshots-sepolia.erigon.network",
						DownloadRate: 0,
					},
					{
						Url:          "erigon2-v1-snapshots-sepolia.erigon.network",
						DownloadRate: 0,
					},
				},
				Peers: []diagnostics.SegmentPeer{},
			},
			"v1-004600-004700-transactions.seg": {
				Name:            "v1-004600-004700-transactions.seg",
				TotalBytes:      5948593398,
				DownloadedBytes: 0,
				Webseeds: []diagnostics.SegmentPeer{
					{
						Url:          "erigon2-v1-snapshots-sepolia.erigon.network",
						DownloadRate: 0,
					},
					{
						Url:          "erigon3-v1-snapshots-sepolia.erigon.network",
						DownloadRate: 0,
					},
					{
						Url:          "erigon3-v3-snapshots-sepolia.erigon.network",
						DownloadRate: 0,
					},
				},
				Peers: []diagnostics.SegmentPeer{},
			},
			"v1-004700-004800-bodies.seg": {
				Name:            "v1-004700-004800-bodies.seg",
				TotalBytes:      13402793,
				DownloadedBytes: 0,
				Webseeds: []diagnostics.SegmentPeer{
					{
						Url:          "erigon2-v1-snapshots-sepolia.erigon.network",
						DownloadRate: 0,
					},
					{
						Url:          "erigon3-v1-snapshots-sepolia.erigon.network",
						DownloadRate: 0,
					},
					{
						Url:          "erigon3-v3-snapshots-sepolia.erigon.network",
						DownloadRate: 0,
					},
				},
				Peers: []diagnostics.SegmentPeer{},
			},
			"v1-004700-004800-headers.seg": {
				Name:            "v1-004700-004800-headers.seg",
				TotalBytes:      47329533,
				DownloadedBytes: 0,
				Webseeds: []diagnostics.SegmentPeer{
					{
						Url:          "erigon2-v1-snapshots-sepolia.erigon.network",
						DownloadRate: 0,
					},
					{
						Url:          "erigon3-v1-snapshots-sepolia.erigon.network",
						DownloadRate: 0,
					},
					{
						Url:          "erigon3-v3-snapshots-sepolia.erigon.network",
						DownloadRate: 0,
					},
				},
				Peers: []diagnostics.SegmentPeer{},
			},
			"v1-004700-004800-transactions.seg": {
				Name:            "v1-004700-004800-transactions.seg",
				TotalBytes:      6525203587,
				DownloadedBytes: 151257088,
				Webseeds: []diagnostics.SegmentPeer{
					{
						Url:          "erigon2-v1-snapshots-sepolia.erigon.network",
						DownloadRate: 136467,
					},
					{
						Url:          "erigon3-v1-snapshots-sepolia.erigon.network",
						DownloadRate: 127927,
					},
					{
						Url:          "erigon3-v3-snapshots-sepolia.erigon.network",
						DownloadRate: 110857,
					},
				},
				Peers: []diagnostics.SegmentPeer{
					{
						Url:          "erigon: 2.57.3-705814b1",
						DownloadRate: 304627,
					},
					{
						Url:          "erigon: 2.54.0",
						DownloadRate: 994352,
					},
					{
						Url:          "erigon: 2.57.3",
						DownloadRate: 988029,
					},
					{
						Url:          "erigon: 2.58.0-dev-507c947d",
						DownloadRate: 931138,
					},
					{
						Url:          "erigon: 2.57.3-705814b1",
						DownloadRate: 952967,
					},
				},
			},
			"v1-004800-004900-bodies.seg": {
				Name:            "v1-004800-004900-bodies.seg",
				TotalBytes:      12878020,
				DownloadedBytes: 0,
				Webseeds: []diagnostics.SegmentPeer{
					{
						Url:          "erigon2-v1-snapshots-sepolia.erigon.network",
						DownloadRate: 0,
					},
					{
						Url:          "erigon3-v3-snapshots-sepolia.erigon.network",
						DownloadRate: 0,
					},
				},
				Peers: []diagnostics.SegmentPeer{},
			},
			"v1-004800-004900-headers.seg": {
				Name:            "v1-004800-004900-headers.seg",
				TotalBytes:      47915131,
				DownloadedBytes: 0,
				Webseeds: []diagnostics.SegmentPeer{
					{
						Url:          "erigon2-v1-snapshots-sepolia.erigon.network",
						DownloadRate: 0,
					},
					{
						Url:          "erigon3-v3-snapshots-sepolia.erigon.network",
						DownloadRate: 0,
					},
				},
				Peers: []diagnostics.SegmentPeer{},
			},
			"v1-004800-004900-transactions.seg": {
				Name:            "v1-004800-004900-transactions.seg",
				TotalBytes:      8562524333,
				DownloadedBytes: 0,
				Webseeds: []diagnostics.SegmentPeer{
					{
						Url:          "erigon3-v3-snapshots-sepolia.erigon.network",
						DownloadRate: 0,
					},
					{
						Url:          "erigon2-v1-snapshots-sepolia.erigon.network",
						DownloadRate: 0,
					},
				},
				Peers: []diagnostics.SegmentPeer{},
			},
			"v1-004900-005000-bodies.seg": {
				Name:            "v1-004900-005000-bodies.seg",
				TotalBytes:      15019825,
				DownloadedBytes: 0,
				Webseeds: []diagnostics.SegmentPeer{
					{
						Url:          "erigon2-v1-snapshots-sepolia.erigon.network",
						DownloadRate: 0,
					},
					{
						Url:          "erigon3-v3-snapshots-sepolia.erigon.network",
						DownloadRate: 0,
					},
				},
				Peers: []diagnostics.SegmentPeer{},
			},
			"v1-004900-005000-headers.seg": {
				Name:            "v1-004900-005000-headers.seg",
				TotalBytes:      48457305,
				DownloadedBytes: 0,
				Webseeds: []diagnostics.SegmentPeer{
					{
						Url:          "erigon2-v1-snapshots-sepolia.erigon.network",
						DownloadRate: 0,
					},
					{
						Url:          "erigon3-v3-snapshots-sepolia.erigon.network",
						DownloadRate: 0,
					},
				},
				Peers: []diagnostics.SegmentPeer{},
			},
			"v1-004900-005000-transactions.seg": {
				Name:            "v1-004900-005000-transactions.seg",
				TotalBytes:      8942510569,
				DownloadedBytes: 0,
				Webseeds: []diagnostics.SegmentPeer{
					{
						Url:          "erigon2-v1-snapshots-sepolia.erigon.network",
						DownloadRate: 0,
					},
					{
						Url:          "erigon3-v3-snapshots-sepolia.erigon.network",
						DownloadRate: 0,
					},
				},
				Peers: []diagnostics.SegmentPeer{},
			},
		},
		TorrentMetadataReady: 94,
	},
	SnapshotIndexing: diagnostics.SnapshotIndexingStatistics{
		Segments:    nil,
		TimeElapsed: 0,
	},
	BlockExecution: diagnostics.BlockExecutionStatistics{
		From:        0,
		To:          0,
		BlockNumber: 0,
		BlkPerSec:   0,
		TxPerSec:    0,
		MgasPerSec:  0,
		GasState:    0,
		Batch:       0,
		Alloc:       0,
		Sys:         0,
		TimeElapsed: 0,
	},
}
