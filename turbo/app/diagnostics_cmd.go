package app

import (
	"fmt"
	"io"
	"net/http"

	"encoding/json"

	"github.com/ledgerwatch/erigon-lib/common"
	"github.com/ledgerwatch/erigon-lib/diagnostics"
	"github.com/ledgerwatch/erigon/turbo/debug"
	"github.com/ledgerwatch/log/v3"
	"github.com/urfave/cli/v2"
)

var (
	diagnosticsPrintDownloader = cli.BoolFlag{
		Name:  "diagnostics.print.downloader",
		Usage: "Prints details about downloader status to console",
	}
)

var diagnosticsCommand = cli.Command{
	Action:    MigrateFlags(printDiagnostics),
	Name:      "diagnostics",
	Usage:     "Print diagnostics data to console",
	ArgsUsage: "--diagnostics.print.snapshot.download",
	Before: func(cliCtx *cli.Context) error {
		_, _, err := debug.Setup(cliCtx, true /* rootLogger */)
		if err != nil {
			return err
		}
		return nil
	},
	Flags: append([]cli.Flag{
		&diagnosticsPrintDownloader,
	}, debug.Flags...),

	Description: `The diagnostics command prints diagnostics data to console.`,
}

func printDiagnostics(cliCtx *cli.Context) error {
	return PrintDiagnostics(cliCtx, log.Root())
}

func PrintDiagnostics(cliCtx *cli.Context, logger log.Logger) error {
	logger.Info("[!!!!Starting diagnostics]")
	MakeHttpGetCall()
	return nil
}

func MakeHttpGetCall() {
	// Make a GET request

	resp, err := http.Get("http://localhost:6060/debug/snapshot-sync")
	if err != nil {
		fmt.Println("Error: ", err)
	}
	defer resp.Body.Close()
	body, err := io.ReadAll(resp.Body)
	if err != nil {
		fmt.Println("Error: ", err)
	}

	var data diagnostics.SyncStatistics
	marshalErr := json.Unmarshal(body, &data)
	if marshalErr == nil {

		/*fmt.Println("Current stage:", data.SyncStages.StagesList[data.SyncStages.CurrentStage])
		fmt.Println("Snapshot download progress:", data.SnapshotDownload.Downloaded)
		fmt.Println("Snapshot download total:", data.SnapshotDownload.Total)
		fmt.Println("Snapshot download speed:", data.SnapshotDownload.DownloadRate)
		fmt.Println("Snapshot download time:", data.SnapshotDownload.TotalTime)
		fmt.Println("Snapshot Upload speed:", data.SnapshotDownload.UploadRate)
		fmt.Println("Peers count:", data.SnapshotDownload.Peers)*/

		var remainingBytes uint64
		percent := 50
		if data.SnapshotDownload.Total > data.SnapshotDownload.Downloaded {
			remainingBytes = data.SnapshotDownload.Total - data.SnapshotDownload.Downloaded
			percent = int((data.SnapshotDownload.Downloaded*100)/data.SnapshotDownload.Total) / 2
		}

		logstr := "["

		for i := 1; i < 50; i++ {
			if i < percent {
				logstr += "#"
			} else {
				logstr += "."
			}
		}

		logstr += "]"

		fmt.Println("Download:", logstr, common.ByteCount(data.SnapshotDownload.Downloaded), "/", common.ByteCount(data.SnapshotDownload.Total))
		downloadTimeLeft := calculateTime(remainingBytes, data.SnapshotDownload.DownloadRate)

		fmt.Println("Time left:", downloadTimeLeft)

		/*log.Info(fmt.Sprintf("[%s] %s", logPrefix, logReason),
			"progress", fmt.Sprintf("%.2f%% %s/%s", stats.Progress, common.ByteCount(stats.BytesCompleted), common.ByteCount(stats.BytesTotal)),
			"time-left", downloadTimeLeft,
			"total-time", time.Since(startTime).Round(time.Second).String(),
			"download", common.ByteCount(stats.DownloadRate)+"/s",
			"upload", common.ByteCount(stats.UploadRate)+"/s",
			"peers", stats.PeersUnique,
			"files", stats.FilesTotal,
			"metadata", fmt.Sprintf("%d/%d", stats.MetadataReady, stats.FilesTotal),
			"connections", stats.ConnectionsTotal,
			"alloc", common.ByteCount(m.Alloc), "sys", common.ByteCount(m.Sys),
		)*/
	}

}

func calculateTime(amountLeft, rate uint64) string {
	if rate == 0 {
		return "999hrs:99m"
	}
	timeLeftInSeconds := amountLeft / rate

	hours := timeLeftInSeconds / 3600
	minutes := (timeLeftInSeconds / 60) % 60

	return fmt.Sprintf("%dhrs:%dm", hours, minutes)
}
