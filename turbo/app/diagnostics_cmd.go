package app

import (
	"errors"
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
		Name:  "print.downloader",
		Usage: "Prints details about downloader status to console",
	}

	debugURLFlag = cli.StringFlag{
		Name:     "debug.addr",
		Usage:    "URL to the debug endpoint",
		Required: false,
		Value:    "localhost:6060",
	}
)

var diagnosticsCommand = cli.Command{
	Action:    MigrateFlags(printDiagnostics),
	Name:      "diagnostics",
	Usage:     "Print diagnostics data to console",
	ArgsUsage: "--print.downloader",
	Before: func(cliCtx *cli.Context) error {
		_, _, err := debug.Setup(cliCtx, true /* rootLogger */)
		if err != nil {
			return err
		}
		return nil
	},
	Flags: append([]cli.Flag{
		&diagnosticsPrintDownloader,
		&debugURLFlag,
	}, debug.Flags...),

	Description: `The diagnostics command prints diagnostics data to console.`,
}

func printDiagnostics(cliCtx *cli.Context) error {
	return PrintDiagnostics(cliCtx, log.Root())
}

func PrintDiagnostics(cliCtx *cli.Context, logger log.Logger) error {
	url := "http://" + cliCtx.String(debugURLFlag.Name) + "/debug/"
	PrintCurentStage(url)
	if cliCtx.Bool(diagnosticsPrintDownloader.Name) {
		PrintSnapshotDownload(url)
	}

	return nil
}

func PrintCurentStage(url string) {
	data, err := GetSyncStats(url)
	if err != nil {
		log.Error("Error:", err)
		return
	}

	fmt.Println("-------------------Stages-------------------")

	for idx, stage := range data.SyncStages.StagesList {
		if idx == int(data.SyncStages.CurrentStage) {
			fmt.Println("[" + stage + "]" + " - Running")
		} else if idx < int(data.SyncStages.CurrentStage) {
			fmt.Println("[" + stage + "]" + " - Completed")
		} else {
			fmt.Println("[" + stage + "]" + " - Queued")
		}
	}
}

func GetSyncStats(url string) (diagnostics.SyncStatistics, error) {
	data, err := MakeHttpGetCall(url + "snapshot-sync")
	if err != nil {
		return diagnostics.SyncStatistics{}, err
	}

	if myValue, ok := data.(diagnostics.SyncStatistics); ok {
		return myValue, nil
	} else {
		return diagnostics.SyncStatistics{}, errors.New("conversion failed: interface is not of type sync statistics")
	}
}

func PrintSnapshotDownload(url string) {
	data, err := GetSyncStats(url)
	if err != nil {
		log.Error("Error:", err)
		return
	}

	fmt.Println("-------------------Snapshot Download-------------------")

	snapDownload := data.SnapshotDownload
	var remainingBytes uint64
	percent := 50
	if snapDownload.Total > snapDownload.Downloaded {
		remainingBytes = snapDownload.Total - snapDownload.Downloaded
		percent = int((snapDownload.Downloaded*100)/snapDownload.Total) / 2
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

	fmt.Println("Download:", logstr, common.ByteCount(snapDownload.Downloaded), "/", common.ByteCount(snapDownload.Total))
	downloadTimeLeft := calculateTime(remainingBytes, snapDownload.DownloadRate)

	fmt.Println("Time left:", downloadTimeLeft)
}

func MakeHttpGetCall(url string) (any, error) {
	resp, err := http.Get(url)
	if err != nil {
		return nil, err
	}

	defer resp.Body.Close()
	body, err := io.ReadAll(resp.Body)
	if err != nil {
		return nil, err
	}

	var data diagnostics.SyncStatistics
	err = json.Unmarshal(body, &data)
	if err != nil {
		return nil, err
	}

	return data, nil
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
