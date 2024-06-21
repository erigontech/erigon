package downloader

import (
	"encoding/json"
	"fmt"

	"github.com/ledgerwatch/erigon-lib/common"
	"github.com/ledgerwatch/erigon-lib/diagnostics"
	"github.com/ledgerwatch/erigon/cmd/diag/flags"
	"github.com/ledgerwatch/erigon/cmd/diag/util"
	"github.com/urfave/cli/v2"
)

var Command = cli.Command{
	Action:    print,
	Name:      "downloader",
	Aliases:   []string{"dl"},
	Usage:     "print snapshot download stats",
	ArgsUsage: "",
	Flags: []cli.Flag{
		&flags.DebugURLFlag,
		&flags.OutputFlag,
	},
	Description: ``,
}

func print(cliCtx *cli.Context) error {
	var data diagnostics.SyncStatistics
	url := "http://" + cliCtx.String(flags.DebugURLFlag.Name) + "/debug/snapshot-sync"

	err := util.MakeHttpGetCall(cliCtx.Context, url, &data)

	if err != nil {
		return err
	}

	switch cliCtx.String(flags.OutputFlag.Name) {
	case "json":
		bytes, err := json.Marshal(data.SnapshotDownload)

		if err != nil {
			return err
		}

		fmt.Println(string(bytes))

	case "text":
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
		downloadTimeLeft := util.CalculateTime(remainingBytes, snapDownload.DownloadRate)

		fmt.Println("Time left:", downloadTimeLeft)
	}

	return nil
}
