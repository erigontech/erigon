package commands

import (
	"context"
	"encoding/csv"
	"errors"
	"fmt"
	"path"

	"github.com/spf13/cobra"

	"github.com/erigontech/erigon-lib/kv"
	"github.com/erigontech/erigon-lib/log/v3"
	"github.com/erigontech/erigon/turbo/debug"
)

var cmdHeadersExport = &cobra.Command{
	Use:   "stage_headers",
	Short: "",
	Run: func(cmd *cobra.Command, args []string) {
		logger := debug.SetupCobra(cmd, "integration")
		if err := exportHeaders(cmd.Context(), startBlockNum, endBlockNum, logger); err != nil {
			if !errors.Is(err, context.Canceled) {
				logger.Error(err.Error())
			}
			return
		}
	},
}

func exportHeaders(ctx context.Context, startBlockNum, endBlockNum uint64, logger log.Logger) error {
	db, err := openDB(dbCfg(kv.ChainDB, path.Join(datadirCli, kv.ChainDB)), true, logger)
	if err != nil {
		return err
	}
	defer db.Close()

	sn, borSn, _, _, _, _ := allSnapshots(ctx, db, logger)
	defer sn.Close()
	defer borSn.Close()
	br, _ := blocksIO(db, logger)

	tx, err := db.BeginRo(ctx)
	if err != nil {
		return err
	}
	defer tx.Rollback()

	csvWriter := csv.NewWriter()
	for blockNum := startBlockNum; blockNum < endBlockNum; blockNum++ {
		header, err := br.HeaderByNumber(ctx, tx, blockNum)
		if err != nil {
			return err
		}
		if header == nil {
			return fmt.Errorf("header not found: blockNum=%d", blockNum)
		}
		header.Number.Uint64()
	}
}
