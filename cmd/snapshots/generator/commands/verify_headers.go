package commands

import (
	"context"
	"errors"
	"github.com/ledgerwatch/turbo-geth/common/dbutils"
	"github.com/ledgerwatch/turbo-geth/core/types"
	"github.com/ledgerwatch/turbo-geth/ethdb"
	"github.com/ledgerwatch/turbo-geth/log"
	"github.com/ledgerwatch/turbo-geth/rlp"
	"github.com/ledgerwatch/turbo-geth/turbo/snapshotsync/migrator"
	"github.com/spf13/cobra"
	"sync/atomic"
	"time"
)


func init() {
	withSnapshotFile(verifyHeadersSnapshotCmd)
	withBlock(verifyHeadersSnapshotCmd)
	rootCmd.AddCommand(verifyHeadersSnapshotCmd)

}
//go run cmd/snapshots/generator/main.go state_copy --block 11000000 --snapshot /media/b00ris/nvme/snapshots/state
var verifyHeadersSnapshotCmd = &cobra.Command{
	Use:     "verify_headers",
	Short:   "Copy from state snapshot",
	Example: "go run cmd/snapshots/generator/main.go verify_headers --block 11000000 --snapshot /media/b00ris/nvme/snapshots/state",
	RunE: func(cmd *cobra.Command, args []string) error {
		return VerifyHeadersSnapshot(cmd.Context(), snapshotFile)
	},
}
func VerifyHeadersSnapshot(ctx context.Context, snapshotPath string) error {
	tt:=time.Now()
	log.Info("Start validation")
	var prevHeader *types.Header
	var lastHeader uint64

	go func() {
		for {
			select {
			case <-ctx.Done():
				return
			default:
				log.Info("Verifying", "t", time.Since(tt), "block", atomic.LoadUint64(&lastHeader))
			}
			time.Sleep(time.Second*10)
		}
	}()
	snKV,err := migrator.OpenHeadersSnapshot(snapshotPath)
	if err!=nil {
		return err
	}
	err =  snKV.View(ctx, func(tx ethdb.Tx) error {
		c, err := tx.Cursor(dbutils.HeadersBucket)
		if err != nil {
			return err
		}
		k, v, innerErr := c.First()
		for {
			if len(k) == 0 && len(v) == 0 {
				break
			}
			if innerErr != nil {
				return innerErr
			}

			header := new(types.Header)
			innerErr := rlp.DecodeBytes(v, header)
			if innerErr != nil {
				return innerErr
			}

			if prevHeader != nil {
				if prevHeader.Number.Uint64()+1 != header.Number.Uint64() {
					log.Error("invalid header number", "p", prevHeader.Number.Uint64(), "c", header.Number.Uint64())
					return errors.New("invalid header number")
				}
				if prevHeader.Hash() != header.ParentHash {
					log.Error("invalid parent hash", "p", prevHeader.Hash(), "c", header.ParentHash)
					return errors.New("invalid parent hash")
				}
			}
			k, v, innerErr = c.Next()
			prevHeader = header

			atomic.StoreUint64(&lastHeader, header.Number.Uint64())
		}
		return nil
	})
	if err!=nil {
		return err
	}
	if block!=0 {
		if lastHeader!=block {
			return errors.New("incorrect last block")
		}
	}
	log.Info("Success","t",time.Since(tt))
	return nil
}