package commands

import (
	"context"
	"errors"
	"sync/atomic"
	"time"

	"github.com/ledgerwatch/erigon-lib/kv"
	"github.com/ledgerwatch/erigon/core/types"
	"github.com/ledgerwatch/erigon/rlp"
	"github.com/ledgerwatch/erigon/turbo/snapshotsync"
	"github.com/ledgerwatch/log/v3"
	"github.com/spf13/cobra"
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
	tt := time.Now()
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
			time.Sleep(time.Second * 10)
		}
	}()
	snKV, err := snapshotsync.OpenHeadersSnapshot(snapshotPath)
	if err != nil {
		return err
	}
	err = snKV.View(ctx, func(tx kv.Tx) error {
		c, err := tx.Cursor(kv.Headers)
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
			if innerErr != nil {
				return innerErr
			}

			prevHeader = header

			atomic.StoreUint64(&lastHeader, header.Number.Uint64())
		}
		if innerErr != nil {
			return innerErr
		}
		return nil
	})
	if err != nil {
		return err
	}
	if block != 0 {
		if lastHeader != block {
			return errors.New("incorrect last block")
		}
	}
	log.Info("Success", "t", time.Since(tt))
	return nil
}
