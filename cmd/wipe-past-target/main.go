// Surgical wipe: delete block-data tables past toBlock without touching state.
// Same operations that node/components/storage/provider_unwind_db_reset.go
// performs (TruncateBlocks + canonical-hash wipe + HeaderNumber sweep), but
// stand-alone — used to test whether a clean post-mode-B chaindata recovers
// via Caplin's DownloadHistoricalBlocks path on fresh boot.
package main

import (
	"context"
	"encoding/binary"
	"flag"
	"fmt"
	"os"

	log "github.com/erigontech/erigon/common/log/v3"

	"github.com/erigontech/erigon/db/kv"
	"github.com/erigontech/erigon/db/kv/dbcfg"
	"github.com/erigontech/erigon/db/kv/mdbx"
	"github.com/erigontech/erigon/db/rawdb"
	"github.com/erigontech/erigon/execution/stagedsync/stages"
)

func main() {
	dir := flag.String("chaindata", "", "path to chaindata directory")
	toBlockArg := flag.Uint64("to-block", 0, "wipe block-data past this block")
	flag.Parse()
	if *dir == "" || *toBlockArg == 0 {
		fmt.Fprintln(os.Stderr, "usage: --chaindata=PATH --to-block=N")
		os.Exit(2)
	}
	toBlock := *toBlockArg
	logger := log.New()

	ctx := context.Background()
	db, err := mdbx.New(dbcfg.ChainDB, logger).Path(*dir).Open(ctx)
	if err != nil {
		log.Crit("open db", "err", err)
	}
	defer db.Close()

	if err := db.Update(ctx, func(tx kv.RwTx) error {
		// HeaderNumber sweep past toBlock (hash → number map).
		if err := sweepHeaderNumber(tx, toBlock); err != nil {
			return fmt.Errorf("sweepHeaderNumber: %w", err)
		}
		// TruncateBlocks: kv.Headers, kv.BlockBody, kv.Senders, kv.EthTx.
		if err := rawdb.TruncateBlocks(ctx, tx, toBlock+1); err != nil {
			return fmt.Errorf("TruncateBlocks: %w", err)
		}
		// TruncateCanonicalHash: kv.HeaderCanonical past toBlock.
		if err := rawdb.TruncateCanonicalHash(tx, toBlock+1, false); err != nil {
			return fmt.Errorf("TruncateCanonicalHash: %w", err)
		}
		// Reset stage progress to toBlock.
		for _, stage := range []stages.SyncStage{
			stages.Headers,
			stages.BlockHashes,
			stages.Bodies,
			stages.Senders,
			stages.Execution,
			stages.TxLookup,
			stages.Finish,
		} {
			if err := stages.SaveStageProgress(tx, stage, toBlock); err != nil {
				return fmt.Errorf("SaveStageProgress(%s): %w", stage, err)
			}
		}
		return nil
	}); err != nil {
		log.Crit("update", "err", err)
	}
	fmt.Printf("wiped block-data past block %d\n", toBlock)
	_ = logger
}

// sweepHeaderNumber walks kv.Headers from toBlock+1 and removes the
// corresponding kv.HeaderNumber (hash → number) entries. TruncateBlocks
// does not touch HeaderNumber.
func sweepHeaderNumber(tx kv.RwTx, toBlock uint64) error {
	c, err := tx.Cursor(kv.Headers)
	if err != nil {
		return err
	}
	defer c.Close()
	startKey := make([]byte, 8)
	binary.BigEndian.PutUint64(startKey, toBlock+1)
	for k, _, err := c.Seek(startKey); k != nil && err == nil; k, _, err = c.Next() {
		if len(k) < 40 {
			continue
		}
		hash := k[8:40]
		if err := tx.Delete(kv.HeaderNumber, hash); err != nil {
			return err
		}
	}
	return nil
}
