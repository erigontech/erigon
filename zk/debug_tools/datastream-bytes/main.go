package main

import (
	"context"
	"flag"
	"fmt"

	"github.com/gateway-fm/cdk-erigon-lib/kv"
	"github.com/gateway-fm/cdk-erigon-lib/kv/mdbx"
	"github.com/ledgerwatch/erigon/core/rawdb"
	"github.com/ledgerwatch/erigon/zk/datastream/server"
	"github.com/ledgerwatch/erigon/zk/hermez_db"
)

var dataDir = ""
var batchNum = 0
var chainId = 1

func main() {
	flag.StringVar(&dataDir, "dataDir", "", "data directory")
	flag.IntVar(&batchNum, "batchNum", 0, "batch number")
	flag.IntVar(&chainId, "chainId", 1, "chain id")
	flag.Parse()

	db := mdbx.MustOpen(dataDir)
	defer db.Close()

	var streamBytes []byte

	err := db.View(context.Background(), func(tx kv.Tx) error {
		hermezDb := hermez_db.NewHermezDbReader(tx)

		blocks, err := hermezDb.GetL2BlockNosByBatch(uint64(batchNum))
		if err != nil {
			return err
		}

		if len(blocks) == 0 {
			return fmt.Errorf("no blocks found for batch %d", batchNum)
		}

		lastBlock, err := rawdb.ReadBlockByNumber(tx, blocks[0]-1)
		if err != nil {
			return err
		}

		previousBatch := batchNum - 1

		for idx, blockNumber := range blocks {
			block, err := rawdb.ReadBlockByNumber(tx, blockNumber)
			if err != nil {
				return err
			}

			//gerUpdates := []dstypes.GerUpdate{}
			var l1InfoTreeMinTimestamps map[uint64]uint64

			isBatchEnd := idx == len(blocks)-1

			sBytes, err := server.CreateAndBuildStreamEntryBytesProto(uint64(chainId), block, hermezDb, tx, lastBlock, uint64(batchNum), uint64(previousBatch), l1InfoTreeMinTimestamps, isBatchEnd, nil)
			if err != nil {
				return err
			}
			streamBytes = append(streamBytes, sBytes...)
			lastBlock = block
			// we only put in the batch bookmark at the start of the stream data once
			previousBatch = batchNum
		}

		return nil
	})

	if err != nil {
		fmt.Println(err)
		return
	}

	fmt.Printf("data:\n0x%x\n", streamBytes)
}
