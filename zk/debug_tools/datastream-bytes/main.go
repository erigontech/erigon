package main

import (
	"context"
	"flag"
	"fmt"

	"github.com/gateway-fm/cdk-erigon-lib/kv"
	"github.com/gateway-fm/cdk-erigon-lib/kv/mdbx"
	"github.com/ledgerwatch/erigon/core/rawdb"
	"github.com/ledgerwatch/erigon/core/types"
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

		blockNumbers, err := hermezDb.GetL2BlockNosByBatch(uint64(batchNum))
		if err != nil {
			return err
		}

		if len(blockNumbers) == 0 {
			return fmt.Errorf("no blocks found for batch %d", batchNum)
		}

		previousBatch := batchNum - 1
		blocks := make([]types.Block, 0, len(blockNumbers))
		txsPerBlock := make(map[uint64][]types.Transaction)

		for _, blockNumber := range blockNumbers {
			block, err := rawdb.ReadBlockByNumber(tx, blockNumber)
			if err != nil {
				return err
			}
			blocks = append(blocks, *block)
			txsPerBlock[blockNumber] = block.Transactions()
		}
		var l1InfoTreeMinTimestamps map[uint64]uint64
		entries, err := server.BuildWholeBatchStreamEntriesProto(tx, hermezDb, uint64(chainId), uint64(previousBatch), uint64(batchNum), blocks, txsPerBlock, l1InfoTreeMinTimestamps)
		if err != nil {
			return err
		}
		streamBytes, err = entries.Marshal()
		if err != nil {
			return err
		}

		return nil
	})

	if err != nil {
		fmt.Println(err)
		return
	}

	fmt.Printf("data:\n0x%x\n", streamBytes)
}
