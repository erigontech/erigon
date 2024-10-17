package main

import (
	"context"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"os"
	"time"

	"github.com/ledgerwatch/erigon/ethclient"
	"github.com/ledgerwatch/erigon/zk/debug_tools"
	"github.com/ledgerwatch/erigon/zk/types"
)

func main() {
	ctx := context.Background()
	cfg, err := debug_tools.GetConf()
	if err != nil {
		panic(fmt.Sprintf("RPGCOnfig: %s", err))
	}

	file, err := os.Open("sequencesMainnet.json")
	if err != nil {
		panic(err)
	}
	defer file.Close()
	sequences := make([]types.L1BatchInfo, 0)

	enc := json.NewDecoder(file)
	if err := enc.Decode(&sequences); err != nil {
		panic(err)
	}

	ethClient, err := ethclient.Dial(cfg.L1Url)
	if err != nil {
		panic(err)
	}

	calldatas := make(map[string]string)
	index := 40001
	for {
		seq := sequences[index]
		// get call data for tx
		l1Transaction, _, err := ethClient.TransactionByHash(ctx, seq.L1TxHash)
		if err != nil {
			fmt.Println("Error fetching transaction: ", err)
			continue
		}
		sequenceBatchesCalldata := l1Transaction.GetData()
		calldatas[seq.L1TxHash.String()] = hex.EncodeToString(sequenceBatchesCalldata)

		index++
		if index >= len(sequences) {
			break
		}

		if index%100 == 0 {
			fmt.Println("Processed ", index, "transactions from ", len(sequences))
		}

		time.Sleep(10 * time.Millisecond)
	}

	// write l1BatchInfos to file
	file2, err := os.Create("calldataFinal.json")
	if err != nil {
		panic(err)
	}
	defer file2.Close()

	enc2 := json.NewEncoder(file2)
	enc2.SetIndent("", "  ")
	if err := enc2.Encode(calldatas); err != nil {
		panic(err)
	}
}
