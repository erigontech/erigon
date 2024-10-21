package main

import (
	"context"
	"encoding/json"
	"fmt"
	"math/big"
	"os"
	"time"

	ethTypes "github.com/ledgerwatch/erigon/core/types"
	"github.com/ledgerwatch/erigon/ethclient"

	ethereum "github.com/ledgerwatch/erigon"
	"github.com/ledgerwatch/erigon-lib/common"
	"github.com/ledgerwatch/erigon/zk/contracts"
	"github.com/ledgerwatch/erigon/zk/debug_tools"
	"github.com/ledgerwatch/erigon/zk/types"
)

func main() {
	ctx := context.Background()
	cfg, err := debug_tools.GetConf()
	if err != nil {
		panic(fmt.Sprintf("RPGCOnfig: %s", err))
	}

	seqTopics := [][]common.Hash{{
		contracts.SequencedBatchTopicPreEtrog,
		contracts.SequencedBatchTopicEtrog,
	}}

	seqAndVerifL1Contracts := []common.Address{common.HexToAddress(cfg.AddressRollup)}

	ethClient, err := ethclient.Dial(cfg.L1Url)
	if err != nil {
		panic(err)

	}
	latestL1Block, err := ethClient.BlockNumber(ctx)
	if err != nil {
		panic(err)
	}

	l1BatchInfos := make([]types.L1BatchInfo, 0)

	query := ethereum.FilterQuery{
		Addresses: seqAndVerifL1Contracts,
		Topics:    seqTopics,
	}
	from := cfg.L1SyncStartBlock
	fmt.Println("Fetching logs from block ", from, "to", latestL1Block)
	defer fmt.Println("Done fetching logs")
	count := 0

	for from < latestL1Block {
		time.Sleep(10 * time.Millisecond)
		if count%10 == 0 {
			fmt.Println("[progress] Fetching logs from block ", from, "to", latestL1Block)
		}
		to := from + 20000
		if to > latestL1Block {
			to = latestL1Block
		}

		query.FromBlock = new(big.Int).SetUint64(from)
		query.ToBlock = new(big.Int).SetUint64(to)

		logs, err := ethClient.FilterLogs(ctx, query)
		if err != nil {
			fmt.Println("Error fetching logs, repeating: ", err)
			continue
		}

		for _, log := range logs {
			l1BatchInfos = append(l1BatchInfos, parseLogType(&log))
		}

		from += 20000
		count++
	}

	// write l1BatchInfos to file
	file, err := os.Create("l1BatchInfos.json")
	if err != nil {
		panic(err)
	}
	defer file.Close()

	enc := json.NewEncoder(file)
	enc.SetIndent("", "  ")
	if err := enc.Encode(l1BatchInfos); err != nil {
		panic(err)
	}
}

func parseLogType(log *ethTypes.Log) (l1BatchInfo types.L1BatchInfo) {
	var (
		batchNum   uint64
		l1InfoRoot common.Hash
	)

	switch log.Topics[0] {
	case contracts.SequencedBatchTopicPreEtrog:
		batchNum = new(big.Int).SetBytes(log.Topics[1].Bytes()).Uint64()
	case contracts.SequencedBatchTopicEtrog:
		batchNum = new(big.Int).SetBytes(log.Topics[1].Bytes()).Uint64()
		l1InfoRoot = common.BytesToHash(log.Data[:32])
	default:
		batchNum = 0
	}

	return types.L1BatchInfo{
		BatchNo:    batchNum,
		L1BlockNo:  log.BlockNumber,
		L1TxHash:   common.BytesToHash(log.TxHash.Bytes()),
		L1InfoRoot: l1InfoRoot,
	}
}
