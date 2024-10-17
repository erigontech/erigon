package main

import (
	"context"
	"encoding/binary"
	"encoding/json"
	"fmt"
	"os"
	"time"

	ethereum "github.com/ledgerwatch/erigon"
	"github.com/ledgerwatch/erigon-lib/common"

	"github.com/iden3/go-iden3-crypto/keccak256"
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

	emptyHash := common.Hash{}
	rollupAddr := common.HexToAddress(cfg.AddressRollup)
	accInputHashes := make(map[uint64]string)
	index := 0
	for {
		seq := sequences[index]
		// get call data for tx

		var accInputHash common.Hash
		var err error
		if seq.BatchNo < cfg.ElderberryBachNo {
			accInputHash, err = callSequencedBatchesMap(ctx, ethClient, &rollupAddr, seq.BatchNo)
		} else {
			accInputHash, _, err = callGetRollupSequencedBatches(ctx, ethClient, &rollupAddr, cfg.RollupId, seq.BatchNo)
		}
		if err != nil {
			fmt.Println("Error fetching transaction: ", err, " for batch ", seq.BatchNo)
			continue
		}

		if accInputHash == emptyHash {
			fmt.Println("Error fetching transaction: accInputHash is empty for batch ", seq.BatchNo)
			panic("Error fetching transaction: accInputHash is empty for batch")
		}
		accInputHashes[seq.BatchNo] = accInputHash.Hex()

		index++
		if index >= len(sequences) {
			break
		}

		if index%100 == 0 {
			fmt.Println("Processed ", index, "sequences from ", len(sequences))
		}

		time.Sleep(2 * time.Millisecond)
	}

	// write l1BatchInfos to file
	file2, err := os.Create("accInputHashes.json")
	if err != nil {
		panic(err)
	}
	defer file2.Close()

	enc2 := json.NewEncoder(file2)
	enc2.SetIndent("", "  ")
	if err := enc2.Encode(accInputHashes); err != nil {
		panic(err)
	}
}

// calls the old rollup contract to get the accInputHash for a certain batch
// returns the accInputHash and lastBatchNumber
func callSequencedBatchesMap(ctx context.Context, client *ethclient.Client, addr *common.Address, batchNum uint64) (accInputHash common.Hash, err error) {
	mapKeyHex := fmt.Sprintf("%064x%064x", batchNum, 114 /* _legacySequencedBatches slot*/)
	mapKey := keccak256.Hash(common.FromHex(mapKeyHex))
	mkh := common.BytesToHash(mapKey)

	resp, err := client.StorageAt(ctx, *addr, mkh, nil)
	if err != nil {
		return
	}

	if err != nil {
		return
	}

	if len(resp) < 32 {
		return
	}
	accInputHash = common.BytesToHash(resp[:32])

	return
}

var (
	errorShortResponseLT32          = fmt.Errorf("response length is less than 32 bytes")
	errorShortResponseLT96          = fmt.Errorf("response length is less than 96 bytes")
	rollupSequencedBatchesSignature = "0x25280169" // hardcoded abi signature
)

func callGetRollupSequencedBatches(ctx context.Context, client *ethclient.Client, addr *common.Address, rollupId, batchNum uint64) (common.Hash, uint64, error) {
	rollupID := fmt.Sprintf("%064x", rollupId)
	batchNumber := fmt.Sprintf("%064x", batchNum)

	resp, err := client.CallContract(ctx, ethereum.CallMsg{
		To:   addr,
		Data: common.FromHex(rollupSequencedBatchesSignature + rollupID + batchNumber),
	}, nil)

	if err != nil {
		return common.Hash{}, 0, err
	}

	if len(resp) < 32 {
		return common.Hash{}, 0, errorShortResponseLT32
	}
	h := common.BytesToHash(resp[:32])

	if len(resp) < 96 {
		return common.Hash{}, 0, errorShortResponseLT96
	}
	lastBatchNumber := binary.BigEndian.Uint64(resp[88:96])

	return h, lastBatchNumber, nil
}
