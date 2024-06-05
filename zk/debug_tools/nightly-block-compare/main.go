package main

import (
	"bytes"
	"encoding/json"
	"flag"
	"fmt"
	"log"
	"math/big"
	"net/http"
	"os"
	"sync"

	"github.com/google/go-cmp/cmp"
	"io"
)

func getLatestBlockNumber(url string) (*big.Int, error) {
	requestBody, _ := json.Marshal(map[string]interface{}{
		"jsonrpc": "2.0",
		"method":  "eth_blockNumber",
		"params":  []interface{}{},
		"id":      1,
	})

	resp, err := http.Post(url, "application/json", bytes.NewBuffer(requestBody))
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()

	body, err := io.ReadAll(resp.Body)
	if err != nil {
		return nil, fmt.Errorf("failed to read response body: %v", err)
	}

	var result map[string]interface{}
	if err := json.Unmarshal(body, &result); err != nil {
		return nil, fmt.Errorf("failed to unmarshal response: %v", err)
	}

	if errorField, ok := result["error"]; ok {
		return nil, fmt.Errorf("node error: %v", errorField)
	}

	blockNumberHex, ok := result["result"].(string)
	if !ok {
		return nil, fmt.Errorf("invalid response format")
	}

	blockNumber := new(big.Int)
	if _, ok := blockNumber.SetString(blockNumberHex[2:], 16); !ok {
		return nil, fmt.Errorf("failed to convert block number to big.Int")
	}
	return blockNumber, nil
}

func getBlockByNumber(url string, number *big.Int) (map[string]interface{}, error) {
	requestBody, _ := json.Marshal(map[string]interface{}{
		"jsonrpc": "2.0",
		"method":  "eth_getBlockByNumber",
		"params":  []interface{}{fmt.Sprintf("0x%x", number), false},
		"id":      1,
	})

	resp, err := http.Post(url, "application/json", bytes.NewBuffer(requestBody))
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()

	body, err := io.ReadAll(resp.Body)
	if err != nil {
		return nil, fmt.Errorf("failed to read response body: %v", err)
	}

	var result map[string]interface{}
	if err := json.Unmarshal(body, &result); err != nil {
		return nil, fmt.Errorf("failed to unmarshal response: %v", err)
	}

	if errorField, ok := result["error"]; ok {
		return nil, fmt.Errorf("node error: %v", errorField)
	}

	blockData, ok := result["result"].(map[string]interface{})
	if !ok || blockData == nil {
		return nil, fmt.Errorf("block not found")
	}

	return blockData, nil
}

func getClientVersion(url string) (string, error) {
	requestBody, _ := json.Marshal(map[string]interface{}{
		"jsonrpc": "2.0",
		"method":  "web3_clientVersion",
		"params":  []interface{}{},
		"id":      1,
	})

	resp, err := http.Post(url, "application/json", bytes.NewBuffer(requestBody))
	if err != nil {
		return "", err
	}
	defer resp.Body.Close()

	body, err := io.ReadAll(resp.Body)
	if err != nil {
		return "", fmt.Errorf("failed to read response body: %v", err)
	}

	var result map[string]interface{}
	if err := json.Unmarshal(body, &result); err != nil {
		return "", fmt.Errorf("failed to unmarshal response: %v", err)
	}

	if errorField, ok := result["error"]; ok {
		return "", fmt.Errorf("node error: %v", errorField)
	}

	clientVersion, ok := result["result"].(string)
	if !ok {
		return "", fmt.Errorf("invalid response format")
	}

	return clientVersion, nil
}

func compareBlocks(erigonURL, zkevmURL, sequencerURL string, blockNumber *big.Int, compareMode string, diffs chan<- string, wg *sync.WaitGroup) {
	defer wg.Done()

	block1, err := getBlockByNumber(erigonURL, blockNumber)
	if err != nil {
		log.Printf("Error getting block %d from Erigon node: %v", blockNumber, err)
		return
	}

	block2, err := getBlockByNumber(zkevmURL, blockNumber)
	if err != nil {
		log.Printf("Error getting block %d from zkEVM node: %v", blockNumber, err)
		return
	}

	block3, err := getBlockByNumber(sequencerURL, blockNumber)
	if err != nil {
		log.Printf("Error getting block %d from Sequencer node: %v", blockNumber, err)
		return
	}

	if compareMode == "full" {
		if !cmp.Equal(block1, block2) || !cmp.Equal(block1, block3) || !cmp.Equal(block2, block3) {
			diff := fmt.Sprintf("Mismatch at block %d:\nErigon vs zkEVM vs Sequencer:\n%s", blockNumber,
				cmp.Diff(block1, block2)+cmp.Diff(block1, block3)+cmp.Diff(block2, block3))
			diffs <- diff
		}
	} else {
		hash1, hash2, hash3 := block1["hash"].(string), block2["hash"].(string), block3["hash"].(string)
		stateRoot1, stateRoot2, stateRoot3 := block1["stateRoot"].(string), block2["stateRoot"].(string), block3["stateRoot"].(string)

		if hash1 != hash2 || hash1 != hash3 || stateRoot1 != stateRoot2 || stateRoot1 != stateRoot3 {
			diff := fmt.Sprintf("Mismatch at block %d:\nErigon vs zkEVM vs Sequencer:\nHash:\n%s vs %s vs %s\nStateRoot:\n%s vs %s vs %s",
				blockNumber, hash1, hash2, hash3, stateRoot1, stateRoot2, stateRoot3)
			diffs <- diff
		}
	}
}

func main() {
	erigonURL := flag.String("erigon", "http://localhost:8545", "RPC URL of the Erigon node")
	zkevmURL := flag.String("zkevm", "http://localhost:8546", "RPC URL of the zkEVM node")
	sequencerURL := flag.String("sequencer", "http://localhost:8547", "RPC URL of the Sequencer node")
	numBlocks := flag.Int("blocks", 1000, "Number of blocks to check")
	blockHeightDiff := flag.Int("diff", 10, "Allowed block height difference between nodes")
	compareMode := flag.String("mode", "full", "Comparison mode: 'full' or 'root_and_hash'")
	flag.Parse()

	var (
		erigonLatestBlock, zkevmLatestBlock, sequencerLatestBlock *big.Int
		erigonVersion, zkevmVersion, sequencerVersion             string
		err                                                       error
	)

	erigonLatestBlock, err = getLatestBlockNumber(*erigonURL)
	if err != nil {
		log.Printf("Warning: Failed to get latest block number from Erigon node: %v", err)
	} else {
		erigonVersion, err = getClientVersion(*erigonURL)
		if err != nil {
			log.Printf("Warning: Failed to get client version from Erigon node: %v", err)
		} else {
			log.Println("Erigon latest block number: ", erigonLatestBlock)
			log.Println("Erigon client version: ", erigonVersion)
		}
	}

	zkevmLatestBlock, err = getLatestBlockNumber(*zkevmURL)
	if err != nil {
		log.Printf("Warning: Failed to get latest block number from zkEVM node: %v", err)
	} else {
		zkevmVersion, err = getClientVersion(*zkevmURL)
		if err != nil {
			log.Printf("Warning: Failed to get client version from zkEVM node: %v", err)
		} else {
			log.Println("zkEVM latest block number: ", zkevmLatestBlock)
			log.Println("zkEVM client version: ", zkevmVersion)
		}
	}

	sequencerLatestBlock, err = getLatestBlockNumber(*sequencerURL)
	if err != nil {
		log.Fatalf("Failed to get latest block number from Sequencer: %v", err)
	} else {
		sequencerVersion, err = getClientVersion(*sequencerURL)
		if err != nil {
			log.Fatalf("Failed to get client version from Sequencer: %v", err)
		} else {
			log.Println("Sequencer latest block number: ", sequencerLatestBlock)
			log.Println("Sequencer client version: ", sequencerVersion)
		}
	}

	log.Printf("Checking %d blocks\n", *numBlocks)

	// sequencer as 'reference block height'
	if erigonLatestBlock != nil && new(big.Int).Abs(new(big.Int).Sub(erigonLatestBlock, sequencerLatestBlock)).Cmp(big.NewInt(int64(*blockHeightDiff))) > 0 {
		log.Printf("Warning: Erigon node is more than %d blocks apart from Sequencer: Erigon at %d, Sequencer at %d", *blockHeightDiff, erigonLatestBlock, sequencerLatestBlock)
	}

	if zkevmLatestBlock != nil && new(big.Int).Abs(new(big.Int).Sub(zkevmLatestBlock, sequencerLatestBlock)).Cmp(big.NewInt(int64(*blockHeightDiff))) > 0 {
		log.Printf("Warning: zkEVM node is more than %d blocks apart from Sequencer: zkEVM at %d, Sequencer at %d", *blockHeightDiff, zkevmLatestBlock, sequencerLatestBlock)
	}

	startBlock := sequencerLatestBlock
	if erigonLatestBlock != nil && erigonLatestBlock.Cmp(startBlock) < 0 {
		startBlock = erigonLatestBlock
	}

	if zkevmLatestBlock != nil && zkevmLatestBlock.Cmp(startBlock) < 0 {
		startBlock = zkevmLatestBlock
	}

	log.Println("Starting block number: ", startBlock)

	if erigonLatestBlock == nil && zkevmLatestBlock == nil {
		log.Fatalf("Failed to get latest block number from both Erigon and zkEVM nodes")
	}

	var wg sync.WaitGroup
	diffs := make(chan string, *numBlocks)
	for i := 0; i < *numBlocks; i++ {
		blockNumber := new(big.Int).Sub(startBlock, big.NewInt(int64(i)))
		if erigonLatestBlock != nil && zkevmLatestBlock != nil {
			wg.Add(1)
			go compareBlocks(*erigonURL, *zkevmURL, *sequencerURL, blockNumber, *compareMode, diffs, &wg)
		}
	}
	wg.Wait()
	close(diffs)

	var foundDiffs bool
	for diff := range diffs {
		foundDiffs = true
		log.Println(diff)
	}

	if foundDiffs {
		os.Exit(1)
	}

	log.Println("No differences found")
}
