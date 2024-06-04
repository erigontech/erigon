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

	body, _ := io.ReadAll(resp.Body)
	if body == nil {
		return nil, fmt.Errorf("failed to read response body")
	}

	var result map[string]interface{}
	if err := json.Unmarshal(body, &result); err != nil {
		return nil, fmt.Errorf("failed to unmarshal response: %v", err)
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

	body, _ := io.ReadAll(resp.Body)
	if body == nil {
		return nil, fmt.Errorf("failed to read response body")
	}

	var result map[string]interface{}
	if err := json.Unmarshal(body, &result); err != nil {
		return nil, fmt.Errorf("failed to unmarshal response: %v", err)
	}

	blockData, ok := result["result"].(map[string]interface{})
	if !ok || blockData == nil {
		return nil, fmt.Errorf("block not found")
	}

	return blockData, nil
}

func compareBlocks(node1URL, node2URL string, blockNumber *big.Int, diffs chan<- string, wg *sync.WaitGroup) {
	defer wg.Done()

	block1, err := getBlockByNumber(node1URL, blockNumber)
	if err != nil {
		log.Printf("Error getting block %d from node 1: %v", blockNumber, err)
		return
	}

	block2, err := getBlockByNumber(node2URL, blockNumber)
	if err != nil {
		log.Printf("Error getting block %d from node 2: %v", blockNumber, err)
		return
	}

	if !cmp.Equal(block1, block2) {
		diff := cmp.Diff(block1, block2)
		diffs <- fmt.Sprintf("Mismatch at block %d:\n%s", blockNumber, diff)
	}
}

func main() {
	node1URL := flag.String("node1", "http://localhost:8545", "RPC URL of the first node")
	node2URL := flag.String("node2", "http://localhost:8546", "RPC URL of the second node")
	numBlocks := flag.Int("blocks", 1000, "Number of blocks to check")
	blockHeightDiff := flag.Int("diff", 10, "Allowed block height difference between nodes")
	flag.Parse()

	node1LatestBlock, err := getLatestBlockNumber(*node1URL)
	if err != nil {
		log.Fatalf("Failed to get latest block number from node 1: %v", err)
	}

	node2LatestBlock, err := getLatestBlockNumber(*node2URL)
	if err != nil {
		log.Fatalf("Failed to get latest block number from node 2: %v", err)
	}

	// print the block numbers
	log.Println("Node 1 latest block number: ", node1LatestBlock)
	log.Println("Node 2 latest block number: ", node2LatestBlock)

	// log out the check height
	log.Printf("Checking %d blocks\n", *numBlocks)

	if new(big.Int).Abs(new(big.Int).Sub(node1LatestBlock, node2LatestBlock)).Cmp(big.NewInt(int64(*blockHeightDiff))) > 0 {
		log.Fatalf("Nodes are more than %d blocks apart: node 1 at %d, node 2 at %d", *blockHeightDiff, node1LatestBlock, node2LatestBlock)
	}

	startBlock := node1LatestBlock
	if node2LatestBlock.Cmp(startBlock) < 0 {
		startBlock = node2LatestBlock
	}

	var wg sync.WaitGroup
	diffs := make(chan string, *numBlocks)
	for i := 0; i < *numBlocks; i++ {
		blockNumber := new(big.Int).Sub(startBlock, big.NewInt(int64(i)))
		wg.Add(1)
		go compareBlocks(*node1URL, *node2URL, blockNumber, diffs, &wg)
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
