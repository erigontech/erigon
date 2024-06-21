package main

import (
	"bytes"
	"context"
	"encoding/json"
	"flag"
	"fmt"
	"log"
	"net/http"
	"os"
	"time"
)

const (
	defaultCheckInterval = 30 * time.Second
	defaultRunDuration   = 20 * time.Minute
	defaultMaxBlockDiff  = 1000
)

type BlockNumberResponse struct {
	Jsonrpc string `json:"jsonrpc"`
	ID      int    `json:"id"`
	Result  string `json:"result"`
}

type BlockResponse struct {
	Jsonrpc string      `json:"jsonrpc"`
	ID      int         `json:"id"`
	Result  interface{} `json:"result"`
	Error   interface{} `json:"error"`
}

func getBlockHeight(url string) (*BlockNumberResponse, error) {
	reqBody := []byte(`{"jsonrpc":"2.0","method":"eth_blockNumber","params":[],"id":1}`)
	req, err := http.NewRequest("POST", url, bytes.NewBuffer(reqBody))
	if err != nil {
		return nil, err
	}

	req.Header.Set("Content-Type", "application/json")

	client := &http.Client{}
	resp, err := client.Do(req)
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		return nil, fmt.Errorf("unexpected status code: %d", resp.StatusCode)
	}

	var jsonResp BlockNumberResponse
	err = json.NewDecoder(resp.Body).Decode(&jsonResp)
	if err != nil {
		return nil, err
	}

	return &jsonResp, nil
}

func getBlockByNumber(url, blockNumber string) (*BlockResponse, error) {
	reqBody := []byte(fmt.Sprintf(`{"jsonrpc":"2.0","method":"eth_getBlockByNumber","params":["%s", true],"id":1}`, blockNumber))
	req, err := http.NewRequest("POST", url, bytes.NewBuffer(reqBody))
	if err != nil {
		return nil, err
	}

	req.Header.Set("Content-Type", "application/json")

	client := &http.Client{}
	resp, err := client.Do(req)
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		return nil, fmt.Errorf("unexpected status code: %d", resp.StatusCode)
	}

	var jsonResp BlockResponse
	err = json.NewDecoder(resp.Body).Decode(&jsonResp)
	if err != nil {
		return nil, err
	}

	return &jsonResp, nil
}

func compareBlocks(erigonBlock, compareBlock *BlockResponse) bool {
	if erigonBlock.Error != nil || compareBlock.Error != nil {
		log.Println("Error in block responses:")
		if erigonBlock.Error != nil {
			log.Println("Erigon error:", erigonBlock.Error)
			return false
		}
		if compareBlock.Error != nil {
			log.Println("Compare node error:", compareBlock.Error)
			return false
		}
	}

	erigonJSON, _ := json.MarshalIndent(erigonBlock.Result, "", "  ")
	compareJSON, _ := json.MarshalIndent(compareBlock.Result, "", "  ")

	if bytes.Equal(erigonJSON, compareJSON) {
		log.Println("Blocks match:", string(erigonJSON))
		return true
	} else {
		log.Println("Blocks do not match:")
		log.Println("Erigon block:", string(erigonJSON))
		log.Println("Compare block:", string(compareJSON))
		return false
	}
}

func main() {
	erigonNodeURL := flag.String("erigon", "http://erigon:8123", "URL for the Erigon node")
	compareNodeURL := flag.String("compare", "http://other-node:8545", "URL for the compare node")
	checkInterval := flag.Duration("interval", defaultCheckInterval, "Interval to check the block height")
	runDuration := flag.Duration("duration", defaultRunDuration, "Total duration to run the checks")
	maxBlockDiff := flag.Int("max-block-diff", defaultMaxBlockDiff, "Maximum allowed block difference")
	flag.Parse()

	ctx, cancel := context.WithTimeout(context.Background(), *runDuration)
	defer cancel()

	ticker := time.NewTicker(*checkInterval)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			log.Println("Stopping block height checks.")
			os.Exit(0)
		case <-ticker.C:
			erigonHeight, err := getBlockHeight(*erigonNodeURL)
			if err != nil {
				log.Println("Error fetching block height from Erigon:", err)
				continue
			}

			compareHeight, err := getBlockHeight(*compareNodeURL)
			if err != nil {
				log.Println("Error fetching block height from compare node:", err)
				continue
			}

			erigonBlockNumber := erigonHeight.Result
			compareBlockNumber := compareHeight.Result

			if absDiff(erigonBlockNumber, compareBlockNumber) > *maxBlockDiff {
				log.Println("Block heights are not within the allowed difference.")
				continue
			}

			erigonBlock, err := getBlockByNumber(*erigonNodeURL, erigonBlockNumber)
			if err != nil {
				log.Println("Error fetching block from Erigon:", err)
				continue
			}

			compareBlock, err := getBlockByNumber(*compareNodeURL, erigonBlockNumber)
			if err != nil {
				log.Println("Error fetching block from compare node:", err)
				continue
			}

			ok := compareBlocks(erigonBlock, compareBlock)
			if !ok {
				// terminate on mismatch
				log.Println("Block mismatch detected at height:", erigonBlock.Result.(map[string]interface{})["number"])
				os.Exit(1)
			}
		}
	}
}

func absDiff(a, b string) int {
	var ia, ib int
	fmt.Sscanf(a, "0x%x", &ia)
	fmt.Sscanf(b, "0x%x", &ib)
	return abs(ia - ib)
}

func abs(x int) int {
	if x < 0 {
		return -x
	}
	return x
}
