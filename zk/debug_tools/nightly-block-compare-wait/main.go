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
	"strconv"
	"time"
)

const (
	defaultCheckInterval = 2 * time.Minute
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

func getBlockByNumber(url string, blockNumber string) (*BlockResponse, error) {
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

func compareBlocks(block1, block2 *BlockResponse) bool {
	if block1.Error != nil || block2.Error != nil {
		log.Println("Error in block responses:")
		if block1.Error != nil {
			log.Println("Erigon error:", block1.Error)
			return false
		}
		if block2.Error != nil {
			log.Println("Compare node error:", block2.Error)
			return false
		}
	}

	for _, tx := range block1.Result.(map[string]interface{})["transactions"].([]interface{}) {
		tx.(map[string]interface{})["chainId"] = "0x00"
	}
	for _, tx := range block2.Result.(map[string]interface{})["transactions"].([]interface{}) {
		tx.(map[string]interface{})["chainId"] = "0x00"
	}

	erigonJSON, _ := json.MarshalIndent(block1.Result, "", "  ")
	compareJSON, _ := json.MarshalIndent(block2.Result, "", "  ")

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

func checkBlocks(erigonNodeURL, compareNodeURL string, maxBlockDiff int) (int64, bool) {
	erigonHeight, err := getBlockHeight(erigonNodeURL)
	if err != nil {
		log.Println("Error fetching block height from Erigon:", err)
		return 0, false
	}

	compareHeight, err := getBlockHeight(compareNodeURL)
	if err != nil {
		log.Println("Error fetching block height from compare node:", err)
		return 0, false
	}

	erigonBlockNumber, err := strconv.ParseInt(erigonHeight.Result[2:], 16, 64)
	if err != nil {
		log.Println("Error converting Erigon block number:", err)
		return 0, false
	}
	compareBlockNumber, err := strconv.ParseInt(compareHeight.Result[2:], 16, 64)
	if err != nil {
		log.Println("Error converting compare block number:", err)
		return 0, false
	}

	if erigonBlockNumber > 0 && compareBlockNumber > 0 {
		if abs(erigonBlockNumber-compareBlockNumber) > int64(maxBlockDiff) {
			log.Println("Block heights are not within the allowed difference (erigon, compare):", erigonBlockNumber, compareBlockNumber)
		}

		log.Println("Erigon height:", erigonBlockNumber, "Compare height:", compareBlockNumber)

		lowestBlockNumber := erigonBlockNumber
		if erigonBlockNumber > compareBlockNumber {
			lowestBlockNumber = compareBlockNumber
		}
		log.Println("Lowest block number:", lowestBlockNumber)

		if lowestBlockNumber == 0 {
			return 0, false
		}

		lowestBlockHex := "0x" + strconv.FormatInt(lowestBlockNumber, 16)

		erigonBlock, err := getBlockByNumber(erigonNodeURL, lowestBlockHex)
		if err != nil {
			log.Println("Error fetching block from Erigon:", err)
			return 0, false
		}

		compareBlock, err := getBlockByNumber(compareNodeURL, lowestBlockHex)
		if err != nil {
			log.Println("Error fetching block from compare node:", err)
			return 0, false
		}

		ok := compareBlocks(erigonBlock, compareBlock)
		if !ok {
			log.Println("Block mismatch detected at height:", erigonBlock.Result.(map[string]interface{})["number"])
			return 0, false
		}

		return lowestBlockNumber, true
	}

	return 0, false
}

func main() {
	erigonNodeURL := flag.String("erigon", "http://erigon:8123", "URL for the Erigon node")
	compareNodeURL := flag.String("compare", "http://other-node:8545", "URL for the compare node")
	compareNode2URL := flag.String("compare2", "http://other-node2:8545", "URL for the second compare node")
	checkInterval := flag.Duration("interval", defaultCheckInterval, "Interval to check the block height")
	runDuration := flag.Duration("duration", defaultRunDuration, "Total duration to run the checks")
	maxBlockDiff := flag.Int("max-block-diff", defaultMaxBlockDiff, "Maximum allowed block difference")
	exitOnFirstCheck := flag.Bool("exit-on-first-check", true, "Exit after the first check")
	flag.Parse()

	log.Println("Starting block comparison")

	ctx, cancel := context.WithTimeout(context.Background(), *runDuration)
	defer cancel()

	ticker1 := time.NewTicker(*checkInterval)
	defer ticker1.Stop()

	ticker2 := time.NewTicker(*checkInterval)
	defer ticker2.Stop()

	for {
		select {
		case <-ctx.Done():
			log.Println("Block checking exceeded timeout.")
			os.Exit(1)
		case <-ticker1.C:
			blockNumber, ok := checkBlocks(*erigonNodeURL, *compareNodeURL, *maxBlockDiff)
			if ok {
				log.Println("Waiting for second node to match the block height:", blockNumber)
				for {
					select {
					case <-ctx.Done():
						log.Println("Block checking exceeded timeout.")
						os.Exit(1)
					case <-ticker2.C:
						_, compare2Ok := checkBlocks(*erigonNodeURL, *compareNode2URL, *maxBlockDiff)
						if compare2Ok {
							if *exitOnFirstCheck {
								os.Exit(0)
							}
						}
					}
				}
			}
		}
	}
}

func abs(x int64) int64 {
	if x < 0 {
		return -x
	}
	return x
}
