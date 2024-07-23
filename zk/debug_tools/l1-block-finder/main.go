package main

import (
	"encoding/json"
	"flag"
	"fmt"
	"io"
	"math/big"
	"net/http"
	"strconv"
	"strings"
)

const SequenceBatchesTopic = "0x303446e6a8cb73c83dff421c0b1d5e5ce0719dab1bff13660fc254e58cc17fce"
const SequenceBatchesTopic2 = "0x3e54d0825ed78523037d00a81759237eb436ce774bd546993ee67a1b67b6e766"
const blockRange = 10_000

var seekBatch uint64
var endpoint string
var contracts string // event addresses
var start uint64

var client = http.Client{}

const req = `{"jsonrpc": "2.0", "method": "eth_getLogs", "params": [{"fromBlock": "%s", "toBlock": "%s", "address": [%s], "topics": [["%s","%s"]]}],"id": 1}`

type Response struct {
	Jsonrpc string `json:"jsonrpc"`
	ID      int    `json:"id"`
	Result  []struct {
		Address          string   `json:"address"`
		Topics           []string `json:"topics"`
		Data             string   `json:"data"`
		BlockNumber      string   `json:"blockNumber"`
		TransactionHash  string   `json:"transactionHash"`
		TransactionIndex string   `json:"transactionIndex"`
		BlockHash        string   `json:"blockHash"`
		LogIndex         string   `json:"logIndex"`
		Removed          bool     `json:"removed"`
	} `json:"result"`
}

func main() {
	flag.Uint64Var(&seekBatch, "batch", 0, "batch number to find")
	flag.StringVar(&endpoint, "endpoint", "http://localhost:8545", "endpoint to query")
	flag.StringVar(&contracts, "contracts", "0x", "contract address comma separated")
	flag.Uint64Var(&start, "start", 0, "start block")
	flag.Parse()

	splitContracts := strings.Split(contracts, ",")
	contractInjected := ""
	for _, c := range splitContracts {
		contractInjected += fmt.Sprintf(`"%s",`, c)
	}
	contractInjected = strings.TrimSuffix(contractInjected, ",")

	var lastBlock uint64
	var lastBatchSequenced uint64

	startBlock := start
	endBlock := startBlock + blockRange

	for {
		fmt.Println("checking", startBlock, endBlock)
		response, err := makeRequest(startBlock, endBlock, contractInjected)
		if err != nil {
			fmt.Println(err)
		}

		for _, log := range response.Result {
			num := strings.Replace(log.Topics[1], "0x", "", 1)
			thisBatch, ok := new(big.Int).SetString(num, 16)
			if !ok {
				fmt.Println("error parsing batch number")
				return
			}
			batchU := thisBatch.Uint64()
			if batchU == seekBatch {
				fmt.Println("found exact block", log.BlockNumber)
				return
			}
			if batchU < seekBatch {
				bNum := strings.Replace(log.BlockNumber, "0x", "", 1)
				blockNum, ok := new(big.Int).SetString(bNum, 16)
				if !ok {
					fmt.Println("error parsing block number")
					return
				}
				lastBlock = blockNum.Uint64()
				lastBatchSequenced = thisBatch.Uint64()
			} else {
				// now we have gone over the batch number so just report the last block we had
				// and return
				fmt.Println("found last block", lastBlock, "batch", lastBatchSequenced)
				return
			}
		}

		startBlock = endBlock + 1
		endBlock = startBlock + blockRange
	}
}

func makeRequest(startBlock, endBlock uint64, contracts string) (*Response, error) {
	startHex := "0x" + strconv.FormatUint(startBlock, 16)
	endHex := "0x" + strconv.FormatUint(endBlock, 16)

	reqBody := fmt.Sprintf(req, startHex, endHex, contracts, SequenceBatchesTopic, SequenceBatchesTopic2)

	req, err := http.NewRequest("POST", endpoint, strings.NewReader(reqBody))
	if err != nil {
		return nil, err
	}
	req.Header.Set("Content-Type", "application/json")

	res, err := client.Do(req)
	if err != nil {
		return nil, err
	}
	defer res.Body.Close()
	body, err := io.ReadAll(res.Body)
	if err != nil {
		return nil, err
	}

	var response *Response
	err = json.Unmarshal(body, &response)

	return response, err
}
