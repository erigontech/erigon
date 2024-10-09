package main

import (
	"encoding/json"
	"flag"
	"fmt"
	"io"
	"math/big"
	"net/http"
	"sort"
	"strconv"
	"strings"

	"github.com/iden3/go-iden3-crypto/keccak256"
	"github.com/ledgerwatch/erigon-lib/common"
	"github.com/ledgerwatch/erigon/zk/l1infotree"
)

type BlockNumberResponse struct {
	Jsonrpc string `json:"jsonrpc"`
	ID      int    `json:"id"`
	Result  string `json:"result"`
}

type Log struct {
	Address          string   `json:"address"`
	Topics           []string `json:"topics"`
	Data             string   `json:"data"`
	BlockNumber      string   `json:"blockNumber"`
	BlockHeight      uint64
	TransactionHash  string `json:"transactionHash"`
	TransactionIndex string `json:"transactionIndex"`
	BlockHash        string `json:"blockHash"`
	LogIndex         string `json:"logIndex"`
	Removed          bool   `json:"removed"`
}

type Response struct {
	Jsonrpc string `json:"jsonrpc"`
	ID      int    `json:"id"`
	Result  []Log  `json:"result"`
}

const BodyRequest = `{"jsonrpc": "2.0", "method": "eth_getBlockByNumber", "params": ["%s", false],"id": 1}`

type BodyResponse struct {
	Result struct {
		ParentHash       string   `json:"parentHash"`
		Sha3Uncles       string   `json:"sha3Uncles"`
		Miner            string   `json:"miner"`
		StateRoot        string   `json:"stateRoot"`
		TransactionsRoot string   `json:"transactionsRoot"`
		ReceiptsRoot     string   `json:"receiptsRoot"`
		LogsBloom        string   `json:"logsBloom"`
		Difficulty       string   `json:"difficulty"`
		TotalDifficulty  string   `json:"totalDifficulty"`
		Size             string   `json:"size"`
		Number           string   `json:"number"`
		GasLimit         string   `json:"gasLimit"`
		GasUsed          string   `json:"gasUsed"`
		Timestamp        string   `json:"timestamp"`
		ExtraData        string   `json:"extraData"`
		MixHash          string   `json:"mixHash"`
		Nonce            string   `json:"nonce"`
		Hash             string   `json:"hash"`
		Transactions     []string `json:"transactions"`
		Uncles           []any    `json:"uncles"`
	} `json:"result"`
}

const blockStep = 10_000
const blockNumberReq = `{"jsonrpc": "2.0", "method": "eth_blockNumber", "params": [],"id": 1}`

const logReq = `{"jsonrpc": "2.0", "method": "eth_getLogs", "params": [{"fromBlock": "%s", "toBlock": "%s", "address": ["%s"], "topics": [["0xda61aa7823fcd807e37b95aabcbe17f03a6f3efd514176444dae191d27fd66b3"]]}],"id": 1}`

var startBlock uint64
var endpoint string
var address string

var client = http.Client{}

func main() {
	flag.Uint64Var(&startBlock, "start", 0, "start block")
	flag.StringVar(&endpoint, "endpoint", "http://localhost:8545", "endpoint to query")
	flag.StringVar(&address, "address", "0x", "contract address")
	flag.Parse()

	req, err := http.NewRequest("POST", endpoint, strings.NewReader(blockNumberReq))
	if err != nil {
		panic(err)
	}
	resp, err := client.Do(req)
	if err != nil {
		panic(err)
	}
	defer resp.Body.Close()
	var blockNumberResponse BlockNumberResponse
	bodyBytes, err := io.ReadAll(resp.Body)
	if err != nil {
		panic(err)
	}
	if err = json.Unmarshal(bodyBytes, &blockNumberResponse); err != nil {
		panic(err)
	}
	lessPrefix := strings.TrimPrefix(blockNumberResponse.Result, "0x")
	blockNumber := new(big.Int)
	latestBlock, ok := blockNumber.SetString(lessPrefix, 16)
	if !ok {
		panic("error parsing block number")
	}

	var logs []Log

	from := startBlock

	stop := false
	for {
		end := from + blockStep
		if end > latestBlock.Uint64() {
			end = latestBlock.Uint64()
			stop = true
		}
		fmt.Println("Requesting logs from", from, "to", end)
		fromAsHex := "0x" + strconv.FormatUint(from, 16)
		endAsHex := "0x" + strconv.FormatUint(end, 16)
		reqBody := fmt.Sprintf(logReq, fromAsHex, endAsHex, address)
		req, err := http.NewRequest("POST", endpoint, strings.NewReader(reqBody))
		if err != nil {
			panic(err)
		}
		req.Header.Add("Content-Type", "application/json")
		resp, err := client.Do(req)
		if err != nil {
			panic(err)
		}
		defer resp.Body.Close()
		bodyBytes, err := io.ReadAll(resp.Body)
		if err != nil {
			panic(err)
		}
		var response Response
		if err = json.Unmarshal(bodyBytes, &response); err != nil {
			panic(err)
		}

		for _, log := range response.Result {
			numStripped := strings.TrimPrefix(log.BlockNumber, "0x")
			blockNum, ok := new(big.Int).SetString(numStripped, 16)
			if !ok {
				panic("error parsing block number")
			}
			log.BlockHeight = blockNum.Uint64()
			logs = append(logs, log)
		}
		if len(response.Result) > 0 {
			fmt.Printf("Found %d logs\n", len(response.Result))
		}

		if stop {
			break
		}

		from = end + 1
	}

	fmt.Printf("Found %d logs\n", len(logs))

	// sort the logs
	sort.Slice(logs, func(i, j int) bool {
		return logs[i].BlockHeight < logs[j].BlockHeight
	})

	leaves := make([][32]byte, 0)
	tree, err := l1infotree.NewL1InfoTree(32, leaves)
	if err != nil {
		panic(err)
	}

	index := 0
	for _, log := range logs {
		headerReq := fmt.Sprintf(BodyRequest, log.BlockNumber)
		req, err := http.NewRequest("POST", endpoint, strings.NewReader(headerReq))
		if err != nil {
			panic(err)
		}
		req.Header.Add("Content-Type", "application/json")
		resp, err := client.Do(req)
		if err != nil {
			panic(err)
		}
		defer resp.Body.Close()
		bodyBytes, err := io.ReadAll(resp.Body)
		if err != nil {
			panic(err)
		}
		var bodyResponse BodyResponse
		if err = json.Unmarshal(bodyBytes, &bodyResponse); err != nil {
			panic(err)
		}

		mainnetExitRoot := log.Topics[1]
		rollupExitRoot := log.Topics[2]

		mainnetHash := common.HexToHash(mainnetExitRoot)
		rollupHash := common.HexToHash(rollupExitRoot)

		combined := append(mainnetHash.Bytes(), rollupHash.Bytes()...)
		ger := keccak256.Hash(combined)
		gerHash := common.BytesToHash(ger)

		parentHash := common.HexToHash(bodyResponse.Result.ParentHash)
		timestamp := bodyResponse.Result.Timestamp
		timstampTrimmed := strings.TrimPrefix(timestamp, "0x")
		timestampInt, ok := new(big.Int).SetString(timstampTrimmed, 16)
		if !ok {
			panic("error parsing timestamp")
		}
		leafHash := l1infotree.HashLeafData(gerHash, parentHash, timestampInt.Uint64())

		leaves = append(leaves, leafHash)

		newRoot, err := tree.AddLeaf(uint32(index), leafHash)
		if err != nil {
			panic(err)
		}

		fmt.Printf("%d: %s\n", index, newRoot.String())

		index++
	}
}
