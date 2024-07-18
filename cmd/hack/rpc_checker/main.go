package main

import (
	"bytes"
	"encoding/json"
	"flag"
	"fmt"
	"io"
	"io/ioutil"
	"log"
	"net/http"
	"strconv"
	"sync"
)

type JSONRPCRequest struct {
	Jsonrpc string        `json:"jsonrpc"`
	Method  string        `json:"method"`
	Params  []interface{} `json:"params"`
	ID      int           `json:"id"`
}

type JSONRPCResponse struct {
	Jsonrpc string      `json:"jsonrpc"`
	ID      int         `json:"id"`
	Result  BlockResult `json:"result"`
	Error   *RPCError   `json:"error"`
}

type BlockResult struct {
	Hash         string        `json:"hash"`
	Transactions []Transaction `json:"transactions"`
}

type Transaction struct {
	BlockHash        string `json:"blockHash"`
	BlockNumber      string `json:"blockNumber"`
	From             string `json:"from"`
	Gas              string `json:"gas"`
	GasPrice         string `json:"gasPrice"`
	Hash             string `json:"hash"`
	Input            string `json:"input"`
	Nonce            string `json:"nonce"`
	To               string `json:"to"`
	TransactionIndex string `json:"transactionIndex"`
	Value            string `json:"value"`
	Type             string `json:"type"`
	ChainID          string `json:"chainId"`
	V                string `json:"v"`
	R                string `json:"r"`
	S                string `json:"s"`
}

type RPCError struct {
	Code    int    `json:"code"`
	Message string `json:"message"`
}

type Receipt struct {
	TransactionHash string `json:"transactionHash"`
}

func getBlockHash(nodeURL string, blockNumber int, wg *sync.WaitGroup, resultChan chan<- BlockResult, errChan chan<- error) {
	defer wg.Done()

	request := JSONRPCRequest{
		Jsonrpc: "2.0",
		Method:  "eth_getBlockByNumber",
		Params:  []interface{}{"0x" + strconv.FormatInt(int64(blockNumber), 16), true},
		ID:      1,
	}

	jsonData, err := json.Marshal(request)
	if err != nil {
		errChan <- err
		return
	}

	resp, err := http.Post(nodeURL, "application/json", bytes.NewBuffer(jsonData))
	if err != nil {
		errChan <- err
		return
	}
	defer resp.Body.Close()

	body, err := io.ReadAll(resp.Body)
	if err != nil {
		errChan <- err
		return
	}

	var response JSONRPCResponse
	if err := json.Unmarshal(body, &response); err != nil {
		log.Printf("Error unmarshaling response from %s: %s\nResponse: %s\n", nodeURL, err, string(body))
		errChan <- err
		return
	}

	if response.Error != nil {
		errChan <- fmt.Errorf("error in JSON-RPC response: %v", response.Error)
		return
	}

	resultChan <- response.Result
}

func getReceipts(nodeURL string, txHashes []string, wg *sync.WaitGroup, resultChan chan<- []Receipt, errChan chan<- error) {
	defer wg.Done()

	var receipts []Receipt
	for _, txHash := range txHashes {
		request := JSONRPCRequest{
			Jsonrpc: "2.0",
			Method:  "eth_getTransactionReceipt",
			Params:  []interface{}{txHash},
			ID:      1,
		}

		jsonData, err := json.Marshal(request)
		if err != nil {
			errChan <- err
			return
		}

		resp, err := http.Post(nodeURL, "application/json", bytes.NewBuffer(jsonData))
		if err != nil {
			errChan <- err
			return
		}
		defer resp.Body.Close()

		body, err := ioutil.ReadAll(resp.Body)
		if err != nil {
			errChan <- err
			return
		}

		var receipt Receipt
		if err := json.Unmarshal(body, &receipt); err != nil {
			log.Printf("Error unmarshaling receipt from %s: %s\nResponse: %s\n", nodeURL, err, string(body))
			errChan <- err
			return
		}

		receipts = append(receipts, receipt)
	}

	resultChan <- receipts
}

func compareReceipts(nodeURL1, nodeURL2 string, txHashes []string) (bool, error) {
	var wg sync.WaitGroup
	receiptChan1 := make(chan []Receipt, 1)
	receiptChan2 := make(chan []Receipt, 1)
	errChan := make(chan error, 2)

	wg.Add(2)

	go getReceipts(nodeURL1, txHashes, &wg, receiptChan1, errChan)
	go getReceipts(nodeURL2, txHashes, &wg, receiptChan2, errChan)

	wg.Wait()
	close(receiptChan1)
	close(receiptChan2)
	close(errChan)

	if err := <-errChan; err != nil {
		return false, err
	}

	receipts1 := <-receiptChan1
	receipts2 := <-receiptChan2

	if len(receipts1) != len(receipts2) {
		return false, fmt.Errorf("mismatch in number of receipts")
	}

	for i := range receipts1 {
		if receipts1[i].TransactionHash != receipts2[i].TransactionHash {
			return false, nil
		}
	}

	return true, nil
}

/*
	EXAMPLE USAGE:

go run cmd/hack/rpc_checker/main.go -node1=http://your-node1-url -node2=http://your-node2-url -fromBlock=3000000 -step=1000 -compare-receipts=true
*/
func main() {
	nodeURL1 := flag.String("node1", "http://0.0.0.0:8123", "First node URL")
	nodeURL2 := flag.String("node2", "https://rpc.cardona.zkevm-rpc.com", "Second node URL")
	compareReceiptsFlag := flag.Bool("compare-receipts", false, "Compare receipts for transactions in the block")
	fromBlock := flag.Int("fromBlock", 3816916, "Starting block number")
	step := flag.Int("step", 1, "Block number increment")
	flag.Parse()

	blockNumber := *fromBlock
	stepValue := *step

	for {
		if blockNumber%stepValue == 0 {
			fmt.Println("block: ", blockNumber)
		}
		var wg sync.WaitGroup
		resultChan := make(chan BlockResult, 2)
		errChan := make(chan error, 2)

		wg.Add(2)

		go getBlockHash(*nodeURL1, blockNumber, &wg, resultChan, errChan)
		go getBlockHash(*nodeURL2, blockNumber, &wg, resultChan, errChan)

		wg.Wait()
		close(resultChan)
		close(errChan)

		if err := <-errChan; err != nil {
			log.Fatalf("Error fetching block hash: %v", err)
		}

		result1 := <-resultChan
		result2 := <-resultChan

		if result1.Hash != result2.Hash {
			fmt.Printf("First divergence at block number %d\n", blockNumber)
			fmt.Printf("Node 1 block hash: %s\n", result1.Hash)
			fmt.Printf("Node 2 block hash: %s\n", result2.Hash)

			if *compareReceiptsFlag {
				if len(result1.Transactions) == 0 || len(result2.Transactions) == 0 {
					fmt.Println("No transactions in one of the blocks, skipping receipt comparison.")
				} else {
					txHashes1 := getTransactionHashes(result1.Transactions)
					matches, err := compareReceipts(*nodeURL1, *nodeURL2, txHashes1)
					if err != nil {
						log.Fatalf("Error comparing receipts: %v", err)
					}
					if !matches {
						fmt.Println("Receipts do not match.")
					} else {
						fmt.Println("Receipts match.")
					}
				}
			}
			break
		}

		blockNumber += stepValue
	}
}

func getTransactionHashes(transactions []Transaction) []string {
	txHashes := make([]string, len(transactions))
	for i, tx := range transactions {
		txHashes[i] = tx.Hash
	}
	return txHashes
}
