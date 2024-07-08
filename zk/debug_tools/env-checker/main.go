package main

import (
	"bytes"
	"encoding/json"
	"fmt"
	"log"
	"net/http"
	"flag"
	"os"
	"io"
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
	Result  interface{} `json:"result"`
}

func main() {
	rpcURL := flag.String("rpcURL", "", "RPC URL")
	nodeName := flag.String("nodeName", "", "Node name")
	flag.Parse()

	if *rpcURL == "" {
		log.Fatal("rpcURL flag not set")
	}
	if *nodeName == "" {
		log.Fatal("nodeName flag not set")
	}

	fmt.Printf("Checking node: %s\n", *nodeName)
	err := checkClientVersion(*rpcURL)
	if err != nil {
		fmt.Printf("Node is down: %s\n", *nodeName)
		os.Exit(1)
	}
	err = checkBlockHeight(*rpcURL)
	if err != nil {
		fmt.Printf("Node is down: %s\n", *nodeName)
		os.Exit(1)
	}
	err = checkBatchNumber(*rpcURL)
	if err != nil {
		fmt.Printf("Node is down: %s\n", *nodeName)
		os.Exit(1)
	}

	fmt.Printf("Node is up: %s\n", *nodeName)
	os.Exit(0)
}

func makeRequest(rpcURL, method string) (*JSONRPCResponse, error) {
	reqBody := JSONRPCRequest{
		Jsonrpc: "2.0",
		Method:  method,
		Params:  []interface{}{},
		ID:      1,
	}

	reqBytes, _ := json.Marshal(reqBody)
	resp, err := http.Post(rpcURL, "application/json", bytes.NewBuffer(reqBytes))
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()

	body, err := io.ReadAll(resp.Body)
	if err != nil {
		return nil, err
	}

	var rpcResp JSONRPCResponse
	err = json.Unmarshal(body, &rpcResp)
	if err != nil {
		return nil, err
	}

	if rpcResp.Result == nil {
		return &rpcResp, fmt.Errorf("%s method returned an error: %v", method, rpcResp)
	}

	return &rpcResp, nil
}

func checkClientVersion(rpcURL string) error {
	resp, err := makeRequest(rpcURL, "web3_clientVersion")
	if err != nil {
		fmt.Println("Client version retrieval failed")
		return err
	}
	fmt.Printf("Client version: %v\n", resp.Result)
	return nil
}

func checkBlockHeight(rpcURL string) error {
	resp, err := makeRequest(rpcURL, "eth_blockNumber")
	if err != nil {
		fmt.Println("Block height retrieval failed")
	}
	fmt.Printf("Block height: %v\n", resp.Result)
	return nil
}

func checkBatchNumber(rpcURL string) error {
	resp, err := makeRequest(rpcURL, "zkevm_batchNumber")
	if err != nil {
		fmt.Println("Batch number retrieval failed")
		return err
	}
	fmt.Printf("Batch number: %v\n", resp.Result)
	return nil
}
