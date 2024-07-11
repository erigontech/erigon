package main

import (
	"bytes"
	"encoding/json"
	"flag"
	"fmt"
	"io"
	"log"
	"net/http"
	"os"
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

type Node struct {
	NodeName string `json:"nodeName"`
	RPCURL   string `json:"rpcURL"`
}

type NodeGroup struct {
	GroupName string `json:"groupName"`
	Nodes     []Node `json:"nodes"`
}

type EnvConfig struct {
	Groups []NodeGroup `json:"groups"`
}

func main() {
	envFile := flag.String("envFile", "", "JSON file containing environments")
	flag.Parse()

	if *envFile == "" {
		log.Fatal("envFile flag not set")
	}

	file, err := os.Open(*envFile)
	if err != nil {
		log.Fatalf("Failed to open environment file: %v", err)
	}
	defer file.Close()

	var config EnvConfig
	decoder := json.NewDecoder(file)
	err = decoder.Decode(&config)
	if err != nil {
		log.Fatalf("Failed to parse environment file: %v", err)
	}

	allSuccess := true
	for _, group := range config.Groups {
		fmt.Printf("\n========== Checking group: %s ==========\n", group.GroupName)
		groupSuccess := true
		for _, node := range group.Nodes {
			fmt.Printf("\n--- Checking node: %s ---\n", node.NodeName)
			if !checkNode(node) {
				groupSuccess = false
				allSuccess = false
			}
		}
		if groupSuccess {
			fmt.Printf("\n***** Group is up: %s *****\n", group.GroupName)
		} else {
			fmt.Printf("\n***** Group is down: %s *****\n", group.GroupName)
		}
	}

	if allSuccess {
		fmt.Println("\n===== All groups are up =====")
		os.Exit(0)
	} else {
		fmt.Println("\n===== One or more groups are down =====")
		os.Exit(1)
	}
}

func checkNode(node Node) bool {
	err := checkClientVersion(node.RPCURL)
	if err != nil {
		fmt.Printf("Node is down: %s\n", node.NodeName)
		return false
	}
	err = checkBlockHeight(node.RPCURL)
	if err != nil {
		fmt.Printf("Node is down: %s\n", node.NodeName)
		return false
	}
	err = checkBatchNumber(node.RPCURL)
	if err != nil {
		fmt.Printf("Node is down: %s\n", node.NodeName)
		return false
	}
	fmt.Printf("Node is up: %s\n", node.NodeName)
	return true
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
		return err
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
