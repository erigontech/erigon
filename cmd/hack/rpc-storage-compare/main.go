package main

import (
	"bytes"
	"encoding/json"
	"fmt"
	"io/ioutil"
	"net/http"
	"os"
	"strconv"
	"strings"

	"github.com/iden3/go-iden3-crypto/keccak256"
	"github.com/ledgerwatch/erigon-lib/common"
)

type HTTPResponse struct {
	Result string `json:"result"`
}

type RequestData struct {
	Method  string   `json:"method"`
	Params  []string `json:"params"`
	ID      int      `json:"id"`
	Jsonrpc string   `json:"jsonrpc"`
}

var block = "0x37D8"
var url = "https://rpc-debug.internal.zkevm-test.net/"
var jsonFile = "addrDump.json"

type AccountDump struct {
	Balance  string
	Nonce    uint64
	Storage  map[string]string
	Codehash common.Hash
}

func main() {
	jsonFile, err := os.Open(jsonFile)
	if err != nil {
		fmt.Println(err)
	}
	defer jsonFile.Close()

	data := make(map[string]AccountDump)
	byteValue, _ := ioutil.ReadAll(jsonFile)
	err = json.Unmarshal(byteValue, &data)
	if err != nil {
		fmt.Println("Error parsing JSON data:", err)
		return
	}

	for accountHash, storageMap := range data {
		compareBalance(accountHash, storageMap.Balance)
		compareNonce(accountHash, storageMap.Nonce)
		// compareCodeHash(accountHash, storageMap.Codehash.Hex())
		for key, value := range storageMap.Storage {
			compareValuesString(accountHash, key, value)
		}
	}

	fmt.Println("Check finished.")
}

func compareValuesString(accountHash, key, value string) {
	payloadbytecode := RequestData{
		Method:  "eth_getStorageAt",
		Params:  []string{accountHash, key, block},
		ID:      1,
		Jsonrpc: "2.0",
	}

	jsonPayload, err := json.Marshal(payloadbytecode)
	if err != nil {
		fmt.Println("Error preparing request:", err)
		return
	}

	resp, err := http.Post(url, "application/json", bytes.NewBuffer(jsonPayload))
	if err != nil {
		fmt.Println("Error sending request:", err)
		return
	}

	defer resp.Body.Close()

	body, _ := ioutil.ReadAll(resp.Body)
	var httpResp HTTPResponse
	json.Unmarshal(body, &httpResp)

	remoteValueDecode := httpResp.Result

	localValueStr := strings.TrimPrefix(value, "0x")
	localValueStr = fmt.Sprintf("%064s", localValueStr)
	remoteValueStr := strings.TrimPrefix(remoteValueDecode, "0x")

	// fmt.Println("Checking", accountHash)
	if !strings.EqualFold(localValueStr, remoteValueStr) {
		fmt.Printf("Mismatch detected for %s and key %s. Local: %s, Remote: %s\n", accountHash, key, localValueStr, remoteValueStr)
	}
}

func compareBalance(accountHash, value string) {
	payloadbytecode := RequestData{
		Method:  "eth_getBalance",
		Params:  []string{accountHash, block},
		ID:      1,
		Jsonrpc: "2.0",
	}

	jsonPayload, err := json.Marshal(payloadbytecode)
	if err != nil {
		fmt.Println("Error preparing request:", err)
		return
	}

	resp, err := http.Post(url, "application/json", bytes.NewBuffer(jsonPayload))
	if err != nil {
		fmt.Println("Error sending request:", err)
		return
	}

	defer resp.Body.Close()

	body, _ := ioutil.ReadAll(resp.Body)
	var httpResp HTTPResponse
	json.Unmarshal(body, &httpResp)

	remoteValueDecode := httpResp.Result

	// fmt.Println("Checking", accountHash)
	if !strings.EqualFold(value, remoteValueDecode) {
		fmt.Printf("Balance ismatch detected for %s. Local: %s, Remote: %s\n", accountHash, value, remoteValueDecode)
	}
}

func compareNonce(accountHash string, value uint64) {
	payloadbytecode := RequestData{
		Method:  "eth_getTransactionCount",
		Params:  []string{accountHash, block},
		ID:      1,
		Jsonrpc: "2.0",
	}

	jsonPayload, err := json.Marshal(payloadbytecode)
	if err != nil {
		fmt.Println("Error preparing request:", err)
		return
	}

	resp, err := http.Post(url, "application/json", bytes.NewBuffer(jsonPayload))
	if err != nil {
		fmt.Println("Error sending request:", err)
		return
	}

	defer resp.Body.Close()

	body, _ := ioutil.ReadAll(resp.Body)
	var httpResp HTTPResponse
	json.Unmarshal(body, &httpResp)

	remoteValueDecode := httpResp.Result
	decimal_num, err := strconv.ParseUint(remoteValueDecode[2:], 16, 64)
	if err != nil {
		fmt.Println("Error parsing remote nonce:", err)
		return
	}
	if value != decimal_num {
		fmt.Printf("Nonce ismatch detected for %s. Local: %d, Remote: %d\n", accountHash, value, decimal_num)
	}
}

func compareCodeHash(accountHash, value string) {
	payloadbytecode := RequestData{
		Method:  "eth_getCode",
		Params:  []string{accountHash, block},
		ID:      1,
		Jsonrpc: "2.0",
	}

	jsonPayload, err := json.Marshal(payloadbytecode)
	if err != nil {
		fmt.Println("Error preparing request:", err)
		return
	}

	resp, err := http.Post(url, "application/json", bytes.NewBuffer(jsonPayload))
	if err != nil {
		fmt.Println("Error sending request:", err)
		return
	}

	defer resp.Body.Close()

	body, _ := ioutil.ReadAll(resp.Body)
	var httpResp HTTPResponse
	json.Unmarshal(body, &httpResp)

	remoteValueDecode := httpResp.Result
	commonHash := common.HexToHash(remoteValueDecode).Bytes()
	value2 := common.Bytes2Hex(keccak256.Hash(commonHash))
	if value != value2 {
		fmt.Printf("Nonce ismatch detected for %s. Local: %d, Remote: %d\n", accountHash, value, value2)
	}
}
