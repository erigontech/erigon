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

	"io"
	"net/url"

	"github.com/iden3/go-iden3-crypto/keccak256"
	libcommon "github.com/ledgerwatch/erigon-lib/common"
	"github.com/ledgerwatch/erigon/common"
	"gopkg.in/yaml.v2"
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

type AccountDump struct {
	Balance  string
	Nonce    uint64
	Storage  map[string]string
	Codehash libcommon.Hash
}

func main() {
	rpcConfig, err := getConf()
	if err != nil {
		panic(fmt.Sprintf("error RPGCOnfig: %s", err))
	}
	jsonFile, err := os.Open(rpcConfig.DumpFileName)
	if err != nil {
		fmt.Println(err)
	}
	defer jsonFile.Close()

	data := make(map[string]AccountDump)
	byteValue, err := io.ReadAll(jsonFile)
	if err != nil {
		fmt.Println("Error reading JSON data:", err)
		return
	}

	err = json.Unmarshal(byteValue, &data)
	if err != nil {
		fmt.Println("Error parsing JSON data:", err)
		return
	}

	for accountHash, storageMap := range data {
		compareBalance(rpcConfig, accountHash, storageMap.Balance)
		compareNonce(rpcConfig, accountHash, storageMap.Nonce)
		compareCodeHash(rpcConfig, accountHash, storageMap.Codehash.Hex())
		for key, value := range storageMap.Storage {
			compareValuesString(rpcConfig, accountHash, key, value)
		}
	}

	fmt.Println("Check finished.")
}

func compareValuesString(cfg RpcConfig, accountHash, key, value string) {
	payloadbytecode := RequestData{
		Method:  "eth_getStorageAt",
		Params:  []string{accountHash, key, cfg.Block},
		ID:      1,
		Jsonrpc: "2.0",
	}

	jsonPayload, err := json.Marshal(payloadbytecode)
	if err != nil {
		fmt.Println("Error preparing request:", err)
		return
	}

	safeUrl, err := url.Parse(cfg.Url)
	if err != nil {
		fmt.Println("Error parsing URL:", err)
		return
	}
	resp, err := http.Post(safeUrl.String(), "application/json", bytes.NewBuffer(jsonPayload))
	if err != nil {
		fmt.Println("Error sending request:", err)
		return
	}

	defer resp.Body.Close()

	body, _ := io.ReadAll(resp.Body)
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

func compareBalance(cfg RpcConfig, accountHash, value string) {
	payloadbytecode := RequestData{
		Method:  "eth_getBalance",
		Params:  []string{accountHash, cfg.Block},
		ID:      1,
		Jsonrpc: "2.0",
	}

	jsonPayload, err := json.Marshal(payloadbytecode)
	if err != nil {
		fmt.Println("Error preparing request:", err)
		return
	}

	safeUrl, err := url.Parse(cfg.Url)
	if err != nil {
		fmt.Println("Error parsing URL:", err)
		return
	}
	resp, err := http.Post(safeUrl.String(), "application/json", bytes.NewBuffer(jsonPayload))
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

func compareNonce(cfg RpcConfig, accountHash string, value uint64) {
	payloadbytecode := RequestData{
		Method:  "eth_getTransactionCount",
		Params:  []string{accountHash, cfg.Block},
		ID:      1,
		Jsonrpc: "2.0",
	}

	jsonPayload, err := json.Marshal(payloadbytecode)
	if err != nil {
		fmt.Println("Error preparing request:", err)
		return
	}

	safeUrl, err := url.Parse(cfg.Url)
	if err != nil {
		fmt.Println("Error parsing URL:", err)
		return
	}
	resp, err := http.Post(safeUrl.String(), "application/json", bytes.NewBuffer(jsonPayload))
	if err != nil {
		fmt.Println("Error sending request:", err)
		return
	}

	defer resp.Body.Close()

	body, _ := io.ReadAll(resp.Body)
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

func compareCodeHash(cfg RpcConfig, accountHash, value string) {
	payloadbytecode := RequestData{
		Method:  "eth_getCode",
		Params:  []string{accountHash, cfg.Block},
		ID:      1,
		Jsonrpc: "2.0",
	}

	jsonPayload, err := json.Marshal(payloadbytecode)
	if err != nil {
		fmt.Println("Error preparing request:", err)
		return
	}

	safeUrl, err := url.Parse(cfg.Url)
	if err != nil {
		fmt.Println("Error parsing URL:", err)
		return
	}
	resp, err := http.Post(safeUrl.String(), "application/json", bytes.NewBuffer(jsonPayload))
	if err != nil {
		fmt.Println("Error sending request:", err)
		return
	}

	defer resp.Body.Close()

	body, _ := io.ReadAll(resp.Body)
	var httpResp HTTPResponse
	json.Unmarshal(body, &httpResp)

	remoteValueDecode := httpResp.Result
	commonHash := libcommon.HexToHash(remoteValueDecode).Bytes()
	value2 := common.Bytes2Hex(keccak256.Hash(commonHash))
	if value != value2 {
		fmt.Printf("Codehash mismatch detected for %s. Local: %s, Remote: %s\n", accountHash, value, value2)
	}
}

type RpcConfig struct {
	Url          string `yaml:"url"`
	DumpFileName string `yaml:"dumpFileName"`
	Block        string `yaml:"block"`
}

func getConf() (RpcConfig, error) {
	yamlFile, err := os.ReadFile("debugToolsConfig.yaml")
	if err != nil {
		return RpcConfig{}, err
	}

	c := RpcConfig{}
	err = yaml.Unmarshal(yamlFile, &c)
	if err != nil {
		return RpcConfig{}, err
	}

	return c, nil
}
