package main

import (
	"context"
	"encoding/json"
	"fmt"
	"log"
	"math/big"
	"os"

	"io"

	libcommon "github.com/ledgerwatch/erigon-lib/common"
	"github.com/ledgerwatch/erigon/ethclient"
	"gopkg.in/yaml.v2"
)

type Result struct {
	Hash string `json:"hash"`
}

type HTTPResponse struct {
	Result Result `json:"result"`
	Error  string `json:"error"`
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

	totalChecks := 2
	wrongChecks := []int{}
	for blockNo := 0; blockNo < totalChecks; blockNo++ {
		remoteHash := getHash(rpcConfig.Url, blockNo)
		localHash := getHash("http://localhost:8545", blockNo)
		if remoteHash != localHash {
			wrongChecks = append(wrongChecks, blockNo)
		}
	}

	fmt.Println("Check finished.")
	fmt.Printf("Wrong checks: %d/%d, %d%%\n", len(wrongChecks), totalChecks, (len(wrongChecks)/totalChecks)*100)
	if len(wrongChecks) > 0 {
		fmt.Println("Wrong checks:", wrongChecks)
	}

}

func getHash(urlString string, blockNo int) string {
	client, err := ethclient.Dial(urlString)
	if err != nil {
		log.Fatal(err)
	}

	block, err := client.BlockByNumber(context.Background(), big.NewInt(int64(blockNo)))
	if err != nil {
		log.Fatal(err)
	}

	return block.Hash().String()
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
