package main

import (
	"context"
	"encoding/json"
	"fmt"
	"log"
	"math/big"
	"os"
	"strings"

	"io"

	libcommon "github.com/ledgerwatch/erigon-lib/common"
	"github.com/ledgerwatch/erigon/ethclient"
	"github.com/ledgerwatch/erigon/zk/debug_tools"
)

type HTTPResponse struct {
	Result string `json:"result"`
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
	rpcConfig, err := debug_tools.GetConf()
	if err != nil {
		panic(fmt.Sprintf("error RPGCOnfig: %s", err))
	}
	jsonFile, err := os.Open(rpcConfig.DumpFileName)
	if err != nil {
		fmt.Println(err)
	}
	defer jsonFile.Close()

	accountDump := make(map[string]AccountDump)
	byteValue, err := io.ReadAll(jsonFile)
	if err != nil {
		fmt.Println("Error reading JSON data:", err)
		return
	}

	err = json.Unmarshal(byteValue, &accountDump)
	if err != nil {
		fmt.Println("Error parsing JSON accountDump:", err)
		return
	}

	client, err := ethclient.Dial(rpcConfig.Url)
	if err != nil {
		log.Fatal(err)
	}

	blockNumber := big.NewInt(rpcConfig.Block)

	for accountHash, storageMap := range accountDump {
		compareBalance(client, blockNumber, accountHash, storageMap.Balance)
		compareNonce(client, blockNumber, accountHash, storageMap.Nonce)
		compareCodeHash(client, blockNumber, accountHash, storageMap.Codehash.Hex())
		for key, value := range storageMap.Storage {
			compareValuesString(client, blockNumber, accountHash, key, value)
		}
	}

	fmt.Println("Check finished.")
}

func compareValuesString(client *ethclient.Client, blockNumber *big.Int, accountHash, key, localValue string) {
	rpcValueBytes, err := client.StorageAt(context.Background(), libcommon.HexToAddress(accountHash), libcommon.HexToHash(key), blockNumber)
	if err != nil {
		log.Fatal(err)
	}

	rpcValue := libcommon.BytesToHash(rpcValueBytes).Hex()

	// remove the leading zeroes from the rpcValue hex string
	rpcValue = strings.TrimLeft(rpcValue, "0x")
	rpcValue = strings.TrimLeft(rpcValue, "0")
	rpcValue = "0x" + rpcValue
	localValue = strings.TrimLeft(localValue, "0x")
	localValue = strings.TrimLeft(localValue, "0")
	localValue = "0x" + localValue
	if !strings.HasSuffix(rpcValue, localValue) {
		fmt.Printf("Mismatch detected for %s and key %s. Local: %s, Rpc: %s\n", accountHash, key, localValue, rpcValue)
	}
}

func compareBalance(client *ethclient.Client, blockNumber *big.Int, accountHash, value string) {
	balance, err := client.BalanceAt(context.Background(), libcommon.HexToAddress(accountHash), blockNumber)
	if err != nil {
		log.Fatal(err)
	}

	// fmt.Println("Checking", accountHash)
	if !strings.EqualFold(value, balance.Hex()) {
		fmt.Printf("Balance ismatch detected for %s. Local: %s, Remote: %s\n", accountHash, value, balance.Hex())
	}
}

func compareNonce(client *ethclient.Client, blockNumber *big.Int, accountHash string, value uint64) {
	nonce, err := client.NonceAt(context.Background(), libcommon.HexToAddress(accountHash), blockNumber)
	if err != nil {
		log.Fatal(err)
	}

	if value != nonce {
		fmt.Printf("Nonce ismatch detected for %s. Local: %d, Remote: %d\n", accountHash, value, nonce)
	}
}

func compareCodeHash(client *ethclient.Client, blockNumber *big.Int, accountHash, value string) {
	rpcCodeBytes, err := client.CodeAt(context.Background(), libcommon.HexToAddress(accountHash), blockNumber)
	if err != nil {
		log.Fatal(err)
	}

	rpcCodeHash := libcommon.BytesToHash(rpcCodeBytes).Hex()
	if value != rpcCodeHash {
		fmt.Printf("Codehash mismatch detected for %s. Local: %s, Remote: %s\n", accountHash, value, rpcCodeHash)
	}
}
