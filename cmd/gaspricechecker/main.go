package main

import (
	"bytes"
	"encoding/json"
	"fmt"
	"math/big"
	"net/http"
	"io"
)

type RequestBody struct {
	JSONRPC string        `json:"jsonrpc"`
	Method  string        `json:"method"`
	Params  []interface{} `json:"params"`
	ID      int           `json:"id"`
}

type ResponseBody struct {
	JSONRPC string `json:"jsonrpc"`
	ID      int    `json:"id"`
	Result  string `json:"result"`
}

func main() {
	nodes := map[string]string{
		"local":  "http://0.0.0.0:8545",
		"legacy": "https://rpc-legacy.internal.zkevm-rpc.com/",
	}

	gasPrices := make(map[string]*big.Int)

	requestBody := RequestBody{
		JSONRPC: "2.0",
		Method:  "eth_gasPrice",
		Params:  make([]interface{}, 0),
		ID:      1,
	}

	requestBodyJson, err := json.Marshal(requestBody)
	if err != nil {
		panic(err)
	}

	for name, url := range nodes {
		resp, err := http.Post(url, "application/json", bytes.NewBuffer(requestBodyJson))
		if err != nil {
			panic(err)
		}

		body, err := io.ReadAll(resp.Body)
		if err != nil {
			panic(err)
		}

		resp.Body.Close()

		var responseBody ResponseBody
		err = json.Unmarshal(body, &responseBody)
		if err != nil {
			panic(err)
		}

		gasPrice := new(big.Int)
		gasPrice, ok := gasPrice.SetString(responseBody.Result[2:], 16)
		if !ok {
			panic("Failed to parse gas price")
		}
		gasPrices[name] = gasPrice
	}

	var names [2]string
	var prices [2]*big.Int
	i := 0
	for name, price := range gasPrices {
		names[i] = name
		prices[i] = price
		i++
	}

	lowerIndex := 0
	if prices[1].Cmp(prices[0]) < 0 {
		lowerIndex = 1
	}
	higherIndex := 1 - lowerIndex

	diff := new(big.Int).Sub(prices[higherIndex], prices[lowerIndex])

	percentDiffLower := new(big.Float).Quo(new(big.Float).SetInt(diff), new(big.Float).SetInt(prices[lowerIndex]))
	percentDiffLower.Mul(percentDiffLower, big.NewFloat(100))

	percentDiffHigher := new(big.Float).Quo(new(big.Float).SetInt(diff), new(big.Float).SetInt(prices[higherIndex]))
	percentDiffHigher.Mul(percentDiffHigher, big.NewFloat(100))

	if diff.Sign() != 0 {
		fmt.Printf("%s has a higher gas price than %s.\n", names[higherIndex], names[lowerIndex])
		fmt.Printf("[%s: %s wei, %s: %s wei, Difference: %s wei]\n",
			names[higherIndex], prices[higherIndex], names[lowerIndex], prices[lowerIndex], diff)
		fmt.Printf("Difference as a percentage of %s's price: %.2f%%\n", names[lowerIndex], percentDiffLower)
		fmt.Printf("Difference as a percentage of %s's price: %.2f%%\n", names[higherIndex], percentDiffHigher)
	} else {
		fmt.Printf("Both nodes have the same gas price. [%s: %s wei, %s: %s wei]\n",
			names[0], prices[0], names[1], prices[1])
	}
}
