package main

import (
	"bytes"
	"encoding/json"
	"flag"
	"fmt"
	"log"
	"math/big"
	"net/http"
	"os"

	"io"

	"github.com/google/go-cmp/cmp"
)

func getBatchNumber(url string) (*big.Int, error) {
	requestBody, _ := json.Marshal(map[string]interface{}{
		"jsonrpc": "2.0",
		"method":  "zkevm_batchNumber",
		"params":  []interface{}{},
		"id":      1,
	})

	resp, err := http.Post(url, "application/json", bytes.NewBuffer(requestBody))
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()

	body, err := io.ReadAll(resp.Body)
	if err != nil {
		return nil, fmt.Errorf("failed to read response body: %v", err)
	}

	var result map[string]interface{}
	if err := json.Unmarshal(body, &result); err != nil {
		return nil, fmt.Errorf("failed to unmarshal response: %v", err)
	}

	if errorField, ok := result["error"]; ok {
		return nil, fmt.Errorf("node error: %v", errorField)
	}

	batchNumberHex, ok := result["result"].(string)
	if !ok {
		return nil, fmt.Errorf("invalid response format")
	}

	batchNumber := new(big.Int)
	if _, ok := batchNumber.SetString(batchNumberHex[2:], 16); !ok {
		return nil, fmt.Errorf("failed to convert batch number to big.Int")
	}
	return batchNumber, nil
}

func getBatchByNumber(url string, number *big.Int) (map[string]interface{}, error) {
	requestBody, _ := json.Marshal(map[string]interface{}{
		"jsonrpc": "2.0",
		"method":  "zkevm_getBatchByNumber",
		"params":  []interface{}{number.String(), false},
		"id":      1,
	})

	resp, err := http.Post(url, "application/json", bytes.NewBuffer(requestBody))
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()

	body, err := io.ReadAll(resp.Body)
	if err != nil {
		return nil, fmt.Errorf("failed to read response body: %v", err)
	}

	var result map[string]interface{}
	if err := json.Unmarshal(body, &result); err != nil {
		return nil, fmt.Errorf("failed to unmarshal response: %v", err)
	}

	if errorField, ok := result["error"]; ok {
		return nil, fmt.Errorf("node error: %v", errorField)
	}

	batchData, ok := result["result"].(map[string]interface{})
	if !ok || batchData == nil {
		return nil, fmt.Errorf("batch not found")
	}

	return batchData, nil
}

func compareBatches(erigonURL, legacyURL string, batchNumber *big.Int) (string, error) {
	batch1, err := getBatchByNumber(erigonURL, batchNumber)
	if err != nil {
		return "", fmt.Errorf("Error getting batch %d from Erigon node: %v", batchNumber, err)
	}

	batch2, err := getBatchByNumber(legacyURL, batchNumber)
	if err != nil {
		return "", fmt.Errorf("Error getting batch %d from Legacy node: %v", batchNumber, err)
	}

	// ignore list
	il := []string{
		"timestamp",
		"accInputHash",
		"transactions",
		"rollupExitRoot",
		"mainnetExitRoot",
	}
	for _, i := range il {
		delete(batch1, i)
		delete(batch2, i)
	}

	if !cmp.Equal(batch1, batch2) {
		return fmt.Sprintf("Mismatch at batch %d:\nErigon vs Legacy:\n%s",
			batchNumber, cmp.Diff(batch1, batch2)), nil
	}
	return "", nil
}

func main() {
	erigonURL := flag.String("erigon", "http://localhost:8545", "RPC URL of the Erigon node")
	legacyURL := flag.String("legacy", "http://localhost:8546", "RPC URL of the Legacy node")
	skip := flag.Int("skip", 1, "Number of batches to skip between each check")
	numBatches := flag.Int("batches", 1000, "Number of batches to check")
	startOffset := flag.Int("offset", 0, "Offset from highest getBatchNumber")
	overrideStartAt := flag.Int("override", 0, "Override start batch number")
	flag.Parse()

	erigonLatestBatch, err := getBatchNumber(*erigonURL)
	if err != nil {
		log.Fatalf("Failed to get latest batch number from Erigon node: %v", err)
	}
	log.Println("Erigon latest batch number: ", erigonLatestBatch)

	legacyLatestBatch, err := getBatchNumber(*legacyURL)
	if err != nil {
		log.Fatalf("Failed to get latest batch number from Legacy node: %v", err)
	}
	log.Println("Legacy latest batch number: ", legacyLatestBatch)

	startBatch := legacyLatestBatch
	if erigonLatestBatch.Cmp(startBatch) < 0 {
		startBatch = erigonLatestBatch
	}

	// offset start batch
	startBatch = new(big.Int).Sub(startBatch, big.NewInt(int64(*startOffset)))

	if *overrideStartAt != 0 {
		startBatch = big.NewInt(int64(*overrideStartAt))
		log.Println("Overriding start batch to", startBatch)
	}

	log.Printf("Checking %d batches\n", *numBatches)
	log.Printf("Starting from batch %d\n", startBatch)
	log.Printf("Skipping %d batches\n", *skip)

	for i := 0; i < *numBatches; i++ {
		log.Println("Checking batch", i+1, "of", *numBatches)
		batchNumber := new(big.Int).Sub(startBatch, big.NewInt(int64(i**skip)))
		diff, err := compareBatches(*erigonURL, *legacyURL, batchNumber)
		if err != nil {
			log.Println(err)
			os.Exit(1)
		}
		if diff != "" {
			log.Println(diff)
			os.Exit(1)
		}
		log.Println("Batch", batchNumber, "matches")
	}

	log.Println("No differences found")
	os.Exit(0)
}
