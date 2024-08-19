package main

import (
	"flag"
	"net/http"
	"strings"
	"io"
	"fmt"
	"encoding/json"

	hcLog "github.com/hashicorp/go-hclog"
	"github.com/google/go-cmp/cmp"
)

var (
	node1 string
	node2 string
	stop  uint64
)

func main() {
	flag.StringVar(&node1, "node1", "", "node 1")
	flag.StringVar(&node2, "node2", "", "node 2")
	flag.Uint64Var(&stop, "stop", 0, "stop count")
	flag.Parse()

	log := hcLog.Default()

	// always start at batch 2 as 0 is genesis and 1 is the injected batch
	var batch uint64 = 2

	var err error

	// start reading the batches back and diving into the details of each one
	for {
		batchReq := fmt.Sprintf(zkevm_getBatchByNumber, batch)
		var node1Batch, node2Batch BatchByNumber
		if err = makeRequest(node1, batchReq, &node1Batch); err != nil {
			log.Error("Error making request", "err", err)
			return
		}
		if err = makeRequest(node2, batchReq, &node2Batch); err != nil {
			log.Error("Error making request", "err", err)
			return
		}

		if node1Batch.Result.StateRoot == "" {
			log.Info("Closing loop as node 1 does not have batch", "batch", batch)
			break
		}
		if node2Batch.Result.StateRoot == "" {
			log.Info("Closing loop as node 2 does not have batch", "batch", batch)
			break
		}

		// now compare the two responses for differences
		if cmp.Equal(node1Batch, node2Batch) {
			log.Info("Batch details were a match", "batch", batch)
		} else {
			diff := cmp.Diff(node1Batch, node2Batch)
			log.Error("Batch details were different", "batch", batch, "diff", diff)
			return
		}

		// now go and get the blocks for the batch
		for _, block := range node1Batch.Result.Blocks {
			blockReq := fmt.Sprintf(zkevm_getFullBlockByHash, block.Hash)
			var node1Block, node2Block ZkevmFullBlock
			if err = makeRequest(node1, blockReq, &node1Block); err != nil {
				log.Error("Error making request", "err", err)
				return
			}
			if err = makeRequest(node2, blockReq, &node2Block); err != nil {
				log.Error("Error making request", "err", err)
				return
			}

			if cmp.Equal(node1Block, node2Block) {
				log.Info("Block details were a match", "hash", block.Hash)
			} else {
				diff := cmp.Diff(node1Block, node2Block)
				log.Error("Block details were different", "hash", block.Hash, "diff", diff)
				return
			}
		}

		if stop > 0 && batch >= stop {
			break
		}
		batch++
	}
}

func makeRequest(endpoint, request string, into interface{}) error {
	body := strings.NewReader(request)

	req, err := http.NewRequest(http.MethodPost, endpoint, body)
	if err != nil {
		return err
	}
	req.Header.Add("Content-Type", "application/json")

	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		return err
	}
	defer resp.Body.Close()

	// read out the body into a slice of bytes
	res, err := io.ReadAll(resp.Body)
	if err != nil {
		return err
	}

	return json.Unmarshal(res, into)
}
