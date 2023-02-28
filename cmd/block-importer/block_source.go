package main

import (
	"bytes"
	"encoding/json"
	"net/http"
	"time"

	"github.com/ledgerwatch/erigon/core/types"
	"github.com/ledgerwatch/erigon/rlp"
)

type BlockSource interface {
	// Returns next block
	PollBlocks(fromBlock uint64) ([]types.Block, error)
}

type HttpBlockSource struct {
	client http.Client
	url    string
	logger types.Log
}

func NewHttpBlockSource(host string) HttpBlockSource {
	url := host + "/ic_getBlocks"

	return HttpBlockSource{
		client: http.Client{},
		url:    url,
	}
}

type blockRequestArgs struct {
	from uint64
}

const pollInterval time.Duration = time.Second

func (blockSource *HttpBlockSource) PollBlocks(fromBlock uint64) ([]types.Block, error) {
	args := blockRequestArgs{
		from: fromBlock,
	}

	requestBody, err := json.Marshal(args)
	if err != nil {
		return nil, err
	}

	resp, err := blockSource.client.Post(blockSource.url, "application/json", bytes.NewBuffer(requestBody))
	if err != nil {
		return nil, err
	}

	defer resp.Body.Close()

	stream := rlp.NewStream(resp.Body, 4*1024*1024)
	blocksNum, err := stream.List()
	if err != nil {
		return nil, err
	}

	result := make([]types.Block, blocksNum)
	for i := range result {
		if err := result[i].DecodeRLP(stream); err != nil {
			return nil, err
		}
	}

	return result, nil
}
