package main

import (
	"bytes"
	"encoding/json"
	"fmt"
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
}

func NewHttpBlockSource(url string) HttpBlockSource {
	return HttpBlockSource{
		client: http.Client{},
		url:    url,
	}
}

type jsonRequest struct {
	Jsonrpc string
	Method  string
	Params  []string
}

func makeBlockRequest(fromBlock uint64) jsonRequest {
	return jsonRequest{
		Jsonrpc: "2.0",
		Method:  "ic_getBlocks",
		Params:  []string{fmt.Sprintf("%x", fromBlock)},
	}
}

func (blockSource *HttpBlockSource) PollBlocks(fromBlock uint64) ([]types.Block, error) {
	args := makeBlockRequest(fromBlock)

	requestBody, err := json.Marshal(args)
	if err != nil {
		return nil, err
	}

	resp, err := blockSource.client.Post(blockSource.url, "application/json", bytes.NewBuffer(requestBody))
	if err != nil {
		return nil, err
	}

	defer resp.Body.Close()

	stream := rlp.NewStream(resp.Body, 0)
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

type intervalBlockSourceDecorator struct {
	decoree      BlockSource
	pollInterval time.Duration
	terminated   chan struct{}
}

func WithPollInterval(decoree BlockSource, pollInterval time.Duration, terminated chan struct{}) BlockSource {
	return &intervalBlockSourceDecorator{
		decoree:      decoree,
		pollInterval: pollInterval,
		terminated:   terminated,
	}
}

func (decorator *intervalBlockSourceDecorator) PollBlocks(fromBlock uint64) ([]types.Block, error) {
	for {
		select {
		case <-decorator.terminated:
			{
				return nil, nil
			}
		default:
			{
				blocks, err := decorator.decoree.PollBlocks(fromBlock)
				if err != nil {
					return nil, err
				}

				if len(blocks) > 0 {
					return blocks, nil
				}

				time.Sleep(decorator.pollInterval)
			}
		}
	}
}

type retryBlockSourceDecorator struct {
	decoree       BlockSource
	retryCount    uint64
	retryInterval time.Duration
	terminated    chan struct{}
}

func WithRetries(decoree BlockSource, retryCount uint64, retryInterval time.Duration, terminated chan struct{}) BlockSource {
	return &retryBlockSourceDecorator{
		decoree:       decoree,
		retryCount:    retryCount,
		retryInterval: retryInterval,
		terminated:    terminated,
	}
}

func (decorator *retryBlockSourceDecorator) PollBlocks(fromBlock uint64) ([]types.Block, error) {
	var err error
	var blocks []types.Block
	for i := 0; i < int(decorator.retryCount); i += 1 {
		select {
		case <-decorator.terminated:
			{
				return nil, nil
			}
		default:
			{

				blocks, err = decorator.decoree.PollBlocks(fromBlock)
				if err == nil {
					return blocks, nil
				}

				time.Sleep(decorator.retryInterval)
			}
		}
	}

	return nil, err
}
