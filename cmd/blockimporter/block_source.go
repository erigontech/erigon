package main

import (
	"bytes"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"math/big"
	"net/http"
	"strings"
	"time"

	"github.com/ledgerwatch/erigon-lib/common"
	"github.com/ledgerwatch/erigon/core/types"
	"github.com/ledgerwatch/erigon/rlp"
)

type BalanceEntry struct {
	Address common.Address
	Balance big.Int
}

type BlockSource interface {
	// Returns next block
	PollBlocks(fromBlock uint64) ([]types.Block, error)

	// Returns initial balances
	GetInitialBalances() ([]BalanceEntry, error)
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

type jsonResponse struct {
	Jsonrpc string
	Id      int
	Result  string
	Error   interface{}
}

func makeBlockRequest(fromBlock uint64) map[string]interface{} {
	return map[string]interface{}{
		"jsonrpc": "2.0",
		"method":  "ic_getBlocksRLP",
		"params": []string{
			fmt.Sprintf("0x%02x", fromBlock), // from block
			"0x05",                           // max blocks per request
		},
		"id": 1,
	}
}

func readBlocksFromRlp(byteStream io.Reader) ([]types.Block, error) {
	stream := rlp.NewStream(byteStream, 0)
	_, err := stream.List()
	if err != nil {
		return nil, err
	}

	var result []types.Block
	var block types.Block
	for err = block.DecodeRLP(stream); err == nil; err = block.DecodeRLP(stream) {
		result = append(result, block)
		block = types.Block{}
	}

	if !errors.Is(err, rlp.EOL) {
		return nil, err
	}

	return result, nil
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

	decoder := json.NewDecoder(resp.Body)
	var response jsonResponse
	if err = decoder.Decode(&response); err != nil {
		return nil, err
	}

	if response.Error != nil {
		return nil, fmt.Errorf("%v", response.Error)
	}

	return readBlocksFromRlp(hex.NewDecoder(strings.NewReader(response.Result)))
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
