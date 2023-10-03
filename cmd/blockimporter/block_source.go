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

	// Returns chain id
	GetChainID() (int64, error)
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
	Result  interface{}
	Error   interface{}
}

func makeJsonRpcRequest(name string, params []string) map[string]interface{} {
	return map[string]interface{}{
		"jsonrpc": "2.0",
		"method":  name,
		"params":  params,
		"id":      1, // we don't send parallel requests, os it's ok to hardcode id
	}
}

func makeBlockRequest(fromBlock uint64) map[string]interface{} {
	params := []string{
		fmt.Sprintf("0x%02x", fromBlock), // from block
		"0x05",                           // max blocks per request
	}
	return makeJsonRpcRequest("ic_getBlocksRLP", params)
}

func makeInitialBalancesRequest() map[string]interface{} {
	return makeJsonRpcRequest("ic_getGenesisBalances", []string{})
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

func (blockSource *HttpBlockSource) makeRpcRequest(args map[string]interface{}) (interface{}, error) {
	requestBody, err := json.Marshal(args)
	if err != nil {
		return "", err
	}

	resp, err := blockSource.client.Post(blockSource.url, "application/json", bytes.NewBuffer(requestBody))
	if err != nil {
		return "", err
	}
	defer resp.Body.Close()

	decoder := json.NewDecoder(resp.Body)
	var response jsonResponse
	if err = decoder.Decode(&response); err != nil {
		return "", err
	}

	if response.Error != nil {
		return "", fmt.Errorf("%v", response.Error)
	}

	return response.Result, nil
}

func parseBalanceEntry(entry interface{}) (BalanceEntry, error) {
	balanceEntry, ok := entry.([]interface{})

	if !ok || len(balanceEntry) != 2 {
		return BalanceEntry{}, fmt.Errorf("invalid balance entry format")
	}

	addressStr, ok := balanceEntry[0].(string)
	if !ok {
		return BalanceEntry{}, fmt.Errorf("invalid address format")
	}

	address := common.HexToAddress(addressStr)

	balanceStr, ok := balanceEntry[1].(string)
	if !ok {
		return BalanceEntry{}, fmt.Errorf("invalid balance format")
	}

	var balance big.Int
	_, ok = balance.SetString(strings.TrimPrefix(strings.ToLower(balanceStr), "0x"), 16)
	if !ok {
		return BalanceEntry{}, fmt.Errorf("failed to parse balance")
	}

	return BalanceEntry{
		Address: address,
		Balance: balance,
	}, nil
}

func (blockSource *HttpBlockSource) PollBlocks(fromBlock uint64) ([]types.Block, error) {
	args := makeBlockRequest(fromBlock)

	if response, err := blockSource.makeRpcRequest(args); err == nil {
		if response, ok := response.(string); ok {
			return readBlocksFromRlp(hex.NewDecoder(strings.NewReader(response)))
		} else {
			return nil, fmt.Errorf("invalid response type")
		}
	} else {
		return nil, err
	}
}

func (blockSource *HttpBlockSource) GetInitialBalances() ([]BalanceEntry, error) {
	args := makeInitialBalancesRequest()

	if response, err := blockSource.makeRpcRequest(args); err == nil {

		if response, ok := response.([]interface{}); ok {
			parsedEntries := make([]BalanceEntry, len(response))
			for i, entry := range response {
				if parsedEntries[i], err = parseBalanceEntry(entry); err != nil {
					return nil, err
				}
			}

			return parsedEntries, nil
		} else {
			return nil, fmt.Errorf("invalid response type")
		}
	} else {
		return nil, err
	}
}

func (blockSource *HttpBlockSource) GetChainID() (int64, error) {
	requestData := makeJsonRpcRequest("eth_chainId", []string{})
	chainIDResponse, err := blockSource.makeRpcRequest(requestData)
	if err != nil {
		return 0, err
	}

	response, ok := chainIDResponse.(string)
	if !ok {
		return 0, fmt.Errorf("invalid response structure")
	}

	var chainID big.Int
	if _, ok = chainID.SetString(strings.TrimPrefix(strings.ToLower(response), "0x"), 16); !ok {
		return 0, fmt.Errorf("failed to parse chain id `%s`", response)
	}

	return chainID.Int64(), nil
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

func (decorator *intervalBlockSourceDecorator) GetInitialBalances() ([]BalanceEntry, error) {
	return decorator.decoree.GetInitialBalances()
}

func (decorator *intervalBlockSourceDecorator) GetChainID() (int64, error) {
	return decorator.decoree.GetChainID()
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

func (decorator *retryBlockSourceDecorator) GetInitialBalances() ([]BalanceEntry, error) {
	var err error
	var balances []BalanceEntry
	for i := 0; i < int(decorator.retryCount); i += 1 {
		select {
		case <-decorator.terminated:
			{
				return nil, nil
			}
		default:
			{

				balances, err = decorator.decoree.GetInitialBalances()
				if err == nil {
					return balances, nil
				}

				time.Sleep(decorator.retryInterval)
			}
		}
	}

	return nil, err
}

func (decorator *retryBlockSourceDecorator) GetChainID() (int64, error) {
	var err error
	var chainID int64
	for i := 0; i < int(decorator.retryCount); i += 1 {
		select {
		case <-decorator.terminated:
			{
				return 0, nil
			}
		default:
			{
				chainID, err = decorator.decoree.GetChainID()
				if err == nil {
					return chainID, nil
				}

				time.Sleep(decorator.retryInterval)
			}
		}
	}

	return 0, err
}
