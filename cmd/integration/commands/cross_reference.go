package commands

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"strings"
	"time"

	"github.com/spf13/cobra"
	"golang.org/x/sync/errgroup"

	libcommon "github.com/erigontech/erigon-lib/common"
	"github.com/erigontech/erigon-lib/log/v3"
	"github.com/erigontech/erigon/turbo/debug"
)

var cmdCrossReferenceBlockHashes = &cobra.Command{
	Use:   "cross_reference_block_hashes",
	Short: "",
	Run: func(cmd *cobra.Command, args []string) {
		logger := debug.SetupCobra(cmd, "integration")
		if err := crossReferenceBlockHashes(cmd.Context(), logger, startBlockNum, endBlockNum); err != nil {
			logger.Error(err.Error())
			return
		}
	},
}

func crossReferenceBlockHashes(ctx context.Context, logger log.Logger, startBlockNum, endBlockNum uint64) error {
	if startBlockNum > endBlockNum || (startBlockNum == 0 && endBlockNum == 0) {
		panic("invalid startBlockNum > endBlockNum || (startBlockNum == 0 && endBlockNum == 0)")
	}
	if rpcUrl == "" {
		panic("rpcUrl is empty")
	}
	if secondaryRpcUrl == "" {
		panic("secondaryRpcUrl is empty")
	}

	logTicker := time.NewTicker(30 * time.Second)
	defer logTicker.Stop()

	eg, ctx := errgroup.WithContext(ctx)
	eg.SetLimit(maxParallelRequests)
LOOP:
	for blockNum := startBlockNum; blockNum < endBlockNum; blockNum++ {
		blockNum := blockNum
		eg.Go(func() error {
			var r1, r2 BlockResponse
			subEg, ctx := errgroup.WithContext(ctx)
			subEg.Go(func() error {
				var err error
				r1, err = fetchBlockViaRpcWithRetry(ctx, logger, rpcUrl, blockNum)
				return err
			})
			subEg.Go(func() error {
				var err error
				r2, err = fetchBlockViaRpcWithRetry(ctx, logger, secondaryRpcUrl, blockNum)
				return err
			})
			if err := subEg.Wait(); err != nil {
				return err
			}

			block1, block2 := r1.Result, r2.Result
			if block1.Hash != block2.Hash {
				return fmt.Errorf("header Hash mismatch: blockNum=%v, %v vs %v", blockNum, block1.Hash, block2.Hash)
			}

			if block1.TransactionsRoot != block2.TransactionsRoot {
				return fmt.Errorf("header TransactionsRoot mismatch: blockNum=%v, %v vs %v", blockNum, block1.TransactionsRoot, block2.TransactionsRoot)
			}

			if block1.BaseFeePerGas != block2.BaseFeePerGas {
				return fmt.Errorf("header BaseFeePerGas mismatch: blockNum=%v, %v vs %v", blockNum, block1.BaseFeePerGas, block2.BaseFeePerGas)
			}

			if block1.Difficulty != block2.Difficulty {
				return fmt.Errorf("header Difficulty mismatch: blockNum=%v, %v vs %v", blockNum, block1.Difficulty, block2.Difficulty)
			}

			if block1.ExtraData != block2.ExtraData {
				return fmt.Errorf("header ExtraData mismatch: blockNum=%v, %v vs %v", blockNum, block1.ExtraData, block2.ExtraData)
			}

			if block1.GasLimit != block2.GasLimit {
				return fmt.Errorf("header GasLimit mismatch: blockNum=%v, %v vs %v", blockNum, block1.GasLimit, block2.GasLimit)
			}

			if block1.GasUsed != block2.GasUsed {
				return fmt.Errorf("header GasUsed mismatch: blockNum=%v, %v vs %v", blockNum, block1.GasUsed, block2.GasUsed)
			}

			if block1.LogsBloom != block2.LogsBloom {
				return fmt.Errorf("header LogsBloom mismatch: blockNum=%v, %v vs %v", blockNum, block1.LogsBloom, block2.LogsBloom)
			}

			if block1.Miner != block2.Miner {
				return fmt.Errorf("header Miner mismatch: blockNum=%v, %v vs %v", blockNum, block1.Miner, block2.Miner)
			}

			if block1.MixHash != block2.MixHash {
				return fmt.Errorf("header MixHash mismatch: blockNum=%v, %v vs %v", blockNum, block1.MixHash, block2.MixHash)
			}

			if block1.Nonce != block2.Nonce {
				return fmt.Errorf("header Nonce mismatch: blockNum=%v, %v vs %v", blockNum, block1.Nonce, block2.Nonce)
			}

			if block1.Number != block2.Number {
				return fmt.Errorf("header Number mismatch: blockNum=%v, %v vs %v", blockNum, block1.Number, block2.Number)
			}

			if block1.ParentHash != block2.ParentHash {
				return fmt.Errorf("header ParentHash mismatch: blockNum=%v, %v vs %v", blockNum, block1.ParentHash, block2.ParentHash)
			}

			if block1.ReceiptsRoot != block2.ReceiptsRoot {
				return fmt.Errorf("header ReceiptsRoot mismatch: blockNum=%v, %v vs %v", blockNum, block1.ReceiptsRoot, block2.ReceiptsRoot)
			}

			if block1.Sha3Uncles != block2.Sha3Uncles {
				return fmt.Errorf("header Sha3Uncles mismatch: blockNum=%v, %v vs %v", blockNum, block1.Sha3Uncles, block2.Sha3Uncles)
			}

			if block1.Size != block2.Size {
				return fmt.Errorf("header Size mismatch: blockNum=%v, %v vs %v", blockNum, block1.Size, block2.Size)
			}

			if block1.StateRoot != block2.StateRoot {
				return fmt.Errorf("header StateRoot mismatch: blockNum=%v, %v vs %v", blockNum, block1.StateRoot, block2.StateRoot)
			}

			if block1.Timestamp != block2.Timestamp {
				return fmt.Errorf("header Timestamp mismatch: blockNum=%v, %v vs %v", blockNum, block1.Timestamp, block2.Timestamp)
			}

			if block1.TotalDifficulty != block2.TotalDifficulty {
				return fmt.Errorf("header TotalDifficulty mismatch: blockNum=%v, %v vs %v", blockNum, block1.TotalDifficulty, block2.TotalDifficulty)
			}

			if len(block1.Uncles) != len(block2.Uncles) {
				return fmt.Errorf("header Uncles length mismatch: blockNum=%v, %v vs %v", blockNum, len(block1.Uncles), len(block2.Uncles))
			}

			if len(block1.Transactions) != len(block2.Transactions) {
				return fmt.Errorf("header Transactions length mismatch: blockNum=%v, %v vs %v", blockNum, len(block1.Transactions), len(block2.Transactions))
			}

			for i, transaction := range block1.Transactions {
				goldenTransaction := block2.Transactions[i]
				if transaction.BlockHash != goldenTransaction.BlockHash {
					return fmt.Errorf("transaction BlockHash mismatch: blockNum=%v, transactionIdx=%v, %v vs %v", blockNum, i, transaction.BlockHash, goldenTransaction.BlockHash)
				}
				if transaction.BlockNumber != goldenTransaction.BlockNumber {
					return fmt.Errorf("transaction BlockNumber mismatch: blockNum=%v, transactionIdx=%v, %v vs %v", blockNum, i, transaction.BlockNumber, goldenTransaction.BlockNumber)
				}
				if transaction.From != goldenTransaction.From {
					return fmt.Errorf("transaction From mismatch: blockNum=%v, transactionIdx=%v, %v vs %v", blockNum, i, transaction.From, goldenTransaction.From)
				}
				if transaction.Gas != goldenTransaction.Gas {
					return fmt.Errorf("transaction Gas mismatch: blockNum=%v, transactionIdx=%v, %v vs %v", blockNum, i, transaction.Gas, goldenTransaction.Gas)
				}
				if transaction.GasPrice != goldenTransaction.GasPrice {
					return fmt.Errorf("transaction GasPrice mismatch: blockNum=%v, transactionIdx=%v, %v vs %v", blockNum, i, transaction.GasPrice, goldenTransaction.GasPrice)
				}
				if transaction.MaxFeePerGas != goldenTransaction.MaxFeePerGas {
					return fmt.Errorf("transaction MaxFeePerGas mismatch: blockNum=%v, transactionIdx=%v, %v vs %v", blockNum, i, transaction.MaxFeePerGas, goldenTransaction.MaxFeePerGas)
				}
				if transaction.MaxPriorityFeePerGas != goldenTransaction.MaxPriorityFeePerGas {
					return fmt.Errorf("transaction MaxPriorityFeePerGas mismatch: blockNum=%v, transactionIdx=%v, %v vs %v", blockNum, i, transaction.MaxPriorityFeePerGas, goldenTransaction.MaxPriorityFeePerGas)
				}
				if transaction.Hash != goldenTransaction.Hash {
					return fmt.Errorf("transaction Hash mismatch: blockNum=%v, transactionIdx=%v, %v vs %v", blockNum, i, transaction.Hash, goldenTransaction.Hash)
				}
				if transaction.Input != goldenTransaction.Input {
					return fmt.Errorf("transaction Input mismatch: blockNum=%v, transactionIdx=%v, %v vs %v", blockNum, i, transaction.Input, goldenTransaction.Input)
				}
				if transaction.Nonce != goldenTransaction.Nonce {
					return fmt.Errorf("transaction Nonce mismatch: blockNum=%v, transactionIdx=%v, %v vs %v", blockNum, i, transaction.Nonce, goldenTransaction.Nonce)
				}
				if transaction.To != goldenTransaction.To {
					return fmt.Errorf("transaction To mismatch: blockNum=%v, transactionIdx=%v, %v vs %v", blockNum, i, transaction.To, goldenTransaction.To)
				}
				if transaction.TransactionIndex != goldenTransaction.TransactionIndex {
					return fmt.Errorf("transaction Value mismatch: blockNum=%v, transactionIdx=%v, %v vs %v", blockNum, i, transaction.TransactionIndex, goldenTransaction.TransactionIndex)
				}
				if transaction.Value != goldenTransaction.Value {
					return fmt.Errorf("transaction Value mismatch: blockNum=%v, transactionIdx=%v, %v vs %v", blockNum, i, transaction.Value, goldenTransaction.Value)
				}
				if transaction.Type != goldenTransaction.Type {
					return fmt.Errorf("transaction Type mismatch: blockNum=%v, transactionIdx=%v, %v vs %v", blockNum, i, transaction.Type, goldenTransaction.Type)
				}
				if len(transaction.AccessList) != len(goldenTransaction.AccessList) {
					return fmt.Errorf("transaction AccessList mismatch: blockNum=%v, transactionIdx=%v, %v vs %v", blockNum, i, len(transaction.AccessList), len(goldenTransaction.AccessList))
				}
				if transaction.ChainId != goldenTransaction.ChainId {
					return fmt.Errorf("transaction ChainId mismatch: blockNum=%v, transactionIdx=%v, %v vs %v", blockNum, i, transaction.ChainId, goldenTransaction.ChainId)
				}
				if transaction.V != goldenTransaction.V {
					return fmt.Errorf("transaction V mismatch: blockNum=%v, transactionIdx=%v, %v vs %v", blockNum, i, transaction.V, goldenTransaction.V)
				}
				if transaction.R != goldenTransaction.R {
					return fmt.Errorf("transaction R mismatch: blockNum=%v, transactionIdx=%v, %v vs %v", blockNum, i, transaction.R, goldenTransaction.R)
				}
				if transaction.S != goldenTransaction.S {
					return fmt.Errorf("transaction S mismatch: blockNum=%v, transactionIdx=%v, %v vs %v", blockNum, i, transaction.S, goldenTransaction.S)
				}
				if transaction.YParity != goldenTransaction.YParity {
					return fmt.Errorf("transaction YParity mismatch: blockNum=%v, transactionIdx=%v, %v vs %v", blockNum, i, transaction.YParity, goldenTransaction.YParity)
				}
			}

			return nil
		})

		select {
		case <-ctx.Done():
			break LOOP // eg Wait will return proper err
		case <-logTicker.C:
			logger.Info(
				"Cross reference block hashes progress",
				"blockNum", blockNum,
				"endBlockNum", endBlockNum,
				"startBlockNum", startBlockNum,
			)
		default: // no-op
		}
	}

	if err := eg.Wait(); err != nil {
		return err
	}

	logger.Info("Cross reference block hashes completed successfully.")
	return nil
}

var (
	errFailedResponse      = errors.New("failed response")
	errResponseStatusNotOk = errors.New("response status not ok")
)

func fetchBlockViaRpcWithRetry(ctx context.Context, logger log.Logger, rpcUrl string, blockNum uint64) (BlockResponse, error) {
	var err error
	var blockResponse BlockResponse
	for i := 0; i < rpcMaxRetries+1; i++ {
		blockResponse, err = fetchBlockViaRpc(logger, rpcUrl, blockNum)
		if err == nil {
			return blockResponse, nil
		}
		if !errors.Is(err, errResponseStatusNotOk) && !errors.Is(err, errFailedResponse) {
			return blockResponse, err
		}

		// otherwise this is a retry-able error - sleep and retry
		logger.Error("Failed to fetch block via RPC - retrying after some time", "backOffDuration", rpcBackOffDuration, "err", err)
		err = libcommon.Sleep(ctx, rpcBackOffDuration)
		if err != nil {
			return blockResponse, err
		}
	}

	return blockResponse, errors.New("max retries reached")
}

func fetchBlockViaRpc(logger log.Logger, rpcUrl string, blockNum uint64) (BlockResponse, error) {
	var blockResponse BlockResponse
	client := &http.Client{}
	payload := fmt.Sprintf(`{"method":"eth_getBlockByNumber","params":["0x%x",true],"id":1,"jsonrpc":"2.0"}`, blockNum)
	var data = strings.NewReader(payload)
	req, err := http.NewRequest("POST", rpcUrl, data)
	if err != nil {
		return blockResponse, err
	}

	req.Header.Set("Content-Type", "application/json")
	resp, err := client.Do(req)
	if err != nil {
		return blockResponse, fmt.Errorf("%w: rpcUrl=%s, blockNum=%d: %w", errFailedResponse, rpcUrl, blockNum, err)
	}
	defer func() {
		if err := resp.Body.Close(); err != nil {
			logger.Error("Could not close body", "err", err)
		}
	}()
	if resp.StatusCode != http.StatusOK {
		return blockResponse, fmt.Errorf("%w: rpcUrl=%s, blockNum=%d", errResponseStatusNotOk, rpcUrl, blockNum)
	}

	body, err := io.ReadAll(resp.Body)
	if err != nil {
		return blockResponse, fmt.Errorf("failed io.ReadAll for response body: rpcUrl=%s, blockNum=%d: %w", rpcUrl, blockNum, err)
	}

	err = json.Unmarshal(body, &blockResponse)
	if err != nil {
		return blockResponse, fmt.Errorf("failed json.Unmarshal for response body: rpcUrl=%s, blockNum=%d: %w", rpcUrl, blockNum, err)
	}

	return blockResponse, nil
}

type BlockResponse struct {
	Jsonrpc string `json:"jsonrpc,omitempty"`
	Id      int    `json:"id,omitempty"`
	Result  *Block `json:"result,omitempty"`
}

type Block struct {
	BaseFeePerGas    string         `json:"baseFeePerGas,omitempty"`
	Difficulty       string         `json:"difficulty,omitempty"`
	ExtraData        string         `json:"extraData,omitempty"`
	GasLimit         string         `json:"gasLimit,omitempty"`
	GasUsed          string         `json:"gasUsed,omitempty"`
	Hash             string         `json:"hash,omitempty"`
	LogsBloom        string         `json:"logsBloom,omitempty"`
	Miner            string         `json:"miner,omitempty"`
	MixHash          string         `json:"mixHash,omitempty"`
	Nonce            string         `json:"nonce,omitempty"`
	Number           string         `json:"number,omitempty"`
	ParentHash       string         `json:"parentHash,omitempty"`
	ReceiptsRoot     string         `json:"receiptsRoot,omitempty"`
	Sha3Uncles       string         `json:"sha3Uncles,omitempty"`
	Size             string         `json:"size,omitempty"`
	StateRoot        string         `json:"stateRoot,omitempty"`
	Timestamp        string         `json:"timestamp,omitempty"`
	TotalDifficulty  string         `json:"totalDifficulty,omitempty"`
	Transactions     []*Transaction `json:"transactions,omitempty"`
	TransactionsRoot string         `json:"transactionsRoot,omitempty"`
	Uncles           []interface{}  `json:"uncles,omitempty"`
}

type Transaction struct {
	BlockHash            string        `json:"blockHash,omitempty"`
	BlockNumber          string        `json:"blockNumber,omitempty"`
	From                 string        `json:"from,omitempty"`
	Gas                  string        `json:"gas,omitempty"`
	GasPrice             string        `json:"gasPrice,omitempty"`
	MaxFeePerGas         string        `json:"maxFeePerGas,omitempty"`
	MaxPriorityFeePerGas string        `json:"maxPriorityFeePerGas,omitempty"`
	Hash                 string        `json:"hash,omitempty"`
	Input                string        `json:"input,omitempty"`
	Nonce                string        `json:"nonce,omitempty"`
	To                   string        `json:"to,omitempty"`
	TransactionIndex     string        `json:"transactionIndex,omitempty"`
	Value                string        `json:"value,omitempty"`
	Type                 string        `json:"type,omitempty"`
	AccessList           []interface{} `json:"accessList,omitempty"`
	ChainId              string        `json:"chainId,omitempty"`
	V                    string        `json:"v,omitempty"`
	R                    string        `json:"r,omitempty"`
	S                    string        `json:"s,omitempty"`
	YParity              string        `json:"yParity,omitempty"`
}
