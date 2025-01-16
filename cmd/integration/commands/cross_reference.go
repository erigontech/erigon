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
			if !errors.Is(err, context.Canceled) {
				logger.Error(err.Error())
			}
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
			var r1, r2 map[string]interface{}
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

			resultMap1 := r1["result"].(map[string]interface{})
			resultMap2 := r2["result"].(map[string]interface{})

			hash1, hash2 := resultMap1["hash"].(string), resultMap2["hash"].(string)
			if hash1 != hash2 {
				return fmt.Errorf("header Hash mismatch: blockNum=%v, %v vs %v", blockNum, hash1, hash2)
			}

			transactionsRoot1, transactionsRoot2 := resultMap1["transactionsRoot"].(string), resultMap2["transactionsRoot"].(string)
			if transactionsRoot1 != transactionsRoot2 {
				return fmt.Errorf("header TransactionsRoot mismatch: blockNum=%v, %v vs %v", blockNum, transactionsRoot1, transactionsRoot2)
			}

			transactionHashes1, transactionHashes2 := resultMap1["transactions"].([]interface{}), resultMap2["transactions"].([]interface{})
			if len(transactionHashes1) != len(transactionHashes2) {
				return fmt.Errorf("header Transactions length mismatch: blockNum=%v, %v vs %v", blockNum, len(transactionHashes1), len(transactionHashes2))
			}
			for i, transactionHash1 := range transactionHashes1 {
				transactionHash2 := transactionHashes2[i]
				if transactionHash1.(string) != transactionHash2.(string) {
					return fmt.Errorf("transaction Hash mismatch: blockNum=%d, transactionIdx=%d, %v vs %v", blockNum, i, transactionHash1, transactionHash2)
				}
			}

			return nil
		})

		select {
		case <-ctx.Done():
			break LOOP // eg Wait will return correct err
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

func fetchBlockViaRpcWithRetry(ctx context.Context, logger log.Logger, rpcUrl string, blockNum uint64) (map[string]interface{}, error) {
	var err error
	var fields map[string]interface{}
	for i := 0; i < rpcMaxRetries+1; i++ {
		fields, err = fetchBlockViaRpc(logger, rpcUrl, blockNum)
		if err == nil {
			return fields, nil
		}
		if !errors.Is(err, errResponseStatusNotOk) && !errors.Is(err, errFailedResponse) {
			return nil, err
		}

		// otherwise this is a retry-able error - sleep and retry
		logger.Error("Failed to fetch block via RPC - retrying after some time", "backOffDuration", rpcBackOffDuration, "err", err)
		err = libcommon.Sleep(ctx, rpcBackOffDuration)
		if err != nil {
			return nil, err
		}
	}

	return nil, errors.New("max retries reached")
}

func fetchBlockViaRpc(logger log.Logger, rpcUrl string, blockNum uint64) (map[string]interface{}, error) {
	client := &http.Client{}
	payload := fmt.Sprintf(`{"method":"eth_getBlockByNumber","params":["0x%x",false],"id":1,"jsonrpc":"2.0"}`, blockNum)
	var data = strings.NewReader(payload)
	req, err := http.NewRequest("POST", rpcUrl, data)
	if err != nil {
		return nil, err
	}

	req.Header.Set("Content-Type", "application/json")
	resp, err := client.Do(req)
	if err != nil {
		return nil, fmt.Errorf("%w: rpcUrl=%s, blockNum=%d: %w", errFailedResponse, rpcUrl, blockNum, err)
	}
	defer func() {
		if err := resp.Body.Close(); err != nil {
			logger.Error("Could not close body", "err", err)
		}
	}()
	if resp.StatusCode != http.StatusOK {
		return nil, fmt.Errorf("%w: rpcUrl=%s, blockNum=%d", errResponseStatusNotOk, rpcUrl, blockNum)
	}

	body, err := io.ReadAll(resp.Body)
	if err != nil {
		return nil, fmt.Errorf("failed io.ReadAll for response body: rpcUrl=%s, blockNum=%d: %w", rpcUrl, blockNum, err)
	}

	var fields map[string]interface{}
	err = json.Unmarshal(body, &fields)
	if err != nil {
		return nil, fmt.Errorf("failed json.Unmarshal for response body: rpcUrl=%s, blockNum=%d: %w", rpcUrl, blockNum, err)
	}

	return fields, nil
}
