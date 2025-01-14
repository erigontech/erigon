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
	for blockNum := startBlockNum; blockNum < endBlockNum; blockNum++ {
		eg.Go(func() error {
			blockFields, err := fetchBlockViaRpcWithRetry(ctx, logger, rpcUrl, blockNum)
			if err != nil {
				return err
			}

			goldenBlockFields, err := fetchBlockViaRpcWithRetry(ctx, logger, secondaryRpcUrl, blockNum)
			if err != nil {
				return err
			}

			resultMap := blockFields["result"].(map[string]interface{})
			goldenResultMap := goldenBlockFields["result"].(map[string]interface{})

			hash, goldenHash := resultMap["hash"].(string), goldenResultMap["hash"].(string)
			if hash != goldenHash {
				return fmt.Errorf("header hash mismatch: blockNum=%d, hash=%s, goldenHash=%s", blockNum, hash, goldenHash)
			}

			transactionsRootHash, goldenTransactionsRootHash := resultMap["transactionsRoot"].(string), goldenResultMap["transactionsRoot"].(string)
			if transactionsRootHash != goldenTransactionsRootHash {
				return fmt.Errorf("transactionsRoot hash mismatch: blockNum=%d, transactionsRootHash=%s, goldenTransactionsRootHash=%s", blockNum, transactionsRootHash, goldenTransactionsRootHash)
			}

			transactions, goldenTransactions := resultMap["transactions"].([]interface{}), goldenResultMap["transactions"].([]interface{})
			if len(transactions) != len(goldenTransactions) {
				return fmt.Errorf("transactions length mismatch: blockNum=%d, transactions=%d, goldenTransactions=%d", blockNum, len(transactions), len(goldenTransactions))
			}
			for i, transaction := range transactions {
				if transaction.(string) != goldenTransactions[i].(string) {
					return fmt.Errorf("transaction mismatch: blockNum=%d, transactionIdx=%d, transaction=%s, goldenTransaction=%s", blockNum, i, transaction, goldenTransactions[i])
				}
			}

			return nil
		})

		select {
		case <-ctx.Done():
			return ctx.Err()
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
