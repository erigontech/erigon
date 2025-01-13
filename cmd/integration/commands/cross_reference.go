package commands

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"strings"

	"github.com/spf13/cobra"

	"github.com/erigontech/erigon-lib/log/v3"
	"github.com/erigontech/erigon/turbo/debug"
)

var cmdCrossReferenceBlockHashes = &cobra.Command{
	Use:   "cross_reference_block_hashes",
	Short: "",
	Run: func(cmd *cobra.Command, args []string) {
		logger := debug.SetupCobra(cmd, "integration")
		if err := crossReferenceBlockHashes(logger, startBlockNum, endBlockNum); err != nil {
			if !errors.Is(err, context.Canceled) {
				logger.Error(err.Error())
			}
			return
		}
	},
}

func crossReferenceBlockHashes(logger log.Logger, startBlockNum, endBlockNum uint64) error {
	if startBlockNum > endBlockNum || (startBlockNum == 0 && endBlockNum == 0) {
		panic("invalid startBlockNum > endBlockNum || (startBlockNum == 0 && endBlockNum == 0)")
	}
	if rpcUrl == "" {
		panic("rpcUrl is empty")
	}
	if secondaryRpcUrl == "" {
		panic("secondaryRpcUrl is empty")
	}

	for blockNum := startBlockNum; blockNum < endBlockNum; blockNum++ {
		blockFields, err := fetchBlockViaRpc(logger, rpcUrl, blockNum)
		if err != nil {
			return err
		}

		goldenBlockFields, err := fetchBlockViaRpc(logger, secondaryRpcUrl, blockNum)
		if err != nil {
			return err
		}

		resultMap := blockFields["result"].(map[string]interface{})
		goldenResultMap := goldenBlockFields["result"].(map[string]interface{})

		hash, goldenHash := resultMap["hash"].(string), goldenResultMap["hash"].(string)
		if hash != goldenHash {
			return fmt.Errorf("header hash mismatch: blockNum=%d, hash=%s, goldenHash=%s", blockNum, hash, goldenHash)
		}
	}

	logger.Info("Cross reference block hashes completed successfully.")
	return nil
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
		return nil, err
	}
	defer func() {
		if err := resp.Body.Close(); err != nil {
			logger.Error("Could not close body", "err", err)
		}
	}()

	body, err := io.ReadAll(resp.Body)
	if err != nil {
		return nil, err
	}

	var fields map[string]interface{}
	err = json.Unmarshal(body, &fields)
	if err != nil {
		return nil, err
	}

	return fields, nil
}
