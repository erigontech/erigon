// Copyright 2024 The Erigon Authors
// This file is part of Erigon.
//
// Erigon is free software: you can redistribute it and/or modify
// it under the terms of the GNU Lesser General Public License as published by
// the Free Software Foundation, either version 3 of the License, or
// (at your option) any later version.
//
// Erigon is distributed in the hope that it will be useful,
// but WITHOUT ANY WARRANTY; without even the implied warranty of
// MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE. See the
// GNU Lesser General Public License for more details.
//
// You should have received a copy of the GNU Lesser General Public License
// along with Erigon. If not, see <http://www.gnu.org/licenses/>.

package mcp

import (
	"context"
	"encoding/json"
	"fmt"
	"math/big"
	"os"
	"strings"

	"github.com/mark3labs/mcp-go/mcp"
	"github.com/mark3labs/mcp-go/server"

	"github.com/erigontech/erigon/rpc"
)

// StandaloneMCPServer is a standalone MCP server that proxies MCP tool calls
// to a running Erigon node via JSON-RPC HTTP. Unlike ErigonMCPServer which
// uses in-process Go API interfaces, this server makes raw JSON-RPC calls
// via rpc.Client and can run as a separate process.
type StandaloneMCPServer struct {
	rpcClient *rpc.Client
	mcpServer *server.MCPServer
	logTools
}

// NewStandaloneMCPServer creates a new standalone MCP server that proxies
// tool calls to a running Erigon node via JSON-RPC.
func NewStandaloneMCPServer(rpcClient *rpc.Client, logDir string) *StandaloneMCPServer {
	s := &StandaloneMCPServer{
		rpcClient: rpcClient,
		logTools:  logTools{logDir: logDir},
	}

	s.mcpServer = server.NewMCPServer(
		"ErigonMCP",
		Version,
		server.WithResourceCapabilities(true, true),
		server.WithToolCapabilities(true),
		server.WithPromptCapabilities(true),
		server.WithLogging(),
		server.WithRecovery(),
	)

	registerTools(s.mcpServer, s.toolHandlers())
	registerPrompts(s.mcpServer)
	registerResources(s.mcpServer, s.resourceHandlers())

	return s
}

// toJSONIndent pretty-prints raw JSON bytes.
func toJSONIndent(raw json.RawMessage) string {
	var parsed any
	if err := json.Unmarshal(raw, &parsed); err != nil {
		return string(raw)
	}
	formatted, err := json.MarshalIndent(parsed, "", "  ")
	if err != nil {
		return string(raw)
	}
	return string(formatted)
}

func (s *StandaloneMCPServer) toolHandlers() map[string]server.ToolHandlerFunc {
	return map[string]server.ToolHandlerFunc{
		"eth_blockNumber":                         s.handleBlockNumber,
		"eth_getBlockByNumber":                    s.handleGetBlockByNumber,
		"eth_getBlockByHash":                      s.handleGetBlockByHash,
		"eth_getBlockTransactionCountByNumber":    s.handleGetBlockTransactionCountByNumber,
		"eth_getBlockTransactionCountByHash":      s.handleGetBlockTransactionCountByHash,
		"eth_getBalance":                          s.handleGetBalance,
		"eth_getTransactionByHash":                s.handleGetTransactionByHash,
		"eth_getTransactionByBlockHashAndIndex":   s.handleGetTransactionByBlockHashAndIndex,
		"eth_getTransactionByBlockNumberAndIndex": s.handleGetTransactionByBlockNumberAndIndex,
		"eth_getTransactionReceipt":               s.handleGetTransactionReceipt,
		"eth_getBlockReceipts":                    s.handleGetBlockReceipts,
		"eth_getLogs":                             s.handleGetLogs,
		"eth_getCode":                             s.handleGetCode,
		"eth_getStorageAt":                        s.handleGetStorageAt,
		"eth_getTransactionCount":                 s.handleGetTransactionCount,
		"eth_call":                                s.handleCall,
		"eth_estimateGas":                         s.handleEstimateGas,
		"eth_gasPrice":                            s.handleGasPrice,
		"eth_chainId":                             s.handleChainId,
		"eth_syncing":                             s.handleSyncing,
		"eth_accounts":                            s.handleAccounts,
		"eth_getProof":                            s.handleGetProof,
		"eth_coinbase":                            s.handleCoinbase,
		"eth_mining":                              s.handleMining,
		"eth_hashrate":                            s.handleHashrate,
		"eth_protocolVersion":                     s.handleProtocolVersion,
		"eth_getUncleByBlockNumberAndIndex":       s.handleGetUncleByBlockNumberAndIndex,
		"eth_getUncleByBlockHashAndIndex":         s.handleGetUncleByBlockHashAndIndex,
		"eth_getUncleCountByBlockNumber":          s.handleGetUncleCountByBlockNumber,
		"eth_getUncleCountByBlockHash":            s.handleGetUncleCountByBlockHash,
		"erigon_forks":                            s.handleErigonForks,
		"erigon_blockNumber":                      s.handleErigonBlockNumber,
		"erigon_getHeaderByNumber":                s.handleErigonGetHeaderByNumber,
		"erigon_getHeaderByHash":                  s.handleErigonGetHeaderByHash,
		"erigon_getBlockByTimestamp":              s.handleErigonGetBlockByTimestamp,
		"erigon_getBalanceChangesInBlock":         s.handleErigonGetBalanceChangesInBlock,
		"erigon_getLogsByHash":                    s.handleErigonGetLogsByHash,
		"erigon_getLogs":                          s.handleErigonGetLogs,
		"erigon_getBlockReceiptsByBlockHash":      s.handleErigonGetBlockReceiptsByBlockHash,
		"erigon_nodeInfo":                         s.handleErigonNodeInfo,
		"metrics_list":                            s.handleMetricsList,
		"metrics_get":                             s.handleMetricsGet,
		"logs_tail":                               s.handleLogsTail,
		"logs_head":                               s.handleLogsHead,
		"logs_grep":                               s.handleLogsGrep,
		"logs_stats":                              s.handleLogsStats,
		"ots_getApiLevel":                         s.handleOtsGetApiLevel,
		"ots_getInternalOperations":               s.handleOtsGetInternalOperations,
		"ots_searchTransactionsBefore":            s.handleOtsSearchTransactionsBefore,
		"ots_searchTransactionsAfter":             s.handleOtsSearchTransactionsAfter,
		"ots_getBlockDetails":                     s.handleOtsGetBlockDetails,
		"ots_getBlockDetailsByHash":               s.handleOtsGetBlockDetailsByHash,
		"ots_getBlockTransactions":                s.handleOtsGetBlockTransactions,
		"ots_hasCode":                             s.handleOtsHasCode,
		"ots_traceTransaction":                    s.handleOtsTraceTransaction,
		"ots_getTransactionError":                 s.handleOtsGetTransactionError,
		"ots_getTransactionBySenderAndNonce":      s.handleOtsGetTransactionBySenderAndNonce,
		"ots_getContractCreator":                  s.handleOtsGetContractCreator,
	}
}

// ===== ETH API HANDLERS =====

func (s *StandaloneMCPServer) handleBlockNumber(ctx context.Context, req mcp.CallToolRequest) (*mcp.CallToolResult, error) {
	var result string
	if err := s.rpcClient.CallContext(ctx, &result, "eth_blockNumber"); err != nil {
		return mcp.NewToolResultError(err.Error()), nil
	}
	// Parse hex block number for human-readable output
	blockNum := new(big.Int)
	if _, ok := blockNum.SetString(strings.TrimPrefix(result, "0x"), 16); !ok {
		return mcp.NewToolResultText(fmt.Sprintf("Current block: %s", result)), nil
	}
	return mcp.NewToolResultText(fmt.Sprintf("Current block: %d (%s)", blockNum, result)), nil
}

func (s *StandaloneMCPServer) handleGetBlockByNumber(ctx context.Context, req mcp.CallToolRequest) (*mcp.CallToolResult, error) {
	blockNum := req.GetString("blockNumber", "latest")
	fullTx := req.GetBool("fullTransactions", false)
	var result json.RawMessage
	if err := s.rpcClient.CallContext(ctx, &result, "eth_getBlockByNumber", blockNum, fullTx); err != nil {
		return mcp.NewToolResultError(err.Error()), nil
	}
	if result == nil || string(result) == "null" {
		return mcp.NewToolResultText("Block not found"), nil
	}
	return mcp.NewToolResultText(toJSONIndent(result)), nil
}

func (s *StandaloneMCPServer) handleGetBlockByHash(ctx context.Context, req mcp.CallToolRequest) (*mcp.CallToolResult, error) {
	blockHash := req.GetString("blockHash", "")
	fullTx := req.GetBool("fullTransactions", false)
	var result json.RawMessage
	if err := s.rpcClient.CallContext(ctx, &result, "eth_getBlockByHash", blockHash, fullTx); err != nil {
		return mcp.NewToolResultError(err.Error()), nil
	}
	if result == nil || string(result) == "null" {
		return mcp.NewToolResultText("Block not found"), nil
	}
	return mcp.NewToolResultText(toJSONIndent(result)), nil
}

func (s *StandaloneMCPServer) handleGetBlockTransactionCountByNumber(ctx context.Context, req mcp.CallToolRequest) (*mcp.CallToolResult, error) {
	blockNum := req.GetString("blockNumber", "latest")
	var result string
	if err := s.rpcClient.CallContext(ctx, &result, "eth_getBlockTransactionCountByNumber", blockNum); err != nil {
		return mcp.NewToolResultError(err.Error()), nil
	}
	count := new(big.Int)
	if _, ok := count.SetString(strings.TrimPrefix(result, "0x"), 16); !ok {
		return mcp.NewToolResultText(fmt.Sprintf("Transaction count: %s", result)), nil
	}
	return mcp.NewToolResultText(fmt.Sprintf("Transaction count: %d", count)), nil
}

func (s *StandaloneMCPServer) handleGetBlockTransactionCountByHash(ctx context.Context, req mcp.CallToolRequest) (*mcp.CallToolResult, error) {
	blockHash := req.GetString("blockHash", "")
	var result string
	if err := s.rpcClient.CallContext(ctx, &result, "eth_getBlockTransactionCountByHash", blockHash); err != nil {
		return mcp.NewToolResultError(err.Error()), nil
	}
	count := new(big.Int)
	if _, ok := count.SetString(strings.TrimPrefix(result, "0x"), 16); !ok {
		return mcp.NewToolResultText(fmt.Sprintf("Transaction count: %s", result)), nil
	}
	return mcp.NewToolResultText(fmt.Sprintf("Transaction count: %d", count)), nil
}

func (s *StandaloneMCPServer) handleGetBalance(ctx context.Context, req mcp.CallToolRequest) (*mcp.CallToolResult, error) {
	address := req.GetString("address", "")
	blockNum := req.GetString("blockNumber", "latest")
	var hexResult string
	if err := s.rpcClient.CallContext(ctx, &hexResult, "eth_getBalance", address, blockNum); err != nil {
		return mcp.NewToolResultError(err.Error()), nil
	}
	// Parse hex balance and convert to ETH
	wei := new(big.Int)
	if _, ok := wei.SetString(strings.TrimPrefix(hexResult, "0x"), 16); !ok {
		return mcp.NewToolResultText(fmt.Sprintf("Balance: %s", hexResult)), nil
	}
	eth := new(big.Float).Quo(new(big.Float).SetInt(wei), big.NewFloat(1e18))
	return mcp.NewToolResultText(fmt.Sprintf("Balance: %s wei (%.6f ETH)", hexResult, eth)), nil
}

func (s *StandaloneMCPServer) handleGetTransactionByHash(ctx context.Context, req mcp.CallToolRequest) (*mcp.CallToolResult, error) {
	txHash := req.GetString("txHash", "")
	var result json.RawMessage
	if err := s.rpcClient.CallContext(ctx, &result, "eth_getTransactionByHash", txHash); err != nil {
		return mcp.NewToolResultError(err.Error()), nil
	}
	if result == nil || string(result) == "null" {
		return mcp.NewToolResultText("Transaction not found"), nil
	}
	return mcp.NewToolResultText(toJSONIndent(result)), nil
}

func (s *StandaloneMCPServer) handleGetTransactionByBlockHashAndIndex(ctx context.Context, req mcp.CallToolRequest) (*mcp.CallToolResult, error) {
	blockHash := req.GetString("blockHash", "")
	index := req.GetInt("index", 0)
	// JSON-RPC expects hex index
	hexIndex := fmt.Sprintf("0x%x", index)
	var result json.RawMessage
	if err := s.rpcClient.CallContext(ctx, &result, "eth_getTransactionByBlockHashAndIndex", blockHash, hexIndex); err != nil {
		return mcp.NewToolResultError(err.Error()), nil
	}
	if result == nil || string(result) == "null" {
		return mcp.NewToolResultText("Transaction not found"), nil
	}
	return mcp.NewToolResultText(toJSONIndent(result)), nil
}

func (s *StandaloneMCPServer) handleGetTransactionByBlockNumberAndIndex(ctx context.Context, req mcp.CallToolRequest) (*mcp.CallToolResult, error) {
	blockNum := req.GetString("blockNumber", "latest")
	index := req.GetInt("index", 0)
	hexIndex := fmt.Sprintf("0x%x", index)
	var result json.RawMessage
	if err := s.rpcClient.CallContext(ctx, &result, "eth_getTransactionByBlockNumberAndIndex", blockNum, hexIndex); err != nil {
		return mcp.NewToolResultError(err.Error()), nil
	}
	if result == nil || string(result) == "null" {
		return mcp.NewToolResultText("Transaction not found"), nil
	}
	return mcp.NewToolResultText(toJSONIndent(result)), nil
}

func (s *StandaloneMCPServer) handleGetTransactionReceipt(ctx context.Context, req mcp.CallToolRequest) (*mcp.CallToolResult, error) {
	txHash := req.GetString("txHash", "")
	var result json.RawMessage
	if err := s.rpcClient.CallContext(ctx, &result, "eth_getTransactionReceipt", txHash); err != nil {
		return mcp.NewToolResultError(err.Error()), nil
	}
	if result == nil || string(result) == "null" {
		return mcp.NewToolResultText("Receipt not found"), nil
	}
	return mcp.NewToolResultText(toJSONIndent(result)), nil
}

func (s *StandaloneMCPServer) handleGetBlockReceipts(ctx context.Context, req mcp.CallToolRequest) (*mcp.CallToolResult, error) {
	blockNumOrHash := req.GetString("blockNumberOrHash", "latest")
	var result json.RawMessage
	if err := s.rpcClient.CallContext(ctx, &result, "eth_getBlockReceipts", blockNumOrHash); err != nil {
		return mcp.NewToolResultError(err.Error()), nil
	}
	if result == nil || string(result) == "null" {
		return mcp.NewToolResultText("Block receipts not found"), nil
	}
	return mcp.NewToolResultText(toJSONIndent(result)), nil
}

func (s *StandaloneMCPServer) handleGetLogs(ctx context.Context, req mcp.CallToolRequest) (*mcp.CallToolResult, error) {
	// Build filter object from individual params
	filter := map[string]any{}
	if fromBlock := req.GetString("fromBlock", ""); fromBlock != "" {
		filter["fromBlock"] = fromBlock
	}
	if toBlock := req.GetString("toBlock", ""); toBlock != "" {
		filter["toBlock"] = toBlock
	}
	if address := req.GetString("address", ""); address != "" {
		// Support both single address and JSON array of addresses
		if strings.HasPrefix(address, "[") {
			var addrs []string
			if err := json.Unmarshal([]byte(address), &addrs); err != nil {
				return mcp.NewToolResultError(fmt.Sprintf("invalid address JSON array: %v", err)), nil
			}
			filter["address"] = addrs
		} else {
			filter["address"] = address
		}
	}
	if topics := req.GetString("topics", ""); topics != "" {
		var t json.RawMessage
		if err := json.Unmarshal([]byte(topics), &t); err != nil {
			return mcp.NewToolResultError(fmt.Sprintf("invalid topics JSON: %v", err)), nil
		}
		filter["topics"] = t
	}
	if blockHash := req.GetString("blockHash", ""); blockHash != "" {
		filter["blockHash"] = blockHash
	}
	var result json.RawMessage
	if err := s.rpcClient.CallContext(ctx, &result, "eth_getLogs", filter); err != nil {
		return mcp.NewToolResultError(err.Error()), nil
	}
	return mcp.NewToolResultText(toJSONIndent(result)), nil
}

func (s *StandaloneMCPServer) handleGetCode(ctx context.Context, req mcp.CallToolRequest) (*mcp.CallToolResult, error) {
	address := req.GetString("address", "")
	blockNum := req.GetString("blockNumber", "latest")
	var result string
	if err := s.rpcClient.CallContext(ctx, &result, "eth_getCode", address, blockNum); err != nil {
		return mcp.NewToolResultError(err.Error()), nil
	}
	if result == "0x" || result == "" {
		return mcp.NewToolResultText("No code (EOA)"), nil
	}
	// Code length in bytes = (hex length - 2 for "0x") / 2
	codeLen := (len(result) - 2) / 2
	return mcp.NewToolResultText(fmt.Sprintf("Code (%d bytes): %s", codeLen, result)), nil
}

func (s *StandaloneMCPServer) handleGetStorageAt(ctx context.Context, req mcp.CallToolRequest) (*mcp.CallToolResult, error) {
	address := req.GetString("address", "")
	position := req.GetString("position", "0x0")
	blockNum := req.GetString("blockNumber", "latest")
	var result string
	if err := s.rpcClient.CallContext(ctx, &result, "eth_getStorageAt", address, position, blockNum); err != nil {
		return mcp.NewToolResultError(err.Error()), nil
	}
	return mcp.NewToolResultText(fmt.Sprintf("Storage: %s", result)), nil
}

func (s *StandaloneMCPServer) handleGetTransactionCount(ctx context.Context, req mcp.CallToolRequest) (*mcp.CallToolResult, error) {
	address := req.GetString("address", "")
	blockNum := req.GetString("blockNumber", "latest")
	var result string
	if err := s.rpcClient.CallContext(ctx, &result, "eth_getTransactionCount", address, blockNum); err != nil {
		return mcp.NewToolResultError(err.Error()), nil
	}
	nonce := new(big.Int)
	if _, ok := nonce.SetString(strings.TrimPrefix(result, "0x"), 16); !ok {
		return mcp.NewToolResultText(fmt.Sprintf("Nonce: %s", result)), nil
	}
	return mcp.NewToolResultText(fmt.Sprintf("Nonce: %d", nonce)), nil
}

func (s *StandaloneMCPServer) handleCall(ctx context.Context, req mcp.CallToolRequest) (*mcp.CallToolResult, error) {
	// Build call object
	callObj := map[string]any{
		"to":   req.GetString("to", ""),
		"data": req.GetString("data", ""),
	}
	if from := req.GetString("from", ""); from != "" {
		callObj["from"] = from
	}
	if value := req.GetString("value", ""); value != "" {
		callObj["value"] = value
	}
	if gas := req.GetString("gas", ""); gas != "" {
		callObj["gas"] = gas
	}
	blockNum := req.GetString("blockNumber", "latest")
	var result string
	if err := s.rpcClient.CallContext(ctx, &result, "eth_call", callObj, blockNum); err != nil {
		return mcp.NewToolResultError(err.Error()), nil
	}
	return mcp.NewToolResultText(fmt.Sprintf("Result: %s", result)), nil
}

func (s *StandaloneMCPServer) handleEstimateGas(ctx context.Context, req mcp.CallToolRequest) (*mcp.CallToolResult, error) {
	callObj := map[string]any{}
	if to := req.GetString("to", ""); to != "" {
		callObj["to"] = to
	}
	if data := req.GetString("data", ""); data != "" {
		callObj["data"] = data
	}
	if from := req.GetString("from", ""); from != "" {
		callObj["from"] = from
	}
	if value := req.GetString("value", ""); value != "" {
		callObj["value"] = value
	}
	var result string
	if err := s.rpcClient.CallContext(ctx, &result, "eth_estimateGas", callObj); err != nil {
		return mcp.NewToolResultError(err.Error()), nil
	}
	gas := new(big.Int)
	if _, ok := gas.SetString(strings.TrimPrefix(result, "0x"), 16); !ok {
		return mcp.NewToolResultText(fmt.Sprintf("Estimated gas: %s", result)), nil
	}
	return mcp.NewToolResultText(fmt.Sprintf("Estimated gas: %d", gas)), nil
}

func (s *StandaloneMCPServer) handleGasPrice(ctx context.Context, req mcp.CallToolRequest) (*mcp.CallToolResult, error) {
	var result string
	if err := s.rpcClient.CallContext(ctx, &result, "eth_gasPrice"); err != nil {
		return mcp.NewToolResultError(err.Error()), nil
	}
	wei := new(big.Int)
	if _, ok := wei.SetString(strings.TrimPrefix(result, "0x"), 16); !ok {
		return mcp.NewToolResultText(fmt.Sprintf("Gas price: %s", result)), nil
	}
	gwei := new(big.Float).Quo(new(big.Float).SetInt(wei), big.NewFloat(1e9))
	return mcp.NewToolResultText(fmt.Sprintf("Gas price: %s wei (%.2f Gwei)", result, gwei)), nil
}

func (s *StandaloneMCPServer) handleChainId(ctx context.Context, req mcp.CallToolRequest) (*mcp.CallToolResult, error) {
	var result string
	if err := s.rpcClient.CallContext(ctx, &result, "eth_chainId"); err != nil {
		return mcp.NewToolResultError(err.Error()), nil
	}
	chainId := new(big.Int)
	if _, ok := chainId.SetString(strings.TrimPrefix(result, "0x"), 16); !ok {
		return mcp.NewToolResultText(fmt.Sprintf("Chain ID: %s", result)), nil
	}
	return mcp.NewToolResultText(fmt.Sprintf("Chain ID: %d (%s)", chainId, result)), nil
}

func (s *StandaloneMCPServer) handleSyncing(ctx context.Context, req mcp.CallToolRequest) (*mcp.CallToolResult, error) {
	var result json.RawMessage
	if err := s.rpcClient.CallContext(ctx, &result, "eth_syncing"); err != nil {
		return mcp.NewToolResultError(err.Error()), nil
	}
	// eth_syncing returns false when fully synced, or an object when syncing
	if string(result) == "false" {
		return mcp.NewToolResultText("Fully synced"), nil
	}
	return mcp.NewToolResultText(toJSONIndent(result)), nil
}

func (s *StandaloneMCPServer) handleAccounts(ctx context.Context, req mcp.CallToolRequest) (*mcp.CallToolResult, error) {
	var result json.RawMessage
	if err := s.rpcClient.CallContext(ctx, &result, "eth_accounts"); err != nil {
		return mcp.NewToolResultError(err.Error()), nil
	}
	if string(result) == "[]" || string(result) == "null" {
		return mcp.NewToolResultText("No accounts"), nil
	}
	return mcp.NewToolResultText(toJSONIndent(result)), nil
}

func (s *StandaloneMCPServer) handleCoinbase(ctx context.Context, req mcp.CallToolRequest) (*mcp.CallToolResult, error) {
	var result string
	if err := s.rpcClient.CallContext(ctx, &result, "eth_coinbase"); err != nil {
		return mcp.NewToolResultError(err.Error()), nil
	}
	return mcp.NewToolResultText(fmt.Sprintf("Coinbase: %s", result)), nil
}

func (s *StandaloneMCPServer) handleMining(ctx context.Context, req mcp.CallToolRequest) (*mcp.CallToolResult, error) {
	var result bool
	if err := s.rpcClient.CallContext(ctx, &result, "eth_mining"); err != nil {
		return mcp.NewToolResultError(err.Error()), nil
	}
	return mcp.NewToolResultText(fmt.Sprintf("Mining: %v", result)), nil
}

func (s *StandaloneMCPServer) handleHashrate(ctx context.Context, req mcp.CallToolRequest) (*mcp.CallToolResult, error) {
	var result string
	if err := s.rpcClient.CallContext(ctx, &result, "eth_hashrate"); err != nil {
		return mcp.NewToolResultError(err.Error()), nil
	}
	hashrate := new(big.Int)
	if _, ok := hashrate.SetString(strings.TrimPrefix(result, "0x"), 16); !ok {
		return mcp.NewToolResultText(fmt.Sprintf("Hashrate: %s", result)), nil
	}
	return mcp.NewToolResultText(fmt.Sprintf("Hashrate: %d H/s", hashrate)), nil
}

func (s *StandaloneMCPServer) handleProtocolVersion(ctx context.Context, req mcp.CallToolRequest) (*mcp.CallToolResult, error) {
	var result string
	if err := s.rpcClient.CallContext(ctx, &result, "eth_protocolVersion"); err != nil {
		return mcp.NewToolResultError(err.Error()), nil
	}
	version := new(big.Int)
	if _, ok := version.SetString(strings.TrimPrefix(result, "0x"), 16); !ok {
		return mcp.NewToolResultText(fmt.Sprintf("Protocol version: %s", result)), nil
	}
	return mcp.NewToolResultText(fmt.Sprintf("Protocol version: %d", version)), nil
}

// ===== UNCLE HANDLERS =====

func (s *StandaloneMCPServer) handleGetUncleByBlockNumberAndIndex(ctx context.Context, req mcp.CallToolRequest) (*mcp.CallToolResult, error) {
	blockNum := req.GetString("blockNumber", "latest")
	index := req.GetInt("index", 0)
	hexIndex := fmt.Sprintf("0x%x", index)
	var result json.RawMessage
	if err := s.rpcClient.CallContext(ctx, &result, "eth_getUncleByBlockNumberAndIndex", blockNum, hexIndex); err != nil {
		return mcp.NewToolResultError(err.Error()), nil
	}
	if result == nil || string(result) == "null" {
		return mcp.NewToolResultText("Uncle not found"), nil
	}
	return mcp.NewToolResultText(toJSONIndent(result)), nil
}

func (s *StandaloneMCPServer) handleGetUncleByBlockHashAndIndex(ctx context.Context, req mcp.CallToolRequest) (*mcp.CallToolResult, error) {
	blockHash := req.GetString("blockHash", "")
	index := req.GetInt("index", 0)
	hexIndex := fmt.Sprintf("0x%x", index)
	var result json.RawMessage
	if err := s.rpcClient.CallContext(ctx, &result, "eth_getUncleByBlockHashAndIndex", blockHash, hexIndex); err != nil {
		return mcp.NewToolResultError(err.Error()), nil
	}
	if result == nil || string(result) == "null" {
		return mcp.NewToolResultText("Uncle not found"), nil
	}
	return mcp.NewToolResultText(toJSONIndent(result)), nil
}

func (s *StandaloneMCPServer) handleGetUncleCountByBlockNumber(ctx context.Context, req mcp.CallToolRequest) (*mcp.CallToolResult, error) {
	blockNum := req.GetString("blockNumber", "latest")
	var result string
	if err := s.rpcClient.CallContext(ctx, &result, "eth_getUncleCountByBlockNumber", blockNum); err != nil {
		return mcp.NewToolResultError(err.Error()), nil
	}
	count := new(big.Int)
	if _, ok := count.SetString(strings.TrimPrefix(result, "0x"), 16); !ok {
		return mcp.NewToolResultText(fmt.Sprintf("Uncle count: %s", result)), nil
	}
	return mcp.NewToolResultText(fmt.Sprintf("Uncle count: %d", count)), nil
}

func (s *StandaloneMCPServer) handleGetUncleCountByBlockHash(ctx context.Context, req mcp.CallToolRequest) (*mcp.CallToolResult, error) {
	blockHash := req.GetString("blockHash", "")
	var result string
	if err := s.rpcClient.CallContext(ctx, &result, "eth_getUncleCountByBlockHash", blockHash); err != nil {
		return mcp.NewToolResultError(err.Error()), nil
	}
	count := new(big.Int)
	if _, ok := count.SetString(strings.TrimPrefix(result, "0x"), 16); !ok {
		return mcp.NewToolResultText(fmt.Sprintf("Uncle count: %s", result)), nil
	}
	return mcp.NewToolResultText(fmt.Sprintf("Uncle count: %d", count)), nil
}

func (s *StandaloneMCPServer) handleGetProof(ctx context.Context, req mcp.CallToolRequest) (*mcp.CallToolResult, error) {
	address := req.GetString("address", "")
	blockNum := req.GetString("blockNumber", "latest")
	// Parse storage keys from JSON array string
	var storageKeys []string
	if k := req.GetString("storageKeys", ""); k != "" {
		if err := json.Unmarshal([]byte(k), &storageKeys); err != nil {
			return mcp.NewToolResultError(fmt.Sprintf("invalid storageKeys JSON: %v", err)), nil
		}
	}
	if storageKeys == nil {
		storageKeys = []string{}
	}
	var result json.RawMessage
	if err := s.rpcClient.CallContext(ctx, &result, "eth_getProof", address, storageKeys, blockNum); err != nil {
		return mcp.NewToolResultError(err.Error()), nil
	}
	return mcp.NewToolResultText(toJSONIndent(result)), nil
}

// ===== ERIGON API HANDLERS =====

func (s *StandaloneMCPServer) handleErigonForks(ctx context.Context, req mcp.CallToolRequest) (*mcp.CallToolResult, error) {
	var result json.RawMessage
	if err := s.rpcClient.CallContext(ctx, &result, "erigon_forks"); err != nil {
		return mcp.NewToolResultError(err.Error()), nil
	}
	return mcp.NewToolResultText(toJSONIndent(result)), nil
}

func (s *StandaloneMCPServer) handleErigonBlockNumber(ctx context.Context, req mcp.CallToolRequest) (*mcp.CallToolResult, error) {
	blockTag := req.GetString("blockNumber", "")
	var result string
	var err error
	if blockTag != "" {
		err = s.rpcClient.CallContext(ctx, &result, "erigon_blockNumber", blockTag)
	} else {
		err = s.rpcClient.CallContext(ctx, &result, "erigon_blockNumber")
	}
	if err != nil {
		return mcp.NewToolResultError(err.Error()), nil
	}
	blockNum := new(big.Int)
	if _, ok := blockNum.SetString(strings.TrimPrefix(result, "0x"), 16); !ok {
		return mcp.NewToolResultText(fmt.Sprintf("Block number: %s", result)), nil
	}
	return mcp.NewToolResultText(fmt.Sprintf("Block number: %d", blockNum)), nil
}

func (s *StandaloneMCPServer) handleErigonGetHeaderByNumber(ctx context.Context, req mcp.CallToolRequest) (*mcp.CallToolResult, error) {
	blockNum := req.GetString("blockNumber", "latest")
	var result json.RawMessage
	if err := s.rpcClient.CallContext(ctx, &result, "erigon_getHeaderByNumber", blockNum); err != nil {
		return mcp.NewToolResultError(err.Error()), nil
	}
	if result == nil || string(result) == "null" {
		return mcp.NewToolResultText("Header not found"), nil
	}
	return mcp.NewToolResultText(toJSONIndent(result)), nil
}

func (s *StandaloneMCPServer) handleErigonGetHeaderByHash(ctx context.Context, req mcp.CallToolRequest) (*mcp.CallToolResult, error) {
	blockHash := req.GetString("blockHash", "")
	var result json.RawMessage
	if err := s.rpcClient.CallContext(ctx, &result, "erigon_getHeaderByHash", blockHash); err != nil {
		return mcp.NewToolResultError(err.Error()), nil
	}
	if result == nil || string(result) == "null" {
		return mcp.NewToolResultText("Header not found"), nil
	}
	return mcp.NewToolResultText(toJSONIndent(result)), nil
}

func (s *StandaloneMCPServer) handleErigonGetBlockByTimestamp(ctx context.Context, req mcp.CallToolRequest) (*mcp.CallToolResult, error) {
	timestamp := req.GetString("timestamp", "")
	fullTx := req.GetBool("fullTransactions", false)
	var result json.RawMessage
	if err := s.rpcClient.CallContext(ctx, &result, "erigon_getBlockByTimestamp", timestamp, fullTx); err != nil {
		return mcp.NewToolResultError(err.Error()), nil
	}
	if result == nil || string(result) == "null" {
		return mcp.NewToolResultText("Block not found"), nil
	}
	return mcp.NewToolResultText(toJSONIndent(result)), nil
}

func (s *StandaloneMCPServer) handleErigonGetBalanceChangesInBlock(ctx context.Context, req mcp.CallToolRequest) (*mcp.CallToolResult, error) {
	blockNumOrHash := req.GetString("blockNumberOrHash", "latest")
	var result json.RawMessage
	if err := s.rpcClient.CallContext(ctx, &result, "erigon_getBalanceChangesInBlock", blockNumOrHash); err != nil {
		return mcp.NewToolResultError(err.Error()), nil
	}
	return mcp.NewToolResultText(toJSONIndent(result)), nil
}

func (s *StandaloneMCPServer) handleErigonGetLogsByHash(ctx context.Context, req mcp.CallToolRequest) (*mcp.CallToolResult, error) {
	blockHash := req.GetString("blockHash", "")
	var result json.RawMessage
	if err := s.rpcClient.CallContext(ctx, &result, "erigon_getLogsByHash", blockHash); err != nil {
		return mcp.NewToolResultError(err.Error()), nil
	}
	return mcp.NewToolResultText(toJSONIndent(result)), nil
}

func (s *StandaloneMCPServer) handleErigonGetLogs(ctx context.Context, req mcp.CallToolRequest) (*mcp.CallToolResult, error) {
	filter := map[string]any{}
	if fromBlock := req.GetString("fromBlock", ""); fromBlock != "" {
		filter["fromBlock"] = fromBlock
	}
	if toBlock := req.GetString("toBlock", ""); toBlock != "" {
		filter["toBlock"] = toBlock
	}
	if address := req.GetString("address", ""); address != "" {
		filter["address"] = address
	}
	if topics := req.GetString("topics", ""); topics != "" {
		var t json.RawMessage
		if err := json.Unmarshal([]byte(topics), &t); err != nil {
			return mcp.NewToolResultError(fmt.Sprintf("invalid topics JSON: %v", err)), nil
		}
		filter["topics"] = t
	}
	var result json.RawMessage
	if err := s.rpcClient.CallContext(ctx, &result, "erigon_getLogs", filter); err != nil {
		return mcp.NewToolResultError(err.Error()), nil
	}
	return mcp.NewToolResultText(toJSONIndent(result)), nil
}

func (s *StandaloneMCPServer) handleErigonGetBlockReceiptsByBlockHash(ctx context.Context, req mcp.CallToolRequest) (*mcp.CallToolResult, error) {
	blockHash := req.GetString("blockHash", "")
	var result json.RawMessage
	if err := s.rpcClient.CallContext(ctx, &result, "erigon_getBlockReceiptsByBlockHash", blockHash); err != nil {
		return mcp.NewToolResultError(err.Error()), nil
	}
	return mcp.NewToolResultText(toJSONIndent(result)), nil
}

func (s *StandaloneMCPServer) handleErigonNodeInfo(ctx context.Context, req mcp.CallToolRequest) (*mcp.CallToolResult, error) {
	var result json.RawMessage
	if err := s.rpcClient.CallContext(ctx, &result, "erigon_nodeInfo"); err != nil {
		return mcp.NewToolResultError(err.Error()), nil
	}
	return mcp.NewToolResultText(toJSONIndent(result)), nil
}

// ===== OTTERSCAN HANDLERS =====

func (s *StandaloneMCPServer) handleOtsGetApiLevel(ctx context.Context, req mcp.CallToolRequest) (*mcp.CallToolResult, error) {
	var result json.RawMessage
	if err := s.rpcClient.CallContext(ctx, &result, "ots_getApiLevel"); err != nil {
		return mcp.NewToolResultError(err.Error()), nil
	}
	return mcp.NewToolResultText(fmt.Sprintf("Otterscan API Level: %s", string(result))), nil
}

func (s *StandaloneMCPServer) handleOtsGetInternalOperations(ctx context.Context, req mcp.CallToolRequest) (*mcp.CallToolResult, error) {
	txHash := req.GetString("txHash", "")
	var result json.RawMessage
	if err := s.rpcClient.CallContext(ctx, &result, "ots_getInternalOperations", txHash); err != nil {
		return mcp.NewToolResultError(err.Error()), nil
	}
	if result == nil || string(result) == "null" || string(result) == "[]" {
		return mcp.NewToolResultText("No internal operations found"), nil
	}
	return mcp.NewToolResultText(toJSONIndent(result)), nil
}

func (s *StandaloneMCPServer) handleOtsSearchTransactionsBefore(ctx context.Context, req mcp.CallToolRequest) (*mcp.CallToolResult, error) {
	address := req.GetString("address", "")
	blockNum := req.GetInt("blockNumber", 0)
	pageSize := req.GetInt("pageSize", 25)
	var result json.RawMessage
	if err := s.rpcClient.CallContext(ctx, &result, "ots_searchTransactionsBefore", address, blockNum, pageSize); err != nil {
		return mcp.NewToolResultError(err.Error()), nil
	}
	return mcp.NewToolResultText(toJSONIndent(result)), nil
}

func (s *StandaloneMCPServer) handleOtsSearchTransactionsAfter(ctx context.Context, req mcp.CallToolRequest) (*mcp.CallToolResult, error) {
	address := req.GetString("address", "")
	blockNum := req.GetInt("blockNumber", 0)
	pageSize := req.GetInt("pageSize", 25)
	var result json.RawMessage
	if err := s.rpcClient.CallContext(ctx, &result, "ots_searchTransactionsAfter", address, blockNum, pageSize); err != nil {
		return mcp.NewToolResultError(err.Error()), nil
	}
	return mcp.NewToolResultText(toJSONIndent(result)), nil
}

func (s *StandaloneMCPServer) handleOtsGetBlockDetails(ctx context.Context, req mcp.CallToolRequest) (*mcp.CallToolResult, error) {
	blockNum := req.GetString("blockNumber", "latest")
	var result json.RawMessage
	if err := s.rpcClient.CallContext(ctx, &result, "ots_getBlockDetails", blockNum); err != nil {
		return mcp.NewToolResultError(err.Error()), nil
	}
	return mcp.NewToolResultText(toJSONIndent(result)), nil
}

func (s *StandaloneMCPServer) handleOtsGetBlockDetailsByHash(ctx context.Context, req mcp.CallToolRequest) (*mcp.CallToolResult, error) {
	blockHash := req.GetString("blockHash", "")
	var result json.RawMessage
	if err := s.rpcClient.CallContext(ctx, &result, "ots_getBlockDetailsByHash", blockHash); err != nil {
		return mcp.NewToolResultError(err.Error()), nil
	}
	return mcp.NewToolResultText(toJSONIndent(result)), nil
}

func (s *StandaloneMCPServer) handleOtsGetBlockTransactions(ctx context.Context, req mcp.CallToolRequest) (*mcp.CallToolResult, error) {
	blockNum := req.GetString("blockNumber", "latest")
	pageNumber := req.GetInt("pageNumber", 0)
	pageSize := req.GetInt("pageSize", 25)
	var result json.RawMessage
	if err := s.rpcClient.CallContext(ctx, &result, "ots_getBlockTransactions", blockNum, pageNumber, pageSize); err != nil {
		return mcp.NewToolResultError(err.Error()), nil
	}
	return mcp.NewToolResultText(toJSONIndent(result)), nil
}

func (s *StandaloneMCPServer) handleOtsHasCode(ctx context.Context, req mcp.CallToolRequest) (*mcp.CallToolResult, error) {
	address := req.GetString("address", "")
	blockNum := req.GetString("blockNumber", "latest")
	var result bool
	if err := s.rpcClient.CallContext(ctx, &result, "ots_hasCode", address, blockNum); err != nil {
		return mcp.NewToolResultError(err.Error()), nil
	}
	if result {
		return mcp.NewToolResultText(fmt.Sprintf("Address %s has code (is a contract)", address)), nil
	}
	return mcp.NewToolResultText(fmt.Sprintf("Address %s has no code (is an EOA)", address)), nil
}

func (s *StandaloneMCPServer) handleOtsTraceTransaction(ctx context.Context, req mcp.CallToolRequest) (*mcp.CallToolResult, error) {
	txHash := req.GetString("txHash", "")
	var result json.RawMessage
	if err := s.rpcClient.CallContext(ctx, &result, "ots_traceTransaction", txHash); err != nil {
		return mcp.NewToolResultError(err.Error()), nil
	}
	if result == nil || string(result) == "null" || string(result) == "[]" {
		return mcp.NewToolResultText("No trace entries found"), nil
	}
	return mcp.NewToolResultText(toJSONIndent(result)), nil
}

func (s *StandaloneMCPServer) handleOtsGetTransactionError(ctx context.Context, req mcp.CallToolRequest) (*mcp.CallToolResult, error) {
	txHash := req.GetString("txHash", "")
	var result string
	if err := s.rpcClient.CallContext(ctx, &result, "ots_getTransactionError", txHash); err != nil {
		return mcp.NewToolResultError(err.Error()), nil
	}
	if result == "" || result == "0x" {
		return mcp.NewToolResultText("Transaction succeeded (no error)"), nil
	}
	return mcp.NewToolResultText(fmt.Sprintf("Transaction error: %s", result)), nil
}

func (s *StandaloneMCPServer) handleOtsGetTransactionBySenderAndNonce(ctx context.Context, req mcp.CallToolRequest) (*mcp.CallToolResult, error) {
	address := req.GetString("address", "")
	nonce := req.GetInt("nonce", 0)
	var result string
	if err := s.rpcClient.CallContext(ctx, &result, "ots_getTransactionBySenderAndNonce", address, nonce); err != nil {
		return mcp.NewToolResultError(err.Error()), nil
	}
	if result == "" {
		return mcp.NewToolResultText("Transaction not found"), nil
	}
	return mcp.NewToolResultText(fmt.Sprintf("Transaction hash: %s", result)), nil
}

func (s *StandaloneMCPServer) handleOtsGetContractCreator(ctx context.Context, req mcp.CallToolRequest) (*mcp.CallToolResult, error) {
	address := req.GetString("address", "")
	var result json.RawMessage
	if err := s.rpcClient.CallContext(ctx, &result, "ots_getContractCreator", address); err != nil {
		return mcp.NewToolResultError(err.Error()), nil
	}
	if result == nil || string(result) == "null" {
		return mcp.NewToolResultText("Contract creator not found"), nil
	}
	return mcp.NewToolResultText(toJSONIndent(result)), nil
}

// ===== METRICS HANDLERS =====
// In standalone mode, metrics are not available since they require in-process
// access to Prometheus registries.

func (s *StandaloneMCPServer) handleMetricsList(ctx context.Context, req mcp.CallToolRequest) (*mcp.CallToolResult, error) {
	return mcp.NewToolResultText("Metrics are not available in standalone mode. Use the embedded MCP server (via Erigon's built-in --mcp flag) for metrics access."), nil
}

func (s *StandaloneMCPServer) handleMetricsGet(ctx context.Context, req mcp.CallToolRequest) (*mcp.CallToolResult, error) {
	return mcp.NewToolResultText("Metrics are not available in standalone mode. Use the embedded MCP server (via Erigon's built-in --mcp flag) for metrics access."), nil
}

// ===== RESOURCES =====

func (s *StandaloneMCPServer) resourceHandlers() resourceHandlerSet {
	return resourceHandlerSet{
		nodeInfo:            s.handleResourceNodeInfo,
		chainConfig:         s.handleResourceChainConfig,
		recentBlocks:        s.handleResourceRecentBlocks,
		networkStatus:       s.handleResourceNetworkStatus,
		gasInfo:             s.handleResourceGasInfo,
		addressSummary:      s.handleResourceAddressSummary,
		blockSummary:        s.handleResourceBlockSummary,
		transactionAnalysis: s.handleResourceTransactionAnalysis,
	}
}

// Resource handlers

func (s *StandaloneMCPServer) handleResourceNodeInfo(ctx context.Context, req mcp.ReadResourceRequest) ([]mcp.ResourceContents, error) {
	var result json.RawMessage
	if err := s.rpcClient.CallContext(ctx, &result, "erigon_nodeInfo"); err != nil {
		return nil, fmt.Errorf("erigon_nodeInfo: %w", err)
	}

	return []mcp.ResourceContents{
		mcp.TextResourceContents{
			URI:      "erigon://node/info",
			MIMEType: "application/json",
			Text:     toJSONIndent(result),
		},
	}, nil
}

func (s *StandaloneMCPServer) handleResourceChainConfig(ctx context.Context, req mcp.ReadResourceRequest) ([]mcp.ResourceContents, error) {
	var blockNum string
	if err := s.rpcClient.CallContext(ctx, &blockNum, "eth_blockNumber"); err != nil {
		return nil, fmt.Errorf("eth_blockNumber: %w", err)
	}

	return []mcp.ResourceContents{
		mcp.TextResourceContents{
			URI:      "erigon://chain/config",
			MIMEType: "application/json",
			Text: toJSONText(map[string]any{
				"current_block": blockNum,
				"note":          "Chain config details would come from Erigon's chain spec",
			}),
		},
	}, nil
}

func (s *StandaloneMCPServer) handleResourceRecentBlocks(ctx context.Context, req mcp.ReadResourceRequest) ([]mcp.ResourceContents, error) {
	var blockNumHex string
	if err := s.rpcClient.CallContext(ctx, &blockNumHex, "eth_blockNumber"); err != nil {
		return nil, fmt.Errorf("eth_blockNumber: %w", err)
	}

	currentBlock := new(big.Int)
	currentBlock.SetString(strings.TrimPrefix(blockNumHex, "0x"), 16)

	const recentBlockCount = 10
	blocks := make([]json.RawMessage, 0, recentBlockCount)
	for i := 0; i < recentBlockCount; i++ {
		blockNum := new(big.Int).Sub(currentBlock, big.NewInt(int64(i)))
		hexNum := fmt.Sprintf("0x%x", blockNum)
		var block json.RawMessage
		if err := s.rpcClient.CallContext(ctx, &block, "eth_getBlockByNumber", hexNum, false); err != nil {
			continue
		}
		if block != nil && string(block) != "null" {
			blocks = append(blocks, block)
		}
	}

	return []mcp.ResourceContents{
		mcp.TextResourceContents{
			URI:      "erigon://blocks/recent",
			MIMEType: "application/json",
			Text:     toJSONText(blocks),
		},
	}, nil
}

func (s *StandaloneMCPServer) handleResourceNetworkStatus(ctx context.Context, req mcp.ReadResourceRequest) ([]mcp.ResourceContents, error) {
	var nodeInfo json.RawMessage
	_ = s.rpcClient.CallContext(ctx, &nodeInfo, "erigon_nodeInfo")

	var blockNum string
	_ = s.rpcClient.CallContext(ctx, &blockNum, "eth_blockNumber")

	// Query actual sync status instead of hardcoding false.
	var syncResult json.RawMessage
	_ = s.rpcClient.CallContext(ctx, &syncResult, "eth_syncing")
	// eth_syncing returns false when fully synced, or a JSON object with progress.
	var syncStatus any = false
	if syncResult != nil && string(syncResult) != "false" {
		syncStatus = syncResult
	}

	status := map[string]any{
		"node_info":     nodeInfo,
		"current_block": blockNum,
		"syncing":       syncStatus,
	}

	return []mcp.ResourceContents{
		mcp.TextResourceContents{
			URI:      "erigon://network/status",
			MIMEType: "application/json",
			Text:     toJSONText(status),
		},
	}, nil
}

func (s *StandaloneMCPServer) handleResourceGasInfo(ctx context.Context, req mcp.ReadResourceRequest) ([]mcp.ResourceContents, error) {
	var gasPriceHex string
	if err := s.rpcClient.CallContext(ctx, &gasPriceHex, "eth_gasPrice"); err != nil {
		return nil, fmt.Errorf("eth_gasPrice: %w", err)
	}

	gasInfo := map[string]any{
		"gas_price": gasPriceHex,
	}

	// Get latest block for base fee
	var block json.RawMessage
	if err := s.rpcClient.CallContext(ctx, &block, "eth_getBlockByNumber", "latest", false); err == nil && block != nil {
		// Extract baseFeePerGas from block JSON
		var blockMap map[string]any
		if json.Unmarshal(block, &blockMap) == nil {
			if baseFee, ok := blockMap["baseFeePerGas"]; ok {
				gasInfo["base_fee"] = baseFee
			}
		}
	}

	return []mcp.ResourceContents{
		mcp.TextResourceContents{
			URI:      "erigon://gas/current",
			MIMEType: "application/json",
			Text:     toJSONText(gasInfo),
		},
	}, nil
}

// Resource template handlers

func (s *StandaloneMCPServer) handleResourceAddressSummary(ctx context.Context, req mcp.ReadResourceRequest) ([]mcp.ResourceContents, error) {
	address := extractURIParam(req.Params.URI, "erigon://address/", "/summary")
	if address == "" {
		return nil, fmt.Errorf("missing address parameter in URI: %s", req.Params.URI)
	}

	var balance string
	_ = s.rpcClient.CallContext(ctx, &balance, "eth_getBalance", address, "latest")

	var nonce string
	_ = s.rpcClient.CallContext(ctx, &nonce, "eth_getTransactionCount", address, "latest")

	var code string
	_ = s.rpcClient.CallContext(ctx, &code, "eth_getCode", address, "latest")

	summary := map[string]any{
		"address":     address,
		"balance":     balance,
		"nonce":       nonce,
		"is_contract": code != "" && code != "0x",
	}

	return []mcp.ResourceContents{
		mcp.TextResourceContents{
			URI:      fmt.Sprintf("erigon://address/%s/summary", address),
			MIMEType: "application/json",
			Text:     toJSONText(summary),
		},
	}, nil
}

func (s *StandaloneMCPServer) handleResourceBlockSummary(ctx context.Context, req mcp.ReadResourceRequest) ([]mcp.ResourceContents, error) {
	blockNumStr := extractURIParam(req.Params.URI, "erigon://block/", "/summary")
	if blockNumStr == "" {
		return nil, fmt.Errorf("missing block number parameter in URI: %s", req.Params.URI)
	}

	var block json.RawMessage
	if err := s.rpcClient.CallContext(ctx, &block, "eth_getBlockByNumber", blockNumStr, false); err != nil {
		return nil, fmt.Errorf("eth_getBlockByNumber: %w", err)
	}

	return []mcp.ResourceContents{
		mcp.TextResourceContents{
			URI:      fmt.Sprintf("erigon://block/%s/summary", blockNumStr),
			MIMEType: "application/json",
			Text:     toJSONIndent(block),
		},
	}, nil
}

func (s *StandaloneMCPServer) handleResourceTransactionAnalysis(ctx context.Context, req mcp.ReadResourceRequest) ([]mcp.ResourceContents, error) {
	txHash := extractURIParam(req.Params.URI, "erigon://transaction/", "/analysis")
	if txHash == "" {
		return nil, fmt.Errorf("missing transaction hash parameter in URI: %s", req.Params.URI)
	}

	var tx json.RawMessage
	if err := s.rpcClient.CallContext(ctx, &tx, "eth_getTransactionByHash", txHash); err != nil {
		return nil, fmt.Errorf("eth_getTransactionByHash: %w", err)
	}

	var receipt json.RawMessage
	if err := s.rpcClient.CallContext(ctx, &receipt, "eth_getTransactionReceipt", txHash); err != nil {
		return nil, fmt.Errorf("eth_getTransactionReceipt: %w", err)
	}

	// Derive status from receipt's "status" field (0x1 = success, 0x0 = reverted).
	status := "unknown"
	if receipt != nil {
		var receiptMap map[string]any
		if json.Unmarshal(receipt, &receiptMap) == nil {
			if s, ok := receiptMap["status"].(string); ok {
				switch s {
				case "0x1":
					status = "success"
				case "0x0":
					status = "reverted"
				default:
					status = s
				}
			}
		}
	}

	analysis := map[string]any{
		"transaction": tx,
		"receipt":     receipt,
		"status":      status,
	}

	return []mcp.ResourceContents{
		mcp.TextResourceContents{
			URI:      fmt.Sprintf("erigon://transaction/%s/analysis", txHash),
			MIMEType: "application/json",
			Text:     toJSONText(analysis),
		},
	}, nil
}

// ===== TRANSPORT =====

// Serve starts the MCP server in stdio mode with its own signal handling.
func (s *StandaloneMCPServer) Serve() error {
	return server.ServeStdio(s.mcpServer)
}

// ServeContext starts the MCP server in stdio mode using the provided context
// for lifecycle management, avoiding duplicate signal handlers.
func (s *StandaloneMCPServer) ServeContext(ctx context.Context) error {
	stdio := server.NewStdioServer(s.mcpServer)
	return stdio.Listen(ctx, os.Stdin, os.Stdout)
}

// ServeSSE starts the MCP server with SSE transport on the given address,
// shutting it down when ctx is cancelled.
func (s *StandaloneMCPServer) ServeSSE(ctx context.Context, addr string) error {
	return serveSSE(ctx, server.NewSSEServer(s.mcpServer), addr)
}
