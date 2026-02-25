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
	"strings"

	"github.com/mark3labs/mcp-go/mcp"
	"github.com/mark3labs/mcp-go/server"

	"github.com/erigontech/erigon/common/log/v3"
	"github.com/erigontech/erigon/rpc"
)

// StandaloneMCPServer is a standalone MCP server that proxies MCP tool calls
// to a running Erigon node via JSON-RPC HTTP. Unlike ErigonMCPServer which
// uses in-process Go API interfaces, this server makes raw JSON-RPC calls
// via rpc.Client and can run as a separate process.
type StandaloneMCPServer struct {
	rpcClient *rpc.Client
	mcpServer *server.MCPServer
	logDir    string
}

// NewStandaloneMCPServer creates a new standalone MCP server that proxies
// tool calls to a running Erigon node via JSON-RPC.
func NewStandaloneMCPServer(rpcClient *rpc.Client, logDir string) *StandaloneMCPServer {
	s := &StandaloneMCPServer{
		rpcClient: rpcClient,
		logDir:    logDir,
	}

	s.mcpServer = server.NewMCPServer(
		"ErigonMCP",
		"0.0.1",
		server.WithResourceCapabilities(true, true),
		server.WithToolCapabilities(true),
		server.WithPromptCapabilities(true),
		server.WithLogging(),
		server.WithRecovery(),
	)

	s.registerTools()
	s.registerPrompts()
	s.registerResources()

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

// extractURIParam extracts a path parameter from an MCP resource template URI.
// For example, given URI "erigon://address/0xABC/summary" and template prefix
// "erigon://address/" with suffix "/summary", it returns "0xABC".
func extractURIParam(uri, prefix, suffix string) string {
	s := strings.TrimPrefix(uri, prefix)
	if suffix != "" {
		s = strings.TrimSuffix(s, suffix)
	}
	return s
}

// registerTools registers all MCP tools matching the embedded server.
func (s *StandaloneMCPServer) registerTools() {
	// ===== ETH TOOLS =====

	s.mcpServer.AddTool(mcp.NewTool("eth_blockNumber",
		mcp.WithDescription("Get the current block number"),
	), s.handleBlockNumber)

	s.mcpServer.AddTool(mcp.NewTool("eth_getBlockByNumber",
		mcp.WithDescription("Get block by number"),
		mcp.WithString("blockNumber", mcp.Required(), mcp.Description("Block number or tag")),
		mcp.WithBoolean("fullTransactions", mcp.Description("Return full tx objects")),
	), s.handleGetBlockByNumber)

	s.mcpServer.AddTool(mcp.NewTool("eth_getBlockByHash",
		mcp.WithDescription("Get block by hash"),
		mcp.WithString("blockHash", mcp.Required(), mcp.Description("Block hash")),
		mcp.WithBoolean("fullTransactions", mcp.Description("Return full tx objects")),
	), s.handleGetBlockByHash)

	s.mcpServer.AddTool(mcp.NewTool("eth_getBlockTransactionCountByNumber",
		mcp.WithDescription("Get transaction count in block by number"),
		mcp.WithString("blockNumber", mcp.Required(), mcp.Description("Block number")),
	), s.handleGetBlockTransactionCountByNumber)

	s.mcpServer.AddTool(mcp.NewTool("eth_getBlockTransactionCountByHash",
		mcp.WithDescription("Get transaction count in block by hash"),
		mcp.WithString("blockHash", mcp.Required(), mcp.Description("Block hash")),
	), s.handleGetBlockTransactionCountByHash)

	s.mcpServer.AddTool(mcp.NewTool("eth_getBalance",
		mcp.WithDescription("Get address balance"),
		mcp.WithString("address", mcp.Required(), mcp.Description("Address")),
		mcp.WithString("blockNumber", mcp.Description("Block number (default: latest)")),
	), s.handleGetBalance)

	s.mcpServer.AddTool(mcp.NewTool("eth_getTransactionByHash",
		mcp.WithDescription("Get transaction by hash"),
		mcp.WithString("txHash", mcp.Required(), mcp.Description("Transaction hash")),
	), s.handleGetTransactionByHash)

	s.mcpServer.AddTool(mcp.NewTool("eth_getTransactionByBlockHashAndIndex",
		mcp.WithDescription("Get transaction by block hash and index"),
		mcp.WithString("blockHash", mcp.Required(), mcp.Description("Block hash")),
		mcp.WithNumber("index", mcp.Required(), mcp.Description("Transaction index")),
	), s.handleGetTransactionByBlockHashAndIndex)

	s.mcpServer.AddTool(mcp.NewTool("eth_getTransactionByBlockNumberAndIndex",
		mcp.WithDescription("Get transaction by block number and index"),
		mcp.WithString("blockNumber", mcp.Required(), mcp.Description("Block number")),
		mcp.WithNumber("index", mcp.Required(), mcp.Description("Transaction index")),
	), s.handleGetTransactionByBlockNumberAndIndex)

	s.mcpServer.AddTool(mcp.NewTool("eth_getTransactionReceipt",
		mcp.WithDescription("Get transaction receipt"),
		mcp.WithString("txHash", mcp.Required(), mcp.Description("Transaction hash")),
	), s.handleGetTransactionReceipt)

	s.mcpServer.AddTool(mcp.NewTool("eth_getBlockReceipts",
		mcp.WithDescription("Get all receipts for a block"),
		mcp.WithString("blockNumberOrHash", mcp.Required(), mcp.Description("Block number or hash")),
	), s.handleGetBlockReceipts)

	s.mcpServer.AddTool(mcp.NewTool("eth_getLogs",
		mcp.WithDescription("Get logs matching filter"),
		mcp.WithString("fromBlock", mcp.Description("Start block")),
		mcp.WithString("toBlock", mcp.Description("End block")),
		mcp.WithString("address", mcp.Description("Contract address(es)")),
		mcp.WithString("topics", mcp.Description("Topics array (JSON)")),
		mcp.WithString("blockHash", mcp.Description("Single block hash")),
	), s.handleGetLogs)

	s.mcpServer.AddTool(mcp.NewTool("eth_getCode",
		mcp.WithDescription("Get contract code"),
		mcp.WithString("address", mcp.Required(), mcp.Description("Contract address")),
		mcp.WithString("blockNumber", mcp.Description("Block number")),
	), s.handleGetCode)

	s.mcpServer.AddTool(mcp.NewTool("eth_getStorageAt",
		mcp.WithDescription("Get storage at position"),
		mcp.WithString("address", mcp.Required(), mcp.Description("Address")),
		mcp.WithString("position", mcp.Required(), mcp.Description("Storage position")),
		mcp.WithString("blockNumber", mcp.Description("Block number")),
	), s.handleGetStorageAt)

	s.mcpServer.AddTool(mcp.NewTool("eth_getTransactionCount",
		mcp.WithDescription("Get nonce (transaction count)"),
		mcp.WithString("address", mcp.Required(), mcp.Description("Address")),
		mcp.WithString("blockNumber", mcp.Description("Block number")),
	), s.handleGetTransactionCount)

	s.mcpServer.AddTool(mcp.NewTool("eth_call",
		mcp.WithDescription("Execute call without transaction"),
		mcp.WithString("to", mcp.Required(), mcp.Description("Contract address")),
		mcp.WithString("data", mcp.Required(), mcp.Description("Call data")),
		mcp.WithString("from", mcp.Description("Sender address")),
		mcp.WithString("value", mcp.Description("Value (hex)")),
		mcp.WithString("gas", mcp.Description("Gas limit (hex)")),
		mcp.WithString("blockNumber", mcp.Description("Block number")),
	), s.handleCall)

	s.mcpServer.AddTool(mcp.NewTool("eth_estimateGas",
		mcp.WithDescription("Estimate gas for transaction"),
		mcp.WithString("to", mcp.Description("To address")),
		mcp.WithString("data", mcp.Description("Call data")),
		mcp.WithString("from", mcp.Description("From address")),
		mcp.WithString("value", mcp.Description("Value (hex)")),
	), s.handleEstimateGas)

	s.mcpServer.AddTool(mcp.NewTool("eth_gasPrice",
		mcp.WithDescription("Get current gas price"),
	), s.handleGasPrice)

	s.mcpServer.AddTool(mcp.NewTool("eth_chainId",
		mcp.WithDescription("Get chain ID"),
	), s.handleChainId)

	s.mcpServer.AddTool(mcp.NewTool("eth_syncing",
		mcp.WithDescription("Get sync status"),
	), s.handleSyncing)

	s.mcpServer.AddTool(mcp.NewTool("eth_accounts",
		mcp.WithDescription("Get accounts"),
	), s.handleAccounts)

	s.mcpServer.AddTool(mcp.NewTool("eth_coinbase",
		mcp.WithDescription("Get coinbase address"),
	), s.handleCoinbase)

	s.mcpServer.AddTool(mcp.NewTool("eth_mining",
		mcp.WithDescription("Check if mining"),
	), s.handleMining)

	s.mcpServer.AddTool(mcp.NewTool("eth_hashrate",
		mcp.WithDescription("Get hashrate"),
	), s.handleHashrate)

	s.mcpServer.AddTool(mcp.NewTool("eth_protocolVersion",
		mcp.WithDescription("Get protocol version"),
	), s.handleProtocolVersion)

	s.mcpServer.AddTool(mcp.NewTool("eth_getUncleByBlockNumberAndIndex",
		mcp.WithDescription("Get uncle by block number and index"),
		mcp.WithString("blockNumber", mcp.Required(), mcp.Description("Block number")),
		mcp.WithNumber("index", mcp.Required(), mcp.Description("Uncle index")),
	), s.handleGetUncleByBlockNumberAndIndex)

	s.mcpServer.AddTool(mcp.NewTool("eth_getUncleByBlockHashAndIndex",
		mcp.WithDescription("Get uncle by block hash and index"),
		mcp.WithString("blockHash", mcp.Required(), mcp.Description("Block hash")),
		mcp.WithNumber("index", mcp.Required(), mcp.Description("Uncle index")),
	), s.handleGetUncleByBlockHashAndIndex)

	s.mcpServer.AddTool(mcp.NewTool("eth_getUncleCountByBlockNumber",
		mcp.WithDescription("Get uncle count by block number"),
		mcp.WithString("blockNumber", mcp.Required(), mcp.Description("Block number")),
	), s.handleGetUncleCountByBlockNumber)

	s.mcpServer.AddTool(mcp.NewTool("eth_getUncleCountByBlockHash",
		mcp.WithDescription("Get uncle count by block hash"),
		mcp.WithString("blockHash", mcp.Required(), mcp.Description("Block hash")),
	), s.handleGetUncleCountByBlockHash)

	s.mcpServer.AddTool(mcp.NewTool("eth_getProof",
		mcp.WithDescription("Get Merkle proof"),
		mcp.WithString("address", mcp.Required(), mcp.Description("Address")),
		mcp.WithString("storageKeys", mcp.Description("Storage keys (JSON array)")),
		mcp.WithString("blockNumber", mcp.Description("Block number")),
	), s.handleGetProof)

	// ===== ERIGON TOOLS =====

	s.mcpServer.AddTool(mcp.NewTool("erigon_forks",
		mcp.WithDescription("Get fork information"),
	), s.handleErigonForks)

	s.mcpServer.AddTool(mcp.NewTool("erigon_blockNumber",
		mcp.WithDescription("Get block number (Erigon)"),
		mcp.WithString("blockNumber", mcp.Description("Block tag")),
	), s.handleErigonBlockNumber)

	s.mcpServer.AddTool(mcp.NewTool("erigon_getHeaderByNumber",
		mcp.WithDescription("Get header by number"),
		mcp.WithString("blockNumber", mcp.Required(), mcp.Description("Block number")),
	), s.handleErigonGetHeaderByNumber)

	s.mcpServer.AddTool(mcp.NewTool("erigon_getHeaderByHash",
		mcp.WithDescription("Get header by hash"),
		mcp.WithString("blockHash", mcp.Required(), mcp.Description("Block hash")),
	), s.handleErigonGetHeaderByHash)

	s.mcpServer.AddTool(mcp.NewTool("erigon_getBlockByTimestamp",
		mcp.WithDescription("Get block by timestamp"),
		mcp.WithString("timestamp", mcp.Required(), mcp.Description("Unix timestamp")),
		mcp.WithBoolean("fullTransactions", mcp.Description("Full tx objects")),
	), s.handleErigonGetBlockByTimestamp)

	s.mcpServer.AddTool(mcp.NewTool("erigon_getBalanceChangesInBlock",
		mcp.WithDescription("Get all balance changes in block"),
		mcp.WithString("blockNumberOrHash", mcp.Required(), mcp.Description("Block")),
	), s.handleErigonGetBalanceChangesInBlock)

	s.mcpServer.AddTool(mcp.NewTool("erigon_getLogsByHash",
		mcp.WithDescription("Get logs by block hash"),
		mcp.WithString("blockHash", mcp.Required(), mcp.Description("Block hash")),
	), s.handleErigonGetLogsByHash)

	s.mcpServer.AddTool(mcp.NewTool("erigon_getLogs",
		mcp.WithDescription("Get logs (Erigon format)"),
		mcp.WithString("fromBlock", mcp.Description("From block")),
		mcp.WithString("toBlock", mcp.Description("To block")),
		mcp.WithString("address", mcp.Description("Address")),
		mcp.WithString("topics", mcp.Description("Topics (JSON)")),
	), s.handleErigonGetLogs)

	s.mcpServer.AddTool(mcp.NewTool("erigon_getBlockReceiptsByBlockHash",
		mcp.WithDescription("Get block receipts by hash"),
		mcp.WithString("blockHash", mcp.Required(), mcp.Description("Block hash")),
	), s.handleErigonGetBlockReceiptsByBlockHash)

	s.mcpServer.AddTool(mcp.NewTool("erigon_nodeInfo",
		mcp.WithDescription("Get P2P node info"),
	), s.handleErigonNodeInfo)

	// ===== OTTERSCAN TOOLS =====

	s.mcpServer.AddTool(mcp.NewTool("ots_getApiLevel",
		mcp.WithDescription("Get Otterscan API level"),
	), s.handleOtsGetApiLevel)

	s.mcpServer.AddTool(mcp.NewTool("ots_getInternalOperations",
		mcp.WithDescription("Get internal operations (internal txs) for a transaction"),
		mcp.WithString("txHash", mcp.Required(), mcp.Description("Transaction hash")),
	), s.handleOtsGetInternalOperations)

	s.mcpServer.AddTool(mcp.NewTool("ots_searchTransactionsBefore",
		mcp.WithDescription("Search transactions before a given block for an address"),
		mcp.WithString("address", mcp.Required(), mcp.Description("Address")),
		mcp.WithNumber("blockNumber", mcp.Required(), mcp.Description("Block number")),
		mcp.WithNumber("pageSize", mcp.Description("Page size (default: 25)")),
	), s.handleOtsSearchTransactionsBefore)

	s.mcpServer.AddTool(mcp.NewTool("ots_searchTransactionsAfter",
		mcp.WithDescription("Search transactions after a given block for an address"),
		mcp.WithString("address", mcp.Required(), mcp.Description("Address")),
		mcp.WithNumber("blockNumber", mcp.Required(), mcp.Description("Block number")),
		mcp.WithNumber("pageSize", mcp.Description("Page size (default: 25)")),
	), s.handleOtsSearchTransactionsAfter)

	s.mcpServer.AddTool(mcp.NewTool("ots_getBlockDetails",
		mcp.WithDescription("Get detailed block information"),
		mcp.WithString("blockNumber", mcp.Required(), mcp.Description("Block number or tag")),
	), s.handleOtsGetBlockDetails)

	s.mcpServer.AddTool(mcp.NewTool("ots_getBlockDetailsByHash",
		mcp.WithDescription("Get detailed block information by hash"),
		mcp.WithString("blockHash", mcp.Required(), mcp.Description("Block hash")),
	), s.handleOtsGetBlockDetailsByHash)

	s.mcpServer.AddTool(mcp.NewTool("ots_getBlockTransactions",
		mcp.WithDescription("Get paginated transactions for a block"),
		mcp.WithString("blockNumber", mcp.Required(), mcp.Description("Block number or tag")),
		mcp.WithNumber("pageNumber", mcp.Description("Page number (default: 0)")),
		mcp.WithNumber("pageSize", mcp.Description("Page size (default: 25)")),
	), s.handleOtsGetBlockTransactions)

	s.mcpServer.AddTool(mcp.NewTool("ots_hasCode",
		mcp.WithDescription("Check if an address has code (is a contract)"),
		mcp.WithString("address", mcp.Required(), mcp.Description("Address")),
		mcp.WithString("blockNumber", mcp.Description("Block number or tag")),
	), s.handleOtsHasCode)

	s.mcpServer.AddTool(mcp.NewTool("ots_traceTransaction",
		mcp.WithDescription("Get trace for a transaction"),
		mcp.WithString("txHash", mcp.Required(), mcp.Description("Transaction hash")),
	), s.handleOtsTraceTransaction)

	s.mcpServer.AddTool(mcp.NewTool("ots_getTransactionError",
		mcp.WithDescription("Get transaction error/revert reason"),
		mcp.WithString("txHash", mcp.Required(), mcp.Description("Transaction hash")),
	), s.handleOtsGetTransactionError)

	s.mcpServer.AddTool(mcp.NewTool("ots_getTransactionBySenderAndNonce",
		mcp.WithDescription("Get transaction hash by sender address and nonce"),
		mcp.WithString("address", mcp.Required(), mcp.Description("Sender address")),
		mcp.WithNumber("nonce", mcp.Required(), mcp.Description("Nonce")),
	), s.handleOtsGetTransactionBySenderAndNonce)

	s.mcpServer.AddTool(mcp.NewTool("ots_getContractCreator",
		mcp.WithDescription("Get contract creator address and transaction"),
		mcp.WithString("address", mcp.Required(), mcp.Description("Contract address")),
	), s.handleOtsGetContractCreator)

	// ===== METRICS TOOLS =====

	s.mcpServer.AddTool(mcp.NewTool("metrics_list",
		mcp.WithDescription("List all available metric names"),
	), s.handleMetricsList)

	s.mcpServer.AddTool(mcp.NewTool("metrics_get",
		mcp.WithDescription("Get metrics with optional filtering by pattern (supports wildcards like 'db_*', '*_size', etc.)"),
		mcp.WithString("pattern", mcp.Description("Metric name pattern (optional, empty = all metrics)")),
	), s.handleMetricsGet)

	// ===== LOG TOOLS =====

	s.mcpServer.AddTool(mcp.NewTool("logs_tail",
		mcp.WithDescription("Get last N lines from erigon or torrent logs"),
		mcp.WithString("log_type", mcp.Description("Log type: 'erigon' or 'torrent' (default: erigon)")),
		mcp.WithNumber("lines", mcp.Description("Number of lines to retrieve (default: 100, max: 10000)")),
		mcp.WithString("filter", mcp.Description("Optional string to filter log lines")),
	), s.handleLogsTail)

	s.mcpServer.AddTool(mcp.NewTool("logs_head",
		mcp.WithDescription("Get first N lines from erigon or torrent logs"),
		mcp.WithString("log_type", mcp.Description("Log type: 'erigon' or 'torrent' (default: erigon)")),
		mcp.WithNumber("lines", mcp.Description("Number of lines to retrieve (default: 100, max: 10000)")),
		mcp.WithString("filter", mcp.Description("Optional string to filter log lines")),
	), s.handleLogsHead)

	s.mcpServer.AddTool(mcp.NewTool("logs_grep",
		mcp.WithDescription("Search for a pattern in erigon or torrent logs"),
		mcp.WithString("log_type", mcp.Description("Log type: 'erigon' or 'torrent' (default: erigon)")),
		mcp.WithString("pattern", mcp.Required(), mcp.Description("Search pattern")),
		mcp.WithNumber("max_lines", mcp.Description("Maximum matching lines to return (default: 1000, max: 10000)")),
		mcp.WithBoolean("case_insensitive", mcp.Description("Case-insensitive search (default: false)")),
	), s.handleLogsGrep)

	s.mcpServer.AddTool(mcp.NewTool("logs_stats",
		mcp.WithDescription("Get statistics about erigon or torrent logs"),
		mcp.WithString("log_type", mcp.Description("Log type: 'erigon' or 'torrent' (default: erigon)")),
	), s.handleLogsStats)
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

// ===== LOG HANDLERS =====
// Log handlers reuse the package-level functions from handlers_logs.go since
// they read files directly and do not need RPC.

func (s *StandaloneMCPServer) handleLogsTail(ctx context.Context, req mcp.CallToolRequest) (*mcp.CallToolResult, error) {
	logType := req.GetString("log_type", "erigon")
	lines := req.GetInt("lines", 100)
	filter := req.GetString("filter", "")

	if lines <= 0 || lines > 10000 {
		return mcp.NewToolResultError("lines must be between 1 and 10000"), nil
	}

	logFile, err := s.resolveLogFile(logType)
	if err != nil {
		return mcp.NewToolResultError(err.Error()), nil
	}

	logLines, err := readLogTail(logFile, lines, filter)
	if err != nil {
		return mcp.NewToolResultError(fmt.Sprintf("Failed to read log: %v", err)), nil
	}

	result := fmt.Sprintf("Last %d lines from %s.log", len(logLines), logType)
	if filter != "" {
		result += fmt.Sprintf(" (filtered by: %s)", filter)
	}
	result += ":\n\n" + strings.Join(logLines, "\n")

	return mcp.NewToolResultText(result), nil
}

func (s *StandaloneMCPServer) handleLogsHead(ctx context.Context, req mcp.CallToolRequest) (*mcp.CallToolResult, error) {
	logType := req.GetString("log_type", "erigon")
	lines := req.GetInt("lines", 100)
	filter := req.GetString("filter", "")

	if lines <= 0 || lines > 10000 {
		return mcp.NewToolResultError("lines must be between 1 and 10000"), nil
	}

	logFile, err := s.resolveLogFile(logType)
	if err != nil {
		return mcp.NewToolResultError(err.Error()), nil
	}

	logLines, err := readLogHead(logFile, lines, filter)
	if err != nil {
		return mcp.NewToolResultError(fmt.Sprintf("Failed to read log: %v", err)), nil
	}

	result := fmt.Sprintf("First %d lines from %s.log", len(logLines), logType)
	if filter != "" {
		result += fmt.Sprintf(" (filtered by: %s)", filter)
	}
	result += ":\n\n" + strings.Join(logLines, "\n")

	return mcp.NewToolResultText(result), nil
}

func (s *StandaloneMCPServer) handleLogsGrep(ctx context.Context, req mcp.CallToolRequest) (*mcp.CallToolResult, error) {
	logType := req.GetString("log_type", "erigon")
	pattern := req.GetString("pattern", "")
	maxLines := req.GetInt("max_lines", 1000)
	caseInsensitive := req.GetBool("case_insensitive", false)

	if pattern == "" {
		return mcp.NewToolResultError("pattern is required"), nil
	}

	if maxLines <= 0 || maxLines > 10000 {
		return mcp.NewToolResultError("max_lines must be between 1 and 10000"), nil
	}

	logFile, err := s.resolveLogFile(logType)
	if err != nil {
		return mcp.NewToolResultError(err.Error()), nil
	}

	logLines, err := grepLog(logFile, pattern, maxLines, caseInsensitive)
	if err != nil {
		return mcp.NewToolResultError(fmt.Sprintf("Failed to grep log: %v", err)), nil
	}

	result := fmt.Sprintf("Found %d matching lines in %s.log for pattern '%s':\n\n", len(logLines), logType, pattern)
	result += strings.Join(logLines, "\n")

	return mcp.NewToolResultText(result), nil
}

func (s *StandaloneMCPServer) handleLogsStats(ctx context.Context, req mcp.CallToolRequest) (*mcp.CallToolResult, error) {
	logType := req.GetString("log_type", "erigon")

	logFile, err := s.resolveLogFile(logType)
	if err != nil {
		return mcp.NewToolResultError(err.Error()), nil
	}

	stats, err := getLogStats(logFile)
	if err != nil {
		return mcp.NewToolResultError(fmt.Sprintf("Failed to get log stats: %v", err)), nil
	}

	return mcp.NewToolResultText(toJSONText(stats)), nil
}

// resolveLogFile returns the path to the log file for the given log type.
func (s *StandaloneMCPServer) resolveLogFile(logType string) (string, error) {
	if s.logDir == "" {
		return "", fmt.Errorf("log directory not configured (use --log.dir or --datadir)")
	}
	switch logType {
	case "erigon":
		return s.logDir + "/erigon.log", nil
	case "torrent":
		return s.logDir + "/torrent.log", nil
	default:
		return "", fmt.Errorf("log_type must be 'erigon' or 'torrent'")
	}
}

// ===== PROMPTS =====

// registerPrompts registers all MCP prompts (identical to the embedded server).
func (s *StandaloneMCPServer) registerPrompts() {
	s.mcpServer.AddPrompt(mcp.NewPrompt("analyze_transaction",
		mcp.WithPromptDescription("Analyze a transaction"),
		mcp.WithArgument("txHash", mcp.ArgumentDescription("Transaction hash"), mcp.RequiredArgument())),
		func(ctx context.Context, req mcp.GetPromptRequest) (*mcp.GetPromptResult, error) {
			return &mcp.GetPromptResult{
				Description: "Analyze transaction",
				Messages: []mcp.PromptMessage{{
					Role: mcp.RoleUser,
					Content: mcp.TextContent{
						Type: "text",
						Text: fmt.Sprintf("Analyze transaction %s using eth_getTransactionByHash and eth_getTransactionReceipt", req.Params.Arguments["txHash"]),
					},
				}},
			}, nil
		})

	s.mcpServer.AddPrompt(mcp.NewPrompt("investigate_address",
		mcp.WithPromptDescription("Investigate an address"),
		mcp.WithArgument("address", mcp.ArgumentDescription("Address"), mcp.RequiredArgument())),
		func(ctx context.Context, req mcp.GetPromptRequest) (*mcp.GetPromptResult, error) {
			return &mcp.GetPromptResult{
				Description: "Investigate address",
				Messages: []mcp.PromptMessage{{
					Role: mcp.RoleUser,
					Content: mcp.TextContent{
						Type: "text",
						Text: fmt.Sprintf("Investigate %s using eth_getBalance, eth_getTransactionCount, eth_getCode", req.Params.Arguments["address"]),
					},
				}},
			}, nil
		})

	s.mcpServer.AddPrompt(mcp.NewPrompt("analyze_block",
		mcp.WithPromptDescription("Analyze a block"),
		mcp.WithArgument("blockNumber", mcp.ArgumentDescription("Block number"), mcp.RequiredArgument())),
		func(ctx context.Context, req mcp.GetPromptRequest) (*mcp.GetPromptResult, error) {
			return &mcp.GetPromptResult{
				Description: "Analyze block",
				Messages: []mcp.PromptMessage{{
					Role: mcp.RoleUser,
					Content: mcp.TextContent{
						Type: "text",
						Text: fmt.Sprintf("Analyze block %s using eth_getBlockByNumber with fullTransactions=true", req.Params.Arguments["blockNumber"]),
					},
				}},
			}, nil
		})

	s.mcpServer.AddPrompt(mcp.NewPrompt("gas_analysis",
		mcp.WithPromptDescription("Analyze gas prices")),
		func(ctx context.Context, req mcp.GetPromptRequest) (*mcp.GetPromptResult, error) {
			return &mcp.GetPromptResult{
				Description: "Gas analysis",
				Messages: []mcp.PromptMessage{{
					Role: mcp.RoleUser,
					Content: mcp.TextContent{
						Type: "text",
						Text: "Analyze gas using eth_gasPrice and latest block's baseFeePerGas",
					},
				}},
			}, nil
		})

	s.mcpServer.AddPrompt(mcp.NewPrompt("debug_logs",
		mcp.WithPromptDescription("Debug issues using Erigon logs"),
		mcp.WithArgument("issue", mcp.ArgumentDescription("Issue description (optional)"))),
		func(ctx context.Context, req mcp.GetPromptRequest) (*mcp.GetPromptResult, error) {
			issue := ""
			if req.Params.Arguments != nil {
				if val, ok := req.Params.Arguments["issue"]; ok {
					issue = val
				}
			}

			text := "Debug Erigon issues by:\n"
			text += "1. Check recent errors: Use logs_grep with pattern='error' or 'ERROR'\n"
			text += "2. Check warnings: Use logs_grep with pattern='warn' or 'WARN'\n"
			text += "3. Get log statistics: Use logs_stats to see error/warning counts\n"
			text += "4. Review recent activity: Use logs_tail to see latest log entries\n"

			if issue != "" {
				text += fmt.Sprintf("\nFocus on investigating: %s", issue)
			}

			return &mcp.GetPromptResult{
				Description: "Debug logs",
				Messages: []mcp.PromptMessage{{
					Role: mcp.RoleUser,
					Content: mcp.TextContent{
						Type: "text",
						Text: text,
					},
				}},
			}, nil
		})

	s.mcpServer.AddPrompt(mcp.NewPrompt("torrent_status",
		mcp.WithPromptDescription("Check torrent/snapshot download status")),
		func(ctx context.Context, req mcp.GetPromptRequest) (*mcp.GetPromptResult, error) {
			return &mcp.GetPromptResult{
				Description: "Torrent status",
				Messages: []mcp.PromptMessage{{
					Role: mcp.RoleUser,
					Content: mcp.TextContent{
						Type: "text",
						Text: "Check torrent download status by:\n" +
							"1. Review torrent logs: Use logs_tail with log_type='torrent'\n" +
							"2. Search for download progress: Use logs_grep with pattern='download' or 'progress'\n" +
							"3. Check for errors: Use logs_grep with pattern='error' on torrent log\n" +
							"4. Get torrent log stats: Use logs_stats with log_type='torrent'",
					},
				}},
			}, nil
		})

	s.mcpServer.AddPrompt(mcp.NewPrompt("sync_analysis",
		mcp.WithPromptDescription("Analyze Erigon sync status and performance")),
		func(ctx context.Context, req mcp.GetPromptRequest) (*mcp.GetPromptResult, error) {
			return &mcp.GetPromptResult{
				Description: "Sync analysis",
				Messages: []mcp.PromptMessage{{
					Role: mcp.RoleUser,
					Content: mcp.TextContent{
						Type: "text",
						Text: "Analyze Erigon sync by:\n" +
							"1. Get current block: Use eth_blockNumber\n" +
							"2. Check sync progress in logs: Use logs_grep with pattern='stage' or 'progress'\n" +
							"3. Look for performance metrics: Use logs_grep with pattern='block/s' or 'tx/s'\n" +
							"4. Check for sync issues: Use logs_grep with pattern='reorg' or 'fork'\n" +
							"5. Review recent sync activity: Use logs_tail with filter='sync' or 'stage'",
					},
				}},
			}, nil
		})
}

// ===== RESOURCES =====

// registerResources registers all MCP resources using JSON-RPC calls.
func (s *StandaloneMCPServer) registerResources() {
	// Static resources
	s.mcpServer.AddResource(
		mcp.NewResource("erigon://node/info",
			"node info",
			mcp.WithResourceDescription("Get node information and capabilities"),
			mcp.WithMIMEType("application/json"),
		),
		s.handleResourceNodeInfo,
	)

	s.mcpServer.AddResource(
		mcp.NewResource("erigon://chain/config",
			"chain config",
			mcp.WithResourceDescription("Get chain configuration"),
			mcp.WithMIMEType("application/json"),
		),
		s.handleResourceChainConfig,
	)

	s.mcpServer.AddResource(
		mcp.NewResource("erigon://blocks/recent",
			"recent blocks",
			mcp.WithResourceDescription("Get recent blocks (default: last 10)"),
			mcp.WithMIMEType("application/json"),
		),
		s.handleResourceRecentBlocks,
	)

	s.mcpServer.AddResource(
		mcp.NewResource("erigon://network/status",
			"network status",
			mcp.WithResourceDescription("Get network sync status and peer info"),
			mcp.WithMIMEType("application/json"),
		),
		s.handleResourceNetworkStatus,
	)

	s.mcpServer.AddResource(
		mcp.NewResource("erigon://gas/current",
			"gas current",
			mcp.WithResourceDescription("Get current gas price information"),
			mcp.WithMIMEType("application/json"),
		),
		s.handleResourceGasInfo,
	)

	// Resource templates
	s.mcpServer.AddResourceTemplate(
		mcp.NewResourceTemplate("erigon://address/{address}/summary",
			"address summary",
			mcp.WithTemplateDescription("Get address summary (balance, nonce, code)"),
			mcp.WithTemplateMIMEType("application/json"),
		),
		s.handleResourceAddressSummary,
	)

	s.mcpServer.AddResourceTemplate(
		mcp.NewResourceTemplate("erigon://block/{number}/summary",
			"block summary",
			mcp.WithTemplateDescription("Get block summary"),
			mcp.WithTemplateMIMEType("application/json"),
		),
		s.handleResourceBlockSummary,
	)

	s.mcpServer.AddResourceTemplate(
		mcp.NewResourceTemplate("erigon://transaction/{hash}/analysis",
			"transaction analysis",
			mcp.WithTemplateDescription("Get transaction analysis"),
			mcp.WithTemplateMIMEType("application/json"),
		),
		s.handleResourceTransactionAnalysis,
	)
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

// Serve starts the MCP server in stdio mode.
func (s *StandaloneMCPServer) Serve() error {
	return server.ServeStdio(s.mcpServer)
}

// ServeSSE starts the MCP server with SSE transport on the given address.
func (s *StandaloneMCPServer) ServeSSE(addr string) (err error) {
	sse := server.NewSSEServer(s.mcpServer)

	defer func() {
		if r := recover(); r != nil {
			log.Error("[MCP] recovered from panic", "panic", r)
			err = fmt.Errorf("mcp sse server panicked: %v", r)
		}
	}()

	return sse.Start(addr)
}
