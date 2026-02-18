package mcp

import (
	"context"
	"encoding/json"
	"fmt"
	"math/big"
	"strings"

	"github.com/mark3labs/mcp-go/mcp"
	"github.com/mark3labs/mcp-go/server"

	"github.com/erigontech/erigon/common"
	"github.com/erigontech/erigon/common/hexutil"
	"github.com/erigontech/erigon/common/log/v3"
	"github.com/erigontech/erigon/execution/chain/spec"
	"github.com/erigontech/erigon/rpc"
	"github.com/erigontech/erigon/rpc/ethapi"
	"github.com/erigontech/erigon/rpc/filters"
	"github.com/erigontech/erigon/rpc/jsonrpc"
)

// ErigonMCPServer wraps Erigon APIs with MCP server capabilities.
type ErigonMCPServer struct {
	mcpServer *server.MCPServer
	ethAPI    jsonrpc.EthAPI
	erigonAPI jsonrpc.ErigonAPI
	otsAPI    jsonrpc.OtterscanAPI
	logDir    string
}

// NewErigonMCPServer creates a new MCP server for Erigon.
func NewErigonMCPServer(ethAPI jsonrpc.EthAPI, erigonAPI jsonrpc.ErigonAPI, otsAPI jsonrpc.OtterscanAPI, logDir string) *ErigonMCPServer {
	e := &ErigonMCPServer{
		ethAPI:    ethAPI,
		erigonAPI: erigonAPI,
		otsAPI:    otsAPI,
		logDir:    logDir,
	}

	e.mcpServer = server.NewMCPServer(
		"ErigonMCP",
		"0.0.1",
		server.WithResourceCapabilities(true, true),
		server.WithToolCapabilities(true),
		server.WithPromptCapabilities(true),
		server.WithLogging(),
		server.WithRecovery(),
	)

	e.registerTools()
	e.registerPrompts()
	e.registerResources()

	return e
}

// parseBlockNumberOrHash parses block number or hash from string
func parseBlockNumberOrHash(s string) (rpc.BlockNumberOrHash, error) {
	var result rpc.BlockNumberOrHash
	if strings.HasPrefix(s, "0x") && len(s) == 66 {
		hash := common.HexToHash(s)
		return rpc.BlockNumberOrHashWithHash(hash, false), nil
	}
	blockNum, err := parseBlockNumber(s)
	if err != nil {
		return result, err
	}
	return rpc.BlockNumberOrHashWithNumber(blockNum), nil
}

// registerTools registers all MCP tools
func (e *ErigonMCPServer) registerTools() {
	// eth_blockNumber
	e.mcpServer.AddTool(mcp.NewTool("eth_blockNumber",
		mcp.WithDescription("Get the current block number"),
	), e.handleBlockNumber)

	// eth_getBlockByNumber
	e.mcpServer.AddTool(mcp.NewTool("eth_getBlockByNumber",
		mcp.WithDescription("Get block by number"),
		mcp.WithString("blockNumber", mcp.Required(), mcp.Description("Block number or tag")),
		mcp.WithBoolean("fullTransactions", mcp.Description("Return full tx objects")),
	), e.handleGetBlockByNumber)

	// eth_getBlockByHash
	e.mcpServer.AddTool(mcp.NewTool("eth_getBlockByHash",
		mcp.WithDescription("Get block by hash"),
		mcp.WithString("blockHash", mcp.Required(), mcp.Description("Block hash")),
		mcp.WithBoolean("fullTransactions", mcp.Description("Return full tx objects")),
	), e.handleGetBlockByHash)

	// eth_getBlockTransactionCountByNumber
	e.mcpServer.AddTool(mcp.NewTool("eth_getBlockTransactionCountByNumber",
		mcp.WithDescription("Get transaction count in block by number"),
		mcp.WithString("blockNumber", mcp.Required(), mcp.Description("Block number")),
	), e.handleGetBlockTransactionCountByNumber)

	// eth_getBlockTransactionCountByHash
	e.mcpServer.AddTool(mcp.NewTool("eth_getBlockTransactionCountByHash",
		mcp.WithDescription("Get transaction count in block by hash"),
		mcp.WithString("blockHash", mcp.Required(), mcp.Description("Block hash")),
	), e.handleGetBlockTransactionCountByHash)

	// eth_getBalance
	e.mcpServer.AddTool(mcp.NewTool("eth_getBalance",
		mcp.WithDescription("Get address balance"),
		mcp.WithString("address", mcp.Required(), mcp.Description("Address")),
		mcp.WithString("blockNumber", mcp.Description("Block number (default: latest)")),
	), e.handleGetBalance)

	// eth_getTransactionByHash
	e.mcpServer.AddTool(mcp.NewTool("eth_getTransactionByHash",
		mcp.WithDescription("Get transaction by hash"),
		mcp.WithString("txHash", mcp.Required(), mcp.Description("Transaction hash")),
	), e.handleGetTransactionByHash)

	// eth_getTransactionByBlockHashAndIndex
	e.mcpServer.AddTool(mcp.NewTool("eth_getTransactionByBlockHashAndIndex",
		mcp.WithDescription("Get transaction by block hash and index"),
		mcp.WithString("blockHash", mcp.Required(), mcp.Description("Block hash")),
		mcp.WithNumber("index", mcp.Required(), mcp.Description("Transaction index")),
	), e.handleGetTransactionByBlockHashAndIndex)

	// eth_getTransactionByBlockNumberAndIndex
	e.mcpServer.AddTool(mcp.NewTool("eth_getTransactionByBlockNumberAndIndex",
		mcp.WithDescription("Get transaction by block number and index"),
		mcp.WithString("blockNumber", mcp.Required(), mcp.Description("Block number")),
		mcp.WithNumber("index", mcp.Required(), mcp.Description("Transaction index")),
	), e.handleGetTransactionByBlockNumberAndIndex)

	// eth_getTransactionReceipt
	e.mcpServer.AddTool(mcp.NewTool("eth_getTransactionReceipt",
		mcp.WithDescription("Get transaction receipt"),
		mcp.WithString("txHash", mcp.Required(), mcp.Description("Transaction hash")),
	), e.handleGetTransactionReceipt)

	// eth_getBlockReceipts
	e.mcpServer.AddTool(mcp.NewTool("eth_getBlockReceipts",
		mcp.WithDescription("Get all receipts for a block"),
		mcp.WithString("blockNumberOrHash", mcp.Required(), mcp.Description("Block number or hash")),
	), e.handleGetBlockReceipts)

	// eth_getLogs
	e.mcpServer.AddTool(mcp.NewTool("eth_getLogs",
		mcp.WithDescription("Get logs matching filter"),
		mcp.WithString("fromBlock", mcp.Description("Start block")),
		mcp.WithString("toBlock", mcp.Description("End block")),
		mcp.WithString("address", mcp.Description("Contract address(es)")),
		mcp.WithString("topics", mcp.Description("Topics array (JSON)")),
		mcp.WithString("blockHash", mcp.Description("Single block hash")),
	), e.handleGetLogs)

	// eth_getCode
	e.mcpServer.AddTool(mcp.NewTool("eth_getCode",
		mcp.WithDescription("Get contract code"),
		mcp.WithString("address", mcp.Required(), mcp.Description("Contract address")),
		mcp.WithString("blockNumber", mcp.Description("Block number")),
	), e.handleGetCode)

	// eth_getStorageAt
	e.mcpServer.AddTool(mcp.NewTool("eth_getStorageAt",
		mcp.WithDescription("Get storage at position"),
		mcp.WithString("address", mcp.Required(), mcp.Description("Address")),
		mcp.WithString("position", mcp.Required(), mcp.Description("Storage position")),
		mcp.WithString("blockNumber", mcp.Description("Block number")),
	), e.handleGetStorageAt)

	// eth_getTransactionCount
	e.mcpServer.AddTool(mcp.NewTool("eth_getTransactionCount",
		mcp.WithDescription("Get nonce (transaction count)"),
		mcp.WithString("address", mcp.Required(), mcp.Description("Address")),
		mcp.WithString("blockNumber", mcp.Description("Block number")),
	), e.handleGetTransactionCount)

	// eth_call
	e.mcpServer.AddTool(mcp.NewTool("eth_call",
		mcp.WithDescription("Execute call without transaction"),
		mcp.WithString("to", mcp.Required(), mcp.Description("Contract address")),
		mcp.WithString("data", mcp.Required(), mcp.Description("Call data")),
		mcp.WithString("from", mcp.Description("Sender address")),
		mcp.WithString("value", mcp.Description("Value (hex)")),
		mcp.WithString("gas", mcp.Description("Gas limit (hex)")),
		mcp.WithString("blockNumber", mcp.Description("Block number")),
	), e.handleCall)

	// eth_estimateGas
	e.mcpServer.AddTool(mcp.NewTool("eth_estimateGas",
		mcp.WithDescription("Estimate gas for transaction"),
		mcp.WithString("to", mcp.Description("To address")),
		mcp.WithString("data", mcp.Description("Call data")),
		mcp.WithString("from", mcp.Description("From address")),
		mcp.WithString("value", mcp.Description("Value (hex)")),
	), e.handleEstimateGas)

	// eth_gasPrice
	e.mcpServer.AddTool(mcp.NewTool("eth_gasPrice",
		mcp.WithDescription("Get current gas price"),
	), e.handleGasPrice)

	// eth_chainId
	e.mcpServer.AddTool(mcp.NewTool("eth_chainId",
		mcp.WithDescription("Get chain ID"),
	), e.handleChainId)

	// eth_syncing
	e.mcpServer.AddTool(mcp.NewTool("eth_syncing",
		mcp.WithDescription("Get sync status"),
	), e.handleSyncing)

	// eth_accounts
	e.mcpServer.AddTool(mcp.NewTool("eth_accounts",
		mcp.WithDescription("Get accounts"),
	), e.handleAccounts)

	// eth_getProof
	e.mcpServer.AddTool(mcp.NewTool("eth_getProof",
		mcp.WithDescription("Get Merkle proof"),
		mcp.WithString("address", mcp.Required(), mcp.Description("Address")),
		mcp.WithString("storageKeys", mcp.Description("Storage keys (JSON array)")),
		mcp.WithString("blockNumber", mcp.Description("Block number")),
	), e.handleGetProof)

	// eth_coinbase
	e.mcpServer.AddTool(mcp.NewTool("eth_coinbase",
		mcp.WithDescription("Get coinbase address"),
	), e.handleCoinbase)

	// eth_mining
	e.mcpServer.AddTool(mcp.NewTool("eth_mining",
		mcp.WithDescription("Check if mining"),
	), e.handleMining)

	// eth_hashrate
	e.mcpServer.AddTool(mcp.NewTool("eth_hashrate",
		mcp.WithDescription("Get hashrate"),
	), e.handleHashrate)

	// eth_protocolVersion
	e.mcpServer.AddTool(mcp.NewTool("eth_protocolVersion",
		mcp.WithDescription("Get protocol version"),
	), e.handleProtocolVersion)

	// Uncle methods
	e.mcpServer.AddTool(mcp.NewTool("eth_getUncleByBlockNumberAndIndex",
		mcp.WithDescription("Get uncle by block number and index"),
		mcp.WithString("blockNumber", mcp.Required(), mcp.Description("Block number")),
		mcp.WithNumber("index", mcp.Required(), mcp.Description("Uncle index")),
	), e.handleGetUncleByBlockNumberAndIndex)

	e.mcpServer.AddTool(mcp.NewTool("eth_getUncleByBlockHashAndIndex",
		mcp.WithDescription("Get uncle by block hash and index"),
		mcp.WithString("blockHash", mcp.Required(), mcp.Description("Block hash")),
		mcp.WithNumber("index", mcp.Required(), mcp.Description("Uncle index")),
	), e.handleGetUncleByBlockHashAndIndex)

	e.mcpServer.AddTool(mcp.NewTool("eth_getUncleCountByBlockNumber",
		mcp.WithDescription("Get uncle count by block number"),
		mcp.WithString("blockNumber", mcp.Required(), mcp.Description("Block number")),
	), e.handleGetUncleCountByBlockNumber)

	e.mcpServer.AddTool(mcp.NewTool("eth_getUncleCountByBlockHash",
		mcp.WithDescription("Get uncle count by block hash"),
		mcp.WithString("blockHash", mcp.Required(), mcp.Description("Block hash")),
	), e.handleGetUncleCountByBlockHash)

	// Erigon-specific tools
	e.mcpServer.AddTool(mcp.NewTool("erigon_forks",
		mcp.WithDescription("Get fork information"),
	), e.handleErigonForks)

	e.mcpServer.AddTool(mcp.NewTool("erigon_blockNumber",
		mcp.WithDescription("Get block number (Erigon)"),
		mcp.WithString("blockNumber", mcp.Description("Block tag")),
	), e.handleErigonBlockNumber)

	e.mcpServer.AddTool(mcp.NewTool("erigon_getHeaderByNumber",
		mcp.WithDescription("Get header by number"),
		mcp.WithString("blockNumber", mcp.Required(), mcp.Description("Block number")),
	), e.handleErigonGetHeaderByNumber)

	e.mcpServer.AddTool(mcp.NewTool("erigon_getHeaderByHash",
		mcp.WithDescription("Get header by hash"),
		mcp.WithString("blockHash", mcp.Required(), mcp.Description("Block hash")),
	), e.handleErigonGetHeaderByHash)

	e.mcpServer.AddTool(mcp.NewTool("erigon_getBlockByTimestamp",
		mcp.WithDescription("Get block by timestamp"),
		mcp.WithString("timestamp", mcp.Required(), mcp.Description("Unix timestamp")),
		mcp.WithBoolean("fullTransactions", mcp.Description("Full tx objects")),
	), e.handleErigonGetBlockByTimestamp)

	e.mcpServer.AddTool(mcp.NewTool("erigon_getBalanceChangesInBlock",
		mcp.WithDescription("Get all balance changes in block"),
		mcp.WithString("blockNumberOrHash", mcp.Required(), mcp.Description("Block")),
	), e.handleErigonGetBalanceChangesInBlock)

	e.mcpServer.AddTool(mcp.NewTool("erigon_getLogsByHash",
		mcp.WithDescription("Get logs by block hash"),
		mcp.WithString("blockHash", mcp.Required(), mcp.Description("Block hash")),
	), e.handleErigonGetLogsByHash)

	e.mcpServer.AddTool(mcp.NewTool("erigon_getLogs",
		mcp.WithDescription("Get logs (Erigon format)"),
		mcp.WithString("fromBlock", mcp.Description("From block")),
		mcp.WithString("toBlock", mcp.Description("To block")),
		mcp.WithString("address", mcp.Description("Address")),
		mcp.WithString("topics", mcp.Description("Topics (JSON)")),
	), e.handleErigonGetLogs)

	e.mcpServer.AddTool(mcp.NewTool("erigon_getBlockReceiptsByBlockHash",
		mcp.WithDescription("Get block receipts by hash"),
		mcp.WithString("blockHash", mcp.Required(), mcp.Description("Block hash")),
	), e.handleErigonGetBlockReceiptsByBlockHash)

	e.mcpServer.AddTool(mcp.NewTool("erigon_nodeInfo",
		mcp.WithDescription("Get P2P node info"),
	), e.handleErigonNodeInfo)

	// Metrics tools
	e.mcpServer.AddTool(mcp.NewTool("metrics_list",
		mcp.WithDescription("List all available metric names"),
	), e.handleMetricsList)

	e.mcpServer.AddTool(mcp.NewTool("metrics_get",
		mcp.WithDescription("Get metrics with optional filtering by pattern (supports wildcards like 'db_*', '*_size', etc.)"),
		mcp.WithString("pattern", mcp.Description("Metric name pattern (optional, empty = all metrics)")),
	), e.handleMetricsGet)

	// Log tools
	e.mcpServer.AddTool(mcp.NewTool("logs_tail",
		mcp.WithDescription("Get last N lines from erigon or torrent logs"),
		mcp.WithString("log_type", mcp.Description("Log type: 'erigon' or 'torrent' (default: erigon)")),
		mcp.WithNumber("lines", mcp.Description("Number of lines to retrieve (default: 100, max: 10000)")),
		mcp.WithString("filter", mcp.Description("Optional string to filter log lines")),
	), e.handleLogsTail)

	e.mcpServer.AddTool(mcp.NewTool("logs_head",
		mcp.WithDescription("Get first N lines from erigon or torrent logs"),
		mcp.WithString("log_type", mcp.Description("Log type: 'erigon' or 'torrent' (default: erigon)")),
		mcp.WithNumber("lines", mcp.Description("Number of lines to retrieve (default: 100, max: 10000)")),
		mcp.WithString("filter", mcp.Description("Optional string to filter log lines")),
	), e.handleLogsHead)

	e.mcpServer.AddTool(mcp.NewTool("logs_grep",
		mcp.WithDescription("Search for a pattern in erigon or torrent logs"),
		mcp.WithString("log_type", mcp.Description("Log type: 'erigon' or 'torrent' (default: erigon)")),
		mcp.WithString("pattern", mcp.Required(), mcp.Description("Search pattern")),
		mcp.WithNumber("max_lines", mcp.Description("Maximum matching lines to return (default: 1000, max: 10000)")),
		mcp.WithBoolean("case_insensitive", mcp.Description("Case-insensitive search (default: false)")),
	), e.handleLogsGrep)

	e.mcpServer.AddTool(mcp.NewTool("logs_stats",
		mcp.WithDescription("Get statistics about erigon or torrent logs"),
		mcp.WithString("log_type", mcp.Description("Log type: 'erigon' or 'torrent' (default: erigon)")),
	), e.handleLogsStats)

	// Otterscan tools
	e.mcpServer.AddTool(mcp.NewTool("ots_getApiLevel",
		mcp.WithDescription("Get Otterscan API level"),
	), e.handleOtsGetApiLevel)

	e.mcpServer.AddTool(mcp.NewTool("ots_getInternalOperations",
		mcp.WithDescription("Get internal operations (internal txs) for a transaction"),
		mcp.WithString("txHash", mcp.Required(), mcp.Description("Transaction hash")),
	), e.handleOtsGetInternalOperations)

	e.mcpServer.AddTool(mcp.NewTool("ots_searchTransactionsBefore",
		mcp.WithDescription("Search transactions before a given block for an address"),
		mcp.WithString("address", mcp.Required(), mcp.Description("Address")),
		mcp.WithNumber("blockNumber", mcp.Required(), mcp.Description("Block number")),
		mcp.WithNumber("pageSize", mcp.Description("Page size (default: 25)")),
	), e.handleOtsSearchTransactionsBefore)

	e.mcpServer.AddTool(mcp.NewTool("ots_searchTransactionsAfter",
		mcp.WithDescription("Search transactions after a given block for an address"),
		mcp.WithString("address", mcp.Required(), mcp.Description("Address")),
		mcp.WithNumber("blockNumber", mcp.Required(), mcp.Description("Block number")),
		mcp.WithNumber("pageSize", mcp.Description("Page size (default: 25)")),
	), e.handleOtsSearchTransactionsAfter)

	e.mcpServer.AddTool(mcp.NewTool("ots_getBlockDetails",
		mcp.WithDescription("Get detailed block information"),
		mcp.WithString("blockNumber", mcp.Required(), mcp.Description("Block number or tag")),
	), e.handleOtsGetBlockDetails)

	e.mcpServer.AddTool(mcp.NewTool("ots_getBlockDetailsByHash",
		mcp.WithDescription("Get detailed block information by hash"),
		mcp.WithString("blockHash", mcp.Required(), mcp.Description("Block hash")),
	), e.handleOtsGetBlockDetailsByHash)

	e.mcpServer.AddTool(mcp.NewTool("ots_getBlockTransactions",
		mcp.WithDescription("Get paginated transactions for a block"),
		mcp.WithString("blockNumber", mcp.Required(), mcp.Description("Block number or tag")),
		mcp.WithNumber("pageNumber", mcp.Description("Page number (default: 0)")),
		mcp.WithNumber("pageSize", mcp.Description("Page size (default: 25)")),
	), e.handleOtsGetBlockTransactions)

	e.mcpServer.AddTool(mcp.NewTool("ots_hasCode",
		mcp.WithDescription("Check if an address has code (is a contract)"),
		mcp.WithString("address", mcp.Required(), mcp.Description("Address")),
		mcp.WithString("blockNumber", mcp.Description("Block number or tag")),
	), e.handleOtsHasCode)

	e.mcpServer.AddTool(mcp.NewTool("ots_traceTransaction",
		mcp.WithDescription("Get trace for a transaction"),
		mcp.WithString("txHash", mcp.Required(), mcp.Description("Transaction hash")),
	), e.handleOtsTraceTransaction)

	e.mcpServer.AddTool(mcp.NewTool("ots_getTransactionError",
		mcp.WithDescription("Get transaction error/revert reason"),
		mcp.WithString("txHash", mcp.Required(), mcp.Description("Transaction hash")),
	), e.handleOtsGetTransactionError)

	e.mcpServer.AddTool(mcp.NewTool("ots_getTransactionBySenderAndNonce",
		mcp.WithDescription("Get transaction hash by sender address and nonce"),
		mcp.WithString("address", mcp.Required(), mcp.Description("Sender address")),
		mcp.WithNumber("nonce", mcp.Required(), mcp.Description("Nonce")),
	), e.handleOtsGetTransactionBySenderAndNonce)

	e.mcpServer.AddTool(mcp.NewTool("ots_getContractCreator",
		mcp.WithDescription("Get contract creator address and transaction"),
		mcp.WithString("address", mcp.Required(), mcp.Description("Contract address")),
	), e.handleOtsGetContractCreator)
}

// ===== ETH API HANDLERS =====

func (e *ErigonMCPServer) handleBlockNumber(ctx context.Context, req mcp.CallToolRequest) (*mcp.CallToolResult, error) {
	blockNum, err := e.ethAPI.BlockNumber(ctx)
	if err != nil {
		return mcp.NewToolResultError(err.Error()), nil
	}
	return mcp.NewToolResultText(fmt.Sprintf("Current block: %d (0x%x)", uint64(blockNum), uint64(blockNum))), nil
}

func (e *ErigonMCPServer) handleGetBlockByNumber(ctx context.Context, req mcp.CallToolRequest) (*mcp.CallToolResult, error) {
	blockNum, err := parseBlockNumber(req.GetString("blockNumber", "latest"))
	if err != nil {
		return mcp.NewToolResultError(err.Error()), nil
	}
	result, err := e.ethAPI.GetBlockByNumber(ctx, blockNum, req.GetBool("fullTransactions", false))
	if err != nil {
		return mcp.NewToolResultError(err.Error()), nil
	}
	if result == nil {
		return mcp.NewToolResultText("Block not found"), nil
	}
	return mcp.NewToolResultText(toJSONText(result)), nil
}

func (e *ErigonMCPServer) handleGetBlockByHash(ctx context.Context, req mcp.CallToolRequest) (*mcp.CallToolResult, error) {
	blockNumOrHash, err := parseBlockNumberOrHash(req.GetString("blockHash", ""))
	if err != nil {
		return mcp.NewToolResultError(err.Error()), nil
	}
	result, err := e.ethAPI.GetBlockByHash(ctx, blockNumOrHash, req.GetBool("fullTransactions", false))
	if err != nil {
		return mcp.NewToolResultError(err.Error()), nil
	}
	if result == nil {
		return mcp.NewToolResultText("Block not found"), nil
	}
	return mcp.NewToolResultText(toJSONText(result)), nil
}

func (e *ErigonMCPServer) handleGetBlockTransactionCountByNumber(ctx context.Context, req mcp.CallToolRequest) (*mcp.CallToolResult, error) {
	blockNum, err := parseBlockNumber(req.GetString("blockNumber", "latest"))
	if err != nil {
		return mcp.NewToolResultError(err.Error()), nil
	}
	count, err := e.ethAPI.GetBlockTransactionCountByNumber(ctx, blockNum)
	if err != nil {
		return mcp.NewToolResultError(err.Error()), nil
	}
	if count == nil {
		return mcp.NewToolResultText("Block not found"), nil
	}
	return mcp.NewToolResultText(fmt.Sprintf("Transaction count: %d", uint64(*count))), nil
}

func (e *ErigonMCPServer) handleGetBlockTransactionCountByHash(ctx context.Context, req mcp.CallToolRequest) (*mcp.CallToolResult, error) {
	hash := common.HexToHash(req.GetString("blockHash", ""))
	count, err := e.ethAPI.GetBlockTransactionCountByHash(ctx, hash)
	if err != nil {
		return mcp.NewToolResultError(err.Error()), nil
	}
	if count == nil {
		return mcp.NewToolResultText("Block not found"), nil
	}
	return mcp.NewToolResultText(fmt.Sprintf("Transaction count: %d", uint64(*count))), nil
}

func (e *ErigonMCPServer) handleGetBalance(ctx context.Context, req mcp.CallToolRequest) (*mcp.CallToolResult, error) {
	addr := common.HexToAddress(req.GetString("address", ""))
	blockNumOrHash, _ := parseBlockNumberOrHash(req.GetString("blockNumber", "latest"))
	balance, err := e.ethAPI.GetBalance(ctx, addr, blockNumOrHash)
	if err != nil {
		return mcp.NewToolResultError(err.Error()), nil
	}
	wei := balance.ToInt()
	eth := new(big.Float).Quo(new(big.Float).SetInt(wei), big.NewFloat(1e18))
	return mcp.NewToolResultText(fmt.Sprintf("Balance: %s wei (%.6f ETH)", balance.String(), eth)), nil
}

func (e *ErigonMCPServer) handleGetTransactionByHash(ctx context.Context, req mcp.CallToolRequest) (*mcp.CallToolResult, error) {
	hash := common.HexToHash(req.GetString("txHash", ""))
	result, err := e.ethAPI.GetTransactionByHash(ctx, hash)
	if err != nil {
		return mcp.NewToolResultError(err.Error()), nil
	}
	if result == nil {
		return mcp.NewToolResultText("Transaction not found"), nil
	}
	return mcp.NewToolResultText(toJSONText(result)), nil
}

func (e *ErigonMCPServer) handleGetTransactionByBlockHashAndIndex(ctx context.Context, req mcp.CallToolRequest) (*mcp.CallToolResult, error) {
	hash := common.HexToHash(req.GetString("blockHash", ""))
	idx := hexutil.Uint64(req.GetInt("index", 0))
	result, err := e.ethAPI.GetTransactionByBlockHashAndIndex(ctx, hash, idx)
	if err != nil {
		return mcp.NewToolResultError(err.Error()), nil
	}
	if result == nil {
		return mcp.NewToolResultText("Transaction not found"), nil
	}
	return mcp.NewToolResultText(toJSONText(result)), nil
}

func (e *ErigonMCPServer) handleGetTransactionByBlockNumberAndIndex(ctx context.Context, req mcp.CallToolRequest) (*mcp.CallToolResult, error) {
	blockNum, _ := parseBlockNumber(req.GetString("blockNumber", "latest"))
	idx := hexutil.Uint(req.GetInt("index", 0))
	result, err := e.ethAPI.GetTransactionByBlockNumberAndIndex(ctx, blockNum, idx)
	if err != nil {
		return mcp.NewToolResultError(err.Error()), nil
	}
	if result == nil {
		return mcp.NewToolResultText("Transaction not found"), nil
	}
	return mcp.NewToolResultText(toJSONText(result)), nil
}

func (e *ErigonMCPServer) handleGetTransactionReceipt(ctx context.Context, req mcp.CallToolRequest) (*mcp.CallToolResult, error) {
	hash := common.HexToHash(req.GetString("txHash", ""))
	result, err := e.ethAPI.GetTransactionReceipt(ctx, hash)
	if err != nil {
		return mcp.NewToolResultError(err.Error()), nil
	}
	if result == nil {
		return mcp.NewToolResultText("Receipt not found"), nil
	}
	return mcp.NewToolResultText(toJSONText(result)), nil
}

func (e *ErigonMCPServer) handleGetBlockReceipts(ctx context.Context, req mcp.CallToolRequest) (*mcp.CallToolResult, error) {
	blockNumOrHash, _ := parseBlockNumberOrHash(req.GetString("blockNumberOrHash", "latest"))
	result, err := e.ethAPI.GetBlockReceipts(ctx, blockNumOrHash)
	if err != nil {
		return mcp.NewToolResultError(err.Error()), nil
	}
	return mcp.NewToolResultText(toJSONText(result)), nil
}

func (e *ErigonMCPServer) handleGetLogs(ctx context.Context, req mcp.CallToolRequest) (*mcp.CallToolResult, error) {
	var crit filters.FilterCriteria

	if from := req.GetString("fromBlock", ""); from != "" {
		if bn, err := parseBlockNumber(from); err == nil {
			crit.FromBlock = big.NewInt(bn.Int64())
		}
	}
	if to := req.GetString("toBlock", ""); to != "" {
		if bn, err := parseBlockNumber(to); err == nil {
			crit.ToBlock = big.NewInt(bn.Int64())
		}
	}
	if addr := req.GetString("address", ""); addr != "" {
		if strings.HasPrefix(addr, "[") {
			var addrs []string
			if json.Unmarshal([]byte(addr), &addrs) == nil {
				for _, a := range addrs {
					crit.Addresses = append(crit.Addresses, common.HexToAddress(a))
				}
			}
		} else {
			crit.Addresses = []common.Address{common.HexToAddress(addr)}
		}
	}
	if topics := req.GetString("topics", ""); topics != "" {
		var t [][]common.Hash
		if json.Unmarshal([]byte(topics), &t) == nil {
			crit.Topics = t
		}
	}
	if bh := req.GetString("blockHash", ""); bh != "" {
		h := common.HexToHash(bh)
		crit.BlockHash = &h
	}

	result, err := e.ethAPI.GetLogs(ctx, crit)
	if err != nil {
		return mcp.NewToolResultError(err.Error()), nil
	}
	return mcp.NewToolResultText(toJSONText(result)), nil
}

func (e *ErigonMCPServer) handleGetCode(ctx context.Context, req mcp.CallToolRequest) (*mcp.CallToolResult, error) {
	addr := common.HexToAddress(req.GetString("address", ""))
	blockNumOrHash, _ := parseBlockNumberOrHash(req.GetString("blockNumber", "latest"))
	code, err := e.ethAPI.GetCode(ctx, addr, blockNumOrHash)
	if err != nil {
		return mcp.NewToolResultError(err.Error()), nil
	}
	if len(code) == 0 {
		return mcp.NewToolResultText("No code (EOA)"), nil
	}
	return mcp.NewToolResultText(fmt.Sprintf("Code (%d bytes): %s", len(code), code.String())), nil
}

func (e *ErigonMCPServer) handleGetStorageAt(ctx context.Context, req mcp.CallToolRequest) (*mcp.CallToolResult, error) {
	addr := common.HexToAddress(req.GetString("address", ""))
	pos := req.GetString("position", "0x0")
	blockNumOrHash, _ := parseBlockNumberOrHash(req.GetString("blockNumber", "latest"))
	result, err := e.ethAPI.GetStorageAt(ctx, addr, pos, blockNumOrHash)
	if err != nil {
		return mcp.NewToolResultError(err.Error()), nil
	}
	return mcp.NewToolResultText(fmt.Sprintf("Storage: %s", result)), nil
}

func (e *ErigonMCPServer) handleGetTransactionCount(ctx context.Context, req mcp.CallToolRequest) (*mcp.CallToolResult, error) {
	addr := common.HexToAddress(req.GetString("address", ""))
	blockNumOrHash, _ := parseBlockNumberOrHash(req.GetString("blockNumber", "latest"))
	count, err := e.ethAPI.GetTransactionCount(ctx, addr, blockNumOrHash)
	if err != nil {
		return mcp.NewToolResultError(err.Error()), nil
	}
	return mcp.NewToolResultText(fmt.Sprintf("Nonce: %d", uint64(*count))), nil
}

func (e *ErigonMCPServer) handleCall(ctx context.Context, req mcp.CallToolRequest) (*mcp.CallToolResult, error) {
	var args ethapi.CallArgs
	to := common.HexToAddress(req.GetString("to", ""))
	args.To = &to

	if data := req.GetString("data", ""); data != "" {
		d := hexutil.MustDecode(data)
		args.Data = (*hexutil.Bytes)(&d)
	}
	if from := req.GetString("from", ""); from != "" {
		f := common.HexToAddress(from)
		args.From = &f
	}
	if val := req.GetString("value", ""); val != "" {
		v := hexutil.MustDecodeBig(val)
		args.Value = (*hexutil.Big)(v)
	}
	if gas := req.GetString("gas", ""); gas != "" {
		g, err := hexutil.DecodeUint64(gas)
		if err != nil {
			return mcp.NewToolResultError(err.Error()), nil
		}
		args.Gas = (*hexutil.Uint64)(&g)
	}

	blockNumOrHash, _ := parseBlockNumberOrHash(req.GetString("blockNumber", "latest"))
	result, err := e.ethAPI.Call(ctx, args, &blockNumOrHash, nil, nil)
	if err != nil {
		return mcp.NewToolResultError(err.Error()), nil
	}
	return mcp.NewToolResultText(fmt.Sprintf("Result: %s", result.String())), nil
}

func (e *ErigonMCPServer) handleEstimateGas(ctx context.Context, req mcp.CallToolRequest) (*mcp.CallToolResult, error) {
	var args ethapi.CallArgs
	if to := req.GetString("to", ""); to != "" {
		t := common.HexToAddress(to)
		args.To = &t
	}
	if data := req.GetString("data", ""); data != "" {
		d := hexutil.MustDecode(data)
		args.Data = (*hexutil.Bytes)(&d)
	}
	if from := req.GetString("from", ""); from != "" {
		f := common.HexToAddress(from)
		args.From = &f
	}
	if val := req.GetString("value", ""); val != "" {
		v := hexutil.MustDecodeBig(val)
		args.Value = (*hexutil.Big)(v)
	}

	gas, err := e.ethAPI.EstimateGas(ctx, &args, nil, nil, nil)
	if err != nil {
		return mcp.NewToolResultError(err.Error()), nil
	}
	return mcp.NewToolResultText(fmt.Sprintf("Estimated gas: %d", uint64(gas))), nil
}

func (e *ErigonMCPServer) handleGasPrice(ctx context.Context, req mcp.CallToolRequest) (*mcp.CallToolResult, error) {
	price, err := e.ethAPI.GasPrice(ctx)
	if err != nil {
		return mcp.NewToolResultError(err.Error()), nil
	}
	gwei := new(big.Float).Quo(new(big.Float).SetInt(price.ToInt()), big.NewFloat(1e9))
	return mcp.NewToolResultText(fmt.Sprintf("Gas price: %s wei (%.2f Gwei)", price.String(), gwei)), nil
}

func (e *ErigonMCPServer) handleChainId(ctx context.Context, req mcp.CallToolRequest) (*mcp.CallToolResult, error) {
	chainId, err := e.ethAPI.ChainId(ctx)
	if err != nil {
		return mcp.NewToolResultError(err.Error()), nil
	}
	return mcp.NewToolResultText(fmt.Sprintf("Chain ID: %d - %s", uint64(chainId), chainspec.NetworkNameByID[chainId.Uint64()])), nil
}

func (e *ErigonMCPServer) handleSyncing(ctx context.Context, req mcp.CallToolRequest) (*mcp.CallToolResult, error) {
	result, err := e.ethAPI.Syncing(ctx)
	if err != nil {
		return mcp.NewToolResultError(err.Error()), nil
	}
	if syncing, ok := result.(bool); ok && !syncing {
		return mcp.NewToolResultText("Fully synced"), nil
	}
	return mcp.NewToolResultText(toJSONText(result)), nil
}

func (e *ErigonMCPServer) handleAccounts(ctx context.Context, req mcp.CallToolRequest) (*mcp.CallToolResult, error) {
	accounts, err := e.ethAPI.Accounts(ctx)
	if err != nil {
		return mcp.NewToolResultError(err.Error()), nil
	}
	if len(accounts) == 0 {
		return mcp.NewToolResultText("No accounts"), nil
	}
	return mcp.NewToolResultText(toJSONText(accounts)), nil
}

func (e *ErigonMCPServer) handleGetProof(ctx context.Context, req mcp.CallToolRequest) (*mcp.CallToolResult, error) {
	addr := common.HexToAddress(req.GetString("address", ""))
	var keys []hexutil.Bytes
	if k := req.GetString("storageKeys", ""); k != "" {
		json.Unmarshal([]byte(k), &keys)
	}
	blockNumOrHash, _ := parseBlockNumberOrHash(req.GetString("blockNumber", "latest"))
	result, err := e.ethAPI.GetProof(ctx, addr, keys, blockNumOrHash)
	if err != nil {
		return mcp.NewToolResultError(err.Error()), nil
	}
	return mcp.NewToolResultText(toJSONText(result)), nil
}

func (e *ErigonMCPServer) handleCoinbase(ctx context.Context, req mcp.CallToolRequest) (*mcp.CallToolResult, error) {
	coinbase, err := e.ethAPI.Coinbase(ctx)
	if err != nil {
		return mcp.NewToolResultError(err.Error()), nil
	}
	return mcp.NewToolResultText(fmt.Sprintf("Coinbase: %s", coinbase.Hex())), nil
}

func (e *ErigonMCPServer) handleMining(ctx context.Context, req mcp.CallToolRequest) (*mcp.CallToolResult, error) {
	mining, err := e.ethAPI.Mining(ctx)
	if err != nil {
		return mcp.NewToolResultError(err.Error()), nil
	}
	return mcp.NewToolResultText(fmt.Sprintf("Mining: %v", mining)), nil
}

func (e *ErigonMCPServer) handleHashrate(ctx context.Context, req mcp.CallToolRequest) (*mcp.CallToolResult, error) {
	hashrate, err := e.ethAPI.Hashrate(ctx)
	if err != nil {
		return mcp.NewToolResultError(err.Error()), nil
	}
	return mcp.NewToolResultText(fmt.Sprintf("Hashrate: %d H/s", hashrate)), nil
}

func (e *ErigonMCPServer) handleProtocolVersion(ctx context.Context, req mcp.CallToolRequest) (*mcp.CallToolResult, error) {
	version, err := e.ethAPI.ProtocolVersion(ctx)
	if err != nil {
		return mcp.NewToolResultError(err.Error()), nil
	}
	return mcp.NewToolResultText(fmt.Sprintf("Protocol version: %d", uint64(version))), nil
}

// ===== UNCLE HANDLERS =====

func (e *ErigonMCPServer) handleGetUncleByBlockNumberAndIndex(ctx context.Context, req mcp.CallToolRequest) (*mcp.CallToolResult, error) {
	blockNum, _ := parseBlockNumber(req.GetString("blockNumber", "latest"))
	result, err := e.ethAPI.GetUncleByBlockNumberAndIndex(ctx, blockNum, hexutil.Uint(req.GetInt("index", 0)))
	if err != nil {
		return mcp.NewToolResultError(err.Error()), nil
	}
	if result == nil {
		return mcp.NewToolResultText("Uncle not found"), nil
	}
	return mcp.NewToolResultText(toJSONText(result)), nil
}

func (e *ErigonMCPServer) handleGetUncleByBlockHashAndIndex(ctx context.Context, req mcp.CallToolRequest) (*mcp.CallToolResult, error) {
	hash := common.HexToHash(req.GetString("blockHash", ""))
	result, err := e.ethAPI.GetUncleByBlockHashAndIndex(ctx, hash, hexutil.Uint(req.GetInt("index", 0)))
	if err != nil {
		return mcp.NewToolResultError(err.Error()), nil
	}
	if result == nil {
		return mcp.NewToolResultText("Uncle not found"), nil
	}
	return mcp.NewToolResultText(toJSONText(result)), nil
}

func (e *ErigonMCPServer) handleGetUncleCountByBlockNumber(ctx context.Context, req mcp.CallToolRequest) (*mcp.CallToolResult, error) {
	blockNum, _ := parseBlockNumber(req.GetString("blockNumber", "latest"))
	count, err := e.ethAPI.GetUncleCountByBlockNumber(ctx, blockNum)
	if err != nil {
		return mcp.NewToolResultError(err.Error()), nil
	}
	if count == nil {
		return mcp.NewToolResultText("Block not found"), nil
	}
	return mcp.NewToolResultText(fmt.Sprintf("Uncle count: %d", uint64(*count))), nil
}

func (e *ErigonMCPServer) handleGetUncleCountByBlockHash(ctx context.Context, req mcp.CallToolRequest) (*mcp.CallToolResult, error) {
	hash := common.HexToHash(req.GetString("blockHash", ""))
	count, err := e.ethAPI.GetUncleCountByBlockHash(ctx, hash)
	if err != nil {
		return mcp.NewToolResultError(err.Error()), nil
	}
	if count == nil {
		return mcp.NewToolResultText("Block not found"), nil
	}
	return mcp.NewToolResultText(fmt.Sprintf("Uncle count: %d", uint64(*count))), nil
}

// ===== ERIGON API HANDLERS =====

func (e *ErigonMCPServer) handleErigonForks(ctx context.Context, req mcp.CallToolRequest) (*mcp.CallToolResult, error) {
	forks, err := e.erigonAPI.Forks(ctx)
	if err != nil {
		return mcp.NewToolResultError(err.Error()), nil
	}
	return mcp.NewToolResultText(toJSONText(forks)), nil
}

func (e *ErigonMCPServer) handleErigonBlockNumber(ctx context.Context, req mcp.CallToolRequest) (*mcp.CallToolResult, error) {
	var blockNumPtr *rpc.BlockNumber
	if s := req.GetString("blockNumber", ""); s != "" {
		bn, _ := parseBlockNumber(s)
		blockNumPtr = &bn
	}
	result, err := e.erigonAPI.BlockNumber(ctx, blockNumPtr)
	if err != nil {
		return mcp.NewToolResultError(err.Error()), nil
	}
	return mcp.NewToolResultText(fmt.Sprintf("Block number: %d", uint64(result))), nil
}

func (e *ErigonMCPServer) handleErigonGetHeaderByNumber(ctx context.Context, req mcp.CallToolRequest) (*mcp.CallToolResult, error) {
	blockNum, _ := parseBlockNumber(req.GetString("blockNumber", "latest"))
	header, err := e.erigonAPI.GetHeaderByNumber(ctx, blockNum)
	if err != nil {
		return mcp.NewToolResultError(err.Error()), nil
	}
	if header == nil {
		return mcp.NewToolResultText("Header not found"), nil
	}
	return mcp.NewToolResultText(toJSONText(header)), nil
}

func (e *ErigonMCPServer) handleErigonGetHeaderByHash(ctx context.Context, req mcp.CallToolRequest) (*mcp.CallToolResult, error) {
	hash := common.HexToHash(req.GetString("blockHash", ""))
	header, err := e.erigonAPI.GetHeaderByHash(ctx, hash)
	if err != nil {
		return mcp.NewToolResultError(err.Error()), nil
	}
	if header == nil {
		return mcp.NewToolResultText("Header not found"), nil
	}
	return mcp.NewToolResultText(toJSONText(header)), nil
}

func (e *ErigonMCPServer) handleErigonGetBlockByTimestamp(ctx context.Context, req mcp.CallToolRequest) (*mcp.CallToolResult, error) {
	tsStr := req.GetString("timestamp", "")
	var ts rpc.Timestamp
	ts.UnmarshalJSON([]byte(`"` + tsStr + `"`))
	result, err := e.erigonAPI.GetBlockByTimestamp(ctx, ts, req.GetBool("fullTransactions", false))
	if err != nil {
		return mcp.NewToolResultError(err.Error()), nil
	}
	if result == nil {
		return mcp.NewToolResultText("Block not found"), nil
	}
	return mcp.NewToolResultText(toJSONText(result)), nil
}

func (e *ErigonMCPServer) handleErigonGetBalanceChangesInBlock(ctx context.Context, req mcp.CallToolRequest) (*mcp.CallToolResult, error) {
	blockNumOrHash, _ := parseBlockNumberOrHash(req.GetString("blockNumberOrHash", "latest"))
	result, err := e.erigonAPI.GetBalanceChangesInBlock(ctx, blockNumOrHash)
	if err != nil {
		return mcp.NewToolResultError(err.Error()), nil
	}
	return mcp.NewToolResultText(toJSONText(result)), nil
}

func (e *ErigonMCPServer) handleErigonGetLogsByHash(ctx context.Context, req mcp.CallToolRequest) (*mcp.CallToolResult, error) {
	hash := common.HexToHash(req.GetString("blockHash", ""))
	logs, err := e.erigonAPI.GetLogsByHash(ctx, hash)
	if err != nil {
		return mcp.NewToolResultError(err.Error()), nil
	}
	return mcp.NewToolResultText(toJSONText(logs)), nil
}

func (e *ErigonMCPServer) handleErigonGetLogs(ctx context.Context, req mcp.CallToolRequest) (*mcp.CallToolResult, error) {
	var crit filters.FilterCriteria
	if from := req.GetString("fromBlock", ""); from != "" {
		if bn, err := parseBlockNumber(from); err == nil {
			crit.FromBlock = big.NewInt(bn.Int64())
		}
	}
	if to := req.GetString("toBlock", ""); to != "" {
		if bn, err := parseBlockNumber(to); err == nil {
			crit.ToBlock = big.NewInt(bn.Int64())
		}
	}
	if addr := req.GetString("address", ""); addr != "" {
		crit.Addresses = []common.Address{common.HexToAddress(addr)}
	}
	if topics := req.GetString("topics", ""); topics != "" {
		var t [][]common.Hash
		json.Unmarshal([]byte(topics), &t)
		crit.Topics = t
	}
	result, err := e.erigonAPI.GetLogs(ctx, crit)
	if err != nil {
		return mcp.NewToolResultError(err.Error()), nil
	}
	return mcp.NewToolResultText(toJSONText(result)), nil
}

func (e *ErigonMCPServer) handleErigonGetBlockReceiptsByBlockHash(ctx context.Context, req mcp.CallToolRequest) (*mcp.CallToolResult, error) {
	hash := common.HexToHash(req.GetString("blockHash", ""))
	receipts, err := e.erigonAPI.GetBlockReceiptsByBlockHash(ctx, hash)
	if err != nil {
		return mcp.NewToolResultError(err.Error()), nil
	}
	return mcp.NewToolResultText(toJSONText(receipts)), nil
}

func (e *ErigonMCPServer) handleErigonNodeInfo(ctx context.Context, req mcp.CallToolRequest) (*mcp.CallToolResult, error) {
	info, err := e.erigonAPI.NodeInfo(ctx)
	if err != nil {
		return mcp.NewToolResultError(err.Error()), nil
	}
	return mcp.NewToolResultText(toJSONText(info)), nil
}

// ===== METRICS HANDLERS =====
// Metrics handlers are now in handlers_metrics.go

// ===== OTTERSCAN HANDLERS =====

func (e *ErigonMCPServer) handleOtsGetApiLevel(ctx context.Context, req mcp.CallToolRequest) (*mcp.CallToolResult, error) {
	level := e.otsAPI.GetApiLevel()
	return mcp.NewToolResultText(fmt.Sprintf("Otterscan API Level: %d", level)), nil
}

func (e *ErigonMCPServer) handleOtsGetInternalOperations(ctx context.Context, req mcp.CallToolRequest) (*mcp.CallToolResult, error) {
	hash := common.HexToHash(req.GetString("txHash", ""))
	operations, err := e.otsAPI.GetInternalOperations(ctx, hash)
	if err != nil {
		return mcp.NewToolResultError(err.Error()), nil
	}
	if len(operations) == 0 {
		return mcp.NewToolResultText("No internal operations found"), nil
	}
	return mcp.NewToolResultText(toJSONText(operations)), nil
}

func (e *ErigonMCPServer) handleOtsSearchTransactionsBefore(ctx context.Context, req mcp.CallToolRequest) (*mcp.CallToolResult, error) {
	addr := common.HexToAddress(req.GetString("address", ""))
	blockNum := uint64(req.GetInt("blockNumber", 0))
	pageSize := uint16(req.GetInt("pageSize", 25))

	result, err := e.otsAPI.SearchTransactionsBefore(ctx, addr, blockNum, pageSize)
	if err != nil {
		return mcp.NewToolResultError(err.Error()), nil
	}
	return mcp.NewToolResultText(toJSONText(result)), nil
}

func (e *ErigonMCPServer) handleOtsSearchTransactionsAfter(ctx context.Context, req mcp.CallToolRequest) (*mcp.CallToolResult, error) {
	addr := common.HexToAddress(req.GetString("address", ""))
	blockNum := uint64(req.GetInt("blockNumber", 0))
	pageSize := uint16(req.GetInt("pageSize", 25))

	result, err := e.otsAPI.SearchTransactionsAfter(ctx, addr, blockNum, pageSize)
	if err != nil {
		return mcp.NewToolResultError(err.Error()), nil
	}
	return mcp.NewToolResultText(toJSONText(result)), nil
}

func (e *ErigonMCPServer) handleOtsGetBlockDetails(ctx context.Context, req mcp.CallToolRequest) (*mcp.CallToolResult, error) {
	blockNum, err := parseBlockNumber(req.GetString("blockNumber", "latest"))
	if err != nil {
		return mcp.NewToolResultError(err.Error()), nil
	}

	result, err := e.otsAPI.GetBlockDetails(ctx, blockNum)
	if err != nil {
		return mcp.NewToolResultError(err.Error()), nil
	}
	return mcp.NewToolResultText(toJSONText(result)), nil
}

func (e *ErigonMCPServer) handleOtsGetBlockDetailsByHash(ctx context.Context, req mcp.CallToolRequest) (*mcp.CallToolResult, error) {
	hash := common.HexToHash(req.GetString("blockHash", ""))

	result, err := e.otsAPI.GetBlockDetailsByHash(ctx, hash)
	if err != nil {
		return mcp.NewToolResultError(err.Error()), nil
	}
	return mcp.NewToolResultText(toJSONText(result)), nil
}

func (e *ErigonMCPServer) handleOtsGetBlockTransactions(ctx context.Context, req mcp.CallToolRequest) (*mcp.CallToolResult, error) {
	blockNum, err := parseBlockNumber(req.GetString("blockNumber", "latest"))
	if err != nil {
		return mcp.NewToolResultError(err.Error()), nil
	}
	pageNumber := uint8(req.GetInt("pageNumber", 0))
	pageSize := uint8(req.GetInt("pageSize", 25))

	result, err := e.otsAPI.GetBlockTransactions(ctx, blockNum, pageNumber, pageSize)
	if err != nil {
		return mcp.NewToolResultError(err.Error()), nil
	}
	return mcp.NewToolResultText(toJSONText(result)), nil
}

func (e *ErigonMCPServer) handleOtsHasCode(ctx context.Context, req mcp.CallToolRequest) (*mcp.CallToolResult, error) {
	addr := common.HexToAddress(req.GetString("address", ""))
	blockNumOrHash, _ := parseBlockNumberOrHash(req.GetString("blockNumber", "latest"))

	hasCode, err := e.otsAPI.HasCode(ctx, addr, blockNumOrHash)
	if err != nil {
		return mcp.NewToolResultError(err.Error()), nil
	}

	if hasCode {
		return mcp.NewToolResultText(fmt.Sprintf("Address %s has code (is a contract)", addr.Hex())), nil
	}
	return mcp.NewToolResultText(fmt.Sprintf("Address %s has no code (is an EOA)", addr.Hex())), nil
}

func (e *ErigonMCPServer) handleOtsTraceTransaction(ctx context.Context, req mcp.CallToolRequest) (*mcp.CallToolResult, error) {
	hash := common.HexToHash(req.GetString("txHash", ""))

	trace, err := e.otsAPI.TraceTransaction(ctx, hash)
	if err != nil {
		return mcp.NewToolResultError(err.Error()), nil
	}
	if len(trace) == 0 {
		return mcp.NewToolResultText("No trace entries found"), nil
	}
	return mcp.NewToolResultText(toJSONText(trace)), nil
}

func (e *ErigonMCPServer) handleOtsGetTransactionError(ctx context.Context, req mcp.CallToolRequest) (*mcp.CallToolResult, error) {
	hash := common.HexToHash(req.GetString("txHash", ""))

	errorData, err := e.otsAPI.GetTransactionError(ctx, hash)
	if err != nil {
		return mcp.NewToolResultError(err.Error()), nil
	}
	if len(errorData) == 0 {
		return mcp.NewToolResultText("Transaction succeeded (no error)"), nil
	}
	return mcp.NewToolResultText(fmt.Sprintf("Transaction error: %s", errorData.String())), nil
}

func (e *ErigonMCPServer) handleOtsGetTransactionBySenderAndNonce(ctx context.Context, req mcp.CallToolRequest) (*mcp.CallToolResult, error) {
	addr := common.HexToAddress(req.GetString("address", ""))
	nonce := uint64(req.GetInt("nonce", 0))

	txHash, err := e.otsAPI.GetTransactionBySenderAndNonce(ctx, addr, nonce)
	if err != nil {
		return mcp.NewToolResultError(err.Error()), nil
	}
	if txHash == nil {
		return mcp.NewToolResultText("Transaction not found"), nil
	}
	return mcp.NewToolResultText(fmt.Sprintf("Transaction hash: %s", txHash.Hex())), nil
}

func (e *ErigonMCPServer) handleOtsGetContractCreator(ctx context.Context, req mcp.CallToolRequest) (*mcp.CallToolResult, error) {
	addr := common.HexToAddress(req.GetString("address", ""))

	creator, err := e.otsAPI.GetContractCreator(ctx, addr)
	if err != nil {
		return mcp.NewToolResultError(err.Error()), nil
	}
	if creator == nil {
		return mcp.NewToolResultText("Contract creator not found"), nil
	}
	return mcp.NewToolResultText(toJSONText(creator)), nil
}

// ===== PROMPTS =====
// Prompts registration is now in prompts.go

// Metrics gathering functions are now in metrics/gather.go

// Serve starts MCP server in stdio mode
func (e *ErigonMCPServer) Serve() error {
	return server.ServeStdio(e.mcpServer)
}

// ServeSSE starts MCP server with SSE transport
func (e *ErigonMCPServer) ServeSSE(addr string) (err error) {
	sse := server.NewSSEServer(e.mcpServer)

	defer func() {
		if r := recover(); r != nil {
			log.Error("[MCP]: recovered from panic:", "panic", r)

			err = fmt.Errorf("mcp sse server panicked: %v", r)
		}
	}()

	return sse.Start(addr)
}
