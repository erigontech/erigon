package mcp

import (
	"fmt"

	"github.com/mark3labs/mcp-go/mcp"
	"github.com/mark3labs/mcp-go/server"
)

// registerTools registers the shared tool set; handlers must cover exactly the
// tools returned by toolSpecs.
func registerTools(srv *server.MCPServer, handlers map[string]server.ToolHandlerFunc) {
	specs := toolSpecs()
	if len(handlers) != len(specs) {
		panic(fmt.Sprintf("mcp: %d tool handlers for %d tools", len(handlers), len(specs)))
	}
	for _, tool := range specs {
		handler, ok := handlers[tool.Name]
		if !ok {
			panic(fmt.Sprintf("mcp: missing tool handler for %s", tool.Name))
		}
		srv.AddTool(tool, handler)
	}
}

// toolSpecs returns the tool definitions common to the embedded and standalone
// servers.
func toolSpecs() []mcp.Tool {
	return []mcp.Tool{
		// ===== ETH TOOLS =====

		mcp.NewTool("eth_blockNumber",
			mcp.WithDescription("Get the current block number"),
		),

		mcp.NewTool("eth_getBlockByNumber",
			mcp.WithDescription("Get block by number"),
			mcp.WithString("blockNumber", mcp.Required(), mcp.Description("Block number or tag")),
			mcp.WithBoolean("fullTransactions", mcp.Description("Return full tx objects")),
		),

		mcp.NewTool("eth_getBlockByHash",
			mcp.WithDescription("Get block by hash"),
			mcp.WithString("blockHash", mcp.Required(), mcp.Description("Block hash")),
			mcp.WithBoolean("fullTransactions", mcp.Description("Return full tx objects")),
		),

		mcp.NewTool("eth_getBlockTransactionCountByNumber",
			mcp.WithDescription("Get transaction count in block by number"),
			mcp.WithString("blockNumber", mcp.Required(), mcp.Description("Block number")),
		),

		mcp.NewTool("eth_getBlockTransactionCountByHash",
			mcp.WithDescription("Get transaction count in block by hash"),
			mcp.WithString("blockHash", mcp.Required(), mcp.Description("Block hash")),
		),

		mcp.NewTool("eth_getBalance",
			mcp.WithDescription("Get address balance"),
			mcp.WithString("address", mcp.Required(), mcp.Description("Address")),
			mcp.WithString("blockNumber", mcp.Description("Block number (default: latest)")),
		),

		mcp.NewTool("eth_getTransactionByHash",
			mcp.WithDescription("Get transaction by hash"),
			mcp.WithString("txHash", mcp.Required(), mcp.Description("Transaction hash")),
		),

		mcp.NewTool("eth_getTransactionByBlockHashAndIndex",
			mcp.WithDescription("Get transaction by block hash and index"),
			mcp.WithString("blockHash", mcp.Required(), mcp.Description("Block hash")),
			mcp.WithNumber("index", mcp.Required(), mcp.Description("Transaction index")),
		),

		mcp.NewTool("eth_getTransactionByBlockNumberAndIndex",
			mcp.WithDescription("Get transaction by block number and index"),
			mcp.WithString("blockNumber", mcp.Required(), mcp.Description("Block number")),
			mcp.WithNumber("index", mcp.Required(), mcp.Description("Transaction index")),
		),

		mcp.NewTool("eth_getTransactionReceipt",
			mcp.WithDescription("Get transaction receipt"),
			mcp.WithString("txHash", mcp.Required(), mcp.Description("Transaction hash")),
		),

		mcp.NewTool("eth_getBlockReceipts",
			mcp.WithDescription("Get all receipts for a block"),
			mcp.WithString("blockNumberOrHash", mcp.Required(), mcp.Description("Block number or hash")),
		),

		mcp.NewTool("eth_getLogs",
			mcp.WithDescription("Get logs matching filter"),
			mcp.WithString("fromBlock", mcp.Description("Start block")),
			mcp.WithString("toBlock", mcp.Description("End block")),
			mcp.WithString("address", mcp.Description("Contract address(es)")),
			mcp.WithString("topics", mcp.Description("Topics array (JSON)")),
			mcp.WithString("blockHash", mcp.Description("Single block hash")),
		),

		mcp.NewTool("eth_getCode",
			mcp.WithDescription("Get contract code"),
			mcp.WithString("address", mcp.Required(), mcp.Description("Contract address")),
			mcp.WithString("blockNumber", mcp.Description("Block number")),
		),

		mcp.NewTool("eth_getStorageAt",
			mcp.WithDescription("Get storage at position"),
			mcp.WithString("address", mcp.Required(), mcp.Description("Address")),
			mcp.WithString("position", mcp.Required(), mcp.Description("Storage position")),
			mcp.WithString("blockNumber", mcp.Description("Block number")),
		),

		mcp.NewTool("eth_getTransactionCount",
			mcp.WithDescription("Get nonce (transaction count)"),
			mcp.WithString("address", mcp.Required(), mcp.Description("Address")),
			mcp.WithString("blockNumber", mcp.Description("Block number")),
		),

		mcp.NewTool("eth_call",
			mcp.WithDescription("Execute call without transaction"),
			mcp.WithString("to", mcp.Required(), mcp.Description("Contract address")),
			mcp.WithString("data", mcp.Required(), mcp.Description("Call data")),
			mcp.WithString("from", mcp.Description("Sender address")),
			mcp.WithString("value", mcp.Description("Value (hex)")),
			mcp.WithString("gas", mcp.Description("Gas limit (hex)")),
			mcp.WithString("blockNumber", mcp.Description("Block number")),
		),

		mcp.NewTool("eth_estimateGas",
			mcp.WithDescription("Estimate gas for transaction"),
			mcp.WithString("to", mcp.Description("To address")),
			mcp.WithString("data", mcp.Description("Call data")),
			mcp.WithString("from", mcp.Description("From address")),
			mcp.WithString("value", mcp.Description("Value (hex)")),
		),

		mcp.NewTool("eth_gasPrice",
			mcp.WithDescription("Get current gas price"),
		),

		mcp.NewTool("eth_chainId",
			mcp.WithDescription("Get chain ID"),
		),

		mcp.NewTool("eth_syncing",
			mcp.WithDescription("Get sync status"),
		),

		mcp.NewTool("eth_accounts",
			mcp.WithDescription("Get accounts"),
		),

		mcp.NewTool("eth_getProof",
			mcp.WithDescription("Get Merkle proof"),
			mcp.WithString("address", mcp.Required(), mcp.Description("Address")),
			mcp.WithString("storageKeys", mcp.Description("Storage keys (JSON array)")),
			mcp.WithString("blockNumber", mcp.Description("Block number")),
		),

		mcp.NewTool("eth_coinbase",
			mcp.WithDescription("Get coinbase address"),
		),

		mcp.NewTool("eth_mining",
			mcp.WithDescription("Check if mining"),
		),

		mcp.NewTool("eth_hashrate",
			mcp.WithDescription("Get hashrate"),
		),

		mcp.NewTool("eth_protocolVersion",
			mcp.WithDescription("Get protocol version"),
		),

		mcp.NewTool("eth_getUncleByBlockNumberAndIndex",
			mcp.WithDescription("Get uncle by block number and index"),
			mcp.WithString("blockNumber", mcp.Required(), mcp.Description("Block number")),
			mcp.WithNumber("index", mcp.Required(), mcp.Description("Uncle index")),
		),

		mcp.NewTool("eth_getUncleByBlockHashAndIndex",
			mcp.WithDescription("Get uncle by block hash and index"),
			mcp.WithString("blockHash", mcp.Required(), mcp.Description("Block hash")),
			mcp.WithNumber("index", mcp.Required(), mcp.Description("Uncle index")),
		),

		mcp.NewTool("eth_getUncleCountByBlockNumber",
			mcp.WithDescription("Get uncle count by block number"),
			mcp.WithString("blockNumber", mcp.Required(), mcp.Description("Block number")),
		),

		mcp.NewTool("eth_getUncleCountByBlockHash",
			mcp.WithDescription("Get uncle count by block hash"),
			mcp.WithString("blockHash", mcp.Required(), mcp.Description("Block hash")),
		),

		// ===== ERIGON TOOLS =====

		mcp.NewTool("erigon_forks",
			mcp.WithDescription("Get fork information"),
		),

		mcp.NewTool("erigon_blockNumber",
			mcp.WithDescription("Get block number (Erigon)"),
			mcp.WithString("blockNumber", mcp.Description("Block tag")),
		),

		mcp.NewTool("erigon_getHeaderByNumber",
			mcp.WithDescription("Get header by number"),
			mcp.WithString("blockNumber", mcp.Required(), mcp.Description("Block number")),
		),

		mcp.NewTool("erigon_getHeaderByHash",
			mcp.WithDescription("Get header by hash"),
			mcp.WithString("blockHash", mcp.Required(), mcp.Description("Block hash")),
		),

		mcp.NewTool("erigon_getBlockByTimestamp",
			mcp.WithDescription("Get block by timestamp"),
			mcp.WithString("timestamp", mcp.Required(), mcp.Description("Unix timestamp")),
			mcp.WithBoolean("fullTransactions", mcp.Description("Full tx objects")),
		),

		mcp.NewTool("erigon_getBalanceChangesInBlock",
			mcp.WithDescription("Get all balance changes in block"),
			mcp.WithString("blockNumberOrHash", mcp.Required(), mcp.Description("Block")),
		),

		mcp.NewTool("erigon_getLogsByHash",
			mcp.WithDescription("Get logs by block hash"),
			mcp.WithString("blockHash", mcp.Required(), mcp.Description("Block hash")),
		),

		mcp.NewTool("erigon_getLogs",
			mcp.WithDescription("Get logs (Erigon format)"),
			mcp.WithString("fromBlock", mcp.Description("From block")),
			mcp.WithString("toBlock", mcp.Description("To block")),
			mcp.WithString("address", mcp.Description("Address")),
			mcp.WithString("topics", mcp.Description("Topics (JSON)")),
		),

		mcp.NewTool("erigon_getBlockReceiptsByBlockHash",
			mcp.WithDescription("Get block receipts by hash"),
			mcp.WithString("blockHash", mcp.Required(), mcp.Description("Block hash")),
		),

		mcp.NewTool("erigon_nodeInfo",
			mcp.WithDescription("Get P2P node info"),
		),

		// ===== METRICS TOOLS =====

		mcp.NewTool("metrics_list",
			mcp.WithDescription("List all available metric names"),
		),

		mcp.NewTool("metrics_get",
			mcp.WithDescription("Get metrics with optional filtering by pattern (supports wildcards like 'db_*', '*_size', etc.)"),
			mcp.WithString("pattern", mcp.Description("Metric name pattern (optional, empty = all metrics)")),
		),

		// ===== LOG TOOLS =====

		mcp.NewTool("logs_tail",
			mcp.WithDescription("Get last N lines from erigon or torrent logs"),
			mcp.WithString("log_type", mcp.Description("Log type: 'erigon' or 'torrent' (default: erigon)")),
			mcp.WithNumber("lines", mcp.Description("Number of lines to retrieve (default: 100, max: 10000)")),
			mcp.WithString("filter", mcp.Description("Optional string to filter log lines")),
		),

		mcp.NewTool("logs_head",
			mcp.WithDescription("Get first N lines from erigon or torrent logs"),
			mcp.WithString("log_type", mcp.Description("Log type: 'erigon' or 'torrent' (default: erigon)")),
			mcp.WithNumber("lines", mcp.Description("Number of lines to retrieve (default: 100, max: 10000)")),
			mcp.WithString("filter", mcp.Description("Optional string to filter log lines")),
		),

		mcp.NewTool("logs_grep",
			mcp.WithDescription("Search for a pattern in erigon or torrent logs"),
			mcp.WithString("log_type", mcp.Description("Log type: 'erigon' or 'torrent' (default: erigon)")),
			mcp.WithString("pattern", mcp.Required(), mcp.Description("Search pattern")),
			mcp.WithNumber("max_lines", mcp.Description("Maximum matching lines to return (default: 1000, max: 10000)")),
			mcp.WithBoolean("case_insensitive", mcp.Description("Case-insensitive search (default: false)")),
		),

		mcp.NewTool("logs_stats",
			mcp.WithDescription("Get statistics about erigon or torrent logs"),
			mcp.WithString("log_type", mcp.Description("Log type: 'erigon' or 'torrent' (default: erigon)")),
		),

		// ===== OTTERSCAN TOOLS =====

		mcp.NewTool("ots_getApiLevel",
			mcp.WithDescription("Get Otterscan API level"),
		),

		mcp.NewTool("ots_getInternalOperations",
			mcp.WithDescription("Get internal operations (internal txs) for a transaction"),
			mcp.WithString("txHash", mcp.Required(), mcp.Description("Transaction hash")),
		),

		mcp.NewTool("ots_searchTransactionsBefore",
			mcp.WithDescription("Search transactions before a given block for an address"),
			mcp.WithString("address", mcp.Required(), mcp.Description("Address")),
			mcp.WithNumber("blockNumber", mcp.Required(), mcp.Description("Block number")),
			mcp.WithNumber("pageSize", mcp.Description("Page size (default: 25)")),
		),

		mcp.NewTool("ots_searchTransactionsAfter",
			mcp.WithDescription("Search transactions after a given block for an address"),
			mcp.WithString("address", mcp.Required(), mcp.Description("Address")),
			mcp.WithNumber("blockNumber", mcp.Required(), mcp.Description("Block number")),
			mcp.WithNumber("pageSize", mcp.Description("Page size (default: 25)")),
		),

		mcp.NewTool("ots_getBlockDetails",
			mcp.WithDescription("Get detailed block information"),
			mcp.WithString("blockNumber", mcp.Required(), mcp.Description("Block number or tag")),
		),

		mcp.NewTool("ots_getBlockDetailsByHash",
			mcp.WithDescription("Get detailed block information by hash"),
			mcp.WithString("blockHash", mcp.Required(), mcp.Description("Block hash")),
		),

		mcp.NewTool("ots_getBlockTransactions",
			mcp.WithDescription("Get paginated transactions for a block"),
			mcp.WithString("blockNumber", mcp.Required(), mcp.Description("Block number or tag")),
			mcp.WithNumber("pageNumber", mcp.Description("Page number (default: 0)")),
			mcp.WithNumber("pageSize", mcp.Description("Page size (default: 25)")),
		),

		mcp.NewTool("ots_hasCode",
			mcp.WithDescription("Check if an address has code (is a contract)"),
			mcp.WithString("address", mcp.Required(), mcp.Description("Address")),
			mcp.WithString("blockNumber", mcp.Description("Block number or tag")),
		),

		mcp.NewTool("ots_traceTransaction",
			mcp.WithDescription("Get trace for a transaction"),
			mcp.WithString("txHash", mcp.Required(), mcp.Description("Transaction hash")),
		),

		mcp.NewTool("ots_getTransactionError",
			mcp.WithDescription("Get transaction error/revert reason"),
			mcp.WithString("txHash", mcp.Required(), mcp.Description("Transaction hash")),
		),

		mcp.NewTool("ots_getTransactionBySenderAndNonce",
			mcp.WithDescription("Get transaction hash by sender address and nonce"),
			mcp.WithString("address", mcp.Required(), mcp.Description("Sender address")),
			mcp.WithNumber("nonce", mcp.Required(), mcp.Description("Nonce")),
		),

		mcp.NewTool("ots_getContractCreator",
			mcp.WithDescription("Get contract creator address and transaction"),
			mcp.WithString("address", mcp.Required(), mcp.Description("Contract address")),
		),
	}
}

// storageValuesTool is registered by the embedded server only.
func storageValuesTool() mcp.Tool {
	return mcp.NewTool("eth_getStorageValues",
		mcp.WithDescription("Get multiple storage slot values for multiple accounts in a single request"),
		mcp.WithString("requests", mcp.Required(), mcp.Description("JSON object mapping addresses to arrays of storage slot keys")),
		mcp.WithString("blockNumber", mcp.Description("Block number or tag (latest, earliest, pending)")),
	)
}
