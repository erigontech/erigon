package mcp

import (
	"context"
	"encoding/json"
	"fmt"
	"math/big"
	"net/http"
	"os"
	"strings"

	"github.com/mark3labs/mcp-go/mcp"
	"github.com/mark3labs/mcp-go/server"

	"github.com/erigontech/erigon/common"
	"github.com/erigontech/erigon/common/hexutil"
	"github.com/erigontech/erigon/common/log/v3"
	"github.com/erigontech/erigon/db/kv"
	chainspec "github.com/erigontech/erigon/execution/chain/spec"
	"github.com/erigontech/erigon/rpc"
	"github.com/erigontech/erigon/rpc/ethapi"
	"github.com/erigontech/erigon/rpc/filters"
	"github.com/erigontech/erigon/rpc/jsonrpc"
)

// Version is the MCP server version reported to clients.
const Version = "0.0.2"

// MCPTransport is the common interface for both embedded and standalone MCP servers.
type MCPTransport interface {
	Serve() error
	ServeContext(ctx context.Context) error
	ServeSSE(addr string) error
}

// ErigonMCPServer wraps Erigon APIs with MCP server capabilities.
type ErigonMCPServer struct {
	mcpServer *server.MCPServer
	ethAPI    jsonrpc.EthAPI
	erigonAPI jsonrpc.ErigonAPI
	otsAPI    jsonrpc.OtterscanAPI
	logTools
}

// NewErigonMCPServer creates a new MCP server for Erigon.
func NewErigonMCPServer(ethAPI jsonrpc.EthAPI, erigonAPI jsonrpc.ErigonAPI, otsAPI jsonrpc.OtterscanAPI, logDir string) *ErigonMCPServer {
	e := &ErigonMCPServer{
		ethAPI:    ethAPI,
		erigonAPI: erigonAPI,
		otsAPI:    otsAPI,
		logTools:  logTools{logDir: logDir},
	}

	e.mcpServer = server.NewMCPServer(
		"ErigonMCP",
		Version,
		server.WithResourceCapabilities(true, true),
		server.WithToolCapabilities(true),
		server.WithPromptCapabilities(true),
		server.WithLogging(),
		server.WithRecovery(),
	)

	registerTools(e.mcpServer, e.toolHandlers())
	e.mcpServer.AddTool(storageValuesTool(), e.handleGetStorageValues)
	registerPrompts(e.mcpServer)
	registerResources(e.mcpServer, e.resourceHandlers())

	return e
}

func parseBlockNumberOrHash(s string) (rpc.BlockNumberOrHash, error) {
	var result rpc.BlockNumberOrHash
	b, err := json.Marshal(s)
	if err != nil {
		return result, err
	}
	return result, result.UnmarshalJSON(b)
}

func (e *ErigonMCPServer) toolHandlers() map[string]server.ToolHandlerFunc {
	return map[string]server.ToolHandlerFunc{
		"eth_blockNumber":                         e.handleBlockNumber,
		"eth_getBlockByNumber":                    e.handleGetBlockByNumber,
		"eth_getBlockByHash":                      e.handleGetBlockByHash,
		"eth_getBlockTransactionCountByNumber":    e.handleGetBlockTransactionCountByNumber,
		"eth_getBlockTransactionCountByHash":      e.handleGetBlockTransactionCountByHash,
		"eth_getBalance":                          e.handleGetBalance,
		"eth_getTransactionByHash":                e.handleGetTransactionByHash,
		"eth_getTransactionByBlockHashAndIndex":   e.handleGetTransactionByBlockHashAndIndex,
		"eth_getTransactionByBlockNumberAndIndex": e.handleGetTransactionByBlockNumberAndIndex,
		"eth_getTransactionReceipt":               e.handleGetTransactionReceipt,
		"eth_getBlockReceipts":                    e.handleGetBlockReceipts,
		"eth_getLogs":                             e.handleGetLogs,
		"eth_getCode":                             e.handleGetCode,
		"eth_getStorageAt":                        e.handleGetStorageAt,
		"eth_getTransactionCount":                 e.handleGetTransactionCount,
		"eth_call":                                e.handleCall,
		"eth_estimateGas":                         e.handleEstimateGas,
		"eth_gasPrice":                            e.handleGasPrice,
		"eth_chainId":                             e.handleChainId,
		"eth_syncing":                             e.handleSyncing,
		"eth_accounts":                            e.handleAccounts,
		"eth_getProof":                            e.handleGetProof,
		"eth_coinbase":                            e.handleCoinbase,
		"eth_mining":                              e.handleMining,
		"eth_hashrate":                            e.handleHashrate,
		"eth_protocolVersion":                     e.handleProtocolVersion,
		"eth_getUncleByBlockNumberAndIndex":       e.handleGetUncleByBlockNumberAndIndex,
		"eth_getUncleByBlockHashAndIndex":         e.handleGetUncleByBlockHashAndIndex,
		"eth_getUncleCountByBlockNumber":          e.handleGetUncleCountByBlockNumber,
		"eth_getUncleCountByBlockHash":            e.handleGetUncleCountByBlockHash,
		"erigon_forks":                            e.handleErigonForks,
		"erigon_blockNumber":                      e.handleErigonBlockNumber,
		"erigon_getHeaderByNumber":                e.handleErigonGetHeaderByNumber,
		"erigon_getHeaderByHash":                  e.handleErigonGetHeaderByHash,
		"erigon_getBlockByTimestamp":              e.handleErigonGetBlockByTimestamp,
		"erigon_getBalanceChangesInBlock":         e.handleErigonGetBalanceChangesInBlock,
		"erigon_getLogsByHash":                    e.handleErigonGetLogsByHash,
		"erigon_getLogs":                          e.handleErigonGetLogs,
		"erigon_getBlockReceiptsByBlockHash":      e.handleErigonGetBlockReceiptsByBlockHash,
		"erigon_nodeInfo":                         e.handleErigonNodeInfo,
		"metrics_list":                            e.handleMetricsList,
		"metrics_get":                             e.handleMetricsGet,
		"logs_tail":                               e.handleLogsTail,
		"logs_head":                               e.handleLogsHead,
		"logs_grep":                               e.handleLogsGrep,
		"logs_stats":                              e.handleLogsStats,
		"ots_getApiLevel":                         e.handleOtsGetApiLevel,
		"ots_getInternalOperations":               e.handleOtsGetInternalOperations,
		"ots_searchTransactionsBefore":            e.handleOtsSearchTransactionsBefore,
		"ots_searchTransactionsAfter":             e.handleOtsSearchTransactionsAfter,
		"ots_getBlockDetails":                     e.handleOtsGetBlockDetails,
		"ots_getBlockDetailsByHash":               e.handleOtsGetBlockDetailsByHash,
		"ots_getBlockTransactions":                e.handleOtsGetBlockTransactions,
		"ots_hasCode":                             e.handleOtsHasCode,
		"ots_traceTransaction":                    e.handleOtsTraceTransaction,
		"ots_getTransactionError":                 e.handleOtsGetTransactionError,
		"ots_getTransactionBySenderAndNonce":      e.handleOtsGetTransactionBySenderAndNonce,
		"ots_getContractCreator":                  e.handleOtsGetContractCreator,
	}
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
	blockNumOrHash, err := parseBlockNumberOrHash(req.GetString("blockNumber", "latest"))
	if err != nil {
		return mcp.NewToolResultError(err.Error()), nil
	}
	balance, err := e.ethAPI.GetBalance(ctx, addr, &blockNumOrHash)
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
	blockNum, err := parseBlockNumber(req.GetString("blockNumber", "latest"))
	if err != nil {
		return mcp.NewToolResultError(err.Error()), nil
	}
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
	blockNumOrHash, err := parseBlockNumberOrHash(req.GetString("blockNumberOrHash", "latest"))
	if err != nil {
		return mcp.NewToolResultError(err.Error()), nil
	}
	result, err := e.ethAPI.GetBlockReceipts(ctx, blockNumOrHash)
	if err != nil {
		return mcp.NewToolResultError(err.Error()), nil
	}
	return mcp.NewToolResultText(toJSONText(result)), nil
}

func (e *ErigonMCPServer) handleGetLogs(ctx context.Context, req mcp.CallToolRequest) (*mcp.CallToolResult, error) {
	var crit filters.FilterCriteria

	if from := req.GetString("fromBlock", ""); from != "" {
		bn, err := parseBlockNumber(from)
		if err != nil {
			return mcp.NewToolResultError(err.Error()), nil
		}
		crit.FromBlock = big.NewInt(bn.Int64())
	}
	if to := req.GetString("toBlock", ""); to != "" {
		bn, err := parseBlockNumber(to)
		if err != nil {
			return mcp.NewToolResultError(err.Error()), nil
		}
		crit.ToBlock = big.NewInt(bn.Int64())
	}
	if addr := req.GetString("address", ""); addr != "" {
		if strings.HasPrefix(addr, "[") {
			var addrs []string
			if err := json.Unmarshal([]byte(addr), &addrs); err != nil {
				return mcp.NewToolResultError(fmt.Sprintf("invalid address JSON array: %v", err)), nil
			}
			for _, a := range addrs {
				crit.Addresses = append(crit.Addresses, common.HexToAddress(a))
			}
		} else {
			crit.Addresses = []common.Address{common.HexToAddress(addr)}
		}
	}
	if topics := req.GetString("topics", ""); topics != "" {
		var t [][]common.Hash
		if err := json.Unmarshal([]byte(topics), &t); err != nil {
			return mcp.NewToolResultError(fmt.Sprintf("invalid topics JSON: %v", err)), nil
		}
		crit.Topics = t
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
	blockNumOrHash, err := parseBlockNumberOrHash(req.GetString("blockNumber", "latest"))
	if err != nil {
		return mcp.NewToolResultError(err.Error()), nil
	}
	code, err := e.ethAPI.GetCode(ctx, addr, &blockNumOrHash)
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
	blockNumOrHash, err := parseBlockNumberOrHash(req.GetString("blockNumber", "latest"))
	if err != nil {
		return mcp.NewToolResultError(err.Error()), nil
	}
	result, err := e.ethAPI.GetStorageAt(ctx, addr, pos, &blockNumOrHash)
	if err != nil {
		return mcp.NewToolResultError(err.Error()), nil
	}
	return mcp.NewToolResultText(fmt.Sprintf("Storage: %s", result)), nil
}

func (e *ErigonMCPServer) handleGetStorageValues(ctx context.Context, req mcp.CallToolRequest) (*mcp.CallToolResult, error) {
	rawRequests := req.GetString("requests", "{}")
	blockNumOrHash, err := parseBlockNumberOrHash(req.GetString("blockNumber", "latest"))
	if err != nil {
		return mcp.NewToolResultError(err.Error()), nil
	}

	// Parse the JSON requests map
	var parsed map[string][]string
	if err := json.Unmarshal([]byte(rawRequests), &parsed); err != nil {
		return mcp.NewToolResultError(fmt.Sprintf("invalid requests format: %v", err)), nil
	}

	// Convert to map[common.Address][]common.Hash
	requests := make(map[common.Address][]common.Hash, len(parsed))
	for addrStr, keys := range parsed {
		addr := common.HexToAddress(addrStr)
		hashes := make([]common.Hash, len(keys))
		for i, k := range keys {
			hashes[i] = common.HexToHash(k)
		}
		requests[addr] = hashes
	}

	result, err := e.ethAPI.GetStorageValues(ctx, requests, &blockNumOrHash)
	if err != nil {
		return mcp.NewToolResultError(err.Error()), nil
	}

	// Format output
	var sb strings.Builder
	for addr, vals := range result {
		sb.WriteString(fmt.Sprintf("Address: %s\n", addr.Hex()))
		for i, v := range vals {
			sb.WriteString(fmt.Sprintf("  Slot %d: %s\n", i, common.BytesToHash(v).Hex()))
		}
	}
	return mcp.NewToolResultText(sb.String()), nil
}

func (e *ErigonMCPServer) handleGetTransactionCount(ctx context.Context, req mcp.CallToolRequest) (*mcp.CallToolResult, error) {
	addr := common.HexToAddress(req.GetString("address", ""))
	blockNumOrHash, err := parseBlockNumberOrHash(req.GetString("blockNumber", "latest"))
	if err != nil {
		return mcp.NewToolResultError(err.Error()), nil
	}
	count, err := e.ethAPI.GetTransactionCount(ctx, addr, &blockNumOrHash)
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
		d, err := hexutil.Decode(data)
		if err != nil {
			return mcp.NewToolResultError(fmt.Sprintf("invalid data hex: %v", err)), nil
		}
		args.Data = (*hexutil.Bytes)(&d)
	}
	if from := req.GetString("from", ""); from != "" {
		f := common.HexToAddress(from)
		args.From = &f
	}
	if val := req.GetString("value", ""); val != "" {
		v, err := hexutil.DecodeBig(val)
		if err != nil {
			return mcp.NewToolResultError(fmt.Sprintf("invalid value hex: %v", err)), nil
		}
		args.Value = (*hexutil.Big)(v)
	}
	if gas := req.GetString("gas", ""); gas != "" {
		g, err := hexutil.DecodeUint64(gas)
		if err != nil {
			return mcp.NewToolResultError(err.Error()), nil
		}
		args.Gas = (*hexutil.Uint64)(&g)
	}

	blockNumOrHash, err := parseBlockNumberOrHash(req.GetString("blockNumber", "latest"))
	if err != nil {
		return mcp.NewToolResultError(err.Error()), nil
	}
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
		d, err := hexutil.Decode(data)
		if err != nil {
			return mcp.NewToolResultError(fmt.Sprintf("invalid data hex: %v", err)), nil
		}
		args.Data = (*hexutil.Bytes)(&d)
	}
	if from := req.GetString("from", ""); from != "" {
		f := common.HexToAddress(from)
		args.From = &f
	}
	if val := req.GetString("value", ""); val != "" {
		v, err := hexutil.DecodeBig(val)
		if err != nil {
			return mcp.NewToolResultError(fmt.Sprintf("invalid value hex: %v", err)), nil
		}
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
		if err := json.Unmarshal([]byte(k), &keys); err != nil {
			return mcp.NewToolResultError(fmt.Sprintf("invalid storageKeys JSON: %v", err)), nil
		}
	}
	blockNumOrHash, err := parseBlockNumberOrHash(req.GetString("blockNumber", "latest"))
	if err != nil {
		return mcp.NewToolResultError(err.Error()), nil
	}
	result, err := e.ethAPI.GetProof(ctx, addr, keys, &blockNumOrHash)
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
	blockNum, err := parseBlockNumber(req.GetString("blockNumber", "latest"))
	if err != nil {
		return mcp.NewToolResultError(err.Error()), nil
	}
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
	blockNum, err := parseBlockNumber(req.GetString("blockNumber", "latest"))
	if err != nil {
		return mcp.NewToolResultError(err.Error()), nil
	}
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
		bn, err := parseBlockNumber(s)
		if err != nil {
			return mcp.NewToolResultError(err.Error()), nil
		}
		blockNumPtr = &bn
	}
	result, err := e.erigonAPI.BlockNumber(ctx, blockNumPtr)
	if err != nil {
		return mcp.NewToolResultError(err.Error()), nil
	}
	return mcp.NewToolResultText(fmt.Sprintf("Block number: %d", uint64(result))), nil
}

func (e *ErigonMCPServer) handleErigonGetHeaderByNumber(ctx context.Context, req mcp.CallToolRequest) (*mcp.CallToolResult, error) {
	blockNum, err := parseBlockNumber(req.GetString("blockNumber", "latest"))
	if err != nil {
		return mcp.NewToolResultError(err.Error()), nil
	}
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
	blockNumOrHash, err := parseBlockNumberOrHash(req.GetString("blockNumberOrHash", "latest"))
	if err != nil {
		return mcp.NewToolResultError(err.Error()), nil
	}
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
		bn, err := parseBlockNumber(from)
		if err != nil {
			return mcp.NewToolResultError(err.Error()), nil
		}
		crit.FromBlock = big.NewInt(bn.Int64())
	}
	if to := req.GetString("toBlock", ""); to != "" {
		bn, err := parseBlockNumber(to)
		if err != nil {
			return mcp.NewToolResultError(err.Error()), nil
		}
		crit.ToBlock = big.NewInt(bn.Int64())
	}
	if addr := req.GetString("address", ""); addr != "" {
		crit.Addresses = []common.Address{common.HexToAddress(addr)}
	}
	if topics := req.GetString("topics", ""); topics != "" {
		var t [][]common.Hash
		if err := json.Unmarshal([]byte(topics), &t); err != nil {
			return mcp.NewToolResultError(fmt.Sprintf("invalid topics JSON: %v", err)), nil
		}
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
	blockNumOrHash, err := parseBlockNumberOrHash(req.GetString("blockNumber", "latest"))
	if err != nil {
		return mcp.NewToolResultError(err.Error()), nil
	}

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

// Serve starts MCP server in stdio mode with its own signal handling.
func (e *ErigonMCPServer) Serve() error {
	return server.ServeStdio(e.mcpServer)
}

// ServeContext starts MCP server in stdio mode using the provided context
// for lifecycle management, avoiding duplicate signal handlers.
func (e *ErigonMCPServer) ServeContext(ctx context.Context) error {
	// Apply NonBlockingAcquire so BeginRo fails fast (ErrServerOverloaded)
	// instead of blocking the entire sequential stdio session indefinitely.
	s := server.NewStdioServer(e.mcpServer)
	s.SetContextFunc(func(ctx context.Context) context.Context {
		return kv.WithNonBlockingAcquire(ctx)
	})
	return s.Listen(ctx, os.Stdin, os.Stdout)
}

// ServeSSE starts MCP server with SSE transport
func (e *ErigonMCPServer) ServeSSE(addr string) (err error) {
	// Apply NonBlockingAcquire so BeginRo fails fast (ErrServerOverloaded)
	// instead of blocking the SSE handler goroutine indefinitely when all DB
	// read slots are held by a concurrent write/ETL transaction. The HTTP RPC
	// layer sets the same flag in node/rpcstack.go.
	sse := server.NewSSEServer(e.mcpServer,
		server.WithSSEContextFunc(func(ctx context.Context, _ *http.Request) context.Context {
			return kv.WithNonBlockingAcquire(ctx)
		}),
	)

	defer func() {
		if r := recover(); r != nil {
			log.Error("[MCP]: recovered from panic:", "panic", r)

			err = fmt.Errorf("mcp sse server panicked: %v", r)
		}
	}()

	return sse.Start(addr)
}
