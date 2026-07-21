package mcp

import (
	"context"
	"encoding/json"
	"fmt"
	"math/big"
	"strings"

	"github.com/mark3labs/mcp-go/mcp"
	"github.com/mark3labs/mcp-go/server"

	chainspec "github.com/erigontech/erigon/execution/chain/spec"
)

// rpcCaller is the JSON-RPC dispatch dependency of the MCP server: a remote
// rpc.Client in proxy mode, an in-process one in embedded and datadir modes.
type rpcCaller interface {
	CallContext(ctx context.Context, result any, method string, args ...any) error
}

type paramKind int

const (
	pString   paramKind = iota // string, passed through
	pBool                      // boolean
	pInt                       // integer, passed as a JSON number
	pHexInt                    // integer, passed as a 0x-prefixed hex string
	pJSON                      // string containing JSON, passed as a raw JSON value
	pBlockNum                  // block number or tag; plain decimal is converted to hex
)

type param struct {
	name     string
	desc     string
	kind     paramKind
	required bool
	def      string // default for pString/pJSON when the argument is absent
	defInt   int    // default for pInt/pHexInt
	omit     bool   // drop from the call when absent; only valid on the last param
}

// toolCall declares one MCP tool backed by the JSON-RPC method of the same
// name. params drive both the tool schema and positional argument building;
// build and format override the generic behavior where a tool needs it.
type toolCall struct {
	name   string
	desc   string
	params []param
	build  func(req mcp.CallToolRequest) ([]any, error)
	format func(req mcp.CallToolRequest, raw json.RawMessage) string
	empty  string // replaces null/[] results, e.g. "Block not found"
}

func (c *toolCall) tool() mcp.Tool {
	opts := []mcp.ToolOption{mcp.WithDescription(c.desc), mcp.WithReadOnlyHintAnnotation(true)}
	for _, p := range c.params {
		popts := []mcp.PropertyOption{mcp.Description(p.desc)}
		if p.required {
			popts = append(popts, mcp.Required())
		}
		switch p.kind {
		case pBool:
			opts = append(opts, mcp.WithBoolean(p.name, popts...))
		case pInt, pHexInt:
			opts = append(opts, mcp.WithNumber(p.name, popts...))
		default:
			opts = append(opts, mcp.WithString(p.name, popts...))
		}
	}
	return mcp.NewTool(c.name, opts...)
}

func (c *toolCall) buildArgs(req mcp.CallToolRequest) ([]any, error) {
	if c.build != nil {
		return c.build(req)
	}
	args := make([]any, 0, len(c.params))
	for _, p := range c.params {
		switch p.kind {
		case pBool:
			args = append(args, req.GetBool(p.name, false))
		case pInt:
			args = append(args, req.GetInt(p.name, p.defInt))
		case pHexInt:
			args = append(args, fmt.Sprintf("0x%x", req.GetInt(p.name, p.defInt)))
		case pJSON:
			s := req.GetString(p.name, p.def)
			if s == "" {
				s = p.def
			}
			if s == "" {
				if p.omit {
					continue
				}
				s = "null"
			}
			raw, err := parseJSONParam(p.name, s)
			if err != nil {
				return nil, err
			}
			args = append(args, raw)
		case pBlockNum:
			v := req.GetString(p.name, p.def)
			if v == "" && p.omit {
				continue
			}
			args = append(args, normalizeBlockRef(v))
		default:
			v := req.GetString(p.name, p.def)
			if v == "" && p.omit {
				continue
			}
			args = append(args, v)
		}
	}
	return args, nil
}

// normalizeBlockRef converts a plain decimal block number to 0x-hex; tags,
// hex quantities and hashes pass through. Erigon's BlockNumber decoders
// accept decimal only as an unquoted JSON number, never as a quoted string.
func normalizeBlockRef(s string) string {
	s = strings.TrimSpace(s)
	if s == "" || strings.HasPrefix(s, "0x") {
		return s
	}
	if n, ok := new(big.Int).SetString(s, 10); ok && n.Sign() >= 0 {
		return "0x" + n.Text(16)
	}
	return s
}

func (c *toolCall) render(req mcp.CallToolRequest, raw json.RawMessage) string {
	if c.empty != "" && isNullResult(raw) {
		return c.empty
	}
	if c.format != nil {
		return c.format(req, raw)
	}
	return toJSONIndent(raw)
}

func (e *ErigonMCPServer) rpcToolHandler(c toolCall) server.ToolHandlerFunc {
	return func(ctx context.Context, req mcp.CallToolRequest) (*mcp.CallToolResult, error) {
		args, err := c.buildArgs(req)
		if err != nil {
			return mcp.NewToolResultError(err.Error()), nil
		}
		var raw json.RawMessage
		if err := e.client.CallContext(ctx, &raw, c.name, args...); err != nil {
			return mcp.NewToolResultError(err.Error()), nil
		}
		return mcp.NewToolResultText(c.render(req, raw)), nil
	}
}

func parseJSONParam(name, s string) (json.RawMessage, error) {
	var raw json.RawMessage
	if err := json.Unmarshal([]byte(s), &raw); err != nil {
		return nil, fmt.Errorf("invalid %s JSON: %w", name, err)
	}
	return raw, nil
}

func isNullResult(raw json.RawMessage) bool {
	return raw == nil || string(raw) == "null"
}

func isEmptyResult(raw json.RawMessage) bool {
	s := string(raw)
	return isNullResult(raw) || s == "[]" || s == `""` || s == `"0x"`
}

// ===== ARGUMENT BUILDERS =====

// buildLogFilter assembles the single filter-object argument of eth_getLogs /
// erigon_getLogs from flat tool arguments; absent tool params stay absent.
func buildLogFilter(req mcp.CallToolRequest) ([]any, error) {
	filter := map[string]any{}
	if v := req.GetString("fromBlock", ""); v != "" {
		filter["fromBlock"] = normalizeBlockRef(v)
	}
	if v := req.GetString("toBlock", ""); v != "" {
		filter["toBlock"] = normalizeBlockRef(v)
	}
	if v := req.GetString("address", ""); v != "" {
		if strings.HasPrefix(v, "[") {
			var addrs []string
			if err := json.Unmarshal([]byte(v), &addrs); err != nil {
				return nil, fmt.Errorf("invalid address JSON array: %w", err)
			}
			filter["address"] = addrs
		} else {
			filter["address"] = v
		}
	}
	if v := req.GetString("topics", ""); v != "" {
		topics, err := parseJSONParam("topics", v)
		if err != nil {
			return nil, err
		}
		filter["topics"] = topics
	}
	if v := req.GetString("blockHash", ""); v != "" {
		filter["blockHash"] = v
	}
	return []any{filter}, nil
}

func buildTxArgs(withBlock bool) func(req mcp.CallToolRequest) ([]any, error) {
	return func(req mcp.CallToolRequest) ([]any, error) {
		callObj := map[string]any{}
		for _, k := range []string{"to", "data", "from", "value", "gas"} {
			if v := req.GetString(k, ""); v != "" {
				callObj[k] = v
			}
		}
		if withBlock {
			return []any{callObj, normalizeBlockRef(req.GetString("blockNumber", "latest"))}, nil
		}
		return []any{callObj}, nil
	}
}

func buildStorageValues(req mcp.CallToolRequest) ([]any, error) {
	requests, err := parseJSONParam("requests", req.GetString("requests", "{}"))
	if err != nil {
		return nil, err
	}
	return []any{requests, normalizeBlockRef(req.GetString("blockNumber", "latest"))}, nil
}

// ===== RESULT FORMATTERS =====

func decodeQuantity(raw json.RawMessage) (string, *big.Int, bool) {
	var s string
	if err := json.Unmarshal(raw, &s); err != nil {
		return string(raw), nil, false
	}
	n, ok := new(big.Int).SetString(strings.TrimPrefix(s, "0x"), 16)
	if !ok {
		return s, nil, false
	}
	return s, n, true
}

// fmtQuantity renders a hex-quantity result as "<prefix><decimal><suffix>".
func fmtQuantity(prefix, suffix string) func(mcp.CallToolRequest, json.RawMessage) string {
	return func(_ mcp.CallToolRequest, raw json.RawMessage) string {
		hex, n, ok := decodeQuantity(raw)
		if !ok {
			return prefix + hex + suffix
		}
		return fmt.Sprintf("%s%d%s", prefix, n, suffix)
	}
}

// fmtQuantityEcho is fmtQuantity plus the original hex in parentheses.
func fmtQuantityEcho(prefix string) func(mcp.CallToolRequest, json.RawMessage) string {
	return func(_ mcp.CallToolRequest, raw json.RawMessage) string {
		hex, n, ok := decodeQuantity(raw)
		if !ok {
			return prefix + hex
		}
		return fmt.Sprintf("%s%d (%s)", prefix, n, hex)
	}
}

// fmtStringResult renders a JSON string result as "<prefix><value>".
func fmtStringResult(prefix string) func(mcp.CallToolRequest, json.RawMessage) string {
	return func(_ mcp.CallToolRequest, raw json.RawMessage) string {
		var s string
		if err := json.Unmarshal(raw, &s); err != nil {
			return prefix + string(raw)
		}
		return prefix + s
	}
}

// fmtRawResult renders the raw JSON result as "<prefix><raw>".
func fmtRawResult(prefix string) func(mcp.CallToolRequest, json.RawMessage) string {
	return func(_ mcp.CallToolRequest, raw json.RawMessage) string {
		return prefix + string(raw)
	}
}

func fmtBalance(_ mcp.CallToolRequest, raw json.RawMessage) string {
	hex, wei, ok := decodeQuantity(raw)
	if !ok {
		return "Balance: " + hex
	}
	eth := new(big.Float).Quo(new(big.Float).SetInt(wei), big.NewFloat(1e18))
	return fmt.Sprintf("Balance: %d wei (%.6f ETH)", wei, eth)
}

func fmtGasPrice(_ mcp.CallToolRequest, raw json.RawMessage) string {
	hex, wei, ok := decodeQuantity(raw)
	if !ok {
		return "Gas price: " + hex
	}
	gwei := new(big.Float).Quo(new(big.Float).SetInt(wei), big.NewFloat(1e9))
	return fmt.Sprintf("Gas price: %d wei (%.2f Gwei)", wei, gwei)
}

func fmtChainID(_ mcp.CallToolRequest, raw json.RawMessage) string {
	hex, n, ok := decodeQuantity(raw)
	if !ok {
		return "Chain ID: " + hex
	}
	if name := chainspec.NetworkNameByID[n.Uint64()]; name != "" {
		return fmt.Sprintf("Chain ID: %d (%s) - %s", n, hex, name)
	}
	return fmt.Sprintf("Chain ID: %d (%s)", n, hex)
}

func fmtCode(_ mcp.CallToolRequest, raw json.RawMessage) string {
	var code string
	if err := json.Unmarshal(raw, &code); err != nil {
		return string(raw)
	}
	if code == "" || code == "0x" {
		return "No code (EOA)"
	}
	return fmt.Sprintf("Code (%d bytes): %s", (len(code)-2)/2, code)
}

func fmtSyncing(_ mcp.CallToolRequest, raw json.RawMessage) string {
	if string(raw) == "false" {
		return "Fully synced"
	}
	return toJSONIndent(raw)
}

// fmtArrayOrMsg replaces a null or empty-array result with msg; tools where
// an empty array means "nothing found" rather than a valid empty answer.
func fmtArrayOrMsg(msg string) func(mcp.CallToolRequest, json.RawMessage) string {
	return func(_ mcp.CallToolRequest, raw json.RawMessage) string {
		if isEmptyResult(raw) {
			return msg
		}
		return toJSONIndent(raw)
	}
}

func fmtHasCode(req mcp.CallToolRequest, raw json.RawMessage) string {
	address := req.GetString("address", "")
	if string(raw) == "true" {
		return fmt.Sprintf("Address %s has code (is a contract)", address)
	}
	return fmt.Sprintf("Address %s has no code (is an EOA)", address)
}

func fmtTxError(_ mcp.CallToolRequest, raw json.RawMessage) string {
	if isEmptyResult(raw) {
		return "Transaction succeeded (no error)"
	}
	var s string
	if err := json.Unmarshal(raw, &s); err != nil {
		return "Transaction error: " + string(raw)
	}
	return "Transaction error: " + s
}

func fmtTxHashResult(_ mcp.CallToolRequest, raw json.RawMessage) string {
	if isEmptyResult(raw) {
		return "Transaction not found"
	}
	var s string
	if err := json.Unmarshal(raw, &s); err != nil {
		return string(raw)
	}
	return "Transaction hash: " + s
}

// ===== TOOL TABLE =====

// rpcToolCalls lists every JSON-RPC-backed MCP tool. Local tools (logs_*,
// metrics_*) are registered separately in NewErigonMCPServer.
func rpcToolCalls() []toolCall {
	blockNumberParam := param{name: "blockNumber", desc: "Block number or tag (default: latest)", kind: pBlockNum, def: "latest"}
	optBlockNumberParam := param{name: "blockNumber", desc: "Block number (default: latest)", kind: pBlockNum, def: "latest"}
	blockHashParam := param{name: "blockHash", desc: "Block hash", kind: pString, required: true}
	txHashParam := param{name: "txHash", desc: "Transaction hash", kind: pString, required: true}
	addressParam := param{name: "address", desc: "Address", kind: pString, required: true}
	fullTxParam := param{name: "fullTransactions", desc: "Return full tx objects", kind: pBool}

	return []toolCall{
		// ===== ETH TOOLS =====
		{
			name: "eth_blockNumber", desc: "Get the current block number",
			format: fmtQuantityEcho("Current block: "),
		},
		{
			name: "eth_getBlockByNumber", desc: "Get block by number",
			params: []param{blockNumberParam, fullTxParam},
			empty:  "Block not found",
		},
		{
			name: "eth_getBlockByHash", desc: "Get block by hash",
			params: []param{blockHashParam, fullTxParam},
			empty:  "Block not found",
		},
		{
			name: "eth_getBlockTransactionCountByNumber", desc: "Get transaction count in block by number",
			params: []param{blockNumberParam},
			format: fmtQuantity("Transaction count: ", ""),
			empty:  "Block not found",
		},
		{
			name: "eth_getBlockTransactionCountByHash", desc: "Get transaction count in block by hash",
			params: []param{blockHashParam},
			format: fmtQuantity("Transaction count: ", ""),
			empty:  "Block not found",
		},
		{
			name: "eth_getBalance", desc: "Get address balance",
			params: []param{addressParam, optBlockNumberParam},
			format: fmtBalance,
		},
		{
			name: "eth_getTransactionByHash", desc: "Get transaction by hash",
			params: []param{txHashParam},
			empty:  "Transaction not found",
		},
		{
			name: "eth_getTransactionByBlockHashAndIndex", desc: "Get transaction by block hash and index",
			params: []param{blockHashParam, {name: "index", desc: "Transaction index", kind: pHexInt, required: true}},
			empty:  "Transaction not found",
		},
		{
			name: "eth_getTransactionByBlockNumberAndIndex", desc: "Get transaction by block number and index",
			params: []param{blockNumberParam, {name: "index", desc: "Transaction index", kind: pHexInt, required: true}},
			empty:  "Transaction not found",
		},
		{
			name: "eth_getTransactionReceipt", desc: "Get transaction receipt",
			params: []param{txHashParam},
			empty:  "Receipt not found",
		},
		{
			name: "eth_getBlockReceipts", desc: "Get all receipts for a block",
			params: []param{{name: "blockNumberOrHash", desc: "Block number or hash (default: latest)", kind: pBlockNum, def: "latest"}},
			empty:  "Block receipts not found",
		},
		{
			name: "eth_getLogs", desc: "Get logs matching filter",
			params: []param{
				{name: "fromBlock", desc: "Start block", kind: pString},
				{name: "toBlock", desc: "End block", kind: pString},
				{name: "address", desc: "Contract address(es)", kind: pString},
				{name: "topics", desc: "Topics array (JSON)", kind: pString},
				{name: "blockHash", desc: "Single block hash", kind: pString},
			},
			build: buildLogFilter,
		},
		{
			name: "eth_getCode", desc: "Get contract code",
			params: []param{{name: "address", desc: "Contract address", kind: pString, required: true}, optBlockNumberParam},
			format: fmtCode,
		},
		{
			name: "eth_getStorageAt", desc: "Get storage at position",
			params: []param{addressParam, {name: "position", desc: "Storage position (default: 0x0)", kind: pString, def: "0x0"}, optBlockNumberParam},
			format: fmtStringResult("Storage: "),
		},
		{
			name: "eth_getTransactionCount", desc: "Get nonce (transaction count)",
			params: []param{addressParam, optBlockNumberParam},
			format: fmtQuantity("Nonce: ", ""),
		},
		{
			name: "eth_call", desc: "Execute call without transaction",
			params: []param{
				{name: "to", desc: "Contract address", kind: pString, required: true},
				{name: "data", desc: "Call data", kind: pString, required: true},
				{name: "from", desc: "Sender address", kind: pString},
				{name: "value", desc: "Value (hex)", kind: pString},
				{name: "gas", desc: "Gas limit (hex)", kind: pString},
				{name: "blockNumber", desc: "Block number", kind: pString},
			},
			build:  buildTxArgs(true),
			format: fmtStringResult("Result: "),
		},
		{
			name: "eth_estimateGas", desc: "Estimate gas for transaction",
			params: []param{
				{name: "to", desc: "To address", kind: pString},
				{name: "data", desc: "Call data", kind: pString},
				{name: "from", desc: "From address", kind: pString},
				{name: "value", desc: "Value (hex)", kind: pString},
			},
			build:  buildTxArgs(false),
			format: fmtQuantity("Estimated gas: ", ""),
		},
		{
			name: "eth_gasPrice", desc: "Get current gas price",
			format: fmtGasPrice,
		},
		{
			name: "eth_chainId", desc: "Get chain ID",
			format: fmtChainID,
		},
		{
			name: "eth_syncing", desc: "Get sync status",
			format: fmtSyncing,
		},
		{
			name: "eth_accounts", desc: "Get accounts",
			format: fmtArrayOrMsg("No accounts"),
		},
		{
			name: "eth_getProof", desc: "Get Merkle proof",
			params: []param{addressParam, {name: "storageKeys", desc: "Storage keys (JSON array)", kind: pJSON, def: "[]"}, optBlockNumberParam},
		},
		{
			name: "eth_coinbase", desc: "Get coinbase address",
			format: fmtStringResult("Coinbase: "),
		},
		{
			name: "eth_mining", desc: "Check if mining",
			format: fmtRawResult("Mining: "),
		},
		{
			name: "eth_hashrate", desc: "Get hashrate",
			format: fmtQuantity("Hashrate: ", " H/s"),
		},
		{
			name: "eth_protocolVersion", desc: "Get protocol version",
			format: fmtQuantity("Protocol version: ", ""),
		},
		{
			name: "eth_getUncleByBlockNumberAndIndex", desc: "Get uncle by block number and index",
			params: []param{blockNumberParam, {name: "index", desc: "Uncle index", kind: pHexInt, required: true}},
			empty:  "Uncle not found",
		},
		{
			name: "eth_getUncleByBlockHashAndIndex", desc: "Get uncle by block hash and index",
			params: []param{blockHashParam, {name: "index", desc: "Uncle index", kind: pHexInt, required: true}},
			empty:  "Uncle not found",
		},
		{
			name: "eth_getUncleCountByBlockNumber", desc: "Get uncle count by block number",
			params: []param{blockNumberParam},
			format: fmtQuantity("Uncle count: ", ""),
			empty:  "Block not found",
		},
		{
			name: "eth_getUncleCountByBlockHash", desc: "Get uncle count by block hash",
			params: []param{blockHashParam},
			format: fmtQuantity("Uncle count: ", ""),
			empty:  "Block not found",
		},
		{
			name: "eth_getStorageValues", desc: "Get multiple storage slot values for multiple accounts in a single request",
			params: []param{
				{name: "requests", desc: "JSON object mapping addresses to arrays of storage slot keys", kind: pString, required: true},
				{name: "blockNumber", desc: "Block number or tag (latest, earliest, pending)", kind: pString},
			},
			build: buildStorageValues,
		},

		// ===== ERIGON TOOLS =====
		{
			name: "erigon_forks", desc: "Get fork information",
		},
		{
			name: "erigon_blockNumber", desc: "Get block number (Erigon)",
			params: []param{{name: "blockNumber", desc: "Block tag", kind: pBlockNum, omit: true}},
			format: fmtQuantity("Block number: ", ""),
		},
		{
			name: "erigon_getHeaderByNumber", desc: "Get header by number",
			params: []param{blockNumberParam},
			empty:  "Header not found",
		},
		{
			name: "erigon_getHeaderByHash", desc: "Get header by hash",
			params: []param{blockHashParam},
			empty:  "Header not found",
		},
		{
			name: "erigon_getBlockByTimestamp", desc: "Get block by timestamp",
			params: []param{{name: "timestamp", desc: "Unix timestamp", kind: pString, required: true}, fullTxParam},
			empty:  "Block not found",
		},
		{
			name: "erigon_getBalanceChangesInBlock", desc: "Get all balance changes in block",
			params: []param{{name: "blockNumberOrHash", desc: "Block (default: latest)", kind: pBlockNum, def: "latest"}},
		},
		{
			name: "erigon_getLogsByHash", desc: "Get logs by block hash",
			params: []param{blockHashParam},
		},
		{
			name: "erigon_getLogs", desc: "Get logs (Erigon format)",
			params: []param{
				{name: "fromBlock", desc: "From block", kind: pString},
				{name: "toBlock", desc: "To block", kind: pString},
				{name: "address", desc: "Address", kind: pString},
				{name: "topics", desc: "Topics (JSON)", kind: pString},
			},
			build: buildLogFilter,
		},
		{
			name: "erigon_getBlockReceiptsByBlockHash", desc: "Get block receipts by hash",
			params: []param{blockHashParam},
		},
		{
			name: "erigon_nodeInfo", desc: "Get P2P node info",
		},

		// ===== OTTERSCAN TOOLS =====
		{
			name: "ots_getApiLevel", desc: "Get Otterscan API level",
			format: fmtRawResult("Otterscan API Level: "),
		},
		{
			name: "ots_getInternalOperations", desc: "Get internal operations (internal txs) for a transaction",
			params: []param{txHashParam},
			format: fmtArrayOrMsg("No internal operations found"),
		},
		{
			name: "ots_searchTransactionsBefore", desc: "Search transactions before a given block for an address",
			params: []param{
				addressParam,
				{name: "blockNumber", desc: "Block number", kind: pInt, required: true},
				{name: "pageSize", desc: "Page size (default: 25)", kind: pInt, defInt: 25},
			},
		},
		{
			name: "ots_searchTransactionsAfter", desc: "Search transactions after a given block for an address",
			params: []param{
				addressParam,
				{name: "blockNumber", desc: "Block number", kind: pInt, required: true},
				{name: "pageSize", desc: "Page size (default: 25)", kind: pInt, defInt: 25},
			},
		},
		{
			name: "ots_getBlockDetails", desc: "Get detailed block information",
			params: []param{blockNumberParam},
		},
		{
			name: "ots_getBlockDetailsByHash", desc: "Get detailed block information by hash",
			params: []param{blockHashParam},
		},
		{
			name: "ots_getBlockTransactions", desc: "Get paginated transactions for a block",
			params: []param{
				blockNumberParam,
				{name: "pageNumber", desc: "Page number (default: 0)", kind: pInt},
				{name: "pageSize", desc: "Page size (default: 25)", kind: pInt, defInt: 25},
			},
		},
		{
			name: "ots_hasCode", desc: "Check if an address has code (is a contract)",
			params: []param{addressParam, {name: "blockNumber", desc: "Block number or tag", kind: pBlockNum, def: "latest"}},
			format: fmtHasCode,
		},
		{
			name: "ots_traceTransaction", desc: "Get trace for a transaction",
			params: []param{txHashParam},
			format: fmtArrayOrMsg("No trace entries found"),
		},
		{
			name: "ots_getTransactionError", desc: "Get transaction error/revert reason",
			params: []param{txHashParam},
			format: fmtTxError,
		},
		{
			name: "ots_getTransactionBySenderAndNonce", desc: "Get transaction hash by sender address and nonce",
			params: []param{{name: "address", desc: "Sender address", kind: pString, required: true}, {name: "nonce", desc: "Nonce", kind: pInt, required: true}},
			format: fmtTxHashResult,
		},
		{
			name: "ots_getContractCreator", desc: "Get contract creator address and transaction",
			params: []param{{name: "address", desc: "Contract address", kind: pString, required: true}},
			empty:  "Contract creator not found",
		},
	}
}
