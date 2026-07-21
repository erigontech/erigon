package mcp

import (
	"context"
	"encoding/json"
	"fmt"
	"math/big"
	"strings"

	"github.com/mark3labs/mcp-go/mcp"
)

func (e *ErigonMCPServer) registerResources() {
	srv := e.mcpServer

	// Static resources
	srv.AddResource(
		mcp.NewResource("erigon://node/info",
			"node info",
			mcp.WithResourceDescription("Get node information and capabilities"),
			mcp.WithMIMEType("application/json"),
		),
		e.handleResourceNodeInfo,
	)

	srv.AddResource(
		mcp.NewResource("erigon://chain/config",
			"chain config",
			mcp.WithResourceDescription("Get chain configuration"),
			mcp.WithMIMEType("application/json"),
		),
		e.handleResourceChainConfig,
	)

	srv.AddResource(
		mcp.NewResource("erigon://blocks/recent",
			"recent blocks",
			mcp.WithResourceDescription("Get recent blocks (default: last 10)"),
			mcp.WithMIMEType("application/json"),
		),
		e.handleResourceRecentBlocks,
	)

	srv.AddResource(
		mcp.NewResource("erigon://network/status",
			"network status",
			mcp.WithResourceDescription("Get network sync status and peer info"),
			mcp.WithMIMEType("application/json"),
		),
		e.handleResourceNetworkStatus,
	)

	srv.AddResource(
		mcp.NewResource("erigon://gas/current",
			"gas current",
			mcp.WithResourceDescription("Get current gas price information"),
			mcp.WithMIMEType("application/json"),
		),
		e.handleResourceGasInfo,
	)

	// Resource templates (with parameters)
	srv.AddResourceTemplate(
		mcp.NewResourceTemplate("erigon://address/{address}/summary",
			"address summary",
			mcp.WithTemplateDescription("Get address summary (balance, nonce, code)"),
			mcp.WithTemplateMIMEType("application/json"),
		),
		e.handleResourceAddressSummary,
	)

	srv.AddResourceTemplate(
		mcp.NewResourceTemplate("erigon://block/{number}/summary",
			"block summary",
			mcp.WithTemplateDescription("Get block summary"),
			mcp.WithTemplateMIMEType("application/json"),
		),
		e.handleResourceBlockSummary,
	)

	srv.AddResourceTemplate(
		mcp.NewResourceTemplate("erigon://transaction/{hash}/analysis",
			"transaction analysis",
			mcp.WithTemplateDescription("Get transaction analysis"),
			mcp.WithTemplateMIMEType("application/json"),
		),
		e.handleResourceTransactionAnalysis,
	)
}

func (e *ErigonMCPServer) handleResourceNodeInfo(ctx context.Context, req mcp.ReadResourceRequest) ([]mcp.ResourceContents, error) {
	var result json.RawMessage
	if err := e.client.CallContext(ctx, &result, "erigon_nodeInfo"); err != nil {
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

func (e *ErigonMCPServer) handleResourceChainConfig(ctx context.Context, req mcp.ReadResourceRequest) ([]mcp.ResourceContents, error) {
	var blockNum string
	if err := e.client.CallContext(ctx, &blockNum, "eth_blockNumber"); err != nil {
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

func (e *ErigonMCPServer) handleResourceRecentBlocks(ctx context.Context, req mcp.ReadResourceRequest) ([]mcp.ResourceContents, error) {
	var blockNumHex string
	if err := e.client.CallContext(ctx, &blockNumHex, "eth_blockNumber"); err != nil {
		return nil, fmt.Errorf("eth_blockNumber: %w", err)
	}

	currentBlock, ok := new(big.Int).SetString(strings.TrimPrefix(blockNumHex, "0x"), 16)
	if !ok {
		return nil, fmt.Errorf("eth_blockNumber: unexpected result %q", blockNumHex)
	}

	const recentBlockCount = 10
	blocks := make([]json.RawMessage, 0, recentBlockCount)
	for i := range recentBlockCount {
		blockNum := new(big.Int).Sub(currentBlock, big.NewInt(int64(i)))
		if blockNum.Sign() < 0 {
			break
		}
		hexNum := fmt.Sprintf("0x%x", blockNum)
		var block json.RawMessage
		if err := e.client.CallContext(ctx, &block, "eth_getBlockByNumber", hexNum, false); err != nil {
			continue
		}
		if !isNullResult(block) {
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

func (e *ErigonMCPServer) handleResourceNetworkStatus(ctx context.Context, req mcp.ReadResourceRequest) ([]mcp.ResourceContents, error) {
	var nodeInfo json.RawMessage
	_ = e.client.CallContext(ctx, &nodeInfo, "erigon_nodeInfo")

	var blockNum string
	_ = e.client.CallContext(ctx, &blockNum, "eth_blockNumber")

	// eth_syncing returns false when fully synced, or a JSON object with progress.
	var syncResult json.RawMessage
	_ = e.client.CallContext(ctx, &syncResult, "eth_syncing")
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

func (e *ErigonMCPServer) handleResourceGasInfo(ctx context.Context, req mcp.ReadResourceRequest) ([]mcp.ResourceContents, error) {
	var gasPriceHex string
	if err := e.client.CallContext(ctx, &gasPriceHex, "eth_gasPrice"); err != nil {
		return nil, fmt.Errorf("eth_gasPrice: %w", err)
	}

	gasInfo := map[string]any{
		"gas_price": gasPriceHex,
	}

	// Get latest block for base fee
	var block json.RawMessage
	if err := e.client.CallContext(ctx, &block, "eth_getBlockByNumber", "latest", false); err == nil && block != nil {
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

func (e *ErigonMCPServer) handleResourceAddressSummary(ctx context.Context, req mcp.ReadResourceRequest) ([]mcp.ResourceContents, error) {
	address := extractURIParam(req.Params.URI, "erigon://address/", "/summary")
	if address == "" {
		return nil, fmt.Errorf("missing address parameter in URI: %s", req.Params.URI)
	}

	// Failed lookups become null rather than empty strings, so a client
	// never mistakes an RPC failure for an empty balance or an EOA.
	summary := map[string]any{
		"address":     address,
		"balance":     nil,
		"nonce":       nil,
		"is_contract": nil,
	}
	var balance string
	if err := e.client.CallContext(ctx, &balance, "eth_getBalance", address, "latest"); err == nil {
		summary["balance"] = balance
	}
	var nonce string
	if err := e.client.CallContext(ctx, &nonce, "eth_getTransactionCount", address, "latest"); err == nil {
		summary["nonce"] = nonce
	}
	var code string
	if err := e.client.CallContext(ctx, &code, "eth_getCode", address, "latest"); err == nil {
		summary["is_contract"] = code != "" && code != "0x"
	}

	return []mcp.ResourceContents{
		mcp.TextResourceContents{
			URI:      fmt.Sprintf("erigon://address/%s/summary", address),
			MIMEType: "application/json",
			Text:     toJSONText(summary),
		},
	}, nil
}

func (e *ErigonMCPServer) handleResourceBlockSummary(ctx context.Context, req mcp.ReadResourceRequest) ([]mcp.ResourceContents, error) {
	blockNumStr := extractURIParam(req.Params.URI, "erigon://block/", "/summary")
	if blockNumStr == "" {
		return nil, fmt.Errorf("missing block number parameter in URI: %s", req.Params.URI)
	}

	var block json.RawMessage
	if err := e.client.CallContext(ctx, &block, "eth_getBlockByNumber", blockNumStr, false); err != nil {
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

func (e *ErigonMCPServer) handleResourceTransactionAnalysis(ctx context.Context, req mcp.ReadResourceRequest) ([]mcp.ResourceContents, error) {
	txHash := extractURIParam(req.Params.URI, "erigon://transaction/", "/analysis")
	if txHash == "" {
		return nil, fmt.Errorf("missing transaction hash parameter in URI: %s", req.Params.URI)
	}

	var tx json.RawMessage
	if err := e.client.CallContext(ctx, &tx, "eth_getTransactionByHash", txHash); err != nil {
		return nil, fmt.Errorf("eth_getTransactionByHash: %w", err)
	}

	var receipt json.RawMessage
	if err := e.client.CallContext(ctx, &receipt, "eth_getTransactionReceipt", txHash); err != nil {
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
