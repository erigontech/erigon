package mcp

import (
	"context"
	"fmt"

	"github.com/mark3labs/mcp-go/mcp"

	"github.com/erigontech/erigon/common"
	"github.com/erigontech/erigon/common/hexutil"
	"github.com/erigontech/erigon/rpc"
)

// registerResources registers all MCP resources
func (e *ErigonMCPServer) registerResources() {
	// Static resources
	e.mcpServer.AddResource(
		mcp.NewResource("erigon://node/info",
			"node info",
			mcp.WithResourceDescription("Get node information and capabilities"),
			mcp.WithMIMEType("application/json"),
		),
		e.handleResourceNodeInfo,
	)

	e.mcpServer.AddResource(
		mcp.NewResource("erigon://chain/config",
			"chain config",
			mcp.WithResourceDescription("Get chain configuration"),
			mcp.WithMIMEType("application/json"),
		),
		e.handleResourceChainConfig,
	)

	// Dynamic resources
	e.mcpServer.AddResource(
		mcp.NewResource("erigon://blocks/recent",
			"recent blocks",
			mcp.WithResourceDescription("Get recent blocks (default: last 10)"),
			mcp.WithMIMEType("application/json"),
		),
		e.handleResourceRecentBlocks,
	)

	e.mcpServer.AddResource(
		mcp.NewResource("erigon://network/status",
			"network status",
			mcp.WithResourceDescription("Get network sync status and peer info"),
			mcp.WithMIMEType("application/json"),
		),
		e.handleResourceNetworkStatus,
	)

	e.mcpServer.AddResource(
		mcp.NewResource("erigon://gas/current",
			"gas current",
			mcp.WithResourceDescription("Get current gas price information"),
			mcp.WithMIMEType("application/json"),
		),
		e.handleResourceGasInfo,
	)

	// Resource templates (with parameters)
	e.mcpServer.AddResourceTemplate(
		mcp.NewResourceTemplate("erigon://address/{address}/summary",
			"address summary",
			mcp.WithTemplateDescription("Get address summary (balance, nonce, code)"),
			mcp.WithTemplateMIMEType("application/json"),
		),
		e.handleResourceAddressSummary,
	)

	e.mcpServer.AddResourceTemplate(
		mcp.NewResourceTemplate("erigon://block/{number}/summary",
			"block summary",
			mcp.WithTemplateDescription("Get block summary"),
			mcp.WithTemplateMIMEType("application/json"),
		),
		e.handleResourceBlockSummary,
	)

	e.mcpServer.AddResourceTemplate(
		mcp.NewResourceTemplate("erigon://transaction/{hash}/analysis",
			"transaction analysis",
			mcp.WithTemplateDescription("Get transaction analysis"),
			mcp.WithTemplateMIMEType("application/json"),
		),
		e.handleResourceTransactionAnalysis,
	)
}

// Resource handlers
func (e *ErigonMCPServer) handleResourceNodeInfo(ctx context.Context, req mcp.ReadResourceRequest) ([]mcp.ResourceContents, error) {
	info, err := e.erigonAPI.NodeInfo(ctx)
	if err != nil {
		return nil, err
	}

	return []mcp.ResourceContents{
		mcp.TextResourceContents{
			URI:      "erigon://node/info",
			MIMEType: "application/json",
			Text:     toJSONText(info),
		},
	}, nil
}

func (e *ErigonMCPServer) handleResourceChainConfig(ctx context.Context, req mcp.ReadResourceRequest) ([]mcp.ResourceContents, error) {
	// Get chain config
	blockNum, err := e.ethAPI.BlockNumber(ctx)
	if err != nil {
		return nil, err
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
	// Get last 10 blocks
	currentBlock, err := e.ethAPI.BlockNumber(ctx)
	if err != nil {
		return nil, err
	}

	blocks := make([]any, 0, 10)
	for i := 0; i < 10; i++ {
		blockNum := currentBlock - hexutil.Uint64(i)
		block, err := e.ethAPI.GetBlockByNumber(ctx, rpc.BlockNumber(blockNum), false)
		if err != nil || block == nil {
			continue
		}
		blocks = append(blocks, block)
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
	nodeInfo, err := e.erigonAPI.NodeInfo(ctx)
	if err != nil {
		return nil, err
	}

	currentBlock, _ := e.ethAPI.BlockNumber(ctx)

	status := map[string]any{
		"node_info":     nodeInfo,
		"current_block": currentBlock,
		"syncing":       false, // Would check actual sync status
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
	gasPrice, err := e.ethAPI.GasPrice(ctx)
	if err != nil {
		return nil, err
	}

	// Get latest block for base fee
	block, _ := e.ethAPI.GetBlockByNumber(ctx, rpc.LatestBlockNumber, false)

	gasInfo := map[string]any{
		"gas_price": gasPrice,
	}
	if block != nil {
		gasInfo["base_fee"] = block["baseFeePerGas"]
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
	// Extract address from URI
	// This is a simplified example - you'd need proper URI parsing
	address := req.Params.URI // Would extract {address} parameter

	balance, _ := e.ethAPI.GetBalance(ctx, common.HexToAddress(address), rpc.BlockNumberOrHashWithNumber(rpc.LatestBlockNumber))
	nonce, _ := e.ethAPI.GetTransactionCount(ctx, common.HexToAddress(address), rpc.BlockNumberOrHashWithNumber(rpc.LatestBlockNumber))
	code, _ := e.ethAPI.GetCode(ctx, common.HexToAddress(address), rpc.BlockNumberOrHashWithNumber(rpc.LatestBlockNumber))

	summary := map[string]any{
		"address":     address,
		"balance":     balance,
		"nonce":       nonce,
		"is_contract": len(code) > 0,
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
	// Extract block number from URI
	// Simplified - needs proper parsing
	blockNumStr := req.Params.URI

	blockNum, err := parseBlockNumber(blockNumStr)
	if err != nil {
		return nil, err
	}

	block, err := e.ethAPI.GetBlockByNumber(ctx, blockNum, false)
	if err != nil {
		return nil, err
	}

	return []mcp.ResourceContents{
		mcp.TextResourceContents{
			URI:      fmt.Sprintf("erigon://block/%s/summary", blockNumStr),
			MIMEType: "application/json",
			Text:     toJSONText(block),
		},
	}, nil
}

func (e *ErigonMCPServer) handleResourceTransactionAnalysis(ctx context.Context, req mcp.ReadResourceRequest) ([]mcp.ResourceContents, error) {
	// Extract tx hash from URI
	txHash := req.Params.URI

	tx, _ := e.ethAPI.GetTransactionByHash(ctx, common.HexToHash(txHash))
	receipt, _ := e.ethAPI.GetTransactionReceipt(ctx, common.HexToHash(txHash))

	analysis := map[string]any{
		"transaction": tx,
		"receipt":     receipt,
		"status":      "success", // Would check receipt status
	}

	return []mcp.ResourceContents{
		mcp.TextResourceContents{
			URI:      fmt.Sprintf("erigon://transaction/%s/analysis", txHash),
			MIMEType: "application/json",
			Text:     toJSONText(analysis),
		},
	}, nil
}
