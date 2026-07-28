package mcp

import (
	"context"
	"fmt"
	"reflect"

	"github.com/mark3labs/mcp-go/mcp"
	"github.com/mark3labs/mcp-go/server"

	"github.com/erigontech/erigon/common"
	"github.com/erigontech/erigon/common/hexutil"
	"github.com/erigontech/erigon/rpc"
)

// resourceHandlerSet holds one handler per resource shared by the embedded and
// standalone servers.
type resourceHandlerSet struct {
	nodeInfo            server.ResourceHandlerFunc
	chainConfig         server.ResourceHandlerFunc
	recentBlocks        server.ResourceHandlerFunc
	networkStatus       server.ResourceHandlerFunc
	gasInfo             server.ResourceHandlerFunc
	addressSummary      server.ResourceTemplateHandlerFunc
	blockSummary        server.ResourceTemplateHandlerFunc
	transactionAnalysis server.ResourceTemplateHandlerFunc
}

func registerResources(srv *server.MCPServer, h resourceHandlerSet) {
	hv := reflect.ValueOf(h)
	for i := range hv.NumField() {
		if hv.Field(i).IsNil() {
			panic(fmt.Sprintf("mcp: missing resource handler %s", hv.Type().Field(i).Name))
		}
	}

	// Static resources
	srv.AddResource(
		mcp.NewResource("erigon://node/info",
			"node info",
			mcp.WithResourceDescription("Get node information and capabilities"),
			mcp.WithMIMEType("application/json"),
		),
		h.nodeInfo,
	)

	srv.AddResource(
		mcp.NewResource("erigon://chain/config",
			"chain config",
			mcp.WithResourceDescription("Get chain configuration"),
			mcp.WithMIMEType("application/json"),
		),
		h.chainConfig,
	)

	srv.AddResource(
		mcp.NewResource("erigon://blocks/recent",
			"recent blocks",
			mcp.WithResourceDescription("Get recent blocks (default: last 10)"),
			mcp.WithMIMEType("application/json"),
		),
		h.recentBlocks,
	)

	srv.AddResource(
		mcp.NewResource("erigon://network/status",
			"network status",
			mcp.WithResourceDescription("Get network sync status and peer info"),
			mcp.WithMIMEType("application/json"),
		),
		h.networkStatus,
	)

	srv.AddResource(
		mcp.NewResource("erigon://gas/current",
			"gas current",
			mcp.WithResourceDescription("Get current gas price information"),
			mcp.WithMIMEType("application/json"),
		),
		h.gasInfo,
	)

	// Resource templates (with parameters)
	srv.AddResourceTemplate(
		mcp.NewResourceTemplate("erigon://address/{address}/summary",
			"address summary",
			mcp.WithTemplateDescription("Get address summary (balance, nonce, code)"),
			mcp.WithTemplateMIMEType("application/json"),
		),
		h.addressSummary,
	)

	srv.AddResourceTemplate(
		mcp.NewResourceTemplate("erigon://block/{number}/summary",
			"block summary",
			mcp.WithTemplateDescription("Get block summary"),
			mcp.WithTemplateMIMEType("application/json"),
		),
		h.blockSummary,
	)

	srv.AddResourceTemplate(
		mcp.NewResourceTemplate("erigon://transaction/{hash}/analysis",
			"transaction analysis",
			mcp.WithTemplateDescription("Get transaction analysis"),
			mcp.WithTemplateMIMEType("application/json"),
		),
		h.transactionAnalysis,
	)
}

func (e *ErigonMCPServer) resourceHandlers() resourceHandlerSet {
	return resourceHandlerSet{
		nodeInfo:            e.handleResourceNodeInfo,
		chainConfig:         e.handleResourceChainConfig,
		recentBlocks:        e.handleResourceRecentBlocks,
		networkStatus:       e.handleResourceNetworkStatus,
		gasInfo:             e.handleResourceGasInfo,
		addressSummary:      e.handleResourceAddressSummary,
		blockSummary:        e.handleResourceBlockSummary,
		transactionAnalysis: e.handleResourceTransactionAnalysis,
	}
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

	syncing, err := e.ethAPI.Syncing(ctx)
	if err != nil || syncing == nil {
		syncing = false
	}

	status := map[string]any{
		"node_info":     nodeInfo,
		"current_block": currentBlock,
		"syncing":       syncing,
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
	address := extractURIParam(req.Params.URI, "erigon://address/", "/summary")
	if address == "" {
		return nil, fmt.Errorf("missing address parameter in URI: %s", req.Params.URI)
	}

	latest := rpc.BlockNumberOrHashWithNumber(rpc.LatestBlockNumber)
	balance, _ := e.ethAPI.GetBalance(ctx, common.HexToAddress(address), &latest)
	nonce, _ := e.ethAPI.GetTransactionCount(ctx, common.HexToAddress(address), &latest)
	code, _ := e.ethAPI.GetCode(ctx, common.HexToAddress(address), &latest)

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
	blockNumStr := extractURIParam(req.Params.URI, "erigon://block/", "/summary")
	if blockNumStr == "" {
		return nil, fmt.Errorf("missing block number parameter in URI: %s", req.Params.URI)
	}

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
	txHash := extractURIParam(req.Params.URI, "erigon://transaction/", "/analysis")
	if txHash == "" {
		return nil, fmt.Errorf("missing transaction hash parameter in URI: %s", req.Params.URI)
	}

	hash := common.HexToHash(txHash)
	tx, _ := e.ethAPI.GetTransactionByHash(ctx, hash)
	receipt, _ := e.ethAPI.GetTransactionReceipt(ctx, hash)

	status := "unknown"
	if receiptStatus, ok := receipt["status"].(hexutil.Uint64); ok {
		if receiptStatus == 0 {
			status = "reverted"
		} else {
			status = "success"
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
