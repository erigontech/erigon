package mcp

import (
	"context"
	"fmt"

	"github.com/mark3labs/mcp-go/mcp"
)

// registerPrompts registers all MCP prompts
func (e *ErigonMCPServer) registerPrompts() {
	e.mcpServer.AddPrompt(mcp.NewPrompt("analyze_transaction",
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

	e.mcpServer.AddPrompt(mcp.NewPrompt("investigate_address",
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

	e.mcpServer.AddPrompt(mcp.NewPrompt("analyze_block",
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

	e.mcpServer.AddPrompt(mcp.NewPrompt("gas_analysis",
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
}
