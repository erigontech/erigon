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

	e.mcpServer.AddPrompt(mcp.NewPrompt("debug_logs",
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

	e.mcpServer.AddPrompt(mcp.NewPrompt("torrent_status",
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

	e.mcpServer.AddPrompt(mcp.NewPrompt("sync_analysis",
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
