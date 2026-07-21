package mcp

import (
	"context"
	"fmt"
	"strings"

	"github.com/mark3labs/mcp-go/mcp"

	"github.com/erigontech/erigon/rpc/mcp/metrics"
)

const metricsUnavailableMsg = "Metrics are not available in this mode. Use the embedded MCP server (Erigon's --mcp.addr/--mcp.port) for metrics access."

func registerMetricsTools(e *ErigonMCPServer) {
	e.mcpServer.AddTool(
		mcp.NewTool("metrics_list",
			mcp.WithDescription("List all available metric names"),
		),
		e.handleMetricsList,
	)
	e.mcpServer.AddTool(
		mcp.NewTool("metrics_get",
			mcp.WithDescription("Get metrics with optional filtering by pattern (supports wildcards like 'db_*', '*_size', etc.)"),
			mcp.WithString("pattern", mcp.Description("Metric name pattern (optional, empty = all metrics)")),
		),
		e.handleMetricsGet,
	)
}

// handleMetricsList handles the metrics_list tool
func (e *ErigonMCPServer) handleMetricsList(ctx context.Context, req mcp.CallToolRequest) (*mcp.CallToolResult, error) {
	if !e.metricsEnabled {
		return mcp.NewToolResultText(metricsUnavailableMsg), nil
	}
	names, err := metrics.ListMetricNames()
	if err != nil {
		return mcp.NewToolResultError(err.Error()), nil
	}

	if len(names) == 0 {
		return mcp.NewToolResultText("No metrics available"), nil
	}

	// Format as a nice list
	var result strings.Builder
	result.WriteString(fmt.Sprintf("Available metrics (%d total):\n\n", len(names)))
	for _, name := range names {
		result.WriteString(fmt.Sprintf("- %s\n", name))
	}

	return mcp.NewToolResultText(result.String()), nil
}

// handleMetricsGet handles the metrics_get tool
func (e *ErigonMCPServer) handleMetricsGet(ctx context.Context, req mcp.CallToolRequest) (*mcp.CallToolResult, error) {
	if !e.metricsEnabled {
		return mcp.NewToolResultText(metricsUnavailableMsg), nil
	}
	pattern := req.GetString("pattern", "")

	metricsData, err := metrics.GatherMetricsFiltered(pattern)
	if err != nil {
		return mcp.NewToolResultError(err.Error()), nil
	}

	if len(metricsData) == 0 {
		if pattern != "" {
			return mcp.NewToolResultText(fmt.Sprintf("No metrics found matching pattern: %s", pattern)), nil
		}
		return mcp.NewToolResultText("No metrics available"), nil
	}

	// Add summary information
	summary := fmt.Sprintf("Metrics (found %d", len(metricsData))
	if pattern != "" {
		summary += fmt.Sprintf(" matching '%s'", pattern)
	}
	summary += "):\n\n"

	return mcp.NewToolResultText(summary + toJSONText(metricsData)), nil
}
