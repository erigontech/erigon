package mcp

import (
	"context"
	"fmt"
	"strings"

	"github.com/mark3labs/mcp-go/mcp"

	"github.com/erigontech/erigon/rpc/mcp/metrics"
)

// handleMetricsList handles the metrics_list tool
func (e *ErigonMCPServer) handleMetricsList(ctx context.Context, req mcp.CallToolRequest) (*mcp.CallToolResult, error) {
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
