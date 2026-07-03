package mcp

import (
	"bufio"
	"context"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"strings"

	"github.com/mark3labs/mcp-go/mcp"
)

// logTools implements the logs_* tool handlers; it is embedded by both the
// embedded and standalone MCP servers.
type logTools struct {
	logDir string
}

func (l logTools) resolveLogFile(logType string) (string, error) {
	if l.logDir == "" {
		return "", errors.New("log directory not configured (use --log.dir or --datadir)")
	}
	switch logType {
	case "erigon":
		return filepath.Join(l.logDir, "erigon.log"), nil
	case "torrent":
		return filepath.Join(l.logDir, "torrent.log"), nil
	default:
		return "", errors.New("log_type must be 'erigon' or 'torrent'")
	}
}

func (l logTools) handleLogsTail(ctx context.Context, req mcp.CallToolRequest) (*mcp.CallToolResult, error) {
	return l.readLogLines(req, "Last", readLogTail)
}

func (l logTools) handleLogsHead(ctx context.Context, req mcp.CallToolRequest) (*mcp.CallToolResult, error) {
	return l.readLogLines(req, "First", readLogHead)
}

func (l logTools) readLogLines(req mcp.CallToolRequest, position string, read func(string, int, string) ([]string, error)) (*mcp.CallToolResult, error) {
	logType := req.GetString("log_type", "erigon")
	lines := req.GetInt("lines", 100)
	filter := req.GetString("filter", "")

	if lines <= 0 || lines > 10000 {
		return mcp.NewToolResultError("lines must be between 1 and 10000"), nil
	}

	logFile, err := l.resolveLogFile(logType)
	if err != nil {
		return mcp.NewToolResultError(err.Error()), nil
	}

	logLines, err := read(logFile, lines, filter)
	if err != nil {
		return mcp.NewToolResultError(fmt.Sprintf("Failed to read log: %v", err)), nil
	}

	result := fmt.Sprintf("%s %d lines from %s.log", position, len(logLines), logType)
	if filter != "" {
		result += fmt.Sprintf(" (filtered by: %s)", filter)
	}
	result += ":\n\n" + strings.Join(logLines, "\n")

	return mcp.NewToolResultText(result), nil
}

func (l logTools) handleLogsGrep(ctx context.Context, req mcp.CallToolRequest) (*mcp.CallToolResult, error) {
	logType := req.GetString("log_type", "erigon")
	pattern := req.GetString("pattern", "")
	maxLines := req.GetInt("max_lines", 1000)
	caseInsensitive := req.GetBool("case_insensitive", false)

	if pattern == "" {
		return mcp.NewToolResultError("pattern is required"), nil
	}

	if maxLines <= 0 || maxLines > 10000 {
		return mcp.NewToolResultError("max_lines must be between 1 and 10000"), nil
	}

	logFile, err := l.resolveLogFile(logType)
	if err != nil {
		return mcp.NewToolResultError(err.Error()), nil
	}

	logLines, err := grepLog(logFile, pattern, maxLines, caseInsensitive)
	if err != nil {
		return mcp.NewToolResultError(fmt.Sprintf("Failed to grep log: %v", err)), nil
	}

	result := fmt.Sprintf("Found %d matching lines in %s.log for pattern '%s':\n\n", len(logLines), logType, pattern)
	result += strings.Join(logLines, "\n")

	return mcp.NewToolResultText(result), nil
}

func (l logTools) handleLogsStats(ctx context.Context, req mcp.CallToolRequest) (*mcp.CallToolResult, error) {
	logType := req.GetString("log_type", "erigon")

	logFile, err := l.resolveLogFile(logType)
	if err != nil {
		return mcp.NewToolResultError(err.Error()), nil
	}

	stats, err := getLogStats(logFile)
	if err != nil {
		return mcp.NewToolResultError(fmt.Sprintf("Failed to get log stats: %v", err)), nil
	}

	return mcp.NewToolResultText(toJSONText(stats)), nil
}

// readLogTail reads the last N lines from a log file with optional filtering
func readLogTail(filename string, lines int, filter string) ([]string, error) {
	file, err := os.Open(filename)
	if err != nil {
		return nil, err
	}
	defer file.Close()

	// Read all lines (for simplicity, could optimize with reverse reading for large files)
	var allLines []string
	scanner := bufio.NewScanner(file)

	// Increase buffer size for long log lines
	buf := make([]byte, 0, 64*1024)
	scanner.Buffer(buf, 1024*1024)

	for scanner.Scan() {
		line := scanner.Text()
		if filter == "" || strings.Contains(line, filter) {
			allLines = append(allLines, line)
		}
	}

	if err := scanner.Err(); err != nil {
		return nil, err
	}

	// Return last N lines
	start := max(len(allLines)-lines, 0)

	return allLines[start:], nil
}

// readLogHead reads the first N lines from a log file with optional filtering
func readLogHead(filename string, lines int, filter string) ([]string, error) {
	file, err := os.Open(filename)
	if err != nil {
		return nil, err
	}
	defer file.Close()

	var result []string
	scanner := bufio.NewScanner(file)

	// Increase buffer size for long log lines
	buf := make([]byte, 0, 64*1024)
	scanner.Buffer(buf, 1024*1024)

	count := 0
	for scanner.Scan() && count < lines {
		line := scanner.Text()
		if filter == "" || strings.Contains(line, filter) {
			result = append(result, line)
			count++
		}
	}

	if err := scanner.Err(); err != nil {
		return nil, err
	}

	return result, nil
}

// grepLog searches for a pattern in log file
func grepLog(filename, pattern string, maxLines int, caseInsensitive bool) ([]string, error) {
	file, err := os.Open(filename)
	if err != nil {
		return nil, err
	}
	defer file.Close()

	var result []string
	scanner := bufio.NewScanner(file)

	// Increase buffer size for long log lines
	buf := make([]byte, 0, 64*1024)
	scanner.Buffer(buf, 1024*1024)

	searchPattern := pattern
	if caseInsensitive {
		searchPattern = strings.ToLower(pattern)
	}

	count := 0
	for scanner.Scan() && count < maxLines {
		line := scanner.Text()
		searchLine := line
		if caseInsensitive {
			searchLine = strings.ToLower(line)
		}

		if strings.Contains(searchLine, searchPattern) {
			result = append(result, line)
			count++
		}
	}

	if err := scanner.Err(); err != nil {
		return nil, err
	}

	return result, nil
}

// getLogStats returns statistics about a log file
func getLogStats(filename string) (map[string]any, error) {
	file, err := os.Open(filename)
	if err != nil {
		return nil, err
	}
	defer file.Close()

	fileInfo, err := file.Stat()
	if err != nil {
		return nil, err
	}

	var totalLines int
	var errorLines int
	var warnLines int
	var infoLines int

	scanner := bufio.NewScanner(file)
	buf := make([]byte, 0, 64*1024)
	scanner.Buffer(buf, 1024*1024)

	for scanner.Scan() {
		totalLines++
		line := strings.ToLower(scanner.Text())

		if strings.Contains(line, "error") || strings.Contains(line, "err=") {
			errorLines++
		} else if strings.Contains(line, "warn") {
			warnLines++
		} else if strings.Contains(line, "info") {
			infoLines++
		}
	}

	if err := scanner.Err(); err != nil {
		return nil, err
	}

	stats := map[string]any{
		"file_name":    filepath.Base(filename),
		"file_size":    fileInfo.Size(),
		"file_size_mb": float64(fileInfo.Size()) / (1024 * 1024),
		"modified":     fileInfo.ModTime().Format("2006-01-02 15:04:05"),
		"total_lines":  totalLines,
		"error_lines":  errorLines,
		"warn_lines":   warnLines,
		"info_lines":   infoLines,
	}

	return stats, nil
}
