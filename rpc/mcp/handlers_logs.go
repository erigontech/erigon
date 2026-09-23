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

// logTools implements the logs_* tool handlers.
type logTools struct {
	logDir string
}

func registerLogTools(e *ErigonMCPServer) {
	e.mcpServer.AddTool(
		mcp.NewTool("logs_tail",
			mcp.WithReadOnlyHintAnnotation(true),
			mcp.WithDescription("Get last N lines from erigon or torrent logs"),
			mcp.WithString("log_type", mcp.Description("Log type: 'erigon' or 'torrent' (default: erigon)")),
			mcp.WithNumber("lines", mcp.Description("Number of lines to retrieve (default: 100, max: 10000)")),
			mcp.WithString("filter", mcp.Description("Optional string to filter log lines")),
		),
		e.handleLogsTail,
	)
	e.mcpServer.AddTool(
		mcp.NewTool("logs_head",
			mcp.WithReadOnlyHintAnnotation(true),
			mcp.WithDescription("Get first N lines from erigon or torrent logs"),
			mcp.WithString("log_type", mcp.Description("Log type: 'erigon' or 'torrent' (default: erigon)")),
			mcp.WithNumber("lines", mcp.Description("Number of lines to retrieve (default: 100, max: 10000)")),
			mcp.WithString("filter", mcp.Description("Optional string to filter log lines")),
		),
		e.handleLogsHead,
	)
	e.mcpServer.AddTool(
		mcp.NewTool("logs_grep",
			mcp.WithReadOnlyHintAnnotation(true),
			mcp.WithDescription("Search for a pattern in erigon or torrent logs"),
			mcp.WithString("log_type", mcp.Description("Log type: 'erigon' or 'torrent' (default: erigon)")),
			mcp.WithString("pattern", mcp.Required(), mcp.Description("Search pattern")),
			mcp.WithNumber("max_lines", mcp.Description("Maximum matching lines to return (default: 1000, max: 10000)")),
			mcp.WithBoolean("case_insensitive", mcp.Description("Case-insensitive search (default: false)")),
		),
		e.handleLogsGrep,
	)
	e.mcpServer.AddTool(
		mcp.NewTool("logs_stats",
			mcp.WithReadOnlyHintAnnotation(true),
			mcp.WithDescription("Get statistics about erigon or torrent logs"),
			mcp.WithString("log_type", mcp.Description("Log type: 'erigon' or 'torrent' (default: erigon)")),
		),
		e.handleLogsStats,
	)
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

	logLines, err := scanLog(logFile, maxLines, pattern, caseInsensitive)
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

// newLogScanner reads a log file line by line, with room for erigon's long
// lines: the scanner's default 64 KB token limit would truncate them.
func newLogScanner(f *os.File) *bufio.Scanner {
	s := bufio.NewScanner(f)
	s.Buffer(make([]byte, 0, 64*1024), 1024*1024)
	return s
}

// readLogTail reads the last N matching lines, holding only those N: a node's
// erigon.log runs to hundreds of MB.
func readLogTail(filename string, lines int, filter string) ([]string, error) {
	file, err := os.Open(filename)
	if err != nil {
		return nil, err
	}
	defer file.Close()

	ring := make([]string, 0, lines)
	oldest := 0
	scanner := newLogScanner(file)
	for scanner.Scan() {
		line := scanner.Text()
		if filter != "" && !strings.Contains(line, filter) {
			continue
		}
		if len(ring) < lines {
			ring = append(ring, line)
			continue
		}
		ring[oldest] = line
		oldest = (oldest + 1) % lines
	}
	if err := scanner.Err(); err != nil {
		return nil, err
	}
	if oldest == 0 {
		return ring, nil
	}
	// Full-capacity slice so the append copies instead of overwriting the head.
	return append(ring[oldest:len(ring):len(ring)], ring[:oldest]...), nil
}

// scanLog returns up to max lines matching match, from the start of the file.
// An empty match keeps every line.
func scanLog(filename string, max int, match string, caseInsensitive bool) ([]string, error) {
	file, err := os.Open(filename)
	if err != nil {
		return nil, err
	}
	defer file.Close()

	if caseInsensitive {
		match = strings.ToLower(match)
	}
	var result []string
	scanner := newLogScanner(file)
	for scanner.Scan() && len(result) < max {
		line := scanner.Text()
		hay := line
		if caseInsensitive {
			hay = strings.ToLower(hay)
		}
		if match == "" || strings.Contains(hay, match) {
			result = append(result, line)
		}
	}
	if err := scanner.Err(); err != nil {
		return nil, err
	}
	return result, nil
}

// readLogHead reads the first N lines from a log file with optional filtering.
func readLogHead(filename string, lines int, filter string) ([]string, error) {
	return scanLog(filename, lines, filter, false)
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

	scanner := newLogScanner(file)
	for scanner.Scan() {
		totalLines++
		switch logLevel(scanner.Text()) {
		case "eror", "error", "crit":
			errorLines++
		case "warn":
			warnLines++
		case "info":
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

// logLevel is the level token a log line carries: "[EROR] [time] msg" in the
// default file format, {"lvl":"eror"} under --log.dir.json, {"level":"WARN"} in
// torrent.log, which the downloader writes through slog. It is empty for a line
// in none of those shapes, such as a panic trace. Matching the token rather than
// the whole line keeps an "err=" key on an info line out of the error count.
func logLevel(line string) string {
	if rest, ok := strings.CutPrefix(line, "["); ok {
		if i := strings.IndexByte(rest, ']'); i > 0 {
			return strings.ToLower(rest[:i])
		}
	}
	for _, key := range []string{`"lvl":"`, `"level":"`} {
		if _, rest, ok := strings.Cut(line, key); ok {
			if i := strings.IndexByte(rest, '"'); i > 0 {
				return strings.ToLower(rest[:i])
			}
		}
	}
	return ""
}
