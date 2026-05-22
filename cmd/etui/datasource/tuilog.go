package datasource

import (
	"fmt"
	"os"
	"path/filepath"
	"sync"
	"time"
)

// TUILog provides structured, file-based logging for internal TUI diagnostics.
// All methods are safe for concurrent use.
type TUILog struct {
	mu   sync.Mutex
	f    *os.File
	path string
}

// NewTUILog creates a logger that appends to <datadir>/etui.log.
// If the file cannot be opened the logger silently discards messages.
func NewTUILog(datadir string) *TUILog {
	logDir := filepath.Join(datadir, "logs")
	os.MkdirAll(logDir, 0755) //nolint:errcheck
	logPath := filepath.Join(logDir, "etui.log")

	f, err := os.OpenFile(logPath, os.O_CREATE|os.O_APPEND|os.O_WRONLY, 0644)
	if err != nil {
		// Can't open — return a no-op logger.
		return &TUILog{path: logPath}
	}
	return &TUILog{f: f, path: logPath}
}

// Info logs an informational message.
func (l *TUILog) Info(msg string, args ...any) {
	l.write("INFO", msg, args...)
}

// Warn logs a warning.
func (l *TUILog) Warn(msg string, args ...any) {
	l.write("WARN", msg, args...)
}

// Error logs an error.
func (l *TUILog) Error(msg string, args ...any) {
	l.write("ERROR", msg, args...)
}

// Close flushes and closes the underlying file.
func (l *TUILog) Close() {
	l.mu.Lock()
	defer l.mu.Unlock()
	if l.f != nil {
		l.f.Close()
		l.f = nil
	}
}

func (l *TUILog) write(level, msg string, args ...any) {
	l.mu.Lock()
	defer l.mu.Unlock()
	if l.f == nil {
		return
	}
	ts := time.Now().Format("2006-01-02T15:04:05.000")
	line := fmt.Sprintf("[%s] %s %s\n", ts, level, fmt.Sprintf(msg, args...))
	l.f.WriteString(line) //nolint:errcheck
}
