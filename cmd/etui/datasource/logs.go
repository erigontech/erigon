package datasource

import (
	"bufio"
	"io"
	"os"
	"strings"
	"sync"
)

// LogLevel classifies a log line's severity.
type LogLevel int

const (
	LogAll   LogLevel = iota // show everything
	LogDebug                 // DEBUG / DBUG
	LogInfo                  // INFO
	LogWarn                  // WARN / WRN
	LogError                 // ERROR / ERR
)

// String returns the canonical label for a LogLevel.
func (l LogLevel) String() string {
	switch l {
	case LogError:
		return "ERROR"
	case LogWarn:
		return "WARN"
	case LogInfo:
		return "INFO"
	case LogDebug:
		return "DEBUG"
	default:
		return "ALL"
	}
}

// LogLine is a single parsed log entry.
type LogLine struct {
	Raw   string
	Level LogLevel
}

const logRingSize = 1000

// LogTailer reads an Erigon log file incrementally, parses log levels,
// and maintains a 1000-line ring buffer of the most recent entries.
// It is safe for concurrent use.
type LogTailer struct {
	mu      sync.Mutex
	logPath string
	offset  int64 // current read position in the file
	ring    [logRingSize]LogLine
	idx     int   // next write position
	count   int   // total lines stored (capped at logRingSize)
	version int64 // bumped on each new line for change detection
}

// NewLogTailer creates a LogTailer for the given log file path.
func NewLogTailer(logPath string) *LogTailer {
	return &LogTailer{logPath: logPath}
}

// LogPath returns the configured path.
func (t *LogTailer) LogPath() string {
	return t.logPath
}

// Poll reads any new content appended to the log file since the last call.
// It should be called periodically from a background goroutine.
// Returns the number of new lines read.
func (t *LogTailer) Poll() int {
	if t.logPath == "" {
		return 0
	}

	f, err := os.Open(t.logPath)
	if err != nil {
		return 0
	}
	defer f.Close()

	fi, err := f.Stat()
	if err != nil {
		return 0
	}

	t.mu.Lock()
	offset := t.offset
	t.mu.Unlock()

	// If the file shrank (log rotation), reset to beginning.
	if fi.Size() < offset {
		offset = 0
	}

	// Nothing new to read.
	if fi.Size() == offset {
		return 0
	}

	if _, err := f.Seek(offset, io.SeekStart); err != nil {
		return 0
	}

	// Use a large buffer for long Erigon log lines (JSON, stack traces).
	const maxScanBuf = 1024 * 1024
	scanner := bufio.NewScanner(f)
	scanner.Buffer(make([]byte, 0, 64*1024), maxScanBuf)

	var newLines []LogLine
	for scanner.Scan() {
		line := scanner.Text()
		newLines = append(newLines, LogLine{
			Raw:   line,
			Level: parseLogLevel(line),
		})
	}

	// Update offset to current file position.
	newOffset, err := f.Seek(0, io.SeekCurrent)
	if err != nil {
		newOffset = fi.Size()
	}

	if len(newLines) == 0 {
		t.mu.Lock()
		t.offset = newOffset
		t.mu.Unlock()
		return 0
	}

	t.mu.Lock()
	for _, ll := range newLines {
		t.ring[t.idx] = ll
		t.idx = (t.idx + 1) % logRingSize
		if t.count < logRingSize {
			t.count++
		}
	}
	t.version += int64(len(newLines))
	t.offset = newOffset
	t.mu.Unlock()

	return len(newLines)
}

// SeedFromEnd reads the last portion of the log file to pre-populate the
// ring buffer so the viewer has content immediately on first open.
// Should be called once before the first Poll.
func (t *LogTailer) SeedFromEnd() {
	if t.logPath == "" {
		return
	}

	f, err := os.Open(t.logPath)
	if err != nil {
		return
	}
	defer f.Close()

	fi, err := f.Stat()
	if err != nil {
		return
	}

	// Read the last 512 KB to capture enough lines.
	const tailBytes int64 = 512 * 1024
	startPos := fi.Size() - tailBytes
	if startPos < 0 {
		startPos = 0
	}
	if _, err := f.Seek(startPos, io.SeekStart); err != nil {
		return
	}

	const maxScanBuf = 1024 * 1024
	scanner := bufio.NewScanner(f)
	scanner.Buffer(make([]byte, 0, 64*1024), maxScanBuf)

	// If we seeked into the middle, discard the first partial line.
	if startPos > 0 {
		scanner.Scan()
	}

	var lines []LogLine
	for scanner.Scan() {
		line := scanner.Text()
		lines = append(lines, LogLine{
			Raw:   line,
			Level: parseLogLevel(line),
		})
	}

	// Keep only the last logRingSize lines.
	if len(lines) > logRingSize {
		lines = lines[len(lines)-logRingSize:]
	}

	t.mu.Lock()
	for _, ll := range lines {
		t.ring[t.idx] = ll
		t.idx = (t.idx + 1) % logRingSize
		if t.count < logRingSize {
			t.count++
		}
	}
	t.version += int64(len(lines))
	t.offset = fi.Size() // subsequent Polls start from EOF
	t.mu.Unlock()
}

// Recent returns up to n most recent lines, oldest first, filtered by minLevel.
// Lines with a level below minLevel are excluded. LogAll includes everything.
func (t *LogTailer) Recent(n int, minLevel LogLevel) []LogLine {
	t.mu.Lock()
	defer t.mu.Unlock()

	// Gather all buffered lines newest-first, then filter and reverse.
	var filtered []LogLine
	for i := range t.count {
		pos := (t.idx - 1 - i + logRingSize) % logRingSize
		ll := t.ring[pos]
		if minLevel != LogAll && ll.Level < minLevel {
			continue
		}
		filtered = append(filtered, ll)
		if len(filtered) >= n {
			break
		}
	}

	// Reverse to oldest-first order for display.
	for i, j := 0, len(filtered)-1; i < j; i, j = i+1, j-1 {
		filtered[i], filtered[j] = filtered[j], filtered[i]
	}
	return filtered
}

// Version returns a counter that increments whenever new lines arrive.
func (t *LogTailer) Version() int64 {
	t.mu.Lock()
	defer t.mu.Unlock()
	return t.version
}

// parseLogLevel detects the log level from an Erigon log line.
// Erigon uses both long (ERROR, WARN, INFO, DEBUG) and short (ERR, WRN, INF, DBG/DBUG) forms.
func parseLogLevel(line string) LogLevel {
	upper := strings.ToUpper(line)
	// Check for level markers — Erigon typically puts them early in the line.
	// We check the first 80 chars to avoid false positives in message bodies.
	check := upper
	if len(check) > 80 {
		check = check[:80]
	}
	switch {
	case strings.Contains(check, "ERROR") || strings.Contains(check, "ERR"):
		return LogError
	case strings.Contains(check, "WARN") || strings.Contains(check, "WRN"):
		return LogWarn
	case strings.Contains(check, "INFO") || strings.Contains(check, "INF"):
		return LogInfo
	case strings.Contains(check, "DEBUG") || strings.Contains(check, "DBUG") || strings.Contains(check, "DBG"):
		return LogDebug
	default:
		return LogDebug // unknown lines treated as debug
	}
}
