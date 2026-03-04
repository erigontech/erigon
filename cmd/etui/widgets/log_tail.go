package widgets

import (
	"bufio"
	"fmt"
	"os"
	"strings"

	"github.com/rivo/tview"
)

// LogTailView shows the last N lines from the Erigon log file,
// filtered to ERROR/WARN by default.
type LogTailView struct {
	*tview.TextView
	logPath   string
	maxLines  int
	filterErr bool // true = only show ERROR/WARN lines
}

// NewLogTailView creates a log tail widget.
// logPath is the path to erigon.log; maxLines controls how many lines to display.
func NewLogTailView(logPath string, maxLines int) *LogTailView {
	tv := tview.NewTextView().SetDynamicColors(true).SetScrollable(true)
	tv.SetBorder(true).SetTitle(" Log Tail ")
	tv.SetText("[::d]Waiting for log data...[-]")
	return &LogTailView{
		TextView:  tv,
		logPath:   logPath,
		maxLines:  maxLines,
		filterErr: true,
	}
}

// ReadLogTail performs all file I/O and returns the formatted text to display.
// This must be called from the poll goroutine, NOT from within QueueUpdateDraw.
func (v *LogTailView) ReadLogTail() string {
	if v.logPath == "" {
		return "[yellow]No log file configured[-]"
	}

	f, err := os.Open(v.logPath)
	if err != nil {
		return fmt.Sprintf("[yellow]No log file found[-] (%s)", v.logPath)
	}
	defer f.Close()

	lines := v.readTailLines(f)
	if len(lines) == 0 {
		if v.filterErr {
			return "[::d]No ERROR/WARN entries found[-]"
		}
		return "[::d]Log file empty[-]"
	}

	var b strings.Builder
	for i, line := range lines {
		if i > 0 {
			b.WriteByte('\n')
		}
		b.WriteString(colorLogLine(line))
	}
	return b.String()
}

// readTailLines reads the file and returns the last maxLines matching lines.
// To avoid reading huge files, it seeks to the last 256KB.
func (v *LogTailView) readTailLines(f *os.File) []string {
	const tailBytes = 256 * 1024

	fi, err := f.Stat()
	if err != nil {
		return nil
	}
	if fi.Size() > tailBytes {
		if _, err := f.Seek(-tailBytes, 2); err != nil {
			f.Seek(0, 0) //nolint:errcheck
		}
	}

	// Erigon log lines can be long (stack traces, JSON payloads).
	// Use a 1 MB buffer so the scanner doesn't silently truncate.
	const maxScanBuf = 1024 * 1024
	scanner := bufio.NewScanner(f)
	scanner.Buffer(make([]byte, 0, 64*1024), maxScanBuf)

	// Collect candidate lines (filtered if enabled).
	var candidates []string
	for scanner.Scan() {
		line := scanner.Text()
		if v.filterErr {
			upper := strings.ToUpper(line)
			if !strings.Contains(upper, "ERROR") && !strings.Contains(upper, "WARN") &&
				!strings.Contains(upper, "ERR") && !strings.Contains(upper, "WRN") {
				continue
			}
		}
		candidates = append(candidates, line)
	}

	// Keep only the last maxLines.
	if len(candidates) > v.maxLines {
		candidates = candidates[len(candidates)-v.maxLines:]
	}
	return candidates
}

// colorLogLine applies tview color tags based on log level keywords.
func colorLogLine(line string) string {
	upper := strings.ToUpper(line)
	switch {
	case strings.Contains(upper, "ERROR") || strings.Contains(upper, "ERR"):
		return "[red]" + tview.Escape(line) + "[-]"
	case strings.Contains(upper, "WARN") || strings.Contains(upper, "WRN"):
		return "[yellow]" + tview.Escape(line) + "[-]"
	default:
		return "[::d]" + tview.Escape(line) + "[-]"
	}
}
