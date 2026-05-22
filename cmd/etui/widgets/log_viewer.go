package widgets

import (
	"fmt"
	"strings"
	"sync"

	"github.com/gdamore/tcell/v2"
	"github.com/rivo/tview"

	"github.com/erigontech/erigon/cmd/etui/datasource"
)

// LogViewerPage is a full-screen log viewer with filtering, search, and autoscroll.
type LogViewerPage struct {
	// Root is the top-level flex to embed in the pages container.
	Root *tview.Flex

	content   *tview.TextView
	statusBar *tview.TextView
	searchBar *tview.InputField

	// mu protects fields that are read from the poll goroutine and written
	// from the tview event loop (or vice versa). Every access of these four
	// fields must go through the helpers below.
	mu          sync.Mutex
	filterLevel datasource.LogLevel
	autoscroll  bool
	searching   bool
	searchTerm  string

	// goBack is called to navigate back to the dashboard.
	goBack func()
}

// --- thread-safe field accessors ---

func (lv *LogViewerPage) getFilterLevel() datasource.LogLevel {
	lv.mu.Lock()
	defer lv.mu.Unlock()
	return lv.filterLevel
}

func (lv *LogViewerPage) setFilterLevel(l datasource.LogLevel) {
	lv.mu.Lock()
	defer lv.mu.Unlock()
	lv.filterLevel = l
}

func (lv *LogViewerPage) getAutoscroll() bool {
	lv.mu.Lock()
	defer lv.mu.Unlock()
	return lv.autoscroll
}

func (lv *LogViewerPage) toggleAutoscroll() {
	lv.mu.Lock()
	defer lv.mu.Unlock()
	lv.autoscroll = !lv.autoscroll
}

func (lv *LogViewerPage) getSearching() bool {
	lv.mu.Lock()
	defer lv.mu.Unlock()
	return lv.searching
}

func (lv *LogViewerPage) setSearching(v bool) {
	lv.mu.Lock()
	defer lv.mu.Unlock()
	lv.searching = v
}

func (lv *LogViewerPage) getSearchTerm() string {
	lv.mu.Lock()
	defer lv.mu.Unlock()
	return lv.searchTerm
}

func (lv *LogViewerPage) setSearchTerm(s string) {
	lv.mu.Lock()
	defer lv.mu.Unlock()
	lv.searchTerm = s
}

// NewLogViewerPage builds the full-screen log viewer page.
// goBack is invoked when the user presses Escape or F1 to return to the dashboard.
func NewLogViewerPage(goBack func()) *LogViewerPage {
	lv := &LogViewerPage{
		filterLevel: datasource.LogAll,
		autoscroll:  true,
		goBack:      goBack,
	}

	lv.content = tview.NewTextView().
		SetDynamicColors(true).
		SetScrollable(true).
		SetWrap(true).
		SetText("[::d]Waiting for log data...[-]")
	lv.content.SetBorder(true).SetTitle(" Log Viewer ")

	lv.statusBar = tview.NewTextView().SetDynamicColors(true)
	lv.updateStatusBar()

	lv.searchBar = tview.NewInputField().
		SetLabel("[yellow]/[-] ").
		SetFieldWidth(0).
		SetDoneFunc(func(key tcell.Key) {
			// DoneFunc fires on Enter or Escape inside the InputField.
			// We record the result but do NOT manipulate the flex layout here —
			// the app-level InputCapture calls DismissSearch afterwards.
			switch key {
			case tcell.KeyEnter:
				lv.setSearchTerm(lv.searchBar.GetText())
			case tcell.KeyEscape:
				lv.setSearchTerm("")
				lv.searchBar.SetText("")
			}
		})

	lv.content.SetInputCapture(func(event *tcell.EventKey) *tcell.EventKey {
		return lv.handleKey(event)
	})

	lv.Root = tview.NewFlex().SetDirection(tview.FlexRow).
		AddItem(lv.content, 0, 1, true).
		AddItem(lv.statusBar, 1, 0, false)

	return lv
}

// handleKey processes keyboard input when the content view has focus.
// '/' is handled by the app-level InputCapture (which calls EnterSearchMode).
func (lv *LogViewerPage) handleKey(event *tcell.EventKey) *tcell.EventKey {
	switch {
	case event.Rune() == ' ':
		lv.toggleAutoscroll()
		lv.updateStatusBar()
		return nil

	case event.Rune() == '1':
		lv.setFilterLevel(datasource.LogAll)
		lv.updateStatusBar()
		return nil
	case event.Rune() == '2':
		lv.setFilterLevel(datasource.LogError)
		lv.updateStatusBar()
		return nil
	case event.Rune() == '3':
		lv.setFilterLevel(datasource.LogWarn)
		lv.updateStatusBar()
		return nil
	case event.Rune() == '4':
		lv.setFilterLevel(datasource.LogInfo)
		lv.updateStatusBar()
		return nil
	}

	return event
}

// EnterSearchMode swaps the status bar for the search input field and marks
// the viewer as searching. Must be called from the tview event loop.
func (lv *LogViewerPage) EnterSearchMode() {
	if lv.getSearching() {
		return // already in search mode
	}
	lv.setSearching(true)
	lv.searchBar.SetText("")
	lv.Root.RemoveItem(lv.statusBar)
	lv.Root.AddItem(lv.searchBar, 1, 0, true)
}

// IsSearching returns true when the search input field is active.
func (lv *LogViewerPage) IsSearching() bool {
	return lv.getSearching()
}

// SearchBar returns the search input field (for focus management).
func (lv *LogViewerPage) SearchBar() *tview.InputField {
	return lv.searchBar
}

// Content returns the main text view (for focus management).
func (lv *LogViewerPage) Content() *tview.TextView {
	return lv.content
}

// DismissSearch restores the status bar after a search completes.
// It is idempotent — calling it when not searching is a no-op.
// Must be called from the tview event loop.
func (lv *LogViewerPage) DismissSearch() {
	if !lv.getSearching() {
		return
	}
	lv.setSearching(false)
	lv.Root.RemoveItem(lv.searchBar)
	lv.Root.AddItem(lv.statusBar, 1, 0, false)
	lv.updateStatusBar()
}

// FilterLevel returns the current minimum log level filter.
// Safe to call from any goroutine.
func (lv *LogViewerPage) FilterLevel() datasource.LogLevel {
	return lv.getFilterLevel()
}

// UpdateContent renders log lines into the viewer. Called from the app
// goroutine inside QueueUpdateDraw (i.e. on the tview event loop).
func (lv *LogViewerPage) UpdateContent(lines []datasource.LogLine) {
	if len(lines) == 0 {
		lv.content.SetText("[::d]No log lines match current filter[-]")
		return
	}

	term := lv.getSearchTerm()

	var b strings.Builder
	for i, ll := range lines {
		if i > 0 {
			b.WriteByte('\n')
		}
		if term != "" {
			b.WriteString(colorAndHighlight(ll, term))
		} else {
			b.WriteString(colorLogViewerLine(ll))
		}
	}
	lv.content.SetText(b.String())
	if lv.getAutoscroll() {
		lv.content.ScrollToEnd()
	}
}

// updateStatusBar refreshes the bottom status line with current mode info.
// Called from the event loop only.
func (lv *LogViewerPage) updateStatusBar() {
	scrollLabel := "[green]AUTO[-]"
	if !lv.getAutoscroll() {
		scrollLabel = "[red]PAUSED[-]"
	}

	filterLabel := formatFilterLabel(lv.getFilterLevel())

	searchInfo := ""
	if term := lv.getSearchTerm(); term != "" {
		searchInfo = fmt.Sprintf("  [cyan]search:[-] %s", term)
	}

	lv.statusBar.SetText(fmt.Sprintf(
		" [yellow]1[-]=ALL [yellow]2[-]=ERR [yellow]3[-]=WARN [yellow]4[-]=INFO  "+
			"Filter: %s  Scroll: %s  [yellow]Space[-]=pause  [yellow]/[-]=search  "+
			"[yellow]Esc/F1[-]=back%s",
		filterLabel, scrollLabel, searchInfo))
}

func formatFilterLabel(level datasource.LogLevel) string {
	switch level {
	case datasource.LogError:
		return "[red]ERROR[-]"
	case datasource.LogWarn:
		return "[yellow]WARN+[-]"
	case datasource.LogInfo:
		return "[green]INFO+[-]"
	default:
		return "[cyan]ALL[-]"
	}
}

// colorLogViewerLine applies color tags based on log level (no search highlight).
func colorLogViewerLine(ll datasource.LogLine) string {
	escaped := tview.Escape(ll.Raw)
	switch ll.Level {
	case datasource.LogError:
		return "[red]" + escaped + "[-]"
	case datasource.LogWarn:
		return "[yellow]" + escaped + "[-]"
	case datasource.LogDebug:
		return "[::d]" + escaped + "[-]"
	default: // INFO and unknown
		return escaped
	}
}

// colorAndHighlight builds the colored line by splitting the raw text at
// search-term boundaries, coloring each segment individually, and wrapping
// matches in a highlight tag. This avoids replacing inside tview color tags.
func colorAndHighlight(ll datasource.LogLine, term string) string {
	raw := ll.Raw
	lowerRaw := strings.ToLower(raw)
	lowerTerm := strings.ToLower(term)

	// If term not found, fall back to plain coloring.
	if !strings.Contains(lowerRaw, lowerTerm) {
		return colorLogViewerLine(ll)
	}

	levelColor, levelReset := levelColorTags(ll.Level)

	var b strings.Builder
	start := 0
	for {
		idx := strings.Index(lowerRaw[start:], lowerTerm)
		if idx < 0 {
			// Remaining tail.
			if start < len(raw) {
				b.WriteString(levelColor)
				b.WriteString(tview.Escape(raw[start:]))
				b.WriteString(levelReset)
			}
			break
		}
		// Text before the match.
		if idx > 0 {
			b.WriteString(levelColor)
			b.WriteString(tview.Escape(raw[start : start+idx]))
			b.WriteString(levelReset)
		}
		// The matched segment — highlighted.
		matchEnd := start + idx + len(term)
		b.WriteString("[black:yellow]")
		b.WriteString(tview.Escape(raw[start+idx : matchEnd]))
		b.WriteString("[-:-]")
		start = matchEnd
	}
	return b.String()
}

// levelColorTags returns the opening and closing tview color tags for a log level.
func levelColorTags(level datasource.LogLevel) (open, close string) {
	switch level {
	case datasource.LogError:
		return "[red]", "[-]"
	case datasource.LogWarn:
		return "[yellow]", "[-]"
	case datasource.LogDebug:
		return "[::d]", "[-]"
	default:
		return "", ""
	}
}
