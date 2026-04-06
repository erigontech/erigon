package cmd

import (
	"fmt"
	"slices"
	"strings"

	tea "charm.land/bubbletea/v2"
	"charm.land/lipgloss/v2"

	"github.com/erigontech/erigon/db/state/statecfg"
)

// SelectorModel is a Bubble Tea model for selecting domains and extensions
// with include/exclude logic and confirming or cancelling

type SelectorModel struct {
	domains  []string
	exts     []string
	selected map[string]struct{}

	cursorCol      int
	cursorRow      int
	confirmCursor  int
	confirmMode    bool
	canceled       bool
	domainTypesMap map[string]string

	width      int
	height     int
	viewOffset int
}

// NewSelectorModel initializes based on include/exclude lists
func NewSelectorModel(includeDomains, includeExts, excludeDomains, excludeExts []string) *SelectorModel {
	res, domains := getNames(&statecfg.Schema)
	exts := make([]string, 0, 10)
	exts = append(exts, extCfgMap[domainType]...)
	exts = append(exts, extCfgMap[idxType]...)

	sel := map[string]struct{}{}
	// determine domains to show
	for _, d := range domains {
		if len(includeDomains) > 0 {
			if slices.Contains(includeDomains, d) {
				sel[d] = struct{}{}
			}
		} else if !slices.Contains(excludeDomains, d) {
			sel[d] = struct{}{}
		}
	}
	// determine exts to show
	for selected := range sel {
		for _, e := range extCfgMap[res[selected]] {
			if slices.Contains(includeExts, e) {
				sel[e] = struct{}{}
				continue
			}
			if !slices.Contains(excludeExts, e) {
				sel[e] = struct{}{}
			}
		}
	}
	return &SelectorModel{domains: domains, exts: exts, selected: sel, domainTypesMap: res}
}

func (m *SelectorModel) Init() tea.Cmd { return nil }

func (m *SelectorModel) Update(msg tea.Msg) (tea.Model, tea.Cmd) {
	switch msg := msg.(type) {
	case tea.WindowSizeMsg:
		m.width = msg.Width
		m.height = msg.Height
	case tea.KeyMsg:
		switch msg.String() {
		case "ctrl+c", "q":
			m.canceled = true
			return m, tea.Quit
		case "left", "h":
			if m.confirmMode {
				if m.confirmCursor > 0 {
					m.confirmCursor--
				}
			} else if m.cursorCol > 0 {
				m.cursorCol--
				m.cursorRow = 0
				m.viewOffset = 0
			}
		case "right", "l":
			if m.confirmMode {
				if m.confirmCursor < 1 {
					m.confirmCursor++
				}
			} else if m.cursorCol < 1 {
				m.cursorCol++
				m.cursorRow = 0
				m.viewOffset = 0
			}
		case "up", "k":
			if m.confirmMode {
				if m.confirmCursor > 0 {
					m.confirmCursor--
				}
			} else if m.cursorRow > 0 {
				m.cursorRow--
				m.clampViewOffset()
			}
		case "down", "j":
			if m.confirmMode {
				if m.confirmCursor < 1 {
					m.confirmCursor++
				}
			} else {
				maxRow := m.columnLength() - 1
				if m.cursorRow < maxRow {
					m.cursorRow++
					m.clampViewOffset()
				}
			}
		case "enter", " ":
			if !m.confirmMode {
				m.toggleCurrent()
			} else {
				if m.confirmCursor == 0 {
					return m, tea.Quit
				}
				m.canceled = true
				return m, tea.Quit
			}
		case "tab":
			m.confirmMode = !m.confirmMode
			m.confirmCursor = 0
		}
	}
	return m, nil
}

// availableListHeight returns how many list rows fit in the current terminal.
// header: margin-top(1) + content(1) + margin-bottom(1) + extra \n(1) = 4 lines
// footer: blank line(1) + confirm/hint line(1) = 2 lines; plus 1 safety margin
func (m *SelectorModel) availableListHeight() int {
	if m.height == 0 {
		return 999
	}
	avail := m.height - 7
	if avail < 1 {
		avail = 1
	}
	return avail
}

func (m *SelectorModel) clampViewOffset() {
	avail := m.availableListHeight()
	if m.cursorRow < m.viewOffset {
		m.viewOffset = m.cursorRow
	}
	if m.cursorRow >= m.viewOffset+avail {
		m.viewOffset = m.cursorRow - avail + 1
	}
	maxOffset := m.columnLength() - avail
	if maxOffset < 0 {
		maxOffset = 0
	}
	if m.viewOffset > maxOffset {
		m.viewOffset = maxOffset
	}
	if m.viewOffset < 0 {
		m.viewOffset = 0
	}
}

func (m *SelectorModel) View() tea.View {
	header := "←/→ to switch columns or OK/Cancel, ↑/↓ to move, enter/space to toggle, tab to confirm"
	var s strings.Builder
	s.WriteString(lipgloss.NewStyle().Margin(1, 2).Render(header) + "\n")

	avail := m.availableListHeight()
	maxRows := max(len(m.domains), len(m.exts))
	start := m.viewOffset
	end := start + avail
	if end > maxRows {
		end = maxRows
	}

	for i := start; i < end; i++ {
		left := "   "
		if m.cursorCol == 0 && m.cursorRow == i && !m.confirmMode {
			left = "> "
		}
		leftChecked := "[ ]"
		if i < len(m.domains) {
			d := m.domains[i]
			if _, ok := m.selected[d]; ok {
				leftChecked = "[x]"
			}
			left = fmt.Sprintf("%s %s %s", left, leftChecked, d)
		}
		right := ""
		if i < len(m.exts) {
			prefix := "   "
			if m.cursorCol == 1 && m.cursorRow == i && !m.confirmMode {
				prefix = "> "
			}
			e := m.exts[i]
			checked := "[ ]"
			if _, ok := m.selected[e]; ok {
				checked = "[x]"
			}
			right = fmt.Sprintf("%s %s %s", prefix, checked, e)
		}
		s.WriteString(fmt.Sprintf("%-30s %s\n", left, right))
	}
	s.WriteString("\n")
	if m.confirmMode {
		opts := []string{"OK", "Cancel"}
		for idx, opt := range opts {
			prefix := "   "
			if m.confirmCursor == idx {
				prefix = "> "
			}
			s.WriteString(fmt.Sprintf("%s%s   ", prefix, opt))
		}
		s.WriteString("\n")
	} else {
		s.WriteString("(Tab to switch to OK/Cancel)\n")
	}
	v := tea.NewView(s.String())
	v.AltScreen = true
	return v
}

func (m *SelectorModel) toggleCurrent() {
	if m.cursorCol == 0 && m.cursorRow < len(m.domains) {
		key := m.domains[m.cursorRow]
		if _, ok := m.selected[key]; ok {
			delete(m.selected, key)
		} else {
			m.selected[key] = struct{}{}
			for _, e := range extCfgMap[m.domainTypesMap[key]] {
				m.selected[e] = struct{}{}
			}
		}
	} else if m.cursorCol == 1 && m.cursorRow < len(m.exts) {
		key := m.exts[m.cursorRow]
		if _, ok := m.selected[key]; ok {
			delete(m.selected, key)
		} else {
			m.selected[key] = struct{}{}
		}
	}
}

func (m *SelectorModel) columnLength() int {
	if m.cursorCol == 0 {
		return len(m.domains)
	}
	return len(m.exts)
}

func (m *SelectorModel) GetSelection() ([]string, []string) {
	var ds, es []string
	for _, d := range m.domains {
		if _, ok := m.selected[d]; ok {
			ds = append(ds, d)
		}
	}
	for _, e := range m.exts {
		if _, ok := m.selected[e]; ok {
			es = append(es, e)
		}
	}
	return ds, es
}
