package cmd

import (
	"fmt"
	"slices"

	tea "github.com/charmbracelet/bubbletea"
	"github.com/charmbracelet/lipgloss"

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
			}
		case "right", "l":
			if m.confirmMode {
				if m.confirmCursor < 1 {
					m.confirmCursor++
				}
			} else if m.cursorCol < 1 {
				m.cursorCol++
				m.cursorRow = 0
			}
		case "up", "k":
			if m.confirmMode {
				if m.confirmCursor > 0 {
					m.confirmCursor--
				}
			} else if m.cursorRow > 0 {
				m.cursorRow--
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

func (m *SelectorModel) View() string {
	header := "←/→ to switch columns or OK/Cancel, ↑/↓ to move, enter/space to toggle, tab to confirm"
	s := lipgloss.NewStyle().Margin(1, 2).Render(header) + "\n"
	maxRows := max(len(m.domains), len(m.exts))
	for i := 0; i < maxRows; i++ {
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
		s += fmt.Sprintf("%-30s %s\n", left, right)
	}
	s += "\n"
	if m.confirmMode {
		opts := []string{"OK", "Cancel"}
		for idx, opt := range opts {
			prefix := "   "
			if m.confirmCursor == idx {
				prefix = "> "
			}
			s += fmt.Sprintf("%s%s   ", prefix, opt)
		}
		s += "\n"
	} else {
		s += "(Tab to switch to OK/Cancel)\n"
	}
	return s
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
