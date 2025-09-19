package tui

import (
	"errors"
	"github.com/erigontech/erigon/db/version"
	"path/filepath"
	"sort"
	"strings"

	"github.com/charmbracelet/bubbles/table"
	"github.com/charmbracelet/bubbles/textinput"
	tea "github.com/charmbracelet/bubbletea"
	"github.com/charmbracelet/lipgloss"

	"github.com/erigontech/erigon/cmd/bumper/internal/schema"
)

type focus int

const (
	fLeft focus = iota
	fRight
	fEdit
	fModal
)

type col int

const (
	cCurrent col = iota
	cMin
)

const (
	major = "major"
	minor = "minor"
)

type rowRef struct {
	cat  string
	part string
	key  string
}

type modalKind int

const (
	mkNone modalKind = iota
	mkQuitConfirm
	mkSaveConfirm
)

type model struct {
	file string
	cur  schema.Schema
	orig schema.Schema

	cats  []string
	left  table.Model
	right table.Model
	rows  []rowRef

	foc    focus
	edit   col
	editor textinput.Model
	err    error

	modal modalKind

	status string
}

func Run(file string) error {
	s, err := schema.Load(file)
	if err != nil {
		return err
	}
	m := newModel(file, s)
	_, err = tea.NewProgram(m, tea.WithAltScreen()).Run()
	return err
}

func newModel(file string, s schema.Schema) *model {
	cats := schema.Cats(s)

	l := table.New(table.WithColumns([]table.Column{{Title: "Schemas", Width: 18}}))
	lrows := make([]table.Row, len(cats))
	for i, c := range cats {
		lrows[i] = table.Row{c}
	}
	l.SetRows(lrows)
	l.Focus()

	ti := textinput.New()
	ti.Placeholder = "1.1"
	ti.CharLimit = 8
	ti.Prompt = "↳ "

	m := &model{
		file:   file,
		cur:    s,
		orig:   clone(s),
		cats:   cats,
		left:   l,
		editor: ti,
	}
	m.rebuildRight()
	m.updateStatus()
	return m
}

func (m *model) rebuildRight() {
	if len(m.cats) == 0 {
		return
	}
	i := m.left.Cursor()
	if i < 0 {
		i = 0
	}
	if i >= len(m.cats) {
		i = len(m.cats) - 1
	}
	name := m.cats[i]
	cat := m.cur[name]

	type it struct{ part, key string }
	var list []it
	add := func(part string, g schema.Group) {
		keys := make([]string, 0, len(g))
		for k := range g {
			keys = append(keys, k)
		}
		sort.Strings(keys)
		for _, k := range keys {
			list = append(list, it{part, k})
		}
	}
	add("domain", cat.Domain)
	add("hist", cat.Hist)
	add("ii", cat.Ii)

	cols := []table.Column{
		{Title: "Part", Width: 8},
		{Title: "Key", Width: 6},
		{Title: "Current", Width: 8},
		{Title: "Min", Width: 6},
		{Title: "Status", Width: 12},
	}
	m.right = table.New(table.WithColumns(cols))
	m.right.SetHeight(18)

	m.rows = m.rows[:0]
	trows := make([]table.Row, 0, len(list))
	for _, it := range list {
		v := m.get(name, it.part, it.key)
		st := "ok"
		if v.Min.Greater(v.Current) {
			st = "min>cur"
		}
		trows = append(trows, table.Row{
			it.part, it.key,
			v.Current.String(), v.Min.String(), st,
		})
		m.rows = append(m.rows, rowRef{cat: name, part: it.part, key: it.key})
	}
	m.right.SetRows(trows)
}

func (m *model) refreshRight() {
	rows := m.right.Rows()
	for i, r := range m.rows {
		v := m.get(r.cat, r.part, r.key)
		rows[i][2] = v.Current.String()
		rows[i][3] = v.Min.String()
		st := "ok"
		if v.Min.Greater(v.Current) {
			st = "min>cur"
		}
		rows[i][4] = st
	}
	m.right.SetRows(rows)
	m.updateStatus()
}

func (m *model) get(cat, part, key string) schema.TwoVers {
	c := m.cur[cat]
	switch part {
	case "domain":
		return c.Domain[key]
	case "hist":
		return c.Hist[key]
	default:
		return c.Ii[key]
	}
}

func (m *model) set(cat, part, key string, fn func(*schema.TwoVers)) {
	c := m.cur[cat]
	switch part {
	case "domain":
		v := c.Domain[key]
		fn(&v)
		c.Domain[key] = v
	case "hist":
		v := c.Hist[key]
		fn(&v)
		c.Hist[key] = v
	default:
		v := c.Ii[key]
		fn(&v)
		c.Ii[key] = v
	}
	m.cur[cat] = c
}

func (m *model) updateStatus() {
	base := filepath.Base(m.file)
	tag := "no changes"
	if !equal(m.orig, m.cur) {
		tag = "unsaved changes"
	}
	m.status = base + " • " + tag + " • Ctrl+S=Save&Exit"
}

func (m *model) Init() tea.Cmd { return nil }

func (m *model) Update(msg tea.Msg) (tea.Model, tea.Cmd) {
	switch msg := msg.(type) {
	case tea.KeyMsg:
		k := msg.String()

		// Modal
		if m.foc == fModal {
			switch k {
			case "y", "Y", "enter":
				if m.modal == mkSaveConfirm || m.modal == mkQuitConfirm {
					if err := schema.Save(m.file, m.cur); err != nil {
						m.err = err
						m.foc = fRight
						m.modal = mkNone
						return m, nil
					}
				}
				return m, tea.Quit
			case "n", "N":
				if m.modal == mkQuitConfirm {
					return m, tea.Quit
				}
				m.foc = fRight
				m.modal = mkNone
				return m, nil
			case "esc":
				m.foc = fRight
				m.modal = mkNone
				return m, nil
			}
			return m, nil
		}

		// Editor
		if m.foc == fEdit {
			switch k {
			case "enter":
				txt := strings.TrimSpace(m.editor.Value())
				ver, err := version.ParseVersion(strings.ReplaceAll(txt, ",", "."))
				if err != nil {
					m.err = errors.New("bad number")
					return m, nil
				}
				r := m.right.Cursor()
				if r >= 0 && r < len(m.rows) {
					row := m.rows[r]
					if m.edit == cCurrent {
						m.set(row.cat, row.part, row.key, func(v *schema.TwoVers) { v.Current = ver })
					} else {
						m.set(row.cat, row.part, row.key, func(v *schema.TwoVers) { v.Min = ver })
					}
				}
				m.refreshRight()
				m.editor.Blur()
				m.foc = fRight
				return m, nil
			case "esc":
				m.editor.Blur()
				m.foc = fRight
				return m, nil
			default:
				var cmd tea.Cmd
				m.editor, cmd = m.editor.Update(msg)
				return m, cmd
			}
		}

		// Normal keys
		switch k {
		case "ctrl+c":
			return m, tea.Quit
		case "q", "Q":
			if equal(m.orig, m.cur) {
				return m, tea.Quit
			}
			m.modal = mkQuitConfirm
			m.foc = fModal
			return m, nil
		case "ctrl+s":
			if err := schema.Save(m.file, m.cur); err != nil {
				m.err = err
				return m, nil
			}
			return m, tea.Quit
		case "S", "s":
			m.modal = mkSaveConfirm
			m.foc = fModal
			return m, nil
		case "tab", "right":
			if m.foc == fLeft {
				m.foc = fRight
			}
			return m, nil
		case "left":
			if m.foc == fRight {
				m.foc = fLeft
			}
			return m, nil
		case "up":
			if m.foc == fLeft {
				m.left.MoveUp(1)
				m.rebuildRight()
			} else {
				m.right.MoveUp(1)
			}
			return m, nil
		case "down":
			if m.foc == fLeft {
				m.left.MoveDown(1)
				m.rebuildRight()
			} else {
				m.right.MoveDown(1)
			}
			return m, nil
		case "e":
			m.edit = cCurrent
			m.beginEdit()
			return m, nil
		case "m":
			m.edit = cMin
			m.beginEdit()
			return m, nil
		case ".":
			m.bump(minor)
			return m, nil
		case "M":
			m.bump(major)
			return m, nil
		}
	}
	return m, nil
}

func (m *model) beginEdit() {
	r := m.right.Cursor()
	if r < 0 || r >= len(m.rows) {
		return
	}
	row := m.rows[r]
	v := m.get(row.cat, row.part, row.key)
	cur := v.Current.String()
	if m.edit == cMin {
		cur = v.Min.String()
	}
	m.editor.SetValue(cur)
	m.editor.CursorEnd()
	m.editor.Focus()
	m.foc = fEdit
}

func (m *model) View() string {
	title := lipgloss.NewStyle().Bold(true).Render("Schema Versions")
	left := lipgloss.NewStyle().Border(lipgloss.RoundedBorder()).Render(
		lipgloss.JoinVertical(lipgloss.Left, "Schemas", m.left.View()),
	)
	cat := ""
	if c := m.left.Cursor(); c >= 0 && c < len(m.cats) {
		cat = m.cats[c]
	}
	right := lipgloss.NewStyle().Border(lipgloss.RoundedBorder()).Render(
		lipgloss.JoinVertical(lipgloss.Left, cat, m.right.View(),
			func() string {
				if m.foc == fEdit {
					return "\nEdit: " + m.editor.View()
				}
				return ""
			}(),
		),
	)
	help := "[↑/↓] move  [Tab] switch  [e] edit current  [m] edit min  [.] +0.1  [M] +1.0  [S] save  [Ctrl+S] save&exit  [Q] quit"
	stat := m.status
	if m.err != nil {
		stat = "Error: " + m.err.Error()
	}
	body := lipgloss.JoinVertical(lipgloss.Left,
		title,
		lipgloss.JoinHorizontal(lipgloss.Top, left, "  ", right),
		lipgloss.NewStyle().Faint(true).Render(help),
		stat,
	)
	if m.foc == fModal {
		box := lipgloss.NewStyle().
			Border(lipgloss.RoundedBorder()).
			Padding(1, 2).
			Align(lipgloss.Center).
			Width(56)
		txt := ""
		switch m.modal {
		case mkQuitConfirm:
			txt = "Quit: [y] Save & Exit • [n] Discard & Exit • [esc] Cancel"
		case mkSaveConfirm:
			txt = "Save changes now? [enter/y] Yes • [esc] Cancel"
		}
		overlay := box.Render(txt)
		return lipgloss.PlaceHorizontal(lipgloss.Width(body), lipgloss.Center,
			lipgloss.JoinVertical(lipgloss.Center, body, overlay))
	}
	return body
}

func (m *model) bump(mode string) {
	r := m.right.Cursor()
	if r >= 0 && r < len(m.rows) {
		row := m.rows[r]
		m.set(row.cat, row.part, row.key, func(v *schema.TwoVers) {
			switch mode {
			case minor:
				v.Current = v.Current.BumpMinor()
			case major:
				v.Current = v.Current.BumpMajor()
			}
		})
		m.refreshRight()
	}
}

// simple deep copy
func clone(s schema.Schema) schema.Schema {
	out := make(schema.Schema, len(s))
	for k, c := range s {
		cc := schema.Category{
			Domain: make(schema.Group, len(c.Domain)),
			Hist:   make(schema.Group, len(c.Hist)),
			Ii:     make(schema.Group, len(c.Ii)),
		}
		for k2, v := range c.Domain {
			cc.Domain[k2] = v
		}
		for k2, v := range c.Hist {
			cc.Hist[k2] = v
		}
		for k2, v := range c.Ii {
			cc.Ii[k2] = v
		}
		out[k] = cc
	}
	return out
}

func equal(a, b schema.Schema) bool {
	if len(a) != len(b) {
		return false
	}
	for k, ca := range a {
		cb, ok := b[k]
		if !ok {
			return false
		}
		if !eqGroup(ca.Domain, cb.Domain) || !eqGroup(ca.Hist, cb.Hist) || !eqGroup(ca.Ii, cb.Ii) {
			return false
		}
	}
	return true
}
func eqGroup(x, y schema.Group) bool {
	if len(x) != len(y) {
		return false
	}
	for k, vx := range x {
		vy, ok := y[k]
		if !ok {
			return false
		}
		if !vx.Current.Eq(vy.Current) || !vx.Min.Eq(vy.Min) {
			return false
		}
	}
	return true
}
