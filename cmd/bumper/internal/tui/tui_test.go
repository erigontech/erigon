package tui

import (
	"reflect"
	"testing"

	tea "charm.land/bubbletea/v2"

	"github.com/erigontech/erigon/cmd/bumper/internal/schema"
	"github.com/erigontech/erigon/db/version"
)

func pv(t *testing.T, s string) version.Version {
	t.Helper()
	v, err := version.ParseVersion(s)
	if err != nil {
		t.Fatalf("ParseVersion(%q): %v", s, err)
	}
	return v
}

func tv(t *testing.T, cur, min string) schema.TwoVers {
	return schema.TwoVers{Current: pv(t, cur), Min: pv(t, min)}
}

func newTestModel(t *testing.T) *model {
	t.Helper()
	s := schema.Schema{
		"accounts": schema.Category{
			Domain: schema.Group{"kv": tv(t, "v2.0", "v1.0")},
		},
		"commitment": schema.Category{
			Domain: schema.Group{"kv": tv(t, "v2.1", "v1.0"), "bt": tv(t, "v1.0", "v1.0")},
			Hist:   schema.Group{"v": tv(t, "v1.0", "v1.0")},
		},
	}
	return newModel("versions.yaml", s)
}

func TestChangesEmptyWhenUnedited(t *testing.T) {
	m := newTestModel(t)
	if got := m.changes(); len(got) != 0 {
		t.Fatalf("unedited model: want no changes, got %v", got)
	}
}

func TestChangesListsCurrentAndMin(t *testing.T) {
	m := newTestModel(t)
	m.set("commitment", "domain", "kv", func(v *schema.TwoVers) { v.Current = pv(t, "v2.2") })
	m.set("accounts", "domain", "kv", func(v *schema.TwoVers) { v.Min = pv(t, "v1.1") })

	want := []string{
		"accounts.kv (min)  v1.0 → v1.1",
		"commitment.kv  v2.1 → v2.2",
	}
	if got := m.changes(); !reflect.DeepEqual(got, want) {
		t.Fatalf("changes() = %#v, want %#v", got, want)
	}
}

func TestTabTogglesFocus(t *testing.T) {
	m := newTestModel(t)
	if m.foc != fLeft {
		t.Fatalf("initial focus = %v, want fLeft", m.foc)
	}
	tab := tea.KeyPressMsg{Code: tea.KeyTab}
	if tab.String() != "tab" {
		t.Fatalf("test setup: KeyTab String() = %q, want \"tab\"", tab.String())
	}
	m.Update(tab)
	if m.foc != fRight {
		t.Fatalf("after first tab: focus = %v, want fRight", m.foc)
	}
	m.Update(tab)
	if m.foc != fLeft {
		t.Fatalf("after second tab: focus = %v, want fLeft", m.foc)
	}
}
