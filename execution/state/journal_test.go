package state

import (
	"testing"
	"unsafe"
)

// TestJournalEntrySize guards the compact union against accidental bloat (e.g.
// inlining the rare *stateObject/[]byte fields), which would reintroduce the
// per-entry memory the interface-free representation removed.
func TestJournalEntrySize(t *testing.T) {
	if got := unsafe.Sizeof(journalEntry{}); got > 72 {
		t.Fatalf("journalEntry grew to %d B (want <= 72)", got)
	}
}
