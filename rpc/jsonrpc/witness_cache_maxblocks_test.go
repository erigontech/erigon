package jsonrpc

import "testing"

// The ceiling has to be overridable per deployment: the cache is really a TIME
// window (blocks x block time), so one fixed count cannot suit a 12s L1 and a 2s
// L2 at once. 96 blocks is ~19 minutes of mainnet but 192 seconds at 2s — shorter
// than a prover's startup, which makes the window unusable rather than merely small.
func TestResolveWitnessCacheMaxBlocks(t *testing.T) {
	for _, tc := range []struct {
		name string
		env  string
		want uint
	}{
		{"unset falls back to the default", "", defaultWitnessCacheMaxBlocks},
		{"raised for a fast chain", "4096", 4096},
		{"lowered is allowed too", "8", 8},
		{"zero is ignored, not honoured", "0", defaultWitnessCacheMaxBlocks},
		{"garbage is ignored rather than fatal", "lots", defaultWitnessCacheMaxBlocks},
		{"negative is ignored", "-1", defaultWitnessCacheMaxBlocks},
	} {
		t.Run(tc.name, func(t *testing.T) {
			if got := resolveWitnessCacheMaxBlocks(tc.env); got != tc.want {
				t.Errorf("resolveWitnessCacheMaxBlocks(%q) = %d, want %d", tc.env, got, tc.want)
			}
		})
	}
}

// A clamp that says nothing is indistinguishable from the flag working. Callers
// need to be able to report it, so the operator is not left inferring the window
// from a log line that only shows the post-clamp number.
func TestWitnessCacheClamped(t *testing.T) {
	if !WitnessCacheClamped(witnessCacheMaxBlocks + 1) {
		t.Error("over-ceiling request must report as clamped")
	}
	if WitnessCacheClamped(witnessCacheMaxBlocks) {
		t.Error("a request exactly at the ceiling is not clamped")
	}
	if got := WitnessCacheCapacity(witnessCacheMaxBlocks + 1000); got != witnessCacheMaxBlocks {
		t.Errorf("capacity = %d, want the ceiling %d", got, witnessCacheMaxBlocks)
	}
}
