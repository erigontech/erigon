package downloader

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestParseChainToml(t *testing.T) {
	t.Run("valid", func(t *testing.T) {
		input := []byte(`"v1-000000-000100-headers.seg" = "abc123"
"v1-000100-000200-headers.seg" = "def456"
`)
		m, err := ParseChainToml(input)
		require.NoError(t, err)
		assert.Len(t, m, 2)
		assert.Equal(t, "abc123", m["v1-000000-000100-headers.seg"])
		assert.Equal(t, "def456", m["v1-000100-000200-headers.seg"])
	})

	t.Run("empty", func(t *testing.T) {
		m, err := ParseChainToml([]byte{})
		require.NoError(t, err)
		assert.Empty(t, m)
	})

	t.Run("invalid", func(t *testing.T) {
		_, err := ParseChainToml([]byte("not valid toml [[["))
		assert.Error(t, err)
	})
}

func TestBuildTomlFromMap(t *testing.T) {
	m := map[string]string{
		"b-file.seg": "hash2",
		"a-file.seg": "hash1",
		"c-file.seg": "hash3",
	}
	result := BuildTomlFromMap(m)

	// Should be sorted by key
	expected := `"a-file.seg" = "hash1"
"b-file.seg" = "hash2"
"c-file.seg" = "hash3"
`
	assert.Equal(t, expected, string(result))
}

func TestParseAndBuildRoundtrip(t *testing.T) {
	original := map[string]string{
		"v1-000000-000100-headers.seg": "abc123",
		"v1-000100-000200-headers.seg": "def456",
	}

	tomlBytes := BuildTomlFromMap(original)
	parsed, err := ParseChainToml(tomlBytes)
	require.NoError(t, err)
	assert.Equal(t, original, parsed)
}

func TestMergeChainToml_NoConflict(t *testing.T) {
	existing := map[string]string{
		"a.seg": "hash1",
		"b.seg": "hash2",
	}
	discovered := map[string]string{
		"c.seg": "hash3",
		"d.seg": "hash4",
	}

	merged, newCount := MergeChainToml(existing, discovered)
	assert.Equal(t, 2, newCount)
	assert.Len(t, merged, 4)
	assert.Equal(t, "hash1", merged["a.seg"])
	assert.Equal(t, "hash3", merged["c.seg"])
}

func TestMergeChainToml_ExistingWins(t *testing.T) {
	existing := map[string]string{
		"a.seg": "existing-hash",
		"b.seg": "hash2",
	}
	discovered := map[string]string{
		"a.seg": "different-hash", // conflict — existing should win
		"c.seg": "hash3",
	}

	merged, newCount := MergeChainToml(existing, discovered)
	assert.Equal(t, 1, newCount) // only c.seg is new
	assert.Equal(t, "existing-hash", merged["a.seg"])
	assert.Equal(t, "hash3", merged["c.seg"])
}

func TestMergeChainToml_EmptyExisting(t *testing.T) {
	existing := map[string]string{}
	discovered := map[string]string{
		"a.seg": "hash1",
		"b.seg": "hash2",
	}

	merged, newCount := MergeChainToml(existing, discovered)
	assert.Equal(t, 2, newCount)
	assert.Len(t, merged, 2)
}

func TestMergeChainToml_EmptyDiscovered(t *testing.T) {
	existing := map[string]string{
		"a.seg": "hash1",
	}
	discovered := map[string]string{}

	merged, newCount := MergeChainToml(existing, discovered)
	assert.Equal(t, 0, newCount)
	assert.Len(t, merged, 1)
}

func TestCompareChainToml_Matching(t *testing.T) {
	local := map[string]string{
		"a.seg": "hash1",
		"b.seg": "hash2",
	}
	discovered := map[string]string{
		"a.seg": "hash1",
		"b.seg": "hash2",
	}

	matching, newEntries, mismatches := CompareChainToml(local, discovered)
	assert.Equal(t, 2, matching)
	assert.Equal(t, 0, newEntries)
	assert.Empty(t, mismatches)
}

func TestCompareChainToml_NewEntries(t *testing.T) {
	local := map[string]string{
		"a.seg": "hash1",
	}
	discovered := map[string]string{
		"a.seg": "hash1",
		"b.seg": "hash2",
		"c.seg": "hash3",
	}

	matching, newEntries, mismatches := CompareChainToml(local, discovered)
	assert.Equal(t, 1, matching)
	assert.Equal(t, 2, newEntries)
	assert.Empty(t, mismatches)
}

func TestCompareChainToml_HashMismatch(t *testing.T) {
	local := map[string]string{
		"a.seg": "hash1",
		"b.seg": "hash2",
	}
	discovered := map[string]string{
		"a.seg": "hash1",
		"b.seg": "different-hash",
	}

	matching, newEntries, mismatches := CompareChainToml(local, discovered)
	assert.Equal(t, 1, matching)
	assert.Equal(t, 0, newEntries)
	assert.Equal(t, []string{"b.seg"}, mismatches)
}

// TestFilterDiscoveredByLocalTip_ColdStart pins the bootstrap case:
// when our local processed tip is 0 (no published block files yet),
// the filter is a pass-through so initial sync can ingest the full
// peer manifest. Without this, a fresh node would reject every
// peer-advertised entry and never bootstrap.
func TestFilterDiscoveredByLocalTip_ColdStart(t *testing.T) {
	discovered := map[string]string{
		"v1.1-000000-000500-headers.seg":      "h0",
		"v1.1-000000-000500-bodies.seg":       "h1",
		"v1.1-000000-000500-transactions.seg": "h2",
		"v1.1-020000-020500-headers.seg":      "h3",
	}
	got := filterDiscoveredByLocalTip(discovered, 0)
	assert.Equal(t, discovered, got, "tip=0 (cold start) must be a pass-through")
}

// TestFilterDiscoveredByLocalTip_PostUnwind pins the 2026-06-28 iter-4
// soak regression: after mode-B sweep our local chain.toml advertises
// up to block 3,047,999 (via the rebuild file 003000-003048). A peer
// hasn't done the same unwind so their chain.toml still includes
// 003040-003050 (extends past our tip) AND 003050-003060 (entirely
// past our tip). Both must be rejected — we'll produce those
// snapshots ourselves via forward exec + retire as our local
// processing catches back up. Accepting peer files for blocks above
// our local processed tip creates overlapping-file state that
// violates the maximality invariant.
//
// Files whose entire range is at-or-below our local tip (i.e. To <=
// localTip+1) are passed through — they're files we've fully
// processed locally and a peer's identical-hash advertisement is a
// no-op dedup.
func TestFilterDiscoveredByLocalTip_PostUnwind(t *testing.T) {
	discovered := map[string]string{
		// Extends past local tip — REJECT (we'll produce locally).
		"v1.1-003040-003050-headers.seg":      "stale-1",
		"v1.1-003040-003050-bodies.seg":       "stale-2",
		"v1.1-003040-003050-transactions.seg": "stale-3",
		// Entirely past local tip — REJECT (we'll produce locally).
		"v1.1-003050-003060-headers.seg":      "future-1",
		"v1.1-003050-003060-bodies.seg":       "future-2",
		"v1.1-003050-003060-transactions.seg": "future-3",
		// Entirely below local tip — ACCEPT (no-op dedup against
		// what we have already).
		"v1.1-003000-003010-headers.seg":      "have-1",
		"v1.1-003000-003010-bodies.seg":       "have-2",
		"v1.1-003000-003010-transactions.seg": "have-3",
	}
	localTip := uint64(3_047_999)
	got := filterDiscoveredByLocalTip(discovered, localTip)

	for _, name := range []string{
		"v1.1-003040-003050-headers.seg",
		"v1.1-003040-003050-bodies.seg",
		"v1.1-003040-003050-transactions.seg",
		"v1.1-003050-003060-headers.seg",
		"v1.1-003050-003060-bodies.seg",
		"v1.1-003050-003060-transactions.seg",
	} {
		assert.NotContains(t, got, name,
			"entry whose To extends past localTip must be rejected (we will produce it locally): "+name)
	}
	for _, name := range []string{
		"v1.1-003000-003010-headers.seg",
		"v1.1-003000-003010-bodies.seg",
		"v1.1-003000-003010-transactions.seg",
	} {
		assert.Contains(t, got, name,
			"entry entirely within local tip must be accepted (no-op dedup): "+name)
	}
}

// TestFilterDiscoveredByLocalTip_NonBlockPassThrough pins that
// state-domain, meta, salt, and CL entries are not gated by the block
// tip. They have separate filtering paths (or none at all) and the
// block-tip filter must not silently drop them.
func TestFilterDiscoveredByLocalTip_NonBlockPassThrough(t *testing.T) {
	discovered := map[string]string{
		"domain/v2.0-accounts.0-128.kv":              "h-state",
		"history/v1.0-accountsHistory.0-1024.v":      "h-hist",
		"salt-state.txt":                             "h-salt",
		"erigondb.toml":                              "h-meta",
		"caplin/v1.1-000000-000010-beaconblocks.seg": "h-cl",
	}
	got := filterDiscoveredByLocalTip(discovered, 3_047_999)
	assert.Equal(t, discovered, got,
		"non-block entries must pass through unconditionally")
}
