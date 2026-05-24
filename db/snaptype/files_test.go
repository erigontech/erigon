package snaptype

import (
	"testing"

	"github.com/stretchr/testify/require"
)

func TestStateSeedable(t *testing.T) {
	tests := []struct {
		name     string
		filename string
		expected bool
	}{
		{
			name:     "valid seedable file",
			filename: "v12.13-accounts.100-164.efi",
			expected: true,
		},
		{
			name:     "seedable: we allow seed files of any size",
			filename: "v12.13-accounts.100-165.efi",
			expected: true,
		},
		{
			name:     "seedable: we allow seed files of any size",
			filename: "v12.13-accounts.100-101.efi",
			expected: true,
		},
		{
			name:     "invalid file name - regex not matching",
			filename: "invalid-file-name",
			expected: false,
		},
		{
			name:     "file with relative path prefix",
			filename: "history/v12.13-accounts.100-164.efi",
			expected: true,
		},
		{
			name:     "invalid file name - capital letters not allowed",
			filename: "v12.13-ACCC.100-164.efi",
			expected: false,
		},
		{
			name:     "block files are not state files",
			filename: "v1.2-headers.seg",
			expected: false,
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			result := IsStateFileSeedable(tc.filename)
			if result != tc.expected {
				t.Errorf("IsStateFileSeedable(%q) = %v; want %v", tc.filename, result, tc.expected)
			}
		})
	}
}

// Dual-mode block-file parsing — coordinates come in two forms:
//
//   - Legacy rounded form: 6-char zero-padded step strings (e.g.
//     "v1.1-000500-001000-headers.seg"). ParseFileName multiplies by
//     1000 to recover the block range — "000500-001000" → blocks
//     [500_000, 1_000_000). This is the convention every mainnet /
//     sepolia / chiado / etc. preverified.toml uses today.
//
//   - Aligned literal form: any other width represents block numbers
//     directly (no multiplier). E.g. "v1.1-19998000-20000000-headers.seg"
//     → blocks [19_998_000, 20_000_000). Block/slot-aligned producers
//     emit this form. See memory/block-slot-aligned-storage-model-2026-05-24.
//
// The parser is dual-mode (defensive permanent): the same code path
// handles both, so a node running mixed-convention inventory parses
// every file correctly.

// Note: block-file tests use ParseRange instead of ParseFileName
// directly because ParseFileName's `ok` requires the Type enum to be
// registered (freezeblocks's init does this in production but isn't
// imported by snaptype tests). ParseRange exposes the same parse
// logic with the weaker ok = "from < to and TypeString set", which
// is what these tests care about.

func TestParseFileName_LegacyRoundedBlockFile(t *testing.T) {
	// Real mainnet preverified entry pattern.
	typeStr, from, to, ok := ParseRange("v1.1-000500-001000-headers.seg")
	require.True(t, ok)
	require.Equal(t, "headers", typeStr)
	require.Equal(t, uint64(500_000), from,
		"6-char step string multiplies by 1000")
	require.Equal(t, uint64(1_000_000), to,
		"6-char step string multiplies by 1000")
}

func TestParseFileName_LegacyRoundedBodies(t *testing.T) {
	typeStr, from, to, ok := ParseRange("v1.1-019998-020000-bodies.seg")
	require.True(t, ok)
	require.Equal(t, "bodies", typeStr)
	require.Equal(t, uint64(19_998_000), from)
	require.Equal(t, uint64(20_000_000), to)
}

func TestParseFileName_AlignedLiteralBlockFile(t *testing.T) {
	// 8-char strings → literal coords (no *1000).
	typeStr, from, to, ok := ParseRange("v1.1-19998000-20000000-headers.seg")
	require.True(t, ok)
	require.Equal(t, "headers", typeStr)
	require.Equal(t, uint64(19_998_000), from,
		"non-6-char string is a literal block coordinate")
	require.Equal(t, uint64(20_000_000), to,
		"non-6-char string is a literal block coordinate")
}

func TestParseFileName_AlignedLiteralBodies(t *testing.T) {
	typeStr, from, to, ok := ParseRange("v1.1-19998000-20000000-bodies.seg")
	require.True(t, ok)
	require.Equal(t, "bodies", typeStr)
	require.Equal(t, uint64(19_998_000), from)
	require.Equal(t, uint64(20_000_000), to)
}

func TestParseFileName_LegacyShortFormStillRounded(t *testing.T) {
	// Short forms like "1-2" are legacy (no zero-padding) and still
	// multiply by 1000 — preserves the long-standing parser contract
	// (TestParseCompressedFileName in db/snapshotsync pins this).
	typeStr, from, to, ok := ParseRange("v1-1-2-bodies.seg")
	require.True(t, ok)
	require.Equal(t, "bodies", typeStr)
	require.Equal(t, uint64(1_000), from, "1-char step still treated as rounded")
	require.Equal(t, uint64(2_000), to)
}

func TestParseFileName_MixedWidthOneWideTriggersLiteral(t *testing.T) {
	// EITHER string >6 chars → both interpreted as literal. The
	// (≤6, ≤6) box is the only place the rounded rule fires.
	typeStr, from, to, ok := ParseRange("v1.1-000500-1000000-headers.seg")
	require.True(t, ok)
	require.Equal(t, "headers", typeStr)
	require.Equal(t, uint64(500), from,
		"to-side width >6 forces literal on both")
	require.Equal(t, uint64(1_000_000), to)
}

func TestParseFileName_LegacyRoundedZeroBoundary(t *testing.T) {
	// First-block segment: 000000-000500 = blocks [0, 500_000).
	// ParseRange requires from < to so 0 is fine on the from side.
	typeStr, from, to, ok := ParseRange("v1.1-000000-000500-headers.seg")
	require.True(t, ok)
	require.Equal(t, "headers", typeStr)
	require.Equal(t, uint64(0), from)
	require.Equal(t, uint64(500_000), to)
}

func TestParseFileName_StateFilesUnchanged(t *testing.T) {
	// State files use the step→block map (stepToBlock) externally; the
	// parser itself does NOT multiply. Dual-mode change is block-file
	// only — state files were always literal-step. Pin this so future
	// changes don't accidentally generalise the *1000 rule to state.
	info, _, ok := ParseFileName("snapshots", "v2.0-accounts.2519-2520.kv")
	require.True(t, ok)
	require.Equal(t, "accounts", info.TypeString)
	require.Equal(t, uint64(2519), info.From, "state-file from is the literal step number")
	require.Equal(t, uint64(2520), info.To, "state-file to is the literal step number")
}
