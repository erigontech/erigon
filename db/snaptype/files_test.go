package snaptype

import (
	"io/fs"
	"os"
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/erigontech/erigon/db/version"
)

type vanishedDirEntry struct{ name string }

func (e vanishedDirEntry) Name() string               { return e.name }
func (e vanishedDirEntry) IsDir() bool                { return false }
func (e vanishedDirEntry) Type() fs.FileMode          { return 0 }
func (e vanishedDirEntry) Info() (fs.FileInfo, error) { return nil, fs.ErrNotExist }

// A file may be deleted between ReadDir and the per-entry stat by concurrent
// snapshot merge/prune; the scan must skip it instead of failing.
func TestParseDirSkipsFileDeletedDuringScan(t *testing.T) {
	d := t.TempDir()
	require.NoError(t, os.WriteFile(filepath.Join(d, "v1.0-047370-047380-beaconblocks.seg"), []byte("x"), 0o644))
	entries, err := os.ReadDir(d)
	require.NoError(t, err)
	entries = append(entries, vanishedDirEntry{name: "v1.1-047370-047371-transactions.seg"})

	res, err := parseDirEntries(d, entries)
	require.NoError(t, err)
	require.Len(t, res, 1)
	require.Equal(t, "v1.0-047370-047380-beaconblocks.seg", res[0].Name())
}

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

// TestFileNameV4_FormatIs7Digit pins the block-side v4 naming: v4
// files always use %07d-%07d, guaranteeing both endpoints are literal
// block coordinates regardless of magnitude. That forces
// ParseFileName's dual-mode branch into raw-block interpretation
// (>6-char literal) even for legacy-scale (< 1M) block numbers.
func TestFileNameV4_FormatIs7Digit(t *testing.T) {
	v := version.V1_1
	name := FileNameV4(v, 3_491_000, 3_491_691, "headers")
	require.Equal(t, "v1.1-3491000-3491691-headers", name,
		"v4 name uses %%07d-%%07d with raw block numbers")

	// Small (devnet-scale) values also get padded to 7 chars so they
	// still fall in the literal branch.
	name = FileNameV4(v, 500, 691, "headers")
	require.Equal(t, "v1.1-0000500-0000691-headers", name,
		"v4 name always pads to 7 chars regardless of magnitude")
}

// TestParseFileName_V4Literal_RoundTrip pins the round-trip: a v4-
// named file emitted by FileNameV4 must parse back to its raw block
// coordinates via ParseFileName. Guards against future dual-mode
// tweaks that would silently regress the v4 read path.
func TestParseFileName_V4Literal_RoundTrip(t *testing.T) {
	cases := []struct {
		from, to uint64
	}{
		{3_491_000, 3_491_691}, // v4 #1 (unwind section)
		{3_491_691, 3_492_000}, // v4 #2 (retire tail)
		{500, 691},             // devnet-scale
		{0, 1},                 // tightest possible non-empty
	}
	for _, tc := range cases {
		name := FileNameV4(version.V1_1, tc.from, tc.to, "headers") + ".seg"
		info, _, ok := ParseFileName("snapshots", name)
		require.True(t, ok, "v4 name %q must parse", name)
		require.Equal(t, tc.from, info.From, "%q: from round-trips", name)
		require.Equal(t, tc.to, info.To, "%q: to round-trips", name)
		require.Equal(t, "headers", info.TypeString)
	}
}

// TestFileInfo_IsRawBlock_TwoEdges pins the block-side sentinel
// predicate: a FileInfo whose From or To is not 1000-aligned is a v4
// file. Block v4 pair members both trip the predicate — v4 #1 has an
// aligned From but non-aligned To; v4 #2 has non-aligned From but
// aligned To. Standard 1000-aligned files must NOT trip it.
func TestFileInfo_IsRawBlock_TwoEdges(t *testing.T) {
	require.False(t, FileInfo{From: 3_491_000, To: 3_492_000}.IsRawBlock(),
		"aligned [3491000, 3492000) is standard, not v4")
	require.True(t, FileInfo{From: 3_491_000, To: 3_491_691}.IsRawBlock(),
		"v4 #1 (non-aligned To) must trip predicate")
	require.True(t, FileInfo{From: 3_491_691, To: 3_492_000}.IsRawBlock(),
		"v4 #2 (non-aligned From) must trip predicate")
	require.True(t, FileInfo{From: 3_491_100, To: 3_491_691}.IsRawBlock(),
		"both edges non-aligned still v4")
}
