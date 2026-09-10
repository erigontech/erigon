package snaptype

import (
	"io/fs"
	"os"
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/require"
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

func TestParseDirSkipsUnknownCaplinType(t *testing.T) {
	d := filepath.Join(t.TempDir(), "caplin")
	require.NoError(t, os.MkdirAll(d, 0o755))
	require.NoError(t, os.WriteFile(filepath.Join(d, "v1.1-000000-000050-BlockProposers.seg"), []byte("x"), 0o644))

	res, err := ParseDir(d)
	require.NoError(t, err)
	require.Empty(t, res)
}

func TestParseFileNameRegisteredCaplinStateType(t *testing.T) {
	d := filepath.Join(t.TempDir(), "caplin")
	name := "v1.1-000000-000050-PendingDepositsDump.seg"

	file, _, ok := ParseFileName(d, name)
	require.True(t, ok)
	require.NotNil(t, file.Type)
	require.Equal(t, uint64(0), file.From)
	require.Equal(t, uint64(50_000), file.To)
	require.Equal(t, "PendingDepositsDump", file.CaplinTypeString)
	require.Equal(t, PendingDepositsDump.Enum(), file.Type.Enum())
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
