package mcp

import (
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestScanLogStopsAtMatchLimit(t *testing.T) {
	for _, tc := range []struct {
		name            string
		prefix          string
		filter          string
		caseInsensitive bool
	}{
		{name: "head"},
		{name: "filtered", prefix: "unrelated\n", filter: "match"},
		{name: "case insensitive", filter: "MATCH", caseInsensitive: true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			path := filepath.Join(t.TempDir(), "erigon.log")
			require.NoError(t, os.WriteFile(path, []byte(tc.prefix+"match\n"+strings.Repeat("x", 1<<20)+"\n"), 0o600))

			lines, err := scanLog(path, 1, tc.filter, tc.caseInsensitive)
			require.NoError(t, err)
			require.Equal(t, []string{"match"}, lines)
		})
	}
}

func TestScanLogReportsErrorBeforeMatchLimit(t *testing.T) {
	path := filepath.Join(t.TempDir(), "erigon.log")
	require.NoError(t, os.WriteFile(path, []byte("match\n"+strings.Repeat("x", 1<<20)+"\n"), 0o600))

	_, err := scanLog(path, 2, "", false)
	require.ErrorContains(t, err, "token too long")
}
