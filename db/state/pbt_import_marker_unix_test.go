//go:build !windows

package state

import (
	"os"
	"os/exec"
	"syscall"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/erigontech/erigon/db/datadir"
)

func TestPBTMarkerWriteFailureKeepsOldMarker(t *testing.T) {
	if os.Getenv("GO_WANT_PBT_MARKER_LIMIT_HELPER") == "1" {
		limit := &syscall.Rlimit{Cur: 4096, Max: 4096}
		if err := syscall.Setrlimit(syscall.RLIMIT_FSIZE, limit); err != nil {
			os.Exit(2)
		}
		dirs := datadir.New(os.Getenv("PBT_MARKER_DATADIR"))
		marker := &PBTImportMarker{SnapshotPath: string(make([]byte, 8192)), SnapshotHash: "hash", Files: []string{"file"}, Settings: &ErigonDBSettings{}}
		if err := WritePBTImportMarker(dirs, marker); err == nil {
			os.Exit(1)
		}
		os.Exit(0)
	}
	dirs := datadir.New(t.TempDir())
	path := PBTImportMarkerPath(dirs)
	old := []byte(`{"snapshot_path":"old"}`)
	require.NoError(t, os.WriteFile(path, old, 0o644))
	command := exec.CommandContext(t.Context(), os.Args[0], "-test.run=^TestPBTMarkerWriteFailureKeepsOldMarker$", "-test.v")
	command.Env = append(os.Environ(),
		"GO_WANT_PBT_MARKER_LIMIT_HELPER=1",
		"PBT_MARKER_DATADIR="+dirs.DataDir,
	)
	output, err := command.CombinedOutput()
	require.NoError(t, err, "%s", output)
	got, err := os.ReadFile(path)
	require.NoError(t, err)
	require.Equal(t, old, got)
}
