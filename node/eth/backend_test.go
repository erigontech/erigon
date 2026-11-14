package eth

import (
	"fmt"
	"maps"
	"os"
	"path/filepath"
	"slices"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/erigontech/erigon/cmd/rpcdaemon/cli/httpcfg"
	"github.com/erigontech/erigon/common/log/v3"
	"github.com/erigontech/erigon/db/snapshotsync/freezeblocks"
	"github.com/erigontech/erigon/rpc"
	"github.com/erigontech/erigon/rpc/jsonrpc"
)

func TestRemoveContents(t *testing.T) {
	tmpDirName := t.TempDir()
	//t.Logf("creating %s/root...", rootName)
	rootName := filepath.Join(tmpDirName, "root")
	err := os.Mkdir(rootName, 0o750)
	require.NoError(t, err)
	//fmt.Println("OK")
	for i := range 3 {
		outerName := filepath.Join(rootName, fmt.Sprintf("outer_%d", i+1))
		//t.Logf("creating %s... ", outerName)
		err = os.Mkdir(outerName, 0o750)
		require.NoError(t, err)
		//t.Logf("OK")
		for j := range 2 {
			innerName := filepath.Join(outerName, fmt.Sprintf("inner_%d", j+1))
			//t.Logf("creating %s... ", innerName)
			err = os.Mkdir(innerName, 0o750)
			require.NoError(t, err)
			//t.Log("OK")
			for k := range 2 {
				innestName := filepath.Join(innerName, fmt.Sprintf("innest_%d", k+1))
				//t.Logf("creating %s... ", innestName)
				err = os.Mkdir(innestName, 0o750)
				require.NoError(t, err)
				//t.Log("OK")
			}
		}
	}
	list, err := os.ReadDir(rootName)
	require.NoError(t, err)

	require.Len(t, list, 3)

	err = RemoveContents(rootName)
	require.NoError(t, err)

	list, err = os.ReadDir(rootName)
	require.NoError(t, err)

	require.Empty(t, list)
}

// MCP serves only the namespaces --http.api enables: it must not force admin, debug
// or trace on, and an empty list gives it no namespace-backed RPC methods.
func TestMCPNamespacesFollowHTTPAPI(t *testing.T) {
	for _, tc := range []struct {
		name string
		api  []string
		want []string
	}{
		{"eth only", []string{"eth"}, []string{"eth"}},
		{"debug enabled", []string{"eth", "debug"}, []string{"debug", "eth"}},
		{"empty", nil, []string{}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			cfg := httpcfg.HttpCfg{API: tc.api}
			logger := log.New()
			apis := jsonrpc.APIList(nil, nil, nil, nil, nil, nil, freezeblocks.NewBlockReader(nil), &cfg, nil, logger, nil, nil)
			srv, err := newMCPRPCServer(apis, &cfg, logger)
			require.NoError(t, err)
			defer srv.Stop()
			client := rpc.DialInProc(srv, logger)
			defer client.Close()

			modules, err := client.SupportedModules()
			require.NoError(t, err)
			got := slices.Sorted(maps.Keys(modules))
			got = slices.DeleteFunc(got, func(ns string) bool { return ns == "rpc" })
			require.Equal(t, tc.want, got)
		})
	}
}

func TestDeletedFilesWatchWarnsOnlyForAFileThatStays(t *testing.T) {
	var w deletedFilesWatch
	t0 := time.Unix(1_000_000, 0)
	_, _, ok := w.warn(t0, []string{"/d/a.seg"})
	require.False(t, ok)
	_, _, ok = w.warn(t0.Add(deletedFileWarnAfter-time.Second), []string{"/d/a.seg", "/d/b.seg"})
	require.False(t, ok, "a file that readers still drain is normal")
	oldest, since, ok := w.warn(t0.Add(deletedFileWarnAfter), []string{"/d/a.seg", "/d/b.seg"})
	require.True(t, ok)
	require.Equal(t, "/d/a.seg", oldest)
	require.Equal(t, t0, since)
	_, _, ok = w.warn(t0.Add(deletedFileWarnAfter+time.Minute), []string{"/d/a.seg"})
	require.False(t, ok, "one warning per period")
	_, _, ok = w.warn(t0.Add(2*deletedFileWarnAfter), []string{"/d/a.seg"})
	require.True(t, ok)
}

func TestDeletedFilesWatchIgnoresChurnOfShortLivedFiles(t *testing.T) {
	var w deletedFilesWatch
	t0 := time.Unix(1_000_000, 0)
	for i := range 3 * int(deletedFileWarnAfter/time.Minute) {
		_, _, ok := w.warn(t0.Add(time.Duration(i)*time.Minute), []string{fmt.Sprintf("/d/%d.seg", i)})
		require.False(t, ok, "every file went away within a minute")
	}
}
