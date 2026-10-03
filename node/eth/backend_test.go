package eth

import (
	"fmt"
	"os"
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/erigontech/erigon/common/log/v3"
	"github.com/erigontech/erigon/db/datadir"
	"github.com/erigontech/erigon/db/state"
	"github.com/erigontech/erigon/node"
	"github.com/erigontech/erigon/node/ethconfig"
	"github.com/erigontech/erigon/node/nodecfg"
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

func TestPBinCommitmentWarningNamesRefusedMethods(t *testing.T) {
	for _, method := range []string{"eth_getProof", "eth_getWitness", "debug_executionWitness"} {
		require.Contains(t, pbinCommitmentUnsupportedMethods, method)
	}
	require.NotContains(t, pbinCommitmentUnsupportedMethods, "debug_executionWitness is supported")
}

func TestRefusePBTStartupMarkersChecksBothMarkers(t *testing.T) {
	for _, name := range []string{"attach", "import"} {
		t.Run(name, func(t *testing.T) {
			dirs := datadir.New(t.TempDir())
			if name == "attach" {
				variant := state.TrieVariantHexBin
				require.NoError(t, state.WritePBTAttachMarker(dirs, &state.PBTAttachMarker{PublishedPath: "/tmp/published", Settings: &state.ErigonDBSettings{TrieVariant: &variant}}))
			} else {
				variant := state.TrieVariantHexBin
				hash := "blake3"
				require.NoError(t, state.WritePBTImportMarker(dirs, &state.PBTImportMarker{SnapshotPath: "/tmp/snapshot", SnapshotHash: "digest", Files: []string{"domain/v3.0-commitment-bin.0-1.kv"}, Settings: &state.ErigonDBSettings{TrieVariant: &variant, TrieHash: &hash}}))
			}
			require.Error(t, refusePBTStartupMarkers(dirs))
		})
	}
}

func TestEthereumNewRefusesPBTImportMarker(t *testing.T) {
	dirs := datadir.New(t.TempDir())
	variant := state.TrieVariantHexBin
	hash := "blake3"
	require.NoError(t, state.WritePBTImportMarker(dirs, &state.PBTImportMarker{
		SnapshotPath: "/tmp/snapshot",
		SnapshotHash: "digest",
		Files:        []string{"domain/v3.0-commitment-bin.0-1.kv"},
		Settings:     &state.ErigonDBSettings{TrieVariant: &variant, TrieHash: &hash},
	}))
	stack, err := node.New(t.Context(), &nodecfg.Config{Dirs: dirs}, log.New())
	require.NoError(t, err)
	defer stack.Close()
	_, err = New(t.Context(), stack, &ethconfig.Config{}, log.New(), nil)
	require.ErrorContains(t, err, "commitment import-pbt")
}
