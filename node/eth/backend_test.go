package eth

import (
	"fmt"
	"os"
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/erigontech/erigon/node/ethconfig"
)

func TestValidateEmbeddedBuilderMode(t *testing.T) {
	for _, test := range []struct {
		name       string
		enabled    bool
		internalCL bool
		networkID  uint64
		wantError  bool
	}{
		{name: "disabled without embedded Caplin"},
		{name: "enabled with embedded Caplin", enabled: true, internalCL: true, networkID: 1},
		{name: "enabled without embedded Caplin", enabled: true, wantError: true},
		{name: "enabled on unsupported network", enabled: true, internalCL: true, networkID: 999, wantError: true},
	} {
		t.Run(test.name, func(t *testing.T) {
			cfg := ethconfig.Config{InternalCL: test.internalCL, NetworkID: test.networkID}
			cfg.CaplinConfig.EpbsBuilder.Enabled = test.enabled
			err := validateEmbeddedBuilderMode(&cfg)
			if test.wantError {
				require.ErrorContains(t, err, "embedded Caplin")
			} else {
				require.NoError(t, err)
			}
		})
	}
}

func TestRemoveContents(t *testing.T) {
	tmpDirName := t.TempDir()
	//t.Logf("creating %s/root...", rootName)
	rootName := filepath.Join(tmpDirName, "root")
	err := os.Mkdir(rootName, 0750)
	require.NoError(t, err)
	//fmt.Println("OK")
	for i := range 3 {
		outerName := filepath.Join(rootName, fmt.Sprintf("outer_%d", i+1))
		//t.Logf("creating %s... ", outerName)
		err = os.Mkdir(outerName, 0750)
		require.NoError(t, err)
		//t.Logf("OK")
		for j := range 2 {
			innerName := filepath.Join(outerName, fmt.Sprintf("inner_%d", j+1))
			//t.Logf("creating %s... ", innerName)
			err = os.Mkdir(innerName, 0750)
			require.NoError(t, err)
			//t.Log("OK")
			for k := range 2 {
				innestName := filepath.Join(innerName, fmt.Sprintf("innest_%d", k+1))
				//t.Logf("creating %s... ", innestName)
				err = os.Mkdir(innestName, 0750)
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
